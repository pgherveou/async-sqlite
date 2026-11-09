use super::*;
use futures_util::FutureExt;
use std::{
    task::{Context, Poll, Wake, Waker},
    time::{Duration, Instant},
};

fn backend(capacity: Option<usize>) -> (Backend, Receiver<Command>) {
    let (conn_tx, commands) = unbounded();
    (
        Backend {
            conn_tx,
            admission: Arc::new(Mutex::new(Admission::Open { worker_idle: true })),
            queue_capacity: capacity,
        },
        commands,
    )
}

fn batch(sql: &str, in_transaction: bool) -> Request {
    Request::Sql {
        operation: sql::Operation::Batch { sql: sql.into() },
        in_transaction,
    }
}

fn wait_until_idle(backend: &Backend) {
    let deadline = Instant::now() + Duration::from_secs(5);
    while !matches!(
        *backend.admission.lock().unwrap(),
        Admission::Open { worker_idle: true }
    ) {
        assert!(Instant::now() < deadline, "worker did not become idle");
        thread::yield_now();
    }
}

#[test]
fn setup_runs_before_the_client_is_ready() {
    smol::block_on(async {
        let client = ClientBuilder::new()
            .open_with(|connection| {
                connection.execute_batch(
                    "CREATE TABLE initialized (value); INSERT INTO initialized VALUES (42)",
                )?;
                Ok(())
            })
            .await
            .unwrap();
        let value: i64 = client
            .query_row("SELECT value FROM initialized", [], |row| row.get(0))
            .await
            .unwrap();
        assert_eq!(value, 42);
        client.close().await.unwrap();
    });
}

#[test]
fn invalid_setup_never_returns_a_client() {
    smol::block_on(async {
        let refused = ClientBuilder::new()
            .open_with(|_| Err(Error::Config { message: "refused" }))
            .await;
        assert!(matches!(refused, Err(Error::Config { message: "refused" })));
        let panic = ClientBuilder::new()
            .open_with(|_| panic!("setup panic"))
            .await;
        assert!(matches!(panic, Err(Error::Panic { message }) if message == "setup panic"));
        let transaction = ClientBuilder::new()
            .open_with(|connection| {
                connection.execute_batch("BEGIN")?;
                Ok(())
            })
            .await;
        assert!(matches!(
            transaction,
            Err(Error::Rusqlite(rusqlite::Error::InvalidQuery))
        ));
    });
}

#[test]
fn zero_capacity_accepts_only_one_request_while_idle() {
    let (backend, commands) = backend(Some(0));
    let first = backend.submit(batch("SELECT 1", false)).unwrap();
    assert!(matches!(
        backend.submit(batch("SELECT 2", false)),
        Err(Error::QueueFull)
    ));
    assert!(matches!(commands.try_recv(), Ok(Command::Sql { .. })));
    assert!(matches!(
        backend.submit(batch("SELECT 3", false)),
        Err(Error::QueueFull)
    ));
    *backend.admission.lock().unwrap() = Admission::Open { worker_idle: true };
    let next = backend.submit(batch("SELECT 4", false)).unwrap();
    drop((first, next));
}

#[test]
fn cleanup_bypasses_full_queues_and_rolls_back_before_later_sql() {
    for capacity in [0, 1] {
        smol::block_on(async {
            let (backend, commands) = backend(Some(capacity));
            let connection = Connection::open_in_memory().unwrap();
            connection
                .execute_batch("CREATE TABLE ledger (value); BEGIN")
                .unwrap();
            let accepted = backend
                .submit(batch("INSERT INTO ledger VALUES (1)", true))
                .unwrap();
            assert!(matches!(backend.close().await, Err(Error::QueueFull)));
            backend.submit_cleanup(Request::Rollback);
            assert_eq!(backend.conn_tx.len(), 2);
            assert!(matches!(
                backend.submit(batch("SELECT 1", false)),
                Err(Error::QueueFull)
            ));
            let admission = backend.admission.clone();
            let worker = thread::spawn(move || Backend::run(connection, commands, &admission));
            assert_eq!(accepted.await.unwrap().unwrap(), Response::Done);
            wait_until_idle(&backend);
            let result = backend
                .submit(Request::Sql {
                    operation: sql::Operation::Query {
                        sql: "SELECT COUNT(*) FROM ledger".into(),
                        params: Vec::new(),
                        first_only: true,
                    },
                    in_transaction: false,
                })
                .unwrap()
                .await
                .unwrap()
                .unwrap();
            assert_eq!(
                result,
                Response::Rows {
                    columns: vec!["COUNT(*)".into()],
                    rows: vec![vec![sql::Cell::Integer(0)]],
                }
            );
            wait_until_idle(&backend);
            backend.close().await.unwrap();
            worker.join().unwrap();
        });
    }
}

#[test]
fn failed_cleanup_rejects_later_work_and_settles_close() {
    smol::block_on(async {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("ledger.sqlite");
        let connection = Connection::open(&path).unwrap();
        connection
            .execute_batch("CREATE TABLE ledger (value)")
            .unwrap();
        let (backend, commands) = backend(None);
        backend.submit_cleanup(batch("SELECT missing_function()", false));
        let pending = backend
            .submit(batch("INSERT INTO ledger VALUES (1)", false))
            .unwrap();
        let mut closing = Box::pin(backend.close());
        assert!(closing.as_mut().now_or_never().is_none());
        Backend::run(connection, commands, &backend.admission);
        assert!(pending.await.is_err());
        assert!(matches!(closing.await, Err(Error::OutcomeUnknown { .. })));
        assert!(matches!(
            backend.submit(batch("SELECT 1", false)),
            Err(Error::Closed)
        ));
        let connection = Connection::open(path).unwrap();
        let count: i64 = connection
            .query_row("SELECT COUNT(*) FROM ledger", [], |row| row.get(0))
            .unwrap();
        assert_eq!(count, 0);
    });
}

#[cfg(feature = "hooks")]
#[test]
fn denied_batch_rollback_closes_before_later_requests() {
    use rusqlite::hooks::{AuthAction, AuthContext, Authorization, TransactionOperation};

    smol::block_on(async {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("ledger.sqlite");
        let mut connection = Connection::open(&path).unwrap();
        connection
            .execute_batch("CREATE TABLE ledger (value)")
            .unwrap();
        connection
            .authorizer(Some(|context: AuthContext<'_>| match context.action {
                AuthAction::Transaction {
                    operation: TransactionOperation::Rollback,
                } => Authorization::Deny,
                _ => Authorization::Allow,
            }))
            .unwrap();
        let (backend, commands) = backend(None);
        let failing = backend
            .submit(batch("BEGIN; INSERT INTO ledger VALUES (1)", false))
            .unwrap();
        let pending = backend
            .submit(batch("SELECT value FROM ledger", false))
            .unwrap();
        let mut closing = Box::pin(backend.close());
        assert!(closing.as_mut().now_or_never().is_none());
        Backend::run(connection, commands, &backend.admission);
        let result = failing.await.unwrap();
        assert!(matches!(result,
            Err(Error::Rusqlite(rusqlite::Error::SqliteFailure(error, _)))
            if error.code == rusqlite::ErrorCode::AuthorizationForStatementDenied
        ));
        assert!(pending.await.is_err());
        assert!(matches!(closing.await, Err(Error::OutcomeUnknown { .. })));
        let connection = Connection::open(path).unwrap();
        let count: i64 = connection
            .query_row("SELECT COUNT(*) FROM ledger", [], |row| row.get(0))
            .unwrap();
        assert_eq!(count, 0);
    });
}

#[test]
fn close_is_shared_and_failure_allows_retry() {
    smol::block_on(async {
        let (backend, commands) = backend(None);
        let mut first = Box::pin(backend.close());
        let mut second = Box::pin(backend.close());
        assert!(first.as_mut().now_or_never().is_none());
        assert!(second.as_mut().now_or_never().is_none());
        assert_eq!(commands.len(), 1);
        assert!(matches!(commands.try_recv(), Ok(Command::Shutdown)));
        finish_close(&backend.admission, Err("unfinished statement".into()));
        for result in [first.await, second.await] {
            assert!(
                matches!(result, Err(Error::OutcomeUnknown { message }) if message == "unfinished statement")
            );
        }
        let mut retry = Box::pin(backend.close());
        assert!(retry.as_mut().now_or_never().is_none());
        assert!(matches!(commands.try_recv(), Ok(Command::Shutdown)));
        finish_close(&backend.admission, Ok(()));
        retry.await.unwrap();
        backend.close().await.unwrap();
    });
}

#[test]
fn worker_panic_releases_pending_sql_and_close_waiters() {
    struct PanicOnWake;
    impl Wake for PanicOnWake {
        fn wake(self: Arc<Self>) {
            panic!("reply wake panic");
        }
    }

    smol::block_on(async {
        let (backend, commands) = backend(None);
        let mut first = backend.submit(batch("SELECT 1", false)).unwrap();
        let waker = Waker::from(Arc::new(PanicOnWake));
        assert!(matches!(
            first.poll_unpin(&mut Context::from_waker(&waker)),
            Poll::Pending
        ));
        let pending = backend.submit(batch("SELECT 2", false)).unwrap();
        let mut closing = Box::pin(backend.close());
        assert!(closing.as_mut().now_or_never().is_none());
        let admission = backend.admission.clone();
        let worker = thread::spawn(move || {
            Backend::run(Connection::open_in_memory().unwrap(), commands, &admission)
        });
        assert!(worker.join().is_err());
        assert!(pending.await.is_err());
        let result = closing.as_mut().now_or_never();
        assert!(
            matches!(result, Some(Err(Error::OutcomeUnknown { .. }))),
            "{result:?}"
        );
        assert!(matches!(
            *backend.admission.lock().unwrap(),
            Admission::Closed
        ));
    });
}
