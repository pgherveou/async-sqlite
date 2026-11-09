#![cfg(not(target_arch = "wasm32"))]

use std::sync::mpsc;
use std::thread::{self, ThreadId};

use async_sqlite::{CallError, Client, ClientBuilder, DatabaseBackend, NativeDatabase};
use futures_util::{future::BoxFuture, FutureExt};
use rusqlite::{params, Connection};

enum Request {
    Insert(i64, Vec<u8>),
    Debit(i64),
    Rows,
    Thread,
    Block(mpsc::Sender<()>, mpsc::Receiver<()>),
    CommitThenPanic,
}

#[derive(Debug, PartialEq)]
struct Record {
    amount: i64,
    payload: Vec<u8>,
}

#[derive(Debug, PartialEq)]
enum Response {
    Done,
    Rows(Vec<Record>),
    Thread(ThreadId),
}

#[derive(Debug, PartialEq)]
enum OperationError {
    Sqlite(String),
    Insufficient,
}

impl From<rusqlite::Error> for OperationError {
    fn from(error: rusqlite::Error) -> Self {
        Self::Sqlite(error.to_string())
    }
}

fn handle(connection: &mut Connection, request: Request) -> Result<Response, OperationError> {
    match request {
        Request::Insert(amount, payload) => {
            connection.execute("INSERT INTO ledger VALUES (?, ?)", params![amount, payload])?;
            Ok(Response::Done)
        }
        Request::Debit(amount) => {
            let transaction = connection.transaction()?;
            let balance: i64 =
                transaction.query_row("SELECT SUM(amount) FROM ledger", [], |row| row.get(0))?;
            transaction.execute(
                "INSERT INTO ledger VALUES (?, ?)",
                params![-amount, Vec::<u8>::new()],
            )?;
            if amount > balance {
                return Err(OperationError::Insufficient);
            }
            transaction.commit()?;
            Ok(Response::Done)
        }
        Request::Rows => {
            let mut statement =
                connection.prepare("SELECT amount, payload FROM ledger ORDER BY rowid")?;
            let rows = statement
                .query_map([], |row| {
                    Ok(Record {
                        amount: row.get(0)?,
                        payload: row.get(1)?,
                    })
                })?
                .collect::<Result<Vec<_>, _>>()?;
            Ok(Response::Rows(rows))
        }
        Request::Thread => Ok(Response::Thread(thread::current().id())),
        Request::Block(entered, release) => {
            entered.send(()).unwrap();
            release.recv().unwrap();
            Ok(Response::Done)
        }
        Request::CommitThenPanic => {
            connection.execute("INSERT INTO ledger VALUES (42, X'')", [])?;
            panic!("response lost after commit");
        }
    }
}

type Database = NativeDatabase<Request, Response, OperationError>;

async fn database(capacity: Option<usize>) -> (Database, Client) {
    let builder = match capacity {
        Some(capacity) => ClientBuilder::new().queue_capacity(capacity),
        None => ClientBuilder::new(),
    };
    let client = builder.open().await.unwrap();
    client
        .conn(|connection| {
            connection.execute_batch(
                "CREATE TABLE ledger (amount INTEGER NOT NULL, payload BLOB NOT NULL)",
            )
        })
        .await
        .unwrap();
    (NativeDatabase::new(client.clone(), handle), client)
}

fn send_sync<T: Send + Sync>() {}
fn send_future<T: Send>(future: T) -> T {
    future
}

fn block(
    database: &Database,
) -> (
    BoxFuture<'_, Result<Response, CallError<OperationError>>>,
    mpsc::Sender<()>,
) {
    let (entered, started) = mpsc::channel();
    let (release, wait) = mpsc::channel();
    let mut running = database.call(Request::Block(entered, wait));
    assert!(running.as_mut().now_or_never().is_none());
    started.recv().unwrap();
    (running, release)
}

#[test]
fn handler_runs_on_connection_thread_with_send_futures() {
    smol::block_on(async {
        send_sync::<Database>();
        let (database, _) = database(None).await;
        let response = send_future(database.call(Request::Thread)).await.unwrap();
        assert_ne!(response, Response::Thread(thread::current().id()));
        send_future(database.close()).await.unwrap();
    });
}

#[test]
fn conditional_transaction_commits_or_rolls_back_as_a_unit() {
    smol::block_on(async {
        let (database, _) = database(None).await;
        database
            .call(Request::Insert(20, vec![0, 128, 255]))
            .await
            .unwrap();
        database.call(Request::Debit(7)).await.unwrap();
        assert!(matches!(
            database.call(Request::Debit(50)).await,
            Err(CallError::Operation(OperationError::Insufficient))
        ));
        assert_eq!(
            database.call(Request::Rows).await.unwrap(),
            Response::Rows(vec![
                Record {
                    amount: 20,
                    payload: vec![0, 128, 255]
                },
                Record {
                    amount: -7,
                    payload: vec![]
                },
            ])
        );
        database.close().await.unwrap();
    });
}

#[test]
fn cloned_handles_keep_queued_order_and_exact_values() {
    smol::block_on(async {
        let (database, _) = database(None).await;
        let clone = database.clone();
        let (running, release) = block(&database);
        let mut first = database.call(Request::Insert(i64::MAX, vec![0, 255]));
        let mut second = clone.call(Request::Insert(i64::MIN, vec![128]));
        assert!(first.as_mut().now_or_never().is_none());
        assert!(second.as_mut().now_or_never().is_none());
        release.send(()).unwrap();
        running.await.unwrap();
        assert_eq!(
            futures_util::join!(first, second),
            (Ok(Response::Done), Ok(Response::Done))
        );
        assert_eq!(
            database.call(Request::Rows).await.unwrap(),
            Response::Rows(vec![
                Record {
                    amount: i64::MAX,
                    payload: vec![0, 255]
                },
                Record {
                    amount: i64::MIN,
                    payload: vec![128]
                },
            ])
        );
        clone.close().await.unwrap();
    });
}

#[test]
fn dropping_an_admitted_call_does_not_cancel_its_write() {
    smol::block_on(async {
        let (database, _) = database(None).await;
        let (running, release) = block(&database);
        let mut abandoned = database.call(Request::Insert(9, vec![1]));
        assert!(abandoned.as_mut().now_or_never().is_none());
        drop(abandoned);
        release.send(()).unwrap();
        running.await.unwrap();
        assert_eq!(
            database.call(Request::Rows).await.unwrap(),
            Response::Rows(vec![Record {
                amount: 9,
                payload: vec![1]
            },])
        );
        database.close().await.unwrap();
    });
}

#[test]
fn dropping_an_unpolled_call_does_not_dispatch_it() {
    smol::block_on(async {
        let (database, _) = database(None).await;
        drop(database.call(Request::Insert(9, vec![])));
        assert_eq!(
            database.call(Request::Rows).await,
            Ok(Response::Rows(vec![]))
        );
        database.close().await.unwrap();
    });
}

#[test]
fn close_drains_admitted_work_and_survives_dropped_waiter() {
    smol::block_on(async {
        let (database, client) = database(None).await;
        let (running, release) = block(&database);
        let mut accepted = database.call(Request::Insert(9, vec![]));
        assert!(accepted.as_mut().now_or_never().is_none());
        let mut closing = database.close();
        assert!(closing.as_mut().now_or_never().is_none());
        drop(closing);
        assert!(matches!(
            database.call(Request::Rows).await,
            Err(CallError::NotDispatched(_))
        ));
        let mut first_waiter = database.close();
        let mut second_waiter = database.close();
        assert!(first_waiter.as_mut().now_or_never().is_none());
        assert!(second_waiter.as_mut().now_or_never().is_none());
        release.send(()).unwrap();
        running.await.unwrap();
        assert_eq!(accepted.await.unwrap(), Response::Done);
        assert_eq!(
            futures_util::join!(first_waiter, second_waiter),
            (Ok(()), Ok(()))
        );
        database.close().await.unwrap();
        assert!(matches!(
            client.conn(|_| Ok(())).await,
            Err(async_sqlite::Error::Closed)
        ));
    });
}

#[test]
fn external_shutdown_does_not_strand_an_admitted_close() {
    smol::block_on(async {
        let (database, client) = database(None).await;
        let (running, release) = block(&database);
        let mut external = Box::pin(client.close());
        assert!(external.as_mut().now_or_never().is_none());
        let mut closing = database.close();
        assert!(closing.as_mut().now_or_never().is_none());
        release.send(()).unwrap();
        running.await.unwrap();
        external.await.unwrap();
        let result = smol::future::race(closing, async {
            smol::Timer::after(std::time::Duration::from_secs(1)).await;
            Err(CallError::NotDispatched(
                "close completion was stranded".into(),
            ))
        })
        .await;
        assert!(matches!(result, Err(CallError::OutcomeUnknown(_))));
        database.close().await.unwrap();
    });
}

#[test]
fn full_queue_rejects_dispatch_and_does_not_latch_close() {
    smol::block_on(async {
        let (database, _) = database(Some(1)).await;
        let (running, release) = block(&database);
        let mut queued = database.call(Request::Insert(1, vec![]));
        assert!(queued.as_mut().now_or_never().is_none());
        assert!(matches!(
            database.call(Request::Rows).await,
            Err(CallError::NotDispatched(_))
        ));
        assert!(matches!(
            database.close().await,
            Err(CallError::NotDispatched(_))
        ));
        release.send(()).unwrap();
        running.await.unwrap();
        queued.await.unwrap();
        assert_eq!(
            database.call(Request::Rows).await.unwrap(),
            Response::Rows(vec![Record {
                amount: 1,
                payload: vec![]
            },])
        );
        database.close().await.unwrap();
    });
}

#[test]
fn panic_after_commit_reports_unknown_outcome_without_replaying() {
    smol::block_on(async {
        let (database, _) = database(None).await;
        assert!(matches!(
            database.call(Request::CommitThenPanic).await,
            Err(CallError::OutcomeUnknown(_))
        ));
        assert_eq!(
            database.call(Request::Rows).await.unwrap(),
            Response::Rows(vec![Record {
                amount: 42,
                payload: vec![]
            },])
        );
        database.close().await.unwrap();
    });
}
