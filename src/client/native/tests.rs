use super::*;
use futures_util::{future::BoxFuture, FutureExt};
use std::{
    sync::mpsc,
    time::{Duration, Instant},
};

fn block(client: &Client) -> (BoxFuture<'_, Result<(), Error>>, mpsc::Sender<()>) {
    let (entered, started) = mpsc::channel();
    let (release, wait) = mpsc::channel();
    let mut running = client
        .conn(move |_| {
            entered.send(()).unwrap();
            wait.recv_timeout(Duration::from_secs(5)).unwrap();
            Ok(())
        })
        .boxed();
    assert!(running.as_mut().now_or_never().is_none());
    started.recv_timeout(Duration::from_secs(5)).unwrap();
    (running, release)
}

fn closing(client: &Client, queued: usize) -> thread::JoinHandle<Result<(), Error>> {
    let clone = client.clone();
    let closing = thread::spawn(move || clone.close_blocking());
    let deadline = Instant::now() + Duration::from_secs(5);
    while client.backend.conn_tx.len() == queued && Instant::now() < deadline {
        thread::yield_now();
    }
    assert_eq!(client.backend.conn_tx.len(), queued + 1);
    closing
}

#[test]
fn blocking_close_drains_accepted_sql_and_rejects_later_calls() {
    smol::block_on(async {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("ledger.sqlite");
        let client = ClientBuilder::new().path(&path).open().await.unwrap();
        client
            .execute_batch("CREATE TABLE ledger (value INTEGER)")
            .await
            .unwrap();
        let (running, release) = block(&client);
        let mut accepted = Box::pin(client.execute("INSERT INTO ledger VALUES (1)", []));
        assert!(accepted.as_mut().now_or_never().is_none());
        let closing = closing(&client, 1);
        let mut request = Box::pin(client.execute("INSERT INTO ledger VALUES (2)", []));
        let first_poll = request.as_mut().now_or_never();
        release.send(()).unwrap();
        running.await.unwrap();
        closing.join().unwrap().unwrap();
        let result = match first_poll {
            Some(result) => result,
            None => request.await,
        };
        assert!(matches!(result, Err(Error::Closed)), "{result:?}");
        let connection = Connection::open(path).unwrap();
        let rows: Vec<i64> = connection
            .prepare("SELECT value FROM ledger")
            .unwrap()
            .query_map([], |row| row.get(0))
            .unwrap()
            .collect::<rusqlite::Result<_>>()
            .unwrap();
        assert_eq!((accepted.await.unwrap(), rows), (1, vec![1]));
    });
}

#[test]
fn blocking_and_async_close_share_one_shutdown() {
    smol::block_on(async {
        let client = ClientBuilder::new().open().await.unwrap();
        let (running, release) = block(&client);
        let closing = closing(&client, 0);
        let mut also_closing = Box::pin(client.close());
        assert!(also_closing.as_mut().now_or_never().is_none());
        let queued = client.backend.conn_tx.len();
        release.send(()).unwrap();
        running.await.unwrap();
        closing.join().unwrap().unwrap();
        let result = also_closing.await;
        assert_eq!(queued, 1);
        result.unwrap();
        client.close_blocking().unwrap();
    });
}

#[test]
fn failed_close_preserves_sqlite_errors_and_allows_retry() {
    let (conn_tx, commands) = unbounded();
    let backend = Backend {
        conn_tx,
        admission: Arc::new(Mutex::new(Admission::Open)),
    };
    let (completed, results) = mpsc::channel();
    for _ in 0..2 {
        let completed = completed.clone();
        let admission = backend.admission.clone();
        backend
            .begin_close(Box::new(move |result| {
                let Err(Error::Rusqlite(rusqlite::Error::SqliteFailure(error, message))) = result
                else {
                    panic!("unexpected close result: {result:?}");
                };
                let open = matches!(*admission.lock().unwrap(), Admission::Open);
                completed
                    .send((error.extended_code, message, open))
                    .unwrap();
            }))
            .unwrap();
    }
    assert!(matches!(commands.try_recv(), Ok(Command::Shutdown)));
    assert!(commands.is_empty());
    finish_close(
        &backend.admission,
        Err(rusqlite::Error::SqliteFailure(
            rusqlite::ffi::Error::new(rusqlite::ffi::SQLITE_BUSY),
            Some("unfinished statement".into()),
        )),
    );
    assert_eq!(
        results.try_iter().collect::<Vec<_>>(),
        vec![
            (
                rusqlite::ffi::SQLITE_BUSY,
                Some("unfinished statement".into()),
                true,
            );
            2
        ]
    );

    let (completed, result) = mpsc::channel();
    backend
        .begin_close(Box::new(move |result| completed.send(result).unwrap()))
        .unwrap();
    assert!(matches!(commands.try_recv(), Ok(Command::Shutdown)));
    finish_close(&backend.admission, Ok(()));
    result.recv().unwrap().unwrap();
    assert!(matches!(
        *backend.admission.lock().unwrap(),
        Admission::Closed
    ));
}

#[test]
fn worker_exit_releases_sql_and_close_waiters() {
    struct PanicOnDrop;
    impl Drop for PanicOnDrop {
        fn drop(&mut self) {
            panic!("canceled closure destructor");
        }
    }

    smol::block_on(async {
        let client = ClientBuilder::new().open().await.unwrap();
        let (running, release) = block(&client);
        let destructor = PanicOnDrop;
        let mut canceled = Box::pin(client.conn(move |_| {
            drop(destructor);
            Ok(())
        }));
        assert!(canceled.as_mut().now_or_never().is_none());
        drop(canceled);
        let mut query = Box::pin(client.execute_batch("CREATE TABLE discarded (value)"));
        assert!(query.as_mut().now_or_never().is_none());
        let mut closing = Box::pin(client.close());
        assert!(closing.as_mut().now_or_never().is_none());
        let (complete, blocking) = oneshot::channel();
        let clone = client.clone();
        let close_thread = thread::spawn(move || {
            _ = complete.send(clone.close_blocking());
        });
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            let admission = client.backend.admission.lock().unwrap();
            if matches!(&*admission, Admission::Closing(replies) if replies.len() == 2) {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "blocking close was not registered"
            );
            drop(admission);
            thread::yield_now();
        }

        release.send(()).unwrap();
        running.await.unwrap();
        let outcome = smol::future::race(
            async { Some((query.await, closing.await, blocking.await.unwrap())) },
            async {
                smol::Timer::after(Duration::from_secs(5)).await;
                None
            },
        )
        .await;
        if outcome.is_none() {
            let previous = {
                let mut admission = client.backend.admission.lock().unwrap();
                core::mem::replace(&mut *admission, Admission::Closed)
            };
            drop(previous);
        }
        close_thread.join().unwrap();
        assert!(
            matches!(
                outcome,
                Some((
                    Err(Error::OutcomeUnknown { .. }),
                    Err(Error::OutcomeUnknown { .. }),
                    Ok(())
                ))
            ),
            "{outcome:?}"
        );
        assert!(matches!(
            *client.backend.admission.lock().unwrap(),
            Admission::Closed
        ));
    });
}
