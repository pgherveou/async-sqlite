#![cfg(not(target_arch = "wasm32"))]

use std::{rc::Rc, sync::mpsc, thread};

use async_sqlite::{Client, ClientBuilder, Error};
use futures_util::{future::BoxFuture, FutureExt};
use rusqlite::params;

async fn database(capacity: Option<usize>) -> Client {
    let builder = match capacity {
        Some(capacity) => ClientBuilder::new().queue_capacity(capacity),
        None => ClientBuilder::new(),
    };
    let client = builder.open().await.unwrap();
    client
        .execute_batch("CREATE TABLE ledger (amount INTEGER, payload BLOB)")
        .await
        .unwrap();
    client
}

fn send_future<T: Send>(future: T) -> T {
    future
}

fn block(client: &Client) -> (BoxFuture<'_, Result<(), Error>>, mpsc::Sender<()>) {
    let (entered, started) = mpsc::channel();
    let (release, wait) = mpsc::channel();
    let mut running = client
        .conn(move |_| {
            entered.send(()).unwrap();
            wait.recv().unwrap();
            Ok(())
        })
        .boxed();
    assert!(running.as_mut().now_or_never().is_none());
    started.recv().unwrap();
    (running, release)
}

#[test]
fn query_maps_on_caller_without_a_registered_handler() {
    smol::block_on(async {
        let client = database(None).await;
        let caller = thread::current().id();
        assert_ne!(
            client.conn(|_| Ok(thread::current().id())).await.unwrap(),
            caller
        );
        let expected = String::from("stored value");
        client
            .execute("INSERT INTO ledger VALUES (?, ?)", params![1, expected])
            .await
            .unwrap();
        let result = send_future(client.query_row(
            "SELECT payload FROM ledger WHERE amount=?",
            [1],
            |row| {
                assert_eq!(thread::current().id(), caller);
                assert_eq!(row.get::<String>(0)?, expected);
                row.get::<String>(0)
            },
        ))
        .await
        .unwrap();
        assert_eq!(result, expected);
        let local = send_future(client.query_row("SELECT 'local'", [], |row| {
            Ok(Rc::new(row.get::<String>(0)?))
        }))
        .await
        .unwrap();
        assert_eq!(*local, "local");
        client.close().await.unwrap();
    });
}

#[test]
fn scalar_values_and_named_columns_survive_owned_rows() {
    smol::block_on(async {
        let client = database(None).await;
        let blob = vec![0, 255, 128];
        for value in [
            i64::MIN,
            -9_007_199_254_740_993,
            9_007_199_254_740_993,
            i64::MAX,
        ] {
            send_future(client.execute("INSERT INTO ledger VALUES (?, ?)", params![value, blob]))
                .await
                .unwrap();
        }
        let rows = client
            .query_map(
                "SELECT amount, payload FROM ledger ORDER BY rowid",
                [],
                |row| Ok((row.get_named::<i64>("amount")?, row.get::<Vec<u8>>(1)?)),
            )
            .await
            .unwrap();
        assert_eq!(
            rows,
            [
                i64::MIN,
                -9_007_199_254_740_993,
                9_007_199_254_740_993,
                i64::MAX
            ]
            .into_iter()
            .map(|value| (value, blob.clone()))
            .collect::<Vec<_>>()
        );
        let scalars = client
            .query_row(
                "SELECT ?, ?, ?, ?",
                params![None::<i64>, 1.25, "", Vec::<u8>::new()],
                |row| {
                    Ok((
                        row.get::<Option<i64>>(0)?,
                        row.get::<f64>(1)?,
                        row.get::<String>(2)?,
                        row.get::<Vec<u8>>(3)?,
                    ))
                },
            )
            .await
            .unwrap();
        assert_eq!(scalars, (None, 1.25, String::new(), vec![]));
        client.close().await.unwrap();
    });
}

#[test]
fn query_row_does_not_evaluate_later_rows() {
    smol::block_on(async {
        let client = database(None).await;
        let sql = "SELECT 1 UNION ALL SELECT abs(-9223372036854775808)";
        assert_eq!(
            client
                .query_row(sql, [], |row| row.get::<i64>(0))
                .await
                .unwrap(),
            1
        );
        assert!(matches!(
            client.query_map(sql, [], |row| row.get::<i64>(0)).await,
            Err(Error::Rusqlite(_))
        ));
        client.close().await.unwrap();
    });
}

#[test]
fn batches_rollback_failed_or_unfinished_transactions() {
    smol::block_on(async {
        let client = database(None).await;
        client
            .execute_batch("BEGIN; INSERT INTO ledger VALUES (1, X'01'); COMMIT")
            .await
            .unwrap();
        assert!(client.execute_batch("BEGIN; INSERT INTO ledger VALUES (2, X'02'); INSERT INTO missing VALUES (1); COMMIT").await.is_err());
        assert!(client
            .execute_batch("BEGIN; INSERT INTO ledger VALUES (3, X'03')")
            .await
            .is_err());
        assert!(client.execute("BEGIN", []).await.is_err());
        assert_eq!(
            client
                .query_map("SELECT amount FROM ledger", [], |row| row.get::<i64>(0))
                .await
                .unwrap(),
            vec![1]
        );
        client
            .execute("INSERT INTO ledger VALUES (4, X'04')", [])
            .await
            .unwrap();
        client.close().await.unwrap();
    });
}

#[test]
fn query_and_conversion_errors_leave_connection_usable() {
    smol::block_on(async {
        let client = database(None).await;
        assert!(matches!(
            client
                .query_row("SELECT amount FROM ledger", [], |row| row.get::<i64>(0))
                .await,
            Err(Error::Rusqlite(rusqlite::Error::QueryReturnedNoRows))
        ));
        assert!(matches!(
            client
                .query_row("SELECT 'text'", [], |row| row.get::<i64>(0))
                .await,
            Err(Error::Rusqlite(rusqlite::Error::InvalidColumnType(..)))
        ));
        assert!(matches!(
            client
                .query_row("SELECT ?", [], |row| row.get::<i64>(0))
                .await,
            Err(Error::Rusqlite(rusqlite::Error::InvalidParameterCount(
                0, 1
            )))
        ));
        assert_eq!(
            client
                .query_row("SELECT 1", [], |row| row.get::<i64>(0))
                .await
                .unwrap(),
            1
        );
        client.close().await.unwrap();
    });
}

#[test]
fn accepted_sql_survives_cancellation_but_unpolled_sql_does_not_run() {
    smol::block_on(async {
        let client = database(None).await;
        let (running, release) = block(&client);
        let mut accepted = Box::pin(client.execute("INSERT INTO ledger VALUES (?, X'')", [1]));
        assert!(accepted.as_mut().now_or_never().is_none());
        drop(accepted);
        drop(client.execute("INSERT INTO ledger VALUES (?, X'')", [2]));
        release.send(()).unwrap();
        running.await.unwrap();
        assert_eq!(
            client
                .query_map("SELECT amount FROM ledger", [], |row| row.get::<i64>(0))
                .await
                .unwrap(),
            vec![1]
        );
        client.close().await.unwrap();
    });
}

#[test]
fn close_drains_accepted_sql_and_survives_a_dropped_waiter() {
    smol::block_on(async {
        let client = database(None).await;
        let clone = client.clone();
        let (running, release) = block(&client);
        let mut accepted = Box::pin(client.execute("INSERT INTO ledger VALUES (?, X'')", [1]));
        assert!(accepted.as_mut().now_or_never().is_none());
        let mut closing = Box::pin(send_future(client.close()));
        assert!(closing.as_mut().now_or_never().is_none());
        drop(closing);
        assert!(matches!(
            clone
                .execute("INSERT INTO ledger VALUES (?, X'')", [2])
                .await,
            Err(Error::Closed)
        ));
        release.send(()).unwrap();
        running.await.unwrap();
        assert_eq!(accepted.await.unwrap(), 1);
        let (first, second) = futures_util::join!(client.close(), clone.close());
        first.unwrap();
        second.unwrap();
        assert!(matches!(client.conn(|_| Ok(())).await, Err(Error::Closed)));
    });
}

#[test]
fn full_queue_rejects_sql_without_latching_close() {
    smol::block_on(async {
        let client = database(Some(1)).await;
        let (running, release) = block(&client);
        let mut accepted = Box::pin(client.execute("INSERT INTO ledger VALUES (?, X'')", [1]));
        assert!(accepted.as_mut().now_or_never().is_none());
        assert!(matches!(
            client
                .execute("INSERT INTO ledger VALUES (?, X'')", [2])
                .await,
            Err(Error::QueueFull)
        ));
        assert!(matches!(client.close().await, Err(Error::QueueFull)));
        release.send(()).unwrap();
        running.await.unwrap();
        accepted.await.unwrap();
        assert_eq!(
            client
                .query_map("SELECT amount FROM ledger", [], |row| row.get::<i64>(0))
                .await
                .unwrap(),
            vec![1]
        );
        client.close().await.unwrap();
    });
}
