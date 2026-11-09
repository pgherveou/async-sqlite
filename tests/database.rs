#![cfg(not(target_arch = "wasm32"))]

use std::{future::Future, rc::Rc, sync::mpsc, thread, time::Duration};

use async_sqlite::{Client, ClientBuilder, Error};
use futures_util::{future::BoxFuture, FutureExt};
use rusqlite::{functions::FunctionFlags, params};

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

struct Gate {
    started: mpsc::Receiver<()>,
    release: mpsc::Sender<()>,
}

async fn gated_database(capacity: Option<usize>) -> (Client, Gate) {
    let (entered, started) = mpsc::channel();
    let (release, wait) = mpsc::channel();
    let builder = match capacity {
        Some(capacity) => ClientBuilder::new().queue_capacity(capacity),
        None => ClientBuilder::new(),
    };
    let client = builder
        .open_with(move |connection| {
            connection.execute_batch("CREATE TABLE ledger (amount INTEGER, payload BLOB)")?;
            connection.create_scalar_function(
                "wait_for_release",
                0,
                FunctionFlags::SQLITE_UTF8,
                move |_| {
                    entered.send(()).unwrap();
                    wait.recv().unwrap();
                    Ok(0)
                },
            )?;
            Ok(())
        })
        .await
        .unwrap();
    (client, Gate { started, release })
}

fn block<'a>(
    query: impl Future<Output = Result<i64, Error>> + Send + 'a,
    gate: &Gate,
) -> BoxFuture<'a, Result<i64, Error>> {
    let mut running = query.boxed();
    assert!(running.as_mut().now_or_never().is_none());
    gate.started.recv_timeout(Duration::from_secs(10)).unwrap();
    running
}

#[test]
fn query_maps_on_caller_without_a_registered_handler() {
    smol::block_on(async {
        let client = database(None).await;
        let caller = thread::current().id();
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
        let (client, gate) = gated_database(None).await;
        let running = block(
            client.query_row("SELECT wait_for_release()", [], |row| row.get::<i64>(0)),
            &gate,
        );
        let mut accepted = Box::pin(client.execute("INSERT INTO ledger VALUES (?, X'')", [1]));
        assert!(accepted.as_mut().now_or_never().is_none());
        drop(accepted);
        drop(client.execute("INSERT INTO ledger VALUES (?, X'')", [2]));
        gate.release.send(()).unwrap();
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
        let (client, gate) = gated_database(None).await;
        let clone = client.clone();
        let running = block(
            client.query_row("SELECT wait_for_release()", [], |row| row.get::<i64>(0)),
            &gate,
        );
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
        gate.release.send(()).unwrap();
        running.await.unwrap();
        assert_eq!(accepted.await.unwrap(), 1);
        let (first, second) = futures_util::join!(client.close(), clone.close());
        first.unwrap();
        second.unwrap();
        assert!(matches!(
            client
                .query_row("SELECT 1", [], |row| row.get::<i64>(0))
                .await,
            Err(Error::Closed)
        ));
    });
}

#[test]
fn full_queue_rejects_sql_without_latching_close() {
    smol::block_on(async {
        let (client, gate) = gated_database(Some(1)).await;
        let running = block(
            client.query_row("SELECT wait_for_release()", [], |row| row.get::<i64>(0)),
            &gate,
        );
        let mut accepted = Box::pin(client.execute("INSERT INTO ledger VALUES (?, X'')", [1]));
        assert!(accepted.as_mut().now_or_never().is_none());
        assert!(matches!(
            client
                .execute("INSERT INTO ledger VALUES (?, X'')", [2])
                .await,
            Err(Error::QueueFull)
        ));
        assert!(matches!(client.close().await, Err(Error::QueueFull)));
        gate.release.send(()).unwrap();
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

#[test]
fn transaction_keeps_rust_branches_isolated_until_commit() {
    smol::block_on(async {
        let client = database(None).await;
        client
            .execute("INSERT INTO ledger VALUES (?, X'')", [100])
            .await
            .unwrap();
        let clone = client.clone();
        let transaction = send_future(client.transaction()).await.unwrap();
        let balance = send_future(
            transaction.query_row("SELECT amount FROM ledger", [], |row| row.get::<i64>(0)),
        )
        .await
        .unwrap();
        if balance >= 25 {
            send_future(transaction.execute("UPDATE ledger SET amount=?", [balance - 25]))
                .await
                .unwrap();
        }
        let mut outside =
            Box::pin(clone.query_row("SELECT amount FROM ledger", [], |row| row.get::<i64>(0)));
        assert!(outside.as_mut().now_or_never().is_none());
        assert_eq!(
            send_future(
                transaction.query_map("SELECT amount FROM ledger", [], |row| { row.get::<i64>(0) })
            )
            .await
            .unwrap(),
            vec![75]
        );
        send_future(transaction.commit()).await.unwrap();
        assert_eq!(outside.await.unwrap(), 75);
        client.close().await.unwrap();
    });
}

#[test]
fn rollback_and_drop_undo_uncommitted_changes() {
    smol::block_on(async {
        let client = database(None).await;
        for explicit in [true, false] {
            let transaction = client.transaction().await.unwrap();
            send_future(transaction.execute_batch("INSERT INTO ledger VALUES (1, X'')"))
                .await
                .unwrap();
            if explicit {
                send_future(transaction.rollback()).await.unwrap();
            } else {
                drop(transaction);
            }
            assert_eq!(
                client
                    .query_map("SELECT amount FROM ledger", [], |row| row.get::<i64>(0))
                    .await
                    .unwrap(),
                Vec::<i64>::new()
            );
        }
        client.close().await.unwrap();
    });
}

#[test]
fn dropping_an_unpolled_commit_rolls_back() {
    smol::block_on(async {
        let client = database(None).await;
        let transaction = client.transaction().await.unwrap();
        transaction
            .execute("INSERT INTO ledger VALUES (1, X'')", [])
            .await
            .unwrap();
        drop(transaction.commit());
        assert_eq!(
            client
                .query_map("SELECT amount FROM ledger", [], |row| row.get::<i64>(0))
                .await
                .unwrap(),
            Vec::<i64>::new()
        );
        client.close().await.unwrap();
    });
}

#[test]
fn canceled_transaction_waiter_does_not_reserve_the_connection() {
    smol::block_on(async {
        let client = database(None).await;
        let transaction = client.transaction().await.unwrap();
        let mut waiting = Box::pin(client.transaction());
        assert!(waiting.as_mut().now_or_never().is_none());
        drop(waiting);
        transaction.commit().await.unwrap();
        let next = client.transaction().await.unwrap();
        next.execute("INSERT INTO ledger VALUES (1, X'')", [])
            .await
            .unwrap();
        next.commit().await.unwrap();
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
fn canceled_admitted_begin_rolls_back_before_releasing_the_connection() {
    smol::block_on(async {
        let (client, gate) = gated_database(None).await;
        let running = block(
            client.query_row("SELECT wait_for_release()", [], |row| row.get::<i64>(0)),
            &gate,
        );
        let mut beginning = Box::pin(client.transaction());
        assert!(beginning.as_mut().now_or_never().is_none());
        drop(beginning);
        gate.release.send(()).unwrap();
        running.await.unwrap();
        client
            .execute("INSERT INTO ledger VALUES (1, X'')", [])
            .await
            .unwrap();
        let next = client.transaction().await.unwrap();
        next.rollback().await.unwrap();
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
fn canceled_admitted_commit_can_persist_and_releases_the_connection() {
    smol::block_on(async {
        let (client, gate) = gated_database(None).await;
        let transaction = client.transaction().await.unwrap();
        transaction
            .execute("INSERT INTO ledger VALUES (1, X'')", [])
            .await
            .unwrap();
        let running = block(
            transaction.query_row("SELECT wait_for_release()", [], |row| row.get::<i64>(0)),
            &gate,
        );
        drop(running);
        let mut committing = Box::pin(send_future(transaction.commit()));
        assert!(committing.as_mut().now_or_never().is_none());
        drop(committing);
        gate.release.send(()).unwrap();
        assert_eq!(
            client
                .query_map("SELECT amount FROM ledger", [], |row| row.get::<i64>(0))
                .await
                .unwrap(),
            vec![1]
        );
        let next = client.transaction().await.unwrap();
        next.rollback().await.unwrap();
        client.close().await.unwrap();
    });
}

#[test]
fn close_waits_for_a_transaction_before_stopping_admission() {
    smol::block_on(async {
        let client = database(None).await;
        let transaction = client.transaction().await.unwrap();
        let mut closing = Box::pin(send_future(client.close()));
        assert!(closing.as_mut().now_or_never().is_none());
        transaction
            .execute("INSERT INTO ledger VALUES (1, X'')", [])
            .await
            .unwrap();
        transaction.commit().await.unwrap();
        closing.await.unwrap();
        assert!(matches!(
            client
                .query_row("SELECT 1", [], |row| row.get::<i64>(0))
                .await,
            Err(Error::Closed)
        ));
    });
}
