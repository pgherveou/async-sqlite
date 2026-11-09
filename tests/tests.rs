#![cfg(not(target_arch = "wasm32"))]

use std::sync::{
    atomic::{AtomicI64, AtomicUsize, Ordering},
    Arc,
};

use async_sqlite::{ClientBuilder, Error, JournalMode, PoolBuilder};
use futures_util::{future::join_all, FutureExt};
use rusqlite::OpenFlags;

static SHARED_MEMORY_ID: AtomicUsize = AtomicUsize::new(0);

fn shared_memory_name(prefix: &str) -> String {
    let id = SHARED_MEMORY_ID.fetch_add(1, Ordering::Relaxed);
    format!("{prefix}-{}-{id}", std::process::id())
}

fn assert_config_message(err: Error, expected: &str) {
    match err {
        Error::Config { message } => assert_eq!(message, expected),
        other => panic!("expected Error::Config, got {other:?}"),
    }
}

fn journal_modes() -> [(JournalMode, &'static str); 6] {
    [
        (JournalMode::Delete, "delete"),
        (JournalMode::Truncate, "truncate"),
        (JournalMode::Persist, "persist"),
        (JournalMode::Memory, "memory"),
        (JournalMode::Wal, "wal"),
        (JournalMode::Off, "off"),
    ]
}

macro_rules! async_test {
    ($name:ident) => {
        paste::item! {
            #[::core::prelude::v1::test]
            fn [< $name _smol >] () {
                ::smol::block_on($name());
            }

            #[::core::prelude::v1::test]
            fn [< $name _tokio >] () {
                ::tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .unwrap()
                    .block_on($name());
            }
        }
    };
}

async_test!(test_journal_mode);
async_test!(test_concurrency);
async_test!(test_default_pool_in_memory_uses_one_connection);
async_test!(test_pool);
async_test!(test_pool_rejects_multi_connection_anonymous_memory);
async_test!(test_shared_memory_pool);
async_test!(test_shared_memory_rejects_empty_name);
async_test!(test_pool_journal_mode);
async_test!(test_pool_initializes_each_connection);
async_test!(test_pool_initializer_failure);
async_test!(test_pool_close_concurrent);
async_test!(test_pool_num_conns_zero_clamps);
async_test!(test_pool_read_only);
async_test!(test_pool_transaction_reserves_one_connection);

async fn test_journal_mode() {
    for (journal_mode, expected) in journal_modes() {
        let tmp_dir = tempfile::tempdir().unwrap();
        let client = ClientBuilder::new()
            .journal_mode(journal_mode)
            .path(tmp_dir.path().join("sqlite.db"))
            .open()
            .await
            .expect("client unable to be opened");
        let mode = client
            .query_row("PRAGMA journal_mode", [], |row| row.get::<String>(0))
            .await
            .expect("client unable to fetch journal_mode");
        assert_eq!(mode, expected, "{journal_mode:?} journal mode mismatch");
        client.close().await.expect("closing client");
    }
}

async fn test_concurrency() {
    let client = ClientBuilder::new().open().await.unwrap();
    client
        .execute_batch("CREATE TABLE testing (id INTEGER PRIMARY KEY, val TEXT NOT NULL)")
        .await
        .unwrap();
    client
        .execute("INSERT INTO testing VALUES (1, ?)", ["value1"])
        .await
        .unwrap();
    let queries = (0..10).map(|_| {
        client.query_row("SELECT val FROM testing WHERE id=?", [1], |row| {
            row.get::<String>(0)
        })
    });
    let results = join_all(queries)
        .await
        .into_iter()
        .collect::<Result<Vec<_>, Error>>()
        .unwrap();
    assert_eq!(results, vec!["value1"; 10]);
    client.close().await.unwrap();
}

async fn test_default_pool_in_memory_uses_one_connection() {
    for builder in [PoolBuilder::new(), PoolBuilder::new().path(":memory:")] {
        let count = Arc::new(AtomicUsize::new(0));
        let initialized = count.clone();
        let pool = builder
            .open_with(move |_| {
                initialized.fetch_add(1, Ordering::Relaxed);
                Ok(())
            })
            .await
            .unwrap();
        assert_eq!(count.load(Ordering::Relaxed), 1);
        pool.execute_batch("CREATE TABLE testing (val TEXT)")
            .await
            .unwrap();
        pool.execute("INSERT INTO testing VALUES (?)", ["value1"])
            .await
            .unwrap();
        assert_eq!(
            pool.query_row("SELECT val FROM testing", [], |row| row.get::<String>(0))
                .await
                .unwrap(),
            "value1"
        );
        pool.close().await.unwrap();
    }
}

async fn test_pool() {
    let tmp_dir = tempfile::tempdir().unwrap();
    let pool = PoolBuilder::new()
        .path(tmp_dir.path().join("sqlite.db"))
        .num_conns(2)
        .open()
        .await
        .unwrap();
    pool.execute_batch("CREATE TABLE testing (id INTEGER PRIMARY KEY, val TEXT NOT NULL)")
        .await
        .unwrap();
    assert_eq!(
        pool.execute("INSERT INTO testing VALUES (1, ?)", ["value1"])
            .await
            .unwrap(),
        1
    );
    let queries = (0..10).map(|_| {
        pool.query_row("SELECT val FROM testing WHERE id=?", [1], |row| {
            row.get::<String>(0)
        })
    });
    let results = join_all(queries)
        .await
        .into_iter()
        .collect::<Result<Vec<_>, Error>>()
        .unwrap();
    assert_eq!(results, vec!["value1"; 10]);
    pool.close().await.unwrap();
}

async fn test_pool_rejects_multi_connection_anonymous_memory() {
    for builder in [PoolBuilder::new(), PoolBuilder::new().path(":memory:")] {
        let err = match builder.num_conns(2).open().await {
            Ok(_) => panic!("expected pool open to fail"),
            Err(err) => err,
        };
        assert_config_message(
            err,
            "anonymous in-memory pools cannot use multiple connections; call path(...) for file-backed pools or shared_memory(...) for named shared in-memory pools",
        );
    }
}

async fn test_shared_memory_pool() {
    let pool = PoolBuilder::new()
        .shared_memory(shared_memory_name("shared pool ?&%/é"))
        .num_conns(2)
        .open()
        .await
        .unwrap();
    pool.execute_batch("CREATE TABLE testing (val TEXT)")
        .await
        .unwrap();
    pool.execute("INSERT INTO testing VALUES (?)", ["value1"])
        .await
        .unwrap();
    for _ in 0..2 {
        assert_eq!(
            pool.query_row("SELECT val FROM testing", [], |row| row.get::<String>(0))
                .await
                .unwrap(),
            "value1"
        );
    }
    pool.close().await.unwrap();
}

async fn test_shared_memory_rejects_empty_name() {
    let err = match PoolBuilder::new().shared_memory("").open().await {
        Ok(_) => panic!("expected pool open to fail"),
        Err(err) => err,
    };
    assert_config_message(err, "shared memory database name must not be empty");
}

async fn test_pool_journal_mode() {
    for (journal_mode, expected) in journal_modes() {
        let tmp_dir = tempfile::tempdir().unwrap();
        let pool = PoolBuilder::new()
            .journal_mode(journal_mode)
            .path(tmp_dir.path().join("sqlite.db"))
            .num_conns(4)
            .open()
            .await
            .unwrap();
        for connection in 0..4 {
            let mode = pool
                .query_row("PRAGMA journal_mode", [], |row| row.get::<String>(0))
                .await
                .unwrap();
            assert_eq!(
                mode, expected,
                "{journal_mode:?} journal mode mismatch on connection {connection}"
            );
        }
        pool.close().await.unwrap();
    }
}

async fn test_pool_initializes_each_connection() {
    let tmp_dir = tempfile::tempdir().unwrap();
    let attached_path = tmp_dir.path().join("attached.db");
    let client = ClientBuilder::new()
        .path(&attached_path)
        .open()
        .await
        .unwrap();
    client
        .execute_batch("CREATE TABLE testing (id INTEGER PRIMARY KEY)")
        .await
        .unwrap();
    client.close().await.unwrap();
    let caller = std::thread::current().id();
    let pool = PoolBuilder::new()
        .path(tmp_dir.path().join("pool.db"))
        .num_conns(3)
        .open_with(move |connection| {
            assert_ne!(std::thread::current().id(), caller);
            connection.execute(
                "ATTACH DATABASE ? AS attached",
                [attached_path.to_str().unwrap()],
            )?;
            Ok(())
        })
        .await
        .unwrap();
    for _ in 0..3 {
        assert_eq!(
            pool.query_map(
                "SELECT name FROM attached.sqlite_master WHERE type='table'",
                [],
                |row| row.get::<String>(0)
            )
            .await
            .unwrap(),
            vec!["testing"]
        );
    }
    pool.close().await.unwrap();
}

async fn test_pool_initializer_failure() {
    let tmp_dir = tempfile::tempdir().unwrap();
    let initialized = AtomicUsize::new(0);
    let result = PoolBuilder::new()
        .path(tmp_dir.path().join("pool.db"))
        .num_conns(2)
        .open_with(move |_| {
            if initialized.fetch_add(1, Ordering::Relaxed) == 1 {
                return Err(Error::Config {
                    message: "initializer failed",
                });
            }
            Ok(())
        })
        .await;
    match result {
        Err(error) => assert_config_message(error, "initializer failed"),
        Ok(_) => panic!("expected initializer failure to prevent pool opening"),
    }
}

async fn test_pool_close_concurrent() {
    let tmp_dir = tempfile::tempdir().unwrap();
    let pool = PoolBuilder::new()
        .path(tmp_dir.path().join("sqlite.db"))
        .num_conns(2)
        .open()
        .await
        .unwrap();
    let clone = pool.clone();
    let (first, second) = futures_util::join!(pool.close(), clone.close());
    first.unwrap();
    second.unwrap();
    assert!(matches!(
        pool.query_row("SELECT 1", [], |row| row.get::<i64>(0))
            .await,
        Err(Error::Closed)
    ));
}

async fn test_pool_num_conns_zero_clamps() {
    let tmp_dir = tempfile::tempdir().unwrap();
    let count = Arc::new(AtomicUsize::new(0));
    let initialized = count.clone();
    let pool = PoolBuilder::new()
        .path(tmp_dir.path().join("clamp.db"))
        .num_conns(0)
        .open_with(move |_| {
            initialized.fetch_add(1, Ordering::Relaxed);
            Ok(())
        })
        .await
        .unwrap();
    assert_eq!(count.load(Ordering::Relaxed), 1);
    pool.close().await.unwrap();
}

async fn test_pool_read_only() {
    let tmp_dir = tempfile::tempdir().unwrap();
    let path = tmp_dir.path().join("readonly.db");
    let client = ClientBuilder::new().path(&path).open().await.unwrap();
    client
        .execute_batch("CREATE TABLE testing (id INTEGER); INSERT INTO testing VALUES (1)")
        .await
        .unwrap();
    client.close().await.unwrap();
    let pool = PoolBuilder::new()
        .path(path)
        .flags(OpenFlags::SQLITE_OPEN_READ_ONLY)
        .num_conns(2)
        .open()
        .await
        .unwrap();
    for _ in 0..2 {
        assert_eq!(
            pool.query_row("SELECT id FROM testing", [], |row| row.get::<i64>(0))
                .await
                .unwrap(),
            1
        );
    }
    for _ in 0..2 {
        assert!(matches!(
            pool.execute("INSERT INTO testing VALUES (2)", []).await,
            Err(Error::Rusqlite(rusqlite::Error::SqliteFailure(error, _)))
                if error.code == rusqlite::ErrorCode::ReadOnly
        ));
    }
    pool.close().await.unwrap();
}

async fn test_pool_transaction_reserves_one_connection() {
    let tmp_dir = tempfile::tempdir().unwrap();
    let identity = AtomicI64::new(0);
    let pool = PoolBuilder::new()
        .path(tmp_dir.path().join("reserved.db"))
        .journal_mode(JournalMode::Wal)
        .num_conns(2)
        .open_with(move |connection| {
            connection.execute_batch(
                "CREATE TABLE IF NOT EXISTS testing (id INTEGER PRIMARY KEY, amount INTEGER);
                 INSERT OR IGNORE INTO testing VALUES (1, 0);
                 CREATE TEMP TABLE connection_id (id INTEGER)",
            )?;
            connection.execute(
                "INSERT INTO connection_id VALUES (?)",
                [identity.fetch_add(1, Ordering::Relaxed)],
            )?;
            Ok(())
        })
        .await
        .unwrap();
    let transaction = pool.transaction().await.unwrap();
    transaction
        .execute("UPDATE testing SET amount=?", [10])
        .await
        .unwrap();
    let sql = "SELECT connection_id.id, testing.amount FROM connection_id, testing";
    let map = |row: &async_sqlite::Row<'_>| Ok((row.get::<i64>(0)?, row.get::<i64>(1)?));
    assert_eq!(transaction.query_row(sql, [], map).await.unwrap(), (0, 10));
    assert_eq!(pool.query_row(sql, [], map).await.unwrap(), (1, 0));
    let mut waiting = Box::pin(pool.query_row(sql, [], map));
    assert!(waiting.as_mut().now_or_never().is_none());
    transaction.commit().await.unwrap();
    assert_eq!(waiting.await.unwrap(), (0, 10));
    pool.close().await.unwrap();
}
