# async-sqlite

[![Crates.io](https://img.shields.io/crates/v/async-sqlite)](https://crates.io/crates/async-sqlite)
[![docs.rs](https://img.shields.io/docsrs/async-sqlite)](https://docs.rs/async-sqlite)

A library to interact with sqlite from an async context.

This library is tested on both [tokio](https://docs.rs/tokio/latest/tokio/)
and [smol](https://docs.rs/smol/latest/smol/), however
it should be compatible with all async runtimes.

## Install

Add `async-sqlite` to your "dependencies" in your Cargo.toml file.

This can be done by running the command:

```
cargo add async-sqlite
```

## Usage

A `Client` represents a single background sqlite3 connection that can be called
concurrently from any thread in your program.

To create a sqlite client and run a query:

```rust
use async_sqlite::{ClientBuilder, JournalMode};

let client = ClientBuilder::new()
                .path("/path/to/db.sqlite3")
                .journal_mode(JournalMode::Wal)
                .open()
                .await?;

let value: String = client
    .query_row("SELECT val FROM testing WHERE id=?", [1], |row| row.get(0))
    .await?;

println!("Value is: {value}");
```

Native `conn` calls canceled before the worker starts them are skipped.
`ClientBuilder::queue_capacity()` and
`PoolBuilder::queue_capacity()` can be used to bound worker queues; operations
return `Error::QueueFull` when the selected queue is full.

A `Pool` represents a collection of background sqlite3 connections that can be
called concurrently from any thread in your program.

`PoolBuilder::new().open()` and `path(":memory:")` use a single anonymous
in-memory connection by default, since separate SQLite `:memory:` connections
do not share schema or data. File-backed pools default to the logical CPU
count. For multiple connections to a named in-memory database, use
`shared_memory("name")`; this uses SQLite shared-cache mode, which has caveats,
so prefer a file-backed database when possible.

To create a sqlite pool and run a query:

```rust
use async_sqlite::{JournalMode, PoolBuilder};

let pool = PoolBuilder::new()
              .path("/path/to/db.sqlite3")
              .journal_mode(JournalMode::Wal)
              .open()
              .await?;

let value: String = pool.conn(|conn| {
    conn.query_row("SELECT val FROM testing WHERE id=?", [1], |row| row.get(0))
}).await?;

println!("Value is: {value}");
```

## Cargo Features

This library tries to export almost all features that the underlying
[rusqlite](https://docs.rs/rusqlite/latest/rusqlite/) library contains.

A notable difference is that the `bundled` feature is **enabled** by default
alongside rusqlite's `cache` and `ffi-sqlite-wasm-rs` defaults. These defaults
can be disabled with the following line in your Cargo.toml:

```toml
async-sqlite = { version = "*", default-features = false }
```

## Browser support

The `sqlite-wasm-rs` feature enables `Client::connect(worker)` on
`wasm32-unknown-unknown`. The database worker initializes its VFS and schema, then
awaits `serve(connection)`. Use `serve_with_shutdown` to release VFS resources on close.

Both backends provide `execute`, `query_row`, `query_map` and `execute_batch`.
Row mappers run on the caller over buffered results. Native `conn` methods remain
available for direct rusqlite access, including transactions that branch in Rust.

Accepted SQL operations survive caller cancellation. A lost reply produces
`Error::OutcomeUnknown`; a write may have committed and is never automatically retried.

See the [worker example](https://github.com/pgherveou/async-sqlite-worker-example)
for shared SQL calls and OPFS tests. Run `cargo test` for the library's tests.
