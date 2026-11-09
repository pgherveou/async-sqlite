//! # async-sqlite
//!
//! A library to interact with sqlite from an async context.
//!
//! This library is tested on both [tokio](https://docs.rs/tokio/latest/tokio/)
//! and [smol](https://docs.rs/smol/latest/smol/), however
//! it should be compatible with all async runtimes.
//!
//! ## Usage
//!
//! A `Client` represents a single background sqlite3 connection that can be called
//! concurrently from any thread in your program.
//!
//! To create a sqlite client and run a query:
//!
//! ```rust
//! use async_sqlite::{ClientBuilder, JournalMode};
//!
//! # async fn run() -> Result<(), async_sqlite::Error> {
//! let client = ClientBuilder::new()
//!                 .path("/path/to/db.sqlite3")
//!                 .journal_mode(JournalMode::Wal)
//!                 .open()
//!                 .await?;
//!
//! let value: String = client
//!     .query_row("SELECT val FROM testing WHERE id=?", [1], |row| row.get(0))
//!     .await?;
//!
//! println!("Value is: {value}");
//! # Ok(())
//! }
//! ```
//!
//! Accepted SQL operations survive caller cancellation.
//! `ClientBuilder::queue_capacity()` and
//! `PoolBuilder::queue_capacity()` can be used to bound worker queues;
//! operations return [`Error::QueueFull`] when the selected queue is full.
//!
//! A `Pool` represents a collection of background sqlite3 connections that can be
//! called concurrently from any thread in your program.
//!
//! `PoolBuilder::new().open()` and `path(":memory:")` use a single anonymous
//! in-memory connection by default, since separate SQLite `:memory:`
//! connections do not share schema or data. File-backed pools default to the
//! logical CPU count. For multiple connections to a named in-memory database,
//! use `shared_memory("name")`; this uses SQLite shared-cache mode, which has
//! caveats, so prefer a file-backed database when possible.
//!
//! To create a sqlite pool and run a query in a transaction:
//!
//! ```rust
//! use async_sqlite::{JournalMode, PoolBuilder};
//!
//! # async fn run() -> Result<(), async_sqlite::Error> {
//! let pool = PoolBuilder::new()
//!               .path("/path/to/db.sqlite3")
//!               .journal_mode(JournalMode::Wal)
//!               .open()
//!               .await?;
//!
//! let transaction = pool.transaction().await?;
//! let value: String = transaction
//!     .query_row("SELECT val FROM testing WHERE id=?", [1], |row| row.get(0))
//!     .await?;
//! transaction.commit().await?;
//!
//! println!("Value is: {value}");
//! # Ok(())
//! # }
//! ```
//!
//! ## Cargo Features
//!
//! This library tries to export almost all features that the underlying
//! [rusqlite](https://docs.rs/rusqlite/latest/rusqlite/) library contains.
//!
//! A notable difference is that the `bundled` feature is **enabled** by default
//! alongside rusqlite's `cache` and `ffi-sqlite-wasm-rs` defaults. These defaults
//! can be disabled with the following line in your Cargo.toml:
//!
//! ```toml
//! async-sqlite = { version = "*", default-features = false }
//! ```

pub use rusqlite;

#[cfg(any(
    not(target_arch = "wasm32"),
    all(feature = "sqlite-wasm-rs", target_os = "unknown")
))]
#[macro_use]
mod client;
mod error;
#[cfg(not(target_arch = "wasm32"))]
mod pool;

#[cfg(all(
    feature = "sqlite-wasm-rs",
    target_arch = "wasm32",
    target_os = "unknown"
))]
pub use client::{report_startup_error, serve, serve_with_shutdown};
#[cfg(any(
    not(target_arch = "wasm32"),
    all(feature = "sqlite-wasm-rs", target_os = "unknown")
))]
pub use client::{Client, Params, Row, Transaction};
#[cfg(not(target_arch = "wasm32"))]
pub use client::{ClientBuilder, JournalMode};
pub use error::Error;
#[cfg(not(target_arch = "wasm32"))]
pub use pool::{Pool, PoolBuilder};
