# async-sqlite

This file provides guidance to AI agents when working with code in this repository. Keep this file updated after making changes.

## Project Overview

async-sqlite is a Rust library providing an asynchronous, runtime-agnostic wrapper around SQLite via `rusqlite`. SQL runs on a native background thread or a browser database worker, without depending on a specific runtime.

## Build Commands

```bash
cargo build                    # Build the library
cargo test                     # Run all tests
cargo fmt --all -- --check     # Check formatting
cargo clippy -- -D warnings    # Lint (CI treats all warnings as errors)
```

To run a single test:
```bash
cargo test <test_name>         # e.g., cargo test transaction
```

The minimum supported Rust version is 1.95.0.

## Architecture

The library exposes SQL clients, transactions and native pools:

- **Client** (`client.rs`): Shared SQL methods over a platform backend. `client/native.rs` owns connection setup and the SQL queue. `client/worker/` owns browser messages and lifecycle. `client/sql.rs` converts parameters and executes SQL; one macro defines SQL methods for Client, Transaction and Pool. Client clones share a transaction reservation.

- **Pool** (`pool.rs`): Manages multiple `Client` instances with round-robin selection via an atomic counter. Forwards the shared SQL API and reserves one connection for each transaction. `open_with` initializes every connection before the pool is returned. File-backed and named shared-memory pools default to CPU-count connections; anonymous in-memory pools default to one connection and reject explicit multi-connection configuration. `PoolBuilder::queue_capacity()` applies the same per-client queue bound to every pool connection.

- **Error** (`error.rs`): Non-exhaustive enum wrapping config errors, queue-full errors, `rusqlite::Error`, channel errors, panics, and pragma failures.

Transaction holds an owned reservation across awaits. Commit and rollback consume it; Drop queues rollback before releasing the reservation. Native construction uses ClientBuilder/PoolBuilder; browser construction uses Client::connect and serve.

Admitted SQL survives caller cancellation. Canceled commits may already have committed. Cleanup bypasses native queue limits but stays in the same FIFO; cleanup failure closes the connection. Close waits for transactions, then drains admitted SQL. Keep transport types internal, preserve application connection hooks, and never replay a request after an unknown outcome.

## Features

SQLite Cargo features are pass-through to `rusqlite`. The `rusqlite` dependency has `default-features = false`; async-sqlite explicitly defaults to `bundled` plus rusqlite's `cache` and `ffi-sqlite-wasm-rs` defaults. The library re-exports `rusqlite` for downstream use. `sqlite-wasm-rs` enables the browser backend and its target-specific dependencies.

## Releases

Version 0.8.0 uses rusqlite 0.41. The library exposes rusqlite types in its public API. Treat incompatible rusqlite upgrades as breaking changes; before version 1.0, increment the minor version.

## Testing

Tests in `tests/tests.rs` use a `async_test!` macro that generates two variants of each async test: one running on tokio and one on smol. This ensures runtime compatibility. Tests use `tempfile` for temporary database files.

`tests/database.rs` covers the SQL API and lifecycle. The standalone worker example linked from README owns browser integration tests.
