# async-sqlite

This file provides guidance to AI agents when working with code in this repository. Keep this file updated after making changes.

## Project Overview

async-sqlite is a Rust library providing an asynchronous, runtime-agnostic wrapper around SQLite via `rusqlite`. It works with any async runtime (tokio, smol, etc.) by using background threads internally rather than depending on a specific runtime.

## Build Commands

```bash
cargo build                    # Build the library
cargo test                     # Run all tests
cargo fmt --all -- --check     # Check formatting
cargo clippy -- -D warnings    # Lint (CI treats all warnings as errors)
```

To run a single test:
```bash
cargo test <test_name>         # e.g., cargo test test_blocking_client
```

The minimum supported Rust version is 1.95.0.

## Architecture

The library has three core types in `src/`:

- **Client** (`client.rs`): Shared SQL methods over a platform backend. `client/native.rs` owns the connection thread, queue and native closure methods. `client/worker/` owns browser messages and lifecycle. `client/sql.rs` converts parameters, executes SQL and maps owned rows on the caller. Client is cheaply cloneable.

- **Pool** (`pool.rs`): Manages multiple `Client` instances with round-robin selection via an atomic counter. Provides the same API as Client plus `conn_for_each()` for executing on all connections. File-backed and named shared-memory pools default to CPU-count connections; anonymous in-memory pools default to one connection and reject explicit multi-connection configuration. `PoolBuilder::queue_capacity()` applies the same per-client queue bound to every pool connection.

- **Error** (`error.rs`): Non-exhaustive enum wrapping config errors, queue-full errors, `rusqlite::Error`, channel errors, panics, and pragma failures.

Native Client and Pool retain closure methods (e.g., `conn(|conn| { ... })`) with blocking and async variants.

Admitted SQL requests survive caller cancellation; queued native closure calls retain their cancellation behavior. Keep transport commands internal. Row mappers run on the caller, and each SQL request must finish its transaction. Never replay requests after an unknown outcome.

## Features

SQLite Cargo features are pass-through to `rusqlite`. The `rusqlite` dependency has `default-features = false`; async-sqlite explicitly defaults to `bundled` plus rusqlite's `cache` and `ffi-sqlite-wasm-rs` defaults. The library re-exports `rusqlite` for downstream use. `sqlite-wasm-rs` enables the browser backend and its target-specific dependencies.

## Releases

Version 0.7.0 uses rusqlite 0.41. The library exposes rusqlite types in its public API. Treat incompatible rusqlite upgrades as breaking changes; before version 1.0, increment the minor version.

## Testing

Tests in `tests/tests.rs` use a `async_test!` macro that generates two variants of each async test: one running on tokio and one on smol. This ensures runtime compatibility. Tests use `tempfile` for temporary database files.

`tests/database.rs` covers the SQL API and lifecycle. The standalone worker example linked from README owns browser integration tests.
