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

- **Client** (`client.rs`): Wraps a single SQLite connection. Spawns a background `std::thread` that receives commands (closures) via a `crossbeam_channel`. Results are returned through `futures_channel::oneshot`. Queued async commands are skipped if their reply channel is already canceled, and `ClientBuilder::queue_capacity()` can bound the worker queue. This design makes it runtime-agnostic. Client is cheaply cloneable.

- **Pool** (`pool.rs`): Manages multiple `Client` instances with round-robin selection via an atomic counter. Provides the same API as Client plus `conn_for_each()` for executing on all connections. File-backed and named shared-memory pools default to CPU-count connections; anonymous in-memory pools default to one connection and reject explicit multi-connection configuration. `PoolBuilder::queue_capacity()` applies the same per-client queue bound to every pool connection.

- **Error** (`error.rs`): Non-exhaustive enum wrapping config errors, queue-full errors, `rusqlite::Error`, channel errors, panics, and pragma failures.

The Client and Pool APIs use closures (e.g., `conn(|conn| { ... })`) to avoid lifetime issues with the cross-thread boundary. Both blocking and async variants exist for these operations.

The optional typed service has a separate ownership contract: once a request is
admitted, dropping its caller does not cancel it. `src/database.rs` defines
`DatabaseBackend` and `CallError`; `src/database/native.rs` dispatches owned work
through the existing `Client`. Keep the closure API's cancellation semantics
unchanged. Native close and request admission must be serialized across clones.

`src/worker/` implements the browser caller bridge, worker endpoint and postcard
wire envelopes. The `WorkerDatabase` handle owns channels and admission state;
JavaScript objects remain on their owning event loop. The database worker runs
the application's compiled Rust handler and owns its connection. A lost reply
can follow a committed transaction, so report an unknown outcome and never
automatically replay requests.

## Features

SQLite Cargo features are pass-through to `rusqlite`. The `rusqlite` dependency has `default-features = false`; async-sqlite explicitly defaults to `bundled` plus rusqlite's `cache` and `ffi-sqlite-wasm-rs` defaults. The library re-exports `rusqlite` for downstream use. `service` exposes the typed API and native adapter; `worker` also exposes the browser backend and enables its target-specific transport and SQLite host bindings.

## Releases

Version 0.7.0 uses rusqlite 0.41. The library exposes rusqlite types in its public API. Treat incompatible rusqlite upgrades as breaking changes; before version 1.0, increment the minor version.

## Testing

Tests in `tests/tests.rs` use a `async_test!` macro that generates two variants of each async test: one running on tokio and one on smol. This ensures runtime compatibility. Tests use `tempfile` for temporary database files.

`tests/database.rs` verifies service dispatch, transaction and shutdown contracts.
`examples/worker-ledger` shares a Rust handler between native tests and real
browser workers. `scripts/test-worker.cjs` checks worker isolation, OPFS durability
and lost replies. Run service tests with `cargo test --features service`; follow
the [README](README.md#testing-database-services) for browser tooling and commands.
Keep fault injection in the example harness, never in the transport library.
