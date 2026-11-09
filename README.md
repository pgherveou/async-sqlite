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

let value: String = client.conn(|conn| {
    conn.query_row("SELECT val FROM testing WHERE id=?", [1], |row| row.get(0))
}).await?;

println!("Value is: {value}");
```

Async operations that are queued and then canceled before the worker starts
them are skipped. `ClientBuilder::queue_capacity()` and
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

## Typed database services

The `service` feature adds `DatabaseBackend`, `CallError` and, on native targets,
`NativeDatabase`. An application supplies owned request and response types and a
synchronous Rust handler. `NativeDatabase` runs that handler on an existing
`Client` connection thread. The `worker` feature adds `WorkerDatabase` and the
worker-side `serve` function for `wasm32-unknown-unknown`.

```rust
use async_sqlite::{ClientBuilder, DatabaseBackend, NativeDatabase};

let client = ClientBuilder::new().open().await?;
let database = NativeDatabase::new(client, |connection, value: i64| {
    connection.query_row("SELECT ?1 + 1", [value], |row| row.get::<_, i64>(0))
});
let result = database.call(41).await?;
assert_eq!(result, 42);
database.close().await?;
```

The portable interface is `call(request)`. Each request identifies an operation
whose handler is compiled into both the native application and the database
worker. Keep database-dependent checks and their writes in one handler-owned
transaction. Request handlers can use the full `rusqlite` API locally; closures,
connections and transactions do not travel between browser workers.

The worker adapter serializes requests, responses and operation errors as
postcard bytes, preserving signed 64-bit integers and blobs. Native execution
passes Rust values directly. The caller-side handle is cloneable, `Send + Sync`,
and returns `Send` futures. Browser objects stay on the event loop that connected
the worker. This architecture uses independent workers without shared WASM memory
or cross-origin isolation headers. A long query occupies the database worker;
the caller worker remains able to run other tasks.

### Dispatch and shutdown

- Polling a call admits it. Dropping a waiting future does not cancel an admitted
  operation. This service contract differs from the closure API's cancellation
  of queued async calls.
- `CallError::Operation` contains the handler's error. `NotDispatched` means the
  handler did not run. `OutcomeUnknown` means a reply could not be confirmed;
  a write may already have committed. Neither backend automatically retries.
- Safe write retries need application-defined idempotency records in the same
  transaction, or a way to reconcile the resulting state. Message IDs only
  correlate replies.
- `close().await` stops admission across clones, drains accepted requests and
  closes the database. An accepted close survives a dropped waiting future.
- `NativeDatabase` respects its client's queue capacity. The browser service uses
  an unbounded request queue; applications should limit concurrent submissions.
- `WorkerDatabase::terminate()` explicitly stops the worker. Pending dispatched
  calls fail with an unknown outcome. Worker errors and protocol failures also
  end the session. Applications own timeouts for silent workers and restart policy.
- Native panic recovery depends on unwinding. A WASM build with abort-on-panic
  can terminate its worker; it cannot return a normal operation error for a panic.

### Browser setup

The application supplies the database worker URL and compiles its handler into
that worker's WASM module. The worker initializes its browser filesystem,
connection and schema before awaiting `serve(connection, handler)`. The caller
passes its `web_sys::Worker` to `WorkerDatabase::connect(worker).await` and waits
for readiness. These functions own the worker's message handlers; use a separate
channel for unrelated application messages.

SQLite's default WASM filesystem is in memory. For persistence, initialize a
browser VFS explicitly. The included [ledger example](examples/worker-ledger)
uses OPFS and a single connection. A native WAL writer and reader pool cannot be
assumed to have the same locking behavior in a browser VFS.

Use `serve_with_shutdown(connection, handler, shutdown)` when the VFS retains
resources after SQLite closes. Its callback runs after the connection closes and
before acknowledging shutdown. The example calls its OPFS pool's `pause()` to
release file locks before a replacement worker opens the database. Cleanup failure
fails close and ends the session. Forced termination cannot run this callback.
Recovery after a crash or termination depends on browser resource cleanup. The
example's OPFS pool has one owning worker; cross-tab ownership needs application
coordination.

The `serve` future must stay running. Report initialization failures with
`report_startup_error`, and propagate failures after serving starts to a worker
error event so the caller can settle pending requests. A runtime Rust worker and
a database worker are distinct; a local task inside the runtime worker does not
isolate SQL execution from other runtime tasks.

## Testing database services

```sh
cargo test --features service
cargo test --manifest-path examples/worker-ledger/Cargo.toml
```

The browser test runs the same ledger handler in two dedicated workers. It checks
transactions, refusals, cancellation, exact values, caller responsiveness,
shutdown, OPFS persistence across worker replacement and a lost reply after commit.
The responsiveness check holds SQLite inside a progress callback until the
separate caller worker answers a ping, so it does not depend on query timing.
It requires Node.js, Playwright/Chromium, the Rust WASM target, and wasm-bindgen CLI
0.2.129 matching the example's pinned bindings. Local tool installation:

```sh
rustup target add wasm32-unknown-unknown
cargo install --root .agent/tools wasm-bindgen-cli --version 0.2.129 --locked
npm install --prefix .agent/browser playwright@1.63.0
PLAYWRIGHT_BROWSERS_PATH=.agent/browsers node .agent/browser/node_modules/playwright/cli.js install chromium
NODE_PATH="$PWD/.agent/browser/node_modules" \
  PLAYWRIGHT_BROWSERS_PATH="$PWD/.agent/browsers" \
  WASM_BINDGEN="$PWD/.agent/tools/bin/wasm-bindgen" \
  node scripts/test-worker.cjs
```

Set `CHROMIUM` to use an existing browser executable. The script builds the example
and serves it on a temporary loopback port. Browser storage belongs to its isolated
browser context. Query subscriptions and native reader-pool routing are outside
this initial request/response service.
