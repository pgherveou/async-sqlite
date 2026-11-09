//! SQL operations executed in a dedicated browser worker.
//!
//! SQLite connections remain in the worker; callers exchange owned SQL values.
//!
//! ```text
//! Caller (page or worker)                        Database worker
//! +-----------------------+                     +-----------------------+
//! | Client                |  SQL + parameters   | serve(connection)     |
//! | caller::Backend       | ------------------> | server                |
//! | row mapper            | <------------------ | SQLite                |
//! +-----------------------+    rows / errors     +-----------------------+
//!                         wire / postMessage
//! ```

mod caller;
mod server;
mod wire;

pub use caller::Backend;
pub use server::{report_startup_error, serve, serve_with_shutdown};
