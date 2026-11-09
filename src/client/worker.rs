//! SQL operations executed in a dedicated browser worker.
//!
//! SQLite connections remain in the worker; callers exchange owned SQL values.

mod caller;
mod server;
mod wire;

pub use caller::Backend;
pub use server::{report_startup_error, serve, serve_with_shutdown};
