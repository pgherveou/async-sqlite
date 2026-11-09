//! Typed database operations executed in a dedicated browser worker.
//!
//! The application supplies the worker entry and its Rust operation handler.
//! Requests use postcard bytes; SQLite connections remain in the database worker.

mod caller;
mod server;
mod wire;

pub use caller::WorkerDatabase;
pub use server::{report_startup_error, serve, serve_with_shutdown};
