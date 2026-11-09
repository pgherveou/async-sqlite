//! Typed database operations dispatched to a connection owner.

use core::{convert::Infallible, fmt};

use futures_util::future::BoxFuture;

#[cfg(not(target_arch = "wasm32"))]
mod native;
#[cfg(not(target_arch = "wasm32"))]
pub use native::NativeDatabase;

/// Executes application-defined requests in their database's owning context.
///
/// Calls are admitted when their futures are polled. Once dispatched, dropping
/// a future stops waiting for its result but does not cancel the operation.
/// Each request must contain a complete transaction when atomicity is needed.
pub trait DatabaseBackend: Send + Sync {
    /// Owned input to one database operation.
    type Request: Send + 'static;
    /// Owned result returned by the application handler.
    type Response: Send + 'static;
    /// Application-defined failure returned by the handler.
    type Error: Send + 'static;

    /// Dispatches one operation and waits for its result.
    ///
    /// An unknown outcome must be reconciled before retrying a write. No
    /// backend automatically retries an operation.
    fn call(
        &self,
        request: Self::Request,
    ) -> BoxFuture<'_, Result<Self::Response, CallError<Self::Error>>>;

    /// Stops admission, finishes accepted requests and closes the database.
    ///
    /// Closing is shared by all cloned handles and is idempotent after success.
    /// Dropping the waiting future does not cancel an accepted close.
    fn close(&self) -> BoxFuture<'_, Result<(), CallError<Infallible>>>;
}

/// Distinguishes an operation result from failure to obtain that result.
///
/// An operation may have committed when its outcome is unknown. Applications
/// must reconcile state or use transactionally recorded idempotency keys
/// before retrying such an operation.
#[derive(Debug, PartialEq)]
pub enum CallError<E> {
    /// The handler completed and returned its application-defined error.
    Operation(E),
    /// The request was not dispatched and its handler did not run.
    NotDispatched(String),
    /// The request was dispatched but its result could not be confirmed.
    OutcomeUnknown(String),
}

impl<E: fmt::Display> fmt::Display for CallError<E> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Operation(error) => write!(formatter, "database operation failed: {error}"),
            Self::NotDispatched(reason) => write!(formatter, "request not dispatched: {reason}"),
            Self::OutcomeUnknown(reason) => write!(formatter, "database outcome unknown: {reason}"),
        }
    }
}

impl<E: std::error::Error + 'static> std::error::Error for CallError<E> {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Operation(error) => Some(error),
            Self::NotDispatched(_) | Self::OutcomeUnknown(_) => None,
        }
    }
}
