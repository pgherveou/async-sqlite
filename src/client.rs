use crate::Error;
use core::future::Future;

#[cfg(not(target_arch = "wasm32"))]
mod native;
#[cfg(not(target_arch = "wasm32"))]
use native::Backend;
#[cfg(not(target_arch = "wasm32"))]
pub use native::{ClientBuilder, JournalMode};

mod sql;
pub use sql::{Params, Row};

#[cfg(all(
    feature = "sqlite-wasm-rs",
    target_arch = "wasm32",
    target_os = "unknown"
))]
mod worker;
#[cfg(all(
    feature = "sqlite-wasm-rs",
    target_arch = "wasm32",
    target_os = "unknown"
))]
use worker::Backend;
#[cfg(all(
    feature = "sqlite-wasm-rs",
    target_arch = "wasm32",
    target_os = "unknown"
))]
pub use worker::{report_startup_error, serve, serve_with_shutdown};

/// A connection whose SQL executes on a native thread or a browser database worker.
///
/// SQL methods copy parameters before returning their future. Polling admits the
/// operation; dropping its future afterward does not cancel it. Row mappers run
/// on the caller; mapping errors do not roll back SQL. SQL methods require a
/// connection outside a transaction. Native closures retain their own behavior.
#[derive(Clone)]
pub struct Client {
    backend: Backend,
}

impl Client {
    /// Executes one SQL statement and returns the number of changed rows.
    pub fn execute(
        &self,
        sql: &str,
        params: impl Params,
    ) -> impl Future<Output = Result<usize, Error>> + Send + '_ {
        let request = sql::parameters(params).map(|params| sql::Request::Execute {
            sql: sql.to_owned(),
            params,
        });
        async move {
            match self.backend.dispatch(request?).await? {
                sql::Response::Changes(changes) => Ok(changes),
                _ => Err(Error::Remote {
                    message: "unexpected execute response".into(),
                }),
            }
        }
    }

    /// Maps the first result row on the caller, or returns `QueryReturnedNoRows`.
    ///
    /// The mapper receives an owned snapshot rather than a live SQLite statement.
    /// Write statements finish before returning so commit errors propagate.
    pub fn query_row<'a, T, F>(
        &'a self,
        sql: &str,
        params: impl Params,
        map: F,
    ) -> impl Future<Output = Result<T, Error>> + Send + 'a
    where
        T: 'a,
        F: FnOnce(&Row<'_>) -> rusqlite::Result<T> + Send + 'a,
    {
        let request = sql::parameters(params).map(|params| sql::Request::Query {
            sql: sql.to_owned(),
            params,
            first_only: true,
        });
        async move {
            match self.backend.dispatch(request?).await? {
                sql::Response::Rows { columns, rows } => {
                    let values = rows.first().ok_or(rusqlite::Error::QueryReturnedNoRows)?;
                    map(&sql::row(&columns, values)).map_err(Into::into)
                }
                _ => Err(Error::Remote {
                    message: "unexpected query response".into(),
                }),
            }
        }
    }

    /// Collects result rows and maps them on the caller.
    ///
    /// All rows are buffered before mapping; this method does not stream results.
    pub fn query_map<'a, T, F>(
        &'a self,
        sql: &str,
        params: impl Params,
        mut map: F,
    ) -> impl Future<Output = Result<Vec<T>, Error>> + Send + 'a
    where
        T: 'a,
        F: FnMut(&Row<'_>) -> rusqlite::Result<T> + Send + 'a,
    {
        let request = sql::parameters(params).map(|params| sql::Request::Query {
            sql: sql.to_owned(),
            params,
            first_only: false,
        });
        async move {
            match self.backend.dispatch(request?).await? {
                sql::Response::Rows { columns, rows } => rows
                    .iter()
                    .map(|values| map(&sql::row(&columns, values)).map_err(Into::into))
                    .collect(),
                _ => Err(Error::Remote {
                    message: "unexpected query response".into(),
                }),
            }
        }
    }

    /// Executes SQL statements in order on the connection owner.
    ///
    /// Use a complete `BEGIN`/`COMMIT` transaction for atomic batches. A transaction
    /// left open or interrupted by an error is rolled back. Statements outside a
    /// transaction may already have committed when a later statement fails.
    pub fn execute_batch(&self, sql: &str) -> impl Future<Output = Result<(), Error>> + Send + '_ {
        let request = sql::Request::Batch {
            sql: sql.to_owned(),
        };
        async move {
            match self.backend.dispatch(request).await? {
                sql::Response::Done => Ok(()),
                _ => Err(Error::Remote {
                    message: "unexpected batch response".into(),
                }),
            }
        }
    }

    /// Stops SQL admission, drains accepted work and closes the connection.
    ///
    /// Closing is shared by cloned clients. Dropping a waiting future does not
    /// cancel an accepted close. Successful close can be repeated.
    pub async fn close(&self) -> Result<(), Error> {
        self.backend.close().await
    }

    /// Takes ownership of a browser worker and waits for its database readiness.
    ///
    /// The worker must initialize its connection and await `serve`. A silent
    /// worker needs an application-owned startup timeout.
    #[cfg(all(
        feature = "sqlite-wasm-rs",
        target_arch = "wasm32",
        target_os = "unknown"
    ))]
    pub async fn connect(worker: web_sys::Worker) -> Result<Self, Error> {
        Ok(Self {
            backend: Backend::connect(worker).await?,
        })
    }

    /// Terminates the browser database worker without promising rollback.
    ///
    /// Pending dispatched requests can fail with an unknown outcome.
    #[cfg(all(
        feature = "sqlite-wasm-rs",
        target_arch = "wasm32",
        target_os = "unknown"
    ))]
    pub fn terminate(&self) {
        self.backend.terminate();
    }
}
