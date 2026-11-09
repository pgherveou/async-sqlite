use crate::Error;
use futures_channel::oneshot;
use futures_util::lock::{Mutex, OwnedMutexGuard};
use std::sync::Arc;

#[cfg(not(target_arch = "wasm32"))]
mod native;
#[cfg(not(target_arch = "wasm32"))]
use native::Backend;
#[cfg(not(target_arch = "wasm32"))]
pub use native::{ClientBuilder, JournalMode};

mod sql;
pub use sql::{parameters, row, Operation, Params, Response, Row};

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

macro_rules! sql_methods {
    () => {
        /// Executes one SQL statement and returns the number of changed rows.
        pub fn execute(
            &self,
            sql: &str,
            params: impl $crate::Params,
        ) -> impl core::future::Future<Output = Result<usize, $crate::Error>> + Send + '_ {
            let request = $crate::client::parameters(params).map(|params| {
                $crate::client::Operation::Execute {
                    sql: sql.to_owned(),
                    params,
                }
            });
            async move {
                match self.dispatch(request?).await? {
                    $crate::client::Response::Changes(changes) => Ok(changes),
                    _ => Err($crate::Error::Remote {
                        message: "unexpected execute response".into(),
                    }),
                }
            }
        }

        /// Maps the first result row on the caller, or returns `QueryReturnedNoRows`.
        ///
        /// The mapper receives an owned snapshot rather than a live SQLite statement.
        /// Write statements finish before mapping.
        pub fn query_row<'a, T, F>(
            &'a self,
            sql: &str,
            params: impl $crate::Params,
            map: F,
        ) -> impl core::future::Future<Output = Result<T, $crate::Error>> + Send + 'a
        where
            T: 'a,
            F: FnOnce(&$crate::Row<'_>) -> $crate::rusqlite::Result<T> + Send + 'a,
        {
            let request =
                $crate::client::parameters(params).map(|params| $crate::client::Operation::Query {
                    sql: sql.to_owned(),
                    params,
                    first_only: true,
                });
            async move {
                match self.dispatch(request?).await? {
                    $crate::client::Response::Rows { columns, rows } => {
                        let values = rows
                            .first()
                            .ok_or($crate::rusqlite::Error::QueryReturnedNoRows)?;
                        map(&$crate::client::row(&columns, values)).map_err(Into::into)
                    }
                    _ => Err($crate::Error::Remote {
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
            params: impl $crate::Params,
            mut map: F,
        ) -> impl core::future::Future<Output = Result<Vec<T>, $crate::Error>> + Send + 'a
        where
            T: 'a,
            F: FnMut(&$crate::Row<'_>) -> $crate::rusqlite::Result<T> + Send + 'a,
        {
            let request =
                $crate::client::parameters(params).map(|params| $crate::client::Operation::Query {
                    sql: sql.to_owned(),
                    params,
                    first_only: false,
                });
            async move {
                match self.dispatch(request?).await? {
                    $crate::client::Response::Rows { columns, rows } => rows
                        .iter()
                        .map(|values| {
                            map(&$crate::client::row(&columns, values)).map_err(Into::into)
                        })
                        .collect(),
                    _ => Err($crate::Error::Remote {
                        message: "unexpected query response".into(),
                    }),
                }
            }
        }

        /// Executes SQL statements in order on the connection owner.
        ///
        /// Use a [`Transaction`](crate::Transaction) for atomic work. Outside a
        /// managed transaction, a batch must finish any transaction it starts.
        /// Earlier statements may have committed when a later statement fails.
        pub fn execute_batch(
            &self,
            sql: &str,
        ) -> impl core::future::Future<Output = Result<(), $crate::Error>> + Send + '_ {
            let request = $crate::client::Operation::Batch {
                sql: sql.to_owned(),
            };
            async move {
                match self.dispatch(request).await? {
                    $crate::client::Response::Done => Ok(()),
                    _ => Err($crate::Error::Remote {
                        message: "unexpected batch response".into(),
                    }),
                }
            }
        }
    };
}

mod transaction;

/// A connection whose SQL executes on a native thread or a browser database worker.
///
/// Parameters are copied before a future is returned. Admitted SQL survives
/// cancellation, and row mapping runs on the caller. Clones share one connection;
/// ordinary calls wait while a [`Transaction`] reserves it.
#[derive(Clone)]
pub struct Client {
    backend: Backend,
    reservation: Arc<Mutex<()>>,
}

/// An exclusive transaction on one client or pool connection.
///
/// Dropping this handle queues rollback before releasing the connection. Use
/// [`Self::commit`] or [`Self::rollback`] to end it. Transaction-control SQL in
/// query or batch methods is unsupported. Mapping errors leave it active.
pub struct Transaction {
    client: Client,
    reservation: Option<OwnedMutexGuard<()>>,
}

impl Client {
    fn from_backend(backend: Backend) -> Self {
        Self {
            backend,
            reservation: Arc::new(Mutex::new(())),
        }
    }

    sql_methods!();

    async fn dispatch(&self, operation: Operation) -> Result<Response, Error> {
        dispatch(self, operation).await
    }

    /// Reserves this connection and starts an immediate transaction.
    ///
    /// Other calls on this client wait until the transaction finishes or is
    /// dropped. Canceling acquisition after admission queues rollback.
    pub async fn transaction(&self) -> Result<Transaction, Error> {
        let reservation = self.reservation.clone().lock_owned().await;
        let reply = self.backend.submit(sql::Request::Begin)?;
        let transaction = Transaction {
            client: self.clone(),
            reservation: Some(reservation),
        };
        done(receive(reply).await?)?;
        Ok(transaction)
    }

    /// Waits for active transactions, then drains accepted work and closes SQLite.
    ///
    /// Finish or drop a transaction before awaiting close on the same task.
    /// Dropping an admitted close future does not cancel shutdown. Successful
    /// close can be repeated across cloned clients.
    pub async fn close(&self) -> Result<(), Error> {
        let _reservation = self.reservation.lock().await;
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
        Ok(Self::from_backend(Backend::connect(worker).await?))
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

async fn receive(reply: oneshot::Receiver<Result<Response, Error>>) -> Result<Response, Error> {
    reply.await.map_err(|error| Error::OutcomeUnknown {
        message: error.to_string(),
    })?
}

fn done(response: Response) -> Result<(), Error> {
    match response {
        Response::Done => Ok(()),
        _ => Err(Error::Remote {
            message: "unexpected transaction response".into(),
        }),
    }
}

/// Admits ordinary SQL while no transaction reserves the chosen connection.
pub async fn dispatch(client: &Client, operation: Operation) -> Result<Response, Error> {
    let reply = {
        let _reservation = client.reservation.lock().await;
        client.backend.submit(sql::Request::Sql {
            operation,
            in_transaction: false,
        })?
    };
    receive(reply).await
}
