use std::{
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
    thread,
};

use super::{
    sql::{self, Request, Response},
    Client,
};
use crate::Error;

use crossbeam_channel::{bounded, unbounded, Receiver, Sender, TrySendError};
use futures_channel::oneshot;
use rusqlite::{Connection, OpenFlags};

/// A `ClientBuilder` can be used to create a [`Client`] with custom
/// configuration.
///
/// For more information on creating a sqlite connection, see the
/// [rusqlite docs](rusqlite::Connection::open()).
///
/// # Examples
///
/// ```rust
/// # use async_sqlite::ClientBuilder;
/// # async fn run() -> Result<(), async_sqlite::Error> {
/// let client = ClientBuilder::new().path("path/to/db.sqlite3").open().await?;
///
/// // ...
///
/// client.close().await?;
/// # Ok(())
/// # }
/// ```
#[derive(Clone, Debug, Default)]
pub struct ClientBuilder {
    pub(crate) path: Option<PathBuf>,
    pub(crate) flags: OpenFlags,
    pub(crate) journal_mode: Option<JournalMode>,
    pub(crate) vfs: Option<String>,
    pub(crate) queue_capacity: Option<usize>,
}

impl ClientBuilder {
    /// Returns a new [`ClientBuilder`] with the default settings.
    pub fn new() -> Self {
        Self::default()
    }

    /// Specify the path of the sqlite3 database to open.
    ///
    /// By default, an in-memory database is used.
    pub fn path<P: AsRef<Path>>(mut self, path: P) -> Self {
        self.path = Some(path.as_ref().into());
        self
    }

    /// Specify the [`OpenFlags`] to use when opening a new connection.
    ///
    /// By default, [`OpenFlags::default()`] is used.
    pub fn flags(mut self, flags: OpenFlags) -> Self {
        self.flags = flags;
        self
    }

    /// Specify the [`JournalMode`] to set when opening a new connection.
    ///
    /// By default, no `journal_mode` is explicity set.
    pub fn journal_mode(mut self, journal_mode: JournalMode) -> Self {
        self.journal_mode = Some(journal_mode);
        self
    }

    /// Specify the name of the [vfs](https://www.sqlite.org/vfs.html) to use.
    pub fn vfs(mut self, vfs: &str) -> Self {
        self.vfs = Some(vfs.to_owned());
        self
    }

    /// Limit the number of commands that may wait in the worker queue.
    ///
    /// By default, the queue is unbounded. If a capacity is configured, calls
    /// return [`Error::QueueFull`] when that many commands are already waiting
    /// for the worker thread. A capacity of `0` allows a command to be accepted
    /// only when the worker is ready to receive it immediately.
    pub fn queue_capacity(mut self, queue_capacity: usize) -> Self {
        self.queue_capacity = Some(queue_capacity);
        self
    }

    /// Returns a new [`Client`] that uses the `ClientBuilder` configuration.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use async_sqlite::ClientBuilder;
    /// # async fn run() -> Result<(), async_sqlite::Error> {
    /// let client = ClientBuilder::new().open().await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn open(self) -> Result<Client, Error> {
        Backend::open_async(self).await
    }

    /// Returns a new [`Client`] that uses the `ClientBuilder` configuration,
    /// blocking the current thread.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use async_sqlite::ClientBuilder;
    /// # fn run() -> Result<(), async_sqlite::Error> {
    /// let client = ClientBuilder::new().open_blocking()?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn open_blocking(self) -> Result<Client, Error> {
        Backend::open_blocking(self)
    }
}

enum Command {
    Func(Box<dyn FnOnce(&mut Connection) + Send>),
    Shutdown,
}

fn run_catching<F, T>(conn: &mut Connection, func: F) -> Result<T, Error>
where
    F: FnOnce(&mut Connection) -> Result<T, rusqlite::Error>,
{
    run_catching_and_then(conn, |conn| func(conn).map_err(Error::from))
}

fn run_catching_and_then<F, T, E>(conn: &mut Connection, func: F) -> Result<T, E>
where
    F: FnOnce(&mut Connection) -> Result<T, E>,
    E: From<Error>,
{
    match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| func(conn))) {
        Ok(res) => res,
        Err(p) => {
            rollback_if_needed(conn);
            Err(E::from(Error::Panic {
                message: panic_message(&*p),
            }))
        }
    }
}

fn rollback_if_needed(conn: &mut Connection) {
    if !conn.is_autocommit() {
        let _ = conn.execute_batch("ROLLBACK");
    }
}

fn panic_message(p: &(dyn std::any::Any + Send)) -> String {
    if let Some(s) = p.downcast_ref::<&'static str>() {
        (*s).to_owned()
    } else if let Some(s) = p.downcast_ref::<String>() {
        s.clone()
    } else {
        "panic".to_owned()
    }
}

type CloseReply = Box<dyn FnOnce(Result<(), Error>) + Send>;

enum Admission {
    Open,
    Closing(Vec<CloseReply>),
    Closed,
}

fn finish_close(admission: &Mutex<Admission>, result: rusqlite::Result<()>) {
    let replies = {
        let mut admission = admission.lock().expect("database admission poisoned");
        let next = if result.is_ok() {
            Admission::Closed
        } else {
            Admission::Open
        };
        match core::mem::replace(&mut *admission, next) {
            Admission::Closing(replies) => replies,
            _ => unreachable!("database close was not admitted"),
        }
    };
    for reply in replies {
        reply(match &result {
            Ok(()) => Ok(()),
            Err(rusqlite::Error::SqliteFailure(error, message)) => {
                Err(rusqlite::Error::SqliteFailure(*error, message.clone()).into())
            }
            Err(error) => Err(Error::OutcomeUnknown {
                message: error.to_string(),
            }),
        });
    }
}

/// Owns the native connection queue and its shutdown state.
#[derive(Clone)]
pub struct Backend {
    conn_tx: Sender<Command>,
    admission: Arc<Mutex<Admission>>,
}

impl Backend {
    async fn open_async(builder: ClientBuilder) -> Result<Client, Error> {
        let (open_tx, open_rx) = oneshot::channel();
        Self::open(builder, |res| {
            _ = open_tx.send(res);
        });
        open_rx.await?
    }

    fn open_blocking(builder: ClientBuilder) -> Result<Client, Error> {
        let (conn_tx, conn_rx) = bounded(1);
        Self::open(builder, move |res| {
            _ = conn_tx.send(res);
        });
        conn_rx.recv()?
    }

    fn open<F>(builder: ClientBuilder, func: F)
    where
        F: FnOnce(Result<Client, Error>) + Send + 'static,
    {
        thread::spawn(move || {
            let (conn_tx, conn_rx) = match builder.queue_capacity {
                Some(queue_capacity) => bounded(queue_capacity),
                None => unbounded(),
            };

            let mut conn = match Self::create_conn(builder) {
                Ok(conn) => conn,
                Err(err) => {
                    func(Err(err));
                    return;
                }
            };

            let admission = Arc::new(Mutex::new(Admission::Open));
            let client = Client {
                backend: Self {
                    conn_tx,
                    admission: admission.clone(),
                },
            };
            func(Ok(client));

            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                for cmd in conn_rx {
                    match cmd {
                        Command::Func(func) => func(&mut conn),
                        Command::Shutdown => match conn.close() {
                            Ok(()) => {
                                finish_close(&admission, Ok(()));
                                return;
                            }
                            Err((connection, error)) => {
                                conn = connection;
                                finish_close(&admission, Err(error));
                            }
                        },
                    }
                }
            }));
            let previous = {
                let mut admission = admission.lock().expect("database admission poisoned");
                core::mem::replace(&mut *admission, Admission::Closed)
            };
            drop(previous);
            if let Err(panic) = result {
                std::panic::resume_unwind(panic);
            }
        });
    }

    fn create_conn(mut builder: ClientBuilder) -> Result<Connection, Error> {
        let path = builder.path.take().unwrap_or_else(|| ":memory:".into());
        let conn = if let Some(vfs) = builder.vfs.take() {
            Connection::open_with_flags_and_vfs(path, builder.flags, vfs.as_str())?
        } else {
            Connection::open_with_flags(path, builder.flags)?
        };

        if let Some(journal_mode) = builder.journal_mode.take() {
            let val = journal_mode.as_str();
            let out: String =
                conn.pragma_update_and_check(None, "journal_mode", val, |row| row.get(0))?;
            if !out.eq_ignore_ascii_case(val) {
                return Err(Error::PragmaUpdate {
                    name: "journal_mode",
                    exp: val,
                    got: out,
                });
            }
        }

        Ok(conn)
    }

    fn enqueue_async<F, T, E>(
        &self,
        func: F,
    ) -> Result<oneshot::Receiver<Result<T, E>>, TrySendError<Command>>
    where
        F: FnOnce(&mut Connection) -> Result<T, E> + Send + 'static,
        T: Send + 'static,
        E: Send + 'static,
    {
        let (tx, rx) = oneshot::channel();
        self.conn_tx.try_send(Command::Func(Box::new(move |conn| {
            if !tx.is_canceled() {
                _ = tx.send(func(conn));
            }
        })))?;
        Ok(rx)
    }

    fn enqueue_blocking<F, T, E>(
        &self,
        func: F,
    ) -> Result<Receiver<Result<T, E>>, TrySendError<Command>>
    where
        F: FnOnce(&mut Connection) -> Result<T, E> + Send + 'static,
        T: Send + 'static,
        E: Send + 'static,
    {
        let (tx, rx) = bounded(1);
        self.conn_tx.try_send(Command::Func(Box::new(move |conn| {
            _ = tx.send(func(conn));
        })))?;
        Ok(rx)
    }

    /// Dispatches SQL without canceling accepted work when its caller stops waiting.
    pub async fn dispatch(&self, request: Request) -> Result<Response, Error> {
        let reply = {
            let admission = self.admission.lock().expect("database admission poisoned");
            if !matches!(*admission, Admission::Open) {
                return Err(Error::Closed);
            }
            let (reply, result) = oneshot::channel();
            self.conn_tx.try_send(Command::Func(Box::new(
                move |connection: &mut Connection| {
                    let _ = reply.send(run_catching(connection, |connection| {
                        sql::handle(connection, request)
                    }));
                },
            )))?;
            result
        };
        match reply.await {
            Ok(Err(Error::Panic { message })) => Err(Error::OutcomeUnknown { message }),
            Ok(result) => result,
            Err(error) => Err(Error::OutcomeUnknown {
                message: error.to_string(),
            }),
        }
    }

    fn begin_close(&self, reply: CloseReply) -> Result<(), Error> {
        let mut admission = self.admission.lock().expect("database admission poisoned");
        match &mut *admission {
            Admission::Open => match self.conn_tx.try_send(Command::Shutdown) {
                Ok(()) => {
                    *admission = Admission::Closing(vec![reply]);
                    return Ok(());
                }
                Err(TrySendError::Disconnected(_)) => *admission = Admission::Closed,
                Err(error) => return Err(error.into()),
            },
            Admission::Closing(replies) => {
                replies.push(reply);
                return Ok(());
            }
            Admission::Closed => {}
        }
        drop(admission);
        reply(Ok(()));
        Ok(())
    }

    /// Drains accepted work and shares close completion across cloned clients.
    pub async fn close(&self) -> Result<(), Error> {
        let (complete, reply) = oneshot::channel();
        self.begin_close(Box::new(move |result| {
            _ = complete.send(result);
        }))?;
        reply
            .await
            .map_err(|error| Error::OutcomeUnknown {
                message: error.to_string(),
            })?
            .map_err(|error| Error::OutcomeUnknown {
                message: error.to_string(),
            })
    }
}

impl Client {
    /// Invokes the provided function with a [`rusqlite::Connection`].
    pub async fn conn<F, T>(&self, func: F) -> Result<T, Error>
    where
        F: FnOnce(&Connection) -> Result<T, rusqlite::Error> + Send + 'static,
        T: Send + 'static,
    {
        let rx = self
            .backend
            .enqueue_async(move |conn| run_catching(conn, |conn| func(conn)))
            .map_err(Error::from)?;
        rx.await?
    }

    /// Invokes the provided function with a mutable [`rusqlite::Connection`].
    pub async fn conn_mut<F, T>(&self, func: F) -> Result<T, Error>
    where
        F: FnOnce(&mut Connection) -> Result<T, rusqlite::Error> + Send + 'static,
        T: Send + 'static,
    {
        let rx = self
            .backend
            .enqueue_async(move |conn| run_catching(conn, func))
            .map_err(Error::from)?;
        rx.await?
    }

    /// Invokes the provided function with a [`rusqlite::Connection`].
    ///
    /// Maps the result error type to a custom error; designed to be
    /// used in conjunction with [`query_and_then`](https://docs.rs/rusqlite/latest/rusqlite/struct.CachedStatement.html#method.query_and_then).
    pub async fn conn_and_then<F, T, E>(&self, func: F) -> Result<T, E>
    where
        F: FnOnce(&Connection) -> Result<T, E> + Send + 'static,
        T: Send + 'static,
        E: From<rusqlite::Error> + From<Error> + Send + 'static,
    {
        let rx = self
            .backend
            .enqueue_async(move |conn| run_catching_and_then(conn, |conn| func(conn)))
            .map_err(Error::from)?;
        rx.await.map_err(Error::from)?
    }

    /// Invokes the provided function with a mutable [`rusqlite::Connection`].
    ///
    /// Maps the result error type to a custom error; designed to be
    /// used in conjunction with [`query_and_then`](https://docs.rs/rusqlite/latest/rusqlite/struct.CachedStatement.html#method.query_and_then).
    pub async fn conn_mut_and_then<F, T, E>(&self, func: F) -> Result<T, E>
    where
        F: FnOnce(&mut Connection) -> Result<T, E> + Send + 'static,
        T: Send + 'static,
        E: From<rusqlite::Error> + From<Error> + Send + 'static,
    {
        let rx = self
            .backend
            .enqueue_async(move |conn| run_catching_and_then(conn, func))
            .map_err(Error::from)?;
        rx.await.map_err(Error::from)?
    }

    /// Invokes the provided function with a [`rusqlite::Connection`], blocking
    /// the current thread until completion.
    pub fn conn_blocking<F, T>(&self, func: F) -> Result<T, Error>
    where
        F: FnOnce(&Connection) -> Result<T, rusqlite::Error> + Send + 'static,
        T: Send + 'static,
    {
        let rx = self
            .backend
            .enqueue_blocking(move |conn| run_catching(conn, |conn| func(conn)))
            .map_err(Error::from)?;
        rx.recv()?
    }

    /// Invokes the provided function with a mutable [`rusqlite::Connection`],
    /// blocking the current thread until completion.
    pub fn conn_mut_blocking<F, T>(&self, func: F) -> Result<T, Error>
    where
        F: FnOnce(&mut Connection) -> Result<T, rusqlite::Error> + Send + 'static,
        T: Send + 'static,
    {
        let rx = self
            .backend
            .enqueue_blocking(move |conn| run_catching(conn, func))
            .map_err(Error::from)?;
        rx.recv()?
    }

    /// Invokes the provided function with a [`rusqlite::Connection`],
    /// blocking the current thread until completion.
    ///
    /// Maps the result error type to a custom error; designed to be
    /// used in conjunction with [`query_and_then`](https://docs.rs/rusqlite/latest/rusqlite/struct.CachedStatement.html#method.query_and_then).
    pub fn conn_and_then_blocking<F, T, E>(&self, func: F) -> Result<T, E>
    where
        F: FnOnce(&Connection) -> Result<T, E> + Send + 'static,
        T: Send + 'static,
        E: From<rusqlite::Error> + From<Error> + Send + 'static,
    {
        let rx = self
            .backend
            .enqueue_blocking(move |conn| run_catching_and_then(conn, |conn| func(conn)))
            .map_err(Error::from)?;
        rx.recv().map_err(Error::from)?
    }

    /// Invokes the provided function with a mutable [`rusqlite::Connection`],
    /// blocking the current thread until completion.
    ///
    /// Maps the result error type to a custom error; designed to be
    /// used in conjunction with [`query_and_then`](https://docs.rs/rusqlite/latest/rusqlite/struct.CachedStatement.html#method.query_and_then).
    pub fn conn_mut_and_then_blocking<F, T, E>(&self, func: F) -> Result<T, E>
    where
        F: FnOnce(&mut Connection) -> Result<T, E> + Send + 'static,
        T: Send + 'static,
        E: From<rusqlite::Error> + From<Error> + Send + 'static,
    {
        let rx = self
            .backend
            .enqueue_blocking(move |conn| run_catching_and_then(conn, func))
            .map_err(Error::from)?;
        rx.recv().map_err(Error::from)?
    }

    /// Stops SQL admission, drains accepted work and closes the connection,
    /// blocking until completion.
    ///
    /// Concurrent close calls share completion. A failed SQLite close leaves
    /// the connection available for retry.
    pub fn close_blocking(&self) -> Result<(), Error> {
        let (tx, rx) = bounded(1);
        self.backend.begin_close(Box::new(move |result| {
            _ = tx.send(result);
        }))?;
        // If receiving fails, the connection is already closed.
        rx.recv().unwrap_or(Ok(()))
    }
}

/// The possible sqlite journal modes.
///
/// For more information, please see the [sqlite docs](https://www.sqlite.org/pragma.html#pragma_journal_mode).
#[derive(Clone, Copy, Debug)]
pub enum JournalMode {
    Delete,
    Truncate,
    Persist,
    Memory,
    Wal,
    Off,
}

impl JournalMode {
    /// Returns the appropriate string representation of the journal mode.
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Delete => "DELETE",
            Self::Truncate => "TRUNCATE",
            Self::Persist => "PERSIST",
            Self::Memory => "MEMORY",
            Self::Wal => "WAL",
            Self::Off => "OFF",
        }
    }
}

#[cfg(test)]
mod tests;
