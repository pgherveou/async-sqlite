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

use crossbeam_channel::{unbounded, Receiver, Sender};
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
    /// only when the worker is idle and no command is waiting. Transaction
    /// rollback on drop bypasses this bound so cleanup always reaches the worker.
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
        self.open_with(|_| Ok(())).await
    }

    /// Initializes the connection on its worker thread before returning the client.
    ///
    /// The initializer must leave no transaction open. Its errors and panics fail
    /// opening the client; accepted SQL uses the shared asynchronous methods.
    pub async fn open_with<F>(self, initialize: F) -> Result<Client, Error>
    where
        F: FnOnce(&mut Connection) -> Result<(), Error> + Send + 'static,
    {
        Backend::open(self, initialize).await
    }
}

enum Command {
    Sql {
        request: Request,
        reply: Option<oneshot::Sender<Result<Response, Error>>>,
    },
    Shutdown,
}

enum Admission {
    Open { worker_idle: bool },
    Closing(Vec<oneshot::Sender<Result<(), String>>>),
    Closed,
}

fn finish_close(admission: &Mutex<Admission>, result: Result<(), String>) {
    let replies = {
        let mut admission = admission.lock().expect("database admission poisoned");
        let next = if result.is_ok() {
            Admission::Closed
        } else {
            Admission::Open { worker_idle: false }
        };
        match core::mem::replace(&mut *admission, next) {
            Admission::Closing(replies) => replies,
            _ => unreachable!("database close was not admitted"),
        }
    };
    for reply in replies {
        _ = reply.send(result.clone());
    }
}

fn panic_message(panic: &(dyn std::any::Any + Send)) -> String {
    if let Some(message) = panic.downcast_ref::<&'static str>() {
        (*message).to_owned()
    } else if let Some(message) = panic.downcast_ref::<String>() {
        message.clone()
    } else {
        "database initialization panicked".to_owned()
    }
}

/// Owns the native connection queue and its shutdown state.
#[derive(Clone)]
pub struct Backend {
    conn_tx: Sender<Command>,
    admission: Arc<Mutex<Admission>>,
    queue_capacity: Option<usize>,
}

impl Backend {
    async fn open<F>(builder: ClientBuilder, initialize: F) -> Result<Client, Error>
    where
        F: FnOnce(&mut Connection) -> Result<(), Error> + Send + 'static,
    {
        let (ready, result) = oneshot::channel();
        thread::spawn(move || {
            let queue_capacity = builder.queue_capacity;
            let opened = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                let mut connection = Self::create_conn(builder)?;
                initialize(&mut connection)?;
                if !connection.is_autocommit() {
                    return Err(rusqlite::Error::InvalidQuery.into());
                }
                Ok(connection)
            }))
            .unwrap_or_else(|panic| {
                Err(Error::Panic {
                    message: panic_message(&*panic),
                })
            });
            let connection = match opened {
                Ok(connection) => connection,
                Err(error) => {
                    _ = ready.send(Err(error));
                    return;
                }
            };
            let (conn_tx, conn_rx) = unbounded();
            let admission = Arc::new(Mutex::new(Admission::Open { worker_idle: true }));
            let backend = Self {
                conn_tx,
                admission: admission.clone(),
                queue_capacity,
            };
            _ = ready.send(Ok(Client::from_backend(backend)));
            Self::run(connection, conn_rx, &admission);
        });
        result.await?
    }

    fn run(mut connection: Connection, commands: Receiver<Command>, admission: &Mutex<Admission>) {
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(move || loop {
            {
                let mut admission = admission.lock().expect("database admission poisoned");
                if let Admission::Open { worker_idle } = &mut *admission {
                    *worker_idle = commands.is_empty();
                }
            }
            let Ok(command) = commands.recv() else {
                return;
            };
            match command {
                Command::Sql { request, reply } => {
                    let requires_autocommit = matches!(
                        &request,
                        Request::Rollback
                            | Request::Sql {
                                in_transaction: false,
                                ..
                            }
                    );
                    let result = sql::handle(&mut connection, request).map_err(Error::from);
                    let close = (requires_autocommit && !connection.is_autocommit())
                        || (reply.is_none() && result.is_err());
                    if let Some(reply) = reply {
                        _ = reply.send(result);
                    }
                    if close {
                        return;
                    }
                }
                Command::Shutdown => match connection.close() {
                    Ok(()) => {
                        finish_close(admission, Ok(()));
                        return;
                    }
                    Err((retained, error)) => {
                        connection = retained;
                        finish_close(admission, Err(error.to_string()));
                    }
                },
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

    fn queue_full(&self, worker_idle: bool) -> bool {
        match self.queue_capacity {
            Some(0) => !worker_idle || !self.conn_tx.is_empty(),
            Some(capacity) => self.conn_tx.len() >= capacity,
            None => false,
        }
    }

    /// Admits SQL synchronously; dropping the receiver does not cancel execution.
    pub fn submit(
        &self,
        request: Request,
    ) -> Result<oneshot::Receiver<Result<Response, Error>>, Error> {
        let mut admission = self.admission.lock().expect("database admission poisoned");
        let Admission::Open { worker_idle } = &mut *admission else {
            return Err(Error::Closed);
        };
        if self.queue_full(*worker_idle) {
            return Err(Error::QueueFull);
        }
        *worker_idle = false;
        let (reply, result) = oneshot::channel();
        if self
            .conn_tx
            .send(Command::Sql {
                request,
                reply: Some(reply),
            })
            .is_err()
        {
            *admission = Admission::Closed;
            return Err(Error::Closed);
        }
        Ok(result)
    }

    /// Queues transaction cleanup in order even when the ordinary queue is full.
    ///
    /// Cleanup failure closes the connection before later requests can execute.
    pub fn submit_cleanup(&self, request: Request) {
        let mut admission = self.admission.lock().expect("database admission poisoned");
        if let Admission::Open { worker_idle } = &mut *admission {
            *worker_idle = false;
            if self
                .conn_tx
                .send(Command::Sql {
                    request,
                    reply: None,
                })
                .is_err()
            {
                *admission = Admission::Closed;
            }
        }
    }

    /// Drains accepted work and shares close completion across cloned clients.
    pub async fn close(&self) -> Result<(), Error> {
        let (complete, reply) = oneshot::channel();
        {
            let mut admission = self.admission.lock().expect("database admission poisoned");
            match &mut *admission {
                Admission::Open { worker_idle } => {
                    if self.queue_full(*worker_idle) {
                        return Err(Error::QueueFull);
                    }
                    *worker_idle = false;
                    if self.conn_tx.send(Command::Shutdown).is_err() {
                        *admission = Admission::Closed;
                        return Ok(());
                    }
                    *admission = Admission::Closing(vec![complete]);
                }
                Admission::Closing(replies) => replies.push(complete),
                Admission::Closed => return Ok(()),
            }
        }
        reply
            .await
            .map_err(|error| Error::OutcomeUnknown {
                message: error.to_string(),
            })?
            .map_err(|message| Error::OutcomeUnknown { message })
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
