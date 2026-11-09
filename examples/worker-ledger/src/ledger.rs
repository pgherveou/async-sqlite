use async_sqlite::rusqlite::{self, Connection};
use serde::{Deserialize, Serialize};

/// Owned operations understood by both execution backends.
#[derive(Serialize, Deserialize)]
pub enum Request {
    /// Reads the current account balance.
    Read,
    /// Debits the balance and records its audit row in the same transaction.
    Debit(i64),
    /// Counts committed debit records.
    AuditCount,
    /// Returns values whose precision must survive the worker transport.
    Echo {
        /// Signed integers, including values outside JavaScript's precise range.
        integers: Vec<i64>,
        /// Binary payload without text conversion.
        bytes: Vec<u8>,
    },
    /// Exercises SQL error propagation.
    InvalidSql,
    /// Runs under the browser test's SQLite progress gate.
    LongQuery,
}

/// Results returned by the shared handler.
#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub enum Response {
    /// The balance after reading or committing a debit.
    Balance(i64),
    /// The number of committed audit rows.
    Count(i64),
    /// An exact round trip of the supplied values.
    Echo {
        /// The supplied integers.
        integers: Vec<i64>,
        /// The supplied bytes.
        bytes: Vec<u8>,
    },
    /// The query's sum.
    Sum(i64),
}

/// An operation refused its transaction or SQLite could not execute it.
#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub enum OperationError {
    /// The debit exceeds the available balance or is not positive.
    InsufficientFunds,
    /// SQLite's extended result code and diagnostic message.
    Sqlite {
        /// The SQLite extended error code, when SQLite supplied one.
        code: Option<i32>,
        /// The error description.
        message: String,
    },
}

impl core::fmt::Display for OperationError {
    fn fmt(&self, formatter: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::InsufficientFunds => formatter.write_str("insufficient funds"),
            Self::Sqlite { message, .. } => formatter.write_str(message),
        }
    }
}

impl From<rusqlite::Error> for OperationError {
    fn from(error: rusqlite::Error) -> Self {
        Self::Sqlite {
            code: error.sqlite_error().map(|error| error.extended_code),
            message: error.to_string(),
        }
    }
}

/// Creates the example schema once, preserving data on subsequent opens.
pub fn initialize(connection: &Connection) -> rusqlite::Result<()> {
    connection.execute_batch(
        "CREATE TABLE IF NOT EXISTS account (id INTEGER PRIMARY KEY, balance INTEGER NOT NULL);
         CREATE TABLE IF NOT EXISTS audit (amount INTEGER NOT NULL);
         INSERT OR IGNORE INTO account VALUES (1, 100);",
    )
}

/// Executes a complete operation on the connection's owning thread or worker.
pub fn handle(connection: &mut Connection, request: Request) -> Result<Response, OperationError> {
    match request {
        Request::Read => Ok(Response::Balance(connection.query_row(
            "SELECT balance FROM account WHERE id = 1",
            [],
            |row| row.get(0),
        )?)),
        Request::Debit(amount) => {
            let transaction = connection.transaction()?;
            transaction.execute("INSERT INTO audit VALUES (?)", [amount])?;
            let balance: i64 =
                transaction.query_row("SELECT balance FROM account WHERE id = 1", [], |row| {
                    row.get(0)
                })?;
            if amount <= 0 || amount > balance {
                return Err(OperationError::InsufficientFunds);
            }
            let remaining = balance - amount;
            transaction.execute("UPDATE account SET balance = ? WHERE id = 1", [remaining])?;
            transaction.commit()?;
            Ok(Response::Balance(remaining))
        }
        Request::AuditCount => Ok(Response::Count(connection.query_row(
            "SELECT COUNT(*) FROM audit",
            [],
            |row| row.get(0),
        )?)),
        Request::Echo { integers, bytes } => Ok(Response::Echo { integers, bytes }),
        Request::InvalidSql => {
            connection.execute("INSERT INTO missing_table VALUES (1)", [])?;
            Ok(Response::Count(0))
        }
        Request::LongQuery => Ok(Response::Sum(connection.query_row(
            "WITH RECURSIVE numbers(value) AS (
                VALUES(1) UNION ALL SELECT value + 1 FROM numbers WHERE value < 10000
             ) SELECT SUM(value) FROM numbers",
            [],
            |row| row.get(0),
        )?)),
    }
}
