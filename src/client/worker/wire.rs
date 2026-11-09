use serde::{Deserialize, Serialize};

use super::super::sql;
use crate::Error;

/// Caller messages, ordered by admission to one worker session.
#[derive(Serialize, Deserialize)]
pub enum Request {
    Hello,
    Call { id: u64, request: sql::Request },
    Close,
}

/// Worker replies, with readiness and close bounding the operation replies.
#[derive(Serialize, Deserialize)]
pub enum Response {
    Ready,
    Reply {
        id: u64,
        result: Result<sql::Response, SqlError>,
    },
    Closed(Result<(), String>),
    Failed(String),
}

/// SQL execution failures that retain their SQLite identity across workers.
#[derive(Serialize, Deserialize)]
pub enum SqlError {
    Sqlite {
        code: i32,
        message: Option<String>,
    },
    Input {
        code: i32,
        message: String,
        sql: String,
        offset: i32,
    },
    InvalidParameterCount(usize, usize),
    InvalidParameterName(String),
    ExecuteReturnedResults,
    InvalidQuery,
    MultipleStatement,
    Other(String),
}

impl From<rusqlite::Error> for SqlError {
    fn from(error: rusqlite::Error) -> Self {
        match error {
            rusqlite::Error::SqliteFailure(error, message) => Self::Sqlite {
                code: error.extended_code,
                message,
            },
            rusqlite::Error::SqlInputError {
                error,
                msg,
                sql,
                offset,
            } => Self::Input {
                code: error.extended_code,
                message: msg,
                sql,
                offset,
            },
            rusqlite::Error::InvalidParameterCount(given, expected) => {
                Self::InvalidParameterCount(given, expected)
            }
            rusqlite::Error::InvalidParameterName(name) => Self::InvalidParameterName(name),
            rusqlite::Error::ExecuteReturnedResults => Self::ExecuteReturnedResults,
            rusqlite::Error::InvalidQuery => Self::InvalidQuery,
            rusqlite::Error::MultipleStatement => Self::MultipleStatement,
            error => Self::Other(error.to_string()),
        }
    }
}

impl From<SqlError> for Error {
    fn from(error: SqlError) -> Self {
        let error = match error {
            SqlError::Sqlite { code, message } => {
                rusqlite::Error::SqliteFailure(rusqlite::ffi::Error::new(code), message)
            }
            SqlError::Input {
                code,
                message,
                sql,
                offset,
            } => rusqlite::Error::SqlInputError {
                error: rusqlite::ffi::Error::new(code),
                msg: message,
                sql,
                offset,
            },
            SqlError::InvalidParameterCount(given, expected) => {
                rusqlite::Error::InvalidParameterCount(given, expected)
            }
            SqlError::InvalidParameterName(name) => rusqlite::Error::InvalidParameterName(name),
            SqlError::ExecuteReturnedResults => rusqlite::Error::ExecuteReturnedResults,
            SqlError::InvalidQuery => rusqlite::Error::InvalidQuery,
            SqlError::MultipleStatement => rusqlite::Error::MultipleStatement,
            SqlError::Other(message) => return Self::Remote { message },
        };
        Self::Rusqlite(error)
    }
}
