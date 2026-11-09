use rusqlite::{
    types::{FromSql, FromSqlError, ToSqlOutput, ValueRef},
    Connection, Error, ToSql,
};

#[cfg_attr(
    all(
        feature = "sqlite-wasm-rs",
        target_arch = "wasm32",
        target_os = "unknown"
    ),
    derive(serde::Serialize, serde::Deserialize)
)]
/// SQL operations sent to the connection owner.
pub enum Request {
    Execute {
        sql: String,
        params: Vec<Cell>,
    },
    Query {
        sql: String,
        params: Vec<Cell>,
        first_only: bool,
    },
    Batch {
        sql: String,
    },
}

#[derive(Debug, PartialEq)]
#[cfg_attr(
    all(
        feature = "sqlite-wasm-rs",
        target_arch = "wasm32",
        target_os = "unknown"
    ),
    derive(serde::Serialize, serde::Deserialize)
)]
/// Owned results returned by the connection owner.
pub enum Response {
    Changes(usize),
    Rows {
        columns: Vec<String>,
        rows: Vec<Vec<Cell>>,
    },
    Done,
}

#[derive(Debug, PartialEq)]
#[cfg_attr(
    all(
        feature = "sqlite-wasm-rs",
        target_arch = "wasm32",
        target_os = "unknown"
    ),
    derive(serde::Serialize, serde::Deserialize)
)]
/// SQLite values with their text and binary bytes preserved.
pub enum Cell {
    Null,
    Integer(i64),
    Real(f64),
    Text(Vec<u8>),
    Blob(Vec<u8>),
}

impl Cell {
    fn from_value(value: ValueRef<'_>) -> Self {
        match value {
            ValueRef::Null => Self::Null,
            ValueRef::Integer(value) => Self::Integer(value),
            ValueRef::Real(value) => Self::Real(value),
            ValueRef::Text(value) => Self::Text(value.to_vec()),
            ValueRef::Blob(value) => Self::Blob(value.to_vec()),
        }
    }

    fn value_ref(&self) -> ValueRef<'_> {
        match self {
            Self::Null => ValueRef::Null,
            Self::Integer(value) => ValueRef::Integer(*value),
            Self::Real(value) => ValueRef::Real(*value),
            Self::Text(value) => ValueRef::Text(value),
            Self::Blob(value) => ValueRef::Blob(value),
        }
    }
}

impl ToSql for Cell {
    fn to_sql(&self) -> rusqlite::Result<ToSqlOutput<'_>> {
        Ok(ToSqlOutput::Borrowed(self.value_ref()))
    }
}

mod sealed {
    use super::Cell;

    pub trait Params {
        fn values(self) -> rusqlite::Result<Vec<Cell>>;
    }
}

/// Positional parameters copied before a query is dispatched.
///
/// Accepts `[]`, `()`, arrays of up to 32 values, slices, vectors and
/// `rusqlite::params!`. Values must convert to SQLite null, integer, real, text
/// or blob values; pointer and other connection-local parameters are unsupported.
pub trait Params: sealed::Params {}

impl<Values: sealed::Params> Params for Values {}

/// Converts caller parameters into owned SQLite values.
pub fn parameters(params: impl Params) -> rusqlite::Result<Vec<Cell>> {
    params.values()
}

fn parameter(value: &impl ToSql) -> rusqlite::Result<Cell> {
    match value.to_sql()? {
        ToSqlOutput::Borrowed(value) => Ok(Cell::from_value(value)),
        ToSqlOutput::Owned(value) => Ok(Cell::from_value(ValueRef::from(&value))),
        _ => Err(Error::ToSqlConversionFailure(
            "parameter is not a portable SQLite value".into(),
        )),
    }
}

impl sealed::Params for () {
    fn values(self) -> rusqlite::Result<Vec<Cell>> {
        Ok(Vec::new())
    }
}

impl sealed::Params for [&(dyn ToSql + Send + Sync); 0] {
    fn values(self) -> rusqlite::Result<Vec<Cell>> {
        Ok(Vec::new())
    }
}

impl<Value: ToSql> sealed::Params for &[Value] {
    fn values(self) -> rusqlite::Result<Vec<Cell>> {
        self.iter().map(parameter).collect()
    }
}

impl<Value: ToSql> sealed::Params for Vec<Value> {
    fn values(self) -> rusqlite::Result<Vec<Cell>> {
        self.as_slice().values()
    }
}

impl<Value: ToSql, const LENGTH: usize> sealed::Params for &[Value; LENGTH] {
    fn values(self) -> rusqlite::Result<Vec<Cell>> {
        self.as_slice().values()
    }
}

macro_rules! array_params {
    ($($length:literal),+) => {$(
        impl<Value: ToSql> sealed::Params for [Value; $length] {
            fn values(self) -> rusqlite::Result<Vec<Cell>> {
                self.as_slice().values()
            }
        }
    )+};
}

array_params!(
    1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26,
    27, 28, 29, 30, 31, 32
);

/// A result row mapped on the caller after SQL execution completes.
///
/// Values use rusqlite's `FromSql` conversions. The row borrows the returned
/// values and cannot outlive the mapping callback.
pub struct Row<'a> {
    columns: &'a [String],
    cells: &'a [Cell],
}

/// Borrows one result row without exposing transport values in its constructor.
pub fn row<'a>(columns: &'a [String], cells: &'a [Cell]) -> Row<'a> {
    Row { columns, cells }
}

impl Row<'_> {
    /// Reads a column using rusqlite's conversion and error rules.
    ///
    /// Fails if the index is out of bounds or the value cannot convert to `Value`.
    pub fn get<Value: FromSql>(&self, index: usize) -> rusqlite::Result<Value> {
        let cell = self
            .cells
            .get(index)
            .ok_or(Error::InvalidColumnIndex(index))?;
        let column = self
            .columns
            .get(index)
            .ok_or(Error::InvalidColumnIndex(index))?;
        let value = cell.value_ref();
        FromSql::column_result(value).map_err(|error| match error {
            FromSqlError::InvalidType => {
                Error::InvalidColumnType(index, column.clone(), value.data_type())
            }
            FromSqlError::OutOfRange(value) => Error::IntegralValueOutOfRange(index, value),
            FromSqlError::Utf8Error(error) => Error::Utf8Error(index, error),
            FromSqlError::Other(error) => {
                Error::FromSqlConversionFailure(index, value.data_type(), error)
            }
            other => Error::FromSqlConversionFailure(index, value.data_type(), Box::new(other)),
        })
    }

    /// Reads the first column matching a name, ignoring ASCII case.
    ///
    /// Fails if the name is absent or its value cannot convert to `Value`.
    pub fn get_named<Value: FromSql>(&self, name: &str) -> rusqlite::Result<Value> {
        let index = self
            .columns
            .iter()
            .position(|column| column.eq_ignore_ascii_case(name))
            .ok_or_else(|| Error::InvalidColumnName(name.into()))?;
        self.get(index)
    }
}

/// Executes a complete SQL operation without retaining a transaction between calls.
pub fn handle(connection: &mut Connection, request: Request) -> rusqlite::Result<Response> {
    if !connection.is_autocommit() {
        return Err(Error::InvalidQuery);
    }
    let result = execute(connection, request);
    if !connection.is_autocommit() {
        connection.execute_batch("ROLLBACK")?;
        return result.and(Err(Error::InvalidQuery));
    }
    result
}

fn execute(connection: &mut Connection, request: Request) -> rusqlite::Result<Response> {
    match request {
        Request::Execute { sql, params } => connection
            .execute(&sql, rusqlite::params_from_iter(params))
            .map(Response::Changes),
        Request::Query {
            sql,
            params,
            first_only,
        } => {
            let mut statement = connection.prepare(&sql)?;
            let read_only = statement.readonly();
            let columns: Vec<String> = statement.column_names().map(str::to_owned).collect();
            let mut source = statement.query(rusqlite::params_from_iter(params))?;
            let mut rows = Vec::new();
            while let Some(row) = source.next()? {
                if first_only && !rows.is_empty() {
                    continue;
                }
                let cells = (0..columns.len())
                    .map(|index| row.get_ref(index).map(Cell::from_value))
                    .collect::<rusqlite::Result<Vec<_>>>()?;
                rows.push(cells);
                if first_only && read_only {
                    break;
                }
            }
            Ok(Response::Rows { columns, rows })
        }
        Request::Batch { sql } => {
            connection.execute_batch(&sql)?;
            Ok(Response::Done)
        }
    }
}

#[cfg(test)]
mod tests;
