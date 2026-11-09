use super::*;

fn query(connection: &mut Connection, sql: &str, first_only: bool) -> rusqlite::Result<Response> {
    outside(
        connection,
        Operation::Query {
            sql: sql.into(),
            params: Vec::new(),
            first_only,
        },
    )
}

fn outside(connection: &mut Connection, operation: Operation) -> rusqlite::Result<Response> {
    handle(
        connection,
        Request::Sql {
            operation,
            in_transaction: false,
        },
    )
}

#[test]
fn parameters_accept_existing_query_syntax_without_precision_loss() {
    assert_eq!(parameters([]).unwrap(), Vec::new());
    assert_eq!(parameters(()).unwrap(), Vec::new());
    assert_eq!(
        parameters([i64::MIN, i64::MAX]).unwrap(),
        vec![Cell::Integer(i64::MIN), Cell::Integer(i64::MAX)]
    );
    assert_eq!(
        parameters(vec![1, 2]).unwrap(),
        vec![Cell::Integer(1), Cell::Integer(2)]
    );
    assert_eq!(
        parameters(&[1, 2][..]).unwrap(),
        vec![Cell::Integer(1), Cell::Integer(2)]
    );
    assert_eq!(
        parameters(rusqlite::params![None::<i64>, "text", &[0u8, 255][..], 1.5]).unwrap(),
        vec![
            Cell::Null,
            Cell::Text(b"text".to_vec()),
            Cell::Blob(vec![0, 255]),
            Cell::Real(1.5)
        ]
    );
}

#[test]
fn custom_parameter_errors_are_preserved() {
    struct Refused;
    impl ToSql for Refused {
        fn to_sql(&self) -> rusqlite::Result<ToSqlOutput<'_>> {
            Err(Error::InvalidQuery)
        }
    }
    assert!(matches!(parameters([Refused]), Err(Error::InvalidQuery)));
}

#[test]
fn rows_keep_sqlite_text_bytes_until_the_caller_maps_them() {
    let mut connection = Connection::open_in_memory().unwrap();
    let response = query(
        &mut connection,
        "SELECT CAST(x'80ff' AS TEXT) AS bad, 42 AS answer",
        false,
    )
    .unwrap();
    assert_eq!(
        response,
        Response::Rows {
            columns: vec!["bad".into(), "answer".into()],
            rows: vec![vec![Cell::Text(vec![128, 255]), Cell::Integer(42)]]
        }
    );
    let Response::Rows { columns, rows } = response else {
        unreachable!()
    };
    let row = row(&columns, &rows[0]);
    assert!(matches!(row.get::<String>(0), Err(Error::Utf8Error(0, _))));
    assert_eq!(row.get_named::<i64>("ANSWER").unwrap(), 42);
    assert!(matches!(
        row.get::<i64>(2),
        Err(Error::InvalidColumnIndex(2))
    ));
    assert!(matches!(
        row.get_named::<i64>("missing"),
        Err(Error::InvalidColumnName(_))
    ));
}

#[test]
fn first_row_query_does_not_evaluate_a_later_failing_row() {
    let mut connection = Connection::open_in_memory().unwrap();
    let sql = "SELECT 1 AS value UNION ALL SELECT abs(-9223372036854775808)";
    assert_eq!(
        query(&mut connection, sql, true).unwrap(),
        Response::Rows {
            columns: vec!["value".into()],
            rows: vec![vec![Cell::Integer(1)]]
        }
    );
    assert!(query(&mut connection, sql, false).is_err());
}

#[test]
fn first_row_write_reports_a_failed_commit() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("locked.sqlite");
    let mut writer = Connection::open(&path).unwrap();
    writer.busy_timeout(std::time::Duration::ZERO).unwrap();
    writer
        .execute_batch("PRAGMA journal_mode = DELETE; CREATE TABLE records (value INTEGER)")
        .unwrap();
    let reader = Connection::open(path).unwrap();
    reader
        .execute_batch("BEGIN; SELECT * FROM records")
        .unwrap();

    let result = query(
        &mut writer,
        "INSERT INTO records VALUES (1) RETURNING value",
        true,
    );
    assert_eq!(
        result.unwrap_err().sqlite_error_code(),
        Some(rusqlite::ErrorCode::DatabaseBusy)
    );
    assert!(writer.is_autocommit());
    reader.execute_batch("ROLLBACK").unwrap();
    assert_eq!(
        query(&mut writer, "SELECT value FROM records", false).unwrap(),
        Response::Rows {
            columns: vec!["value".into()],
            rows: Vec::new()
        }
    );
}

#[test]
fn first_row_write_commits_all_changes_and_returns_only_one_row() {
    let mut connection = Connection::open_in_memory().unwrap();
    connection
        .execute_batch("CREATE TABLE records (value INTEGER)")
        .unwrap();
    assert_eq!(
        query(
            &mut connection,
            "INSERT INTO records VALUES (1), (2) RETURNING value",
            true
        )
        .unwrap(),
        Response::Rows {
            columns: vec!["value".into()],
            rows: vec![vec![Cell::Integer(1)]]
        }
    );
    assert_eq!(
        query(
            &mut connection,
            "SELECT value FROM records ORDER BY value",
            false
        )
        .unwrap(),
        Response::Rows {
            columns: vec!["value".into()],
            rows: vec![vec![Cell::Integer(1)], vec![Cell::Integer(2)]]
        }
    );
}

#[test]
fn failed_batch_rolls_back_the_whole_explicit_transaction() {
    let mut connection = Connection::open_in_memory().unwrap();
    connection
        .execute_batch("CREATE TABLE records (value INTEGER)")
        .unwrap();
    let failed = outside(
        &mut connection,
        Operation::Batch {
            sql: "BEGIN; INSERT INTO records VALUES (1); INSERT INTO missing VALUES (2); COMMIT;"
                .into(),
        },
    );
    assert!(failed.is_err());
    assert!(connection.is_autocommit());
    assert_eq!(
        query(
            &mut connection,
            "SELECT COUNT(*) AS count FROM records",
            true
        )
        .unwrap(),
        Response::Rows {
            columns: vec!["count".into()],
            rows: vec![vec![Cell::Integer(0)]]
        }
    );
    outside(
        &mut connection,
        Operation::Batch {
            sql: "BEGIN; INSERT INTO records VALUES (3); INSERT INTO records VALUES (4); COMMIT;"
                .into(),
        },
    )
    .unwrap();
    assert_eq!(
        query(
            &mut connection,
            "SELECT value FROM records ORDER BY value",
            false
        )
        .unwrap(),
        Response::Rows {
            columns: vec!["value".into()],
            rows: vec![vec![Cell::Integer(3)], vec![Cell::Integer(4)]]
        }
    );
}

#[test]
fn unfinished_transaction_cannot_leak_into_another_request() {
    let mut connection = Connection::open_in_memory().unwrap();
    connection
        .execute_batch("CREATE TABLE records (value INTEGER)")
        .unwrap();
    let failed = outside(
        &mut connection,
        Operation::Batch {
            sql: "BEGIN; INSERT INTO records VALUES (1);".into(),
        },
    );
    assert!(matches!(failed, Err(Error::InvalidQuery)));
    assert!(connection.is_autocommit());
    assert_eq!(
        query(
            &mut connection,
            "SELECT COUNT(*) AS count FROM records",
            true
        )
        .unwrap(),
        Response::Rows {
            columns: vec!["count".into()],
            rows: vec![vec![Cell::Integer(0)]]
        }
    );
}

#[test]
fn an_existing_native_transaction_is_not_rolled_back_or_modified() {
    let mut connection = Connection::open_in_memory().unwrap();
    connection
        .execute_batch("CREATE TABLE records(value INTEGER); BEGIN; INSERT INTO records VALUES(7)")
        .unwrap();
    assert!(matches!(
        outside(
            &mut connection,
            Operation::Execute {
                sql: "INSERT INTO records VALUES(?)".into(),
                params: parameters([8]).unwrap(),
            }
        ),
        Err(Error::InvalidQuery)
    ));
    assert!(!connection.is_autocommit());
    let values = connection
        .prepare("SELECT value FROM records")
        .unwrap()
        .query_map([], |row| row.get::<_, i64>(0))
        .unwrap()
        .collect::<rusqlite::Result<Vec<_>>>()
        .unwrap();
    assert_eq!(values, vec![7]);
    connection.execute_batch("ROLLBACK").unwrap();
}

fn managed_batch(connection: &mut Connection, sql: &str) -> rusqlite::Result<Response> {
    handle(
        connection,
        Request::Sql {
            operation: Operation::Batch { sql: sql.into() },
            in_transaction: true,
        },
    )
}

#[test]
fn managed_transaction_keeps_calls_together_until_commit() {
    let mut connection = Connection::open_in_memory().unwrap();
    connection
        .execute_batch("CREATE TABLE records(value INTEGER UNIQUE)")
        .unwrap();
    assert_eq!(
        handle(&mut connection, Request::Begin).unwrap(),
        Response::Done
    );
    managed_batch(&mut connection, "INSERT INTO records VALUES(1)").unwrap();
    assert!(matches!(
        query(&mut connection, "SELECT value FROM records", false),
        Err(Error::InvalidQuery)
    ));
    assert!(matches!(
        handle(&mut connection, Request::Begin),
        Err(Error::InvalidQuery)
    ));
    assert!(managed_batch(&mut connection, "INSERT INTO records VALUES(1)").is_err());
    managed_batch(&mut connection, "INSERT INTO records VALUES(2)").unwrap();
    assert_eq!(
        handle(&mut connection, Request::Commit).unwrap(),
        Response::Done
    );
    assert_eq!(
        query(
            &mut connection,
            "SELECT value FROM records ORDER BY value",
            false
        )
        .unwrap(),
        Response::Rows {
            columns: vec!["value".into()],
            rows: vec![vec![Cell::Integer(1)], vec![Cell::Integer(2)]],
        }
    );
}

#[test]
fn rollback_discards_changes_and_can_be_repeated() {
    let mut connection = Connection::open_in_memory().unwrap();
    connection
        .execute_batch("CREATE TABLE records(value INTEGER)")
        .unwrap();
    handle(&mut connection, Request::Begin).unwrap();
    managed_batch(&mut connection, "INSERT INTO records VALUES(1)").unwrap();
    handle(&mut connection, Request::Rollback).unwrap();
    handle(&mut connection, Request::Rollback).unwrap();
    assert_eq!(
        query(&mut connection, "SELECT value FROM records", false).unwrap(),
        Response::Rows {
            columns: vec!["value".into()],
            rows: Vec::new(),
        }
    );
}

#[test]
fn automatic_rollback_prevents_later_transaction_calls_from_committing() {
    let mut connection = Connection::open_in_memory().unwrap();
    connection
        .execute_batch("CREATE TABLE records(value INTEGER UNIQUE); INSERT INTO records VALUES(1)")
        .unwrap();
    handle(&mut connection, Request::Begin).unwrap();
    managed_batch(&mut connection, "INSERT INTO records VALUES(2)").unwrap();
    let error =
        managed_batch(&mut connection, "INSERT OR ROLLBACK INTO records VALUES(1)").unwrap_err();
    assert_eq!(
        error.sqlite_error_code(),
        Some(rusqlite::ErrorCode::ConstraintViolation)
    );
    assert!(connection.is_autocommit());
    assert!(matches!(
        managed_batch(&mut connection, "INSERT INTO records VALUES(3)"),
        Err(Error::InvalidQuery)
    ));
    assert!(matches!(
        handle(&mut connection, Request::Commit),
        Err(Error::InvalidQuery)
    ));
    handle(&mut connection, Request::Rollback).unwrap();
    assert_eq!(
        query(&mut connection, "SELECT value FROM records", false).unwrap(),
        Response::Rows {
            columns: vec!["value".into()],
            rows: vec![vec![Cell::Integer(1)]],
        }
    );
}

#[test]
fn raw_transaction_end_is_reported_before_another_managed_call() {
    let mut connection = Connection::open_in_memory().unwrap();
    handle(&mut connection, Request::Begin).unwrap();
    assert!(matches!(
        managed_batch(&mut connection, "ROLLBACK"),
        Err(Error::InvalidQuery)
    ));
    assert!(matches!(
        managed_batch(&mut connection, "CREATE TABLE records(value)"),
        Err(Error::InvalidQuery)
    ));
}

#[test]
fn failed_commit_leaves_changes_available_for_rollback() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("commit.sqlite");
    let mut writer = Connection::open(&path).unwrap();
    writer.busy_timeout(std::time::Duration::ZERO).unwrap();
    writer
        .execute_batch("PRAGMA journal_mode=DELETE; CREATE TABLE records(value INTEGER)")
        .unwrap();
    let reader = Connection::open(path).unwrap();
    reader
        .execute_batch("BEGIN; SELECT * FROM records")
        .unwrap();
    handle(&mut writer, Request::Begin).unwrap();
    managed_batch(&mut writer, "INSERT INTO records VALUES(1)").unwrap();
    let error = handle(&mut writer, Request::Commit).unwrap_err();
    assert_eq!(
        error.sqlite_error_code(),
        Some(rusqlite::ErrorCode::DatabaseBusy)
    );
    assert!(!writer.is_autocommit());
    handle(&mut writer, Request::Rollback).unwrap();
    reader.execute_batch("ROLLBACK").unwrap();
    assert_eq!(
        query(&mut writer, "SELECT value FROM records", false).unwrap(),
        Response::Rows {
            columns: vec!["value".into()],
            rows: Vec::new(),
        }
    );
}

#[cfg(feature = "hooks")]
#[test]
fn managed_transactions_preserve_the_application_authorizer() {
    use rusqlite::hooks::{AuthAction, AuthContext, Authorization};

    let mut connection = Connection::open_in_memory().unwrap();
    connection
        .execute_batch("CREATE TABLE records(value INTEGER)")
        .unwrap();
    connection
        .authorizer(Some(|context: AuthContext<'_>| match context.action {
            AuthAction::Insert {
                table_name: "records",
            } => Authorization::Deny,
            _ => Authorization::Allow,
        }))
        .unwrap();
    handle(&mut connection, Request::Begin).unwrap();
    let error = managed_batch(&mut connection, "INSERT INTO records VALUES(1)").unwrap_err();
    assert_eq!(
        error.sqlite_error_code(),
        Some(rusqlite::ErrorCode::AuthorizationForStatementDenied)
    );
    handle(&mut connection, Request::Rollback).unwrap();
    assert_eq!(
        query(&mut connection, "SELECT value FROM records", false).unwrap(),
        Response::Rows {
            columns: vec!["value".into()],
            rows: Vec::new(),
        }
    );
}
