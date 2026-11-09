//! A shared ledger handler executed by native threads or a browser database worker.

mod ledger;

#[cfg(target_arch = "wasm32")]
mod browser;

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests;

pub use ledger::{handle, initialize, OperationError, Request, Response};

use async_sqlite::DatabaseBackend;

/// Exercises atomic writes, refusals and exact value transport on either backend.
pub async fn exercise(
    database: &impl DatabaseBackend<Request = Request, Response = Response, Error = OperationError>,
) -> Result<(), String> {
    let initial = database
        .call(Request::Read)
        .await
        .map_err(|error| error.to_string())?;
    assert_eq!(initial, Response::Balance(100));
    assert_eq!(
        database.call(Request::Debit(30)).await.unwrap(),
        Response::Balance(70)
    );
    assert!(matches!(
        database.call(Request::Debit(90)).await,
        Err(async_sqlite::CallError::Operation(
            OperationError::InsufficientFunds
        ))
    ));
    assert_eq!(
        database.call(Request::Read).await.unwrap(),
        Response::Balance(70)
    );
    assert_eq!(
        database.call(Request::AuditCount).await.unwrap(),
        Response::Count(1)
    );
    let integers = vec![
        i64::MIN,
        -9_007_199_254_740_993,
        9_007_199_254_740_993,
        i64::MAX,
    ];
    let bytes = vec![0, 255, 128, 1];
    assert_eq!(
        database
            .call(Request::Echo {
                integers: integers.clone(),
                bytes: bytes.clone()
            })
            .await
            .unwrap(),
        Response::Echo { integers, bytes }
    );
    assert!(matches!(
        database.call(Request::InvalidSql).await,
        Err(async_sqlite::CallError::Operation(
            OperationError::Sqlite { .. }
        ))
    ));
    let replies = futures_util::future::join_all((0..12).map(|value| {
        database.call(Request::Echo {
            integers: vec![value],
            bytes: vec![],
        })
    }))
    .await;
    let expected: Vec<_> = (0..12)
        .map(|value| {
            Ok(Response::Echo {
                integers: vec![value],
                bytes: vec![],
            })
        })
        .collect();
    assert_eq!(replies, expected);
    Ok(())
}
