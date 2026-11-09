use crate::{exercise, handle, initialize, OperationError, Request, Response};
use async_sqlite::{DatabaseBackend, WorkerDatabase};
use futures_util::FutureExt;
use wasm_bindgen::prelude::*;
use web_sys::{Worker, WorkerOptions, WorkerType};

#[wasm_bindgen(inline_js = "
export function waitForRelease() {
    const request = new XMLHttpRequest();
    request.open('GET', '/query-gate', false);
    request.send();
    if (request.status !== 200) throw new Error('query gate failed');
}
export function loseNextReply() {
    globalThis.postMessage = () => { throw new Error('reply lost after commit'); };
}
")]
extern "C" {
    #[wasm_bindgen(js_name = waitForRelease)]
    fn wait_for_release();
    #[wasm_bindgen(js_name = loseNextReply)]
    fn lose_next_reply();
}

fn message(error: impl core::fmt::Display) -> JsValue {
    JsValue::from_str(&error.to_string())
}

fn send_future<T: Send>(future: T) -> T {
    future
}

/// A caller-side handle whose SQL executes in another dedicated worker.
#[wasm_bindgen]
pub struct LedgerClient {
    database: WorkerDatabase<Request, Response, OperationError>,
}

#[wasm_bindgen]
impl LedgerClient {
    /// Starts the supplied database worker and waits for schema initialization.
    pub async fn open(url: String) -> Result<LedgerClient, JsValue> {
        let options = WorkerOptions::new();
        options.set_type(WorkerType::Module);
        let worker = Worker::new_with_options(&url, &options)?;
        let database = WorkerDatabase::connect(worker).await.map_err(message)?;
        Ok(Self { database })
    }

    /// Runs the same transaction and transport checks as the native example.
    pub async fn exercise(&self) -> Result<(), JsValue> {
        exercise(&self.database).await.map_err(message)
    }

    /// Reads the persisted balance.
    pub async fn balance(&self) -> Result<i64, JsValue> {
        match send_future(self.database.call(Request::Read))
            .await
            .map_err(message)?
        {
            Response::Balance(balance) => Ok(balance),
            _ => Err(JsValue::from_str("unexpected response")),
        }
    }

    /// Commits a debit and returns its resulting balance.
    pub async fn debit(&self, amount: i64) -> Result<i64, JsValue> {
        match self
            .database
            .call(Request::Debit(amount))
            .await
            .map_err(message)?
        {
            Response::Balance(balance) => Ok(balance),
            _ => Err(JsValue::from_str("unexpected response")),
        }
    }

    /// Admits a write and drops the waiting future before its reply arrives.
    pub fn cancel_debit(&self, amount: i64) -> Result<(), JsValue> {
        let mut call = Box::pin(self.database.call(Request::Debit(amount)));
        if let Some(result) = call.as_mut().now_or_never() {
            result.map_err(message)?;
            return Err(JsValue::from_str(
                "call completed before it could be abandoned",
            ));
        }
        drop(call);
        Ok(())
    }

    /// Executes a SQLite query held by the test server's progress gate.
    pub async fn long_query(&self) -> Result<i64, JsValue> {
        match self
            .database
            .call(Request::LongQuery)
            .await
            .map_err(message)?
        {
            Response::Sum(sum) => Ok(sum),
            _ => Err(JsValue::from_str("unexpected response")),
        }
    }

    /// Drains accepted work and closes the database.
    pub async fn close(&self) -> Result<(), JsValue> {
        send_future(self.database.close()).await.map_err(message)
    }

    /// Terminates the worker without promising rollback of pending writes.
    pub fn terminate(&self) {
        self.database.terminate();
    }
}

/// Opens OPFS storage before serving any database request.
#[wasm_bindgen]
pub async fn serve_ledger(
    name: String,
    lose_reply: bool,
    fail_cleanup: bool,
) -> Result<(), JsValue> {
    use sqlite_wasm_vfs::sahpool::{install, OpfsSAHPoolCfg};

    let pool = install::<sqlite_wasm_rs::WasmOsCallback>(&OpfsSAHPoolCfg::default(), true)
        .await
        .map_err(message)?;
    let connection = async_sqlite::rusqlite::Connection::open(name).map_err(message)?;
    initialize(&connection).map_err(message)?;
    async_sqlite::serve_with_shutdown(
        connection,
        move |connection, request| {
            let long_query = matches!(request, Request::LongQuery);
            let debit = matches!(request, Request::Debit(_));
            if long_query {
                let mut waiting = true;
                connection.progress_handler(
                    1,
                    Some(move || {
                        if waiting {
                            waiting = false;
                            wait_for_release();
                        }
                        false
                    }),
                )?;
            }
            let result = handle(connection, request);
            if long_query {
                connection.progress_handler(0, None::<fn() -> bool>)?;
            }
            if lose_reply && debit && result.is_ok() {
                lose_next_reply();
            }
            result
        },
        move || {
            pool.pause().map_err(message)?;
            if fail_cleanup {
                return Err(JsValue::from_str("requested cleanup failure"));
            }
            Ok(())
        },
    )
    .await
}

/// Reports initialization failure through the worker's startup handshake.
#[wasm_bindgen]
pub async fn startup_failed(reason: String) -> Result<(), JsValue> {
    async_sqlite::report_startup_error(reason).await
}
