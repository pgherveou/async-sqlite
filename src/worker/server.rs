use futures_channel::mpsc;
use futures_util::StreamExt;
use js_sys::Uint8Array;
use rusqlite::Connection;
use serde::{de::DeserializeOwned, Serialize};
use wasm_bindgen::{closure::Closure, JsCast, JsValue};
use web_sys::{DedicatedWorkerGlobalScope, Event, MessageEvent};

use super::wire::{Request, Response};

struct Listener {
    scope: DedicatedWorkerGlobalScope,
    messages: mpsc::UnboundedReceiver<Result<Vec<u8>, String>>,
    _message: Closure<dyn FnMut(MessageEvent)>,
    _message_error: Closure<dyn FnMut(Event)>,
}

impl Listener {
    fn new() -> Result<Self, JsValue> {
        let scope = js_sys::global().dyn_into::<DedicatedWorkerGlobalScope>()?;
        let (sender, receiver) = mpsc::unbounded();
        let errors = sender.clone();
        let message = Closure::new(move |event: MessageEvent| {
            let bytes = event
                .data()
                .dyn_into::<Uint8Array>()
                .map(|bytes| bytes.to_vec())
                .map_err(|_| "database request must contain a Uint8Array".to_owned());
            let _ = sender.unbounded_send(bytes);
        });
        let message_error = Closure::new(move |_: Event| {
            let _ = errors.unbounded_send(Err("database request could not be received".into()));
        });
        scope.set_onmessage(Some(message.as_ref().unchecked_ref()));
        scope.set_onmessageerror(Some(message_error.as_ref().unchecked_ref()));
        Ok(Self {
            scope,
            messages: receiver,
            _message: message,
            _message_error: message_error,
        })
    }

    fn send(&self, response: &Response) -> Result<(), JsValue> {
        let bytes = postcard::to_allocvec(response)
            .map_err(|error| JsValue::from_str(&error.to_string()))?;
        self.scope.post_message(&Uint8Array::from(bytes.as_slice()))
    }
}

impl Drop for Listener {
    fn drop(&mut self) {
        self.scope.set_onmessage(None);
        self.scope.set_onmessageerror(None);
    }
}

/// Serves application operations on this dedicated worker's connection.
///
/// Initialize the browser VFS, connection and schema before calling this function.
/// The handler runs synchronously and owns transaction boundaries. Requests are
/// processed in order, including requests whose callers have stopped waiting.
/// The future completes after close or a protocol failure; dropping it stops
/// serving and drops the connection. It must be awaited or spawned locally.
/// Close errors terminate this session and are reported to the caller. Propagate
/// a serving error as an uncaught worker error so the caller detects lost replies.
/// Use [`serve_with_shutdown`] when the VFS holds resources beyond SQLite close.
pub async fn serve<RequestValue, ResponseValue, OperationError>(
    connection: Connection,
    handler: impl FnMut(&mut Connection, RequestValue) -> Result<ResponseValue, OperationError>,
) -> Result<(), JsValue>
where
    RequestValue: DeserializeOwned,
    ResponseValue: Serialize,
    OperationError: Serialize,
{
    serve_with_shutdown(connection, handler, || Ok(())).await
}

/// Serves operations and releases application resources before acknowledging close.
///
/// Follows [`serve`]'s contract. On orderly close, `shutdown` runs once after
/// SQLite closes successfully and before the close reply, so it can release VFS
/// locks before a replacement worker opens the database. A cleanup error fails
/// close with [`crate::CallError::OutcomeUnknown`] and terminates the session.
/// The callback does not run on protocol failure or when this future is dropped.
pub async fn serve_with_shutdown<RequestValue, ResponseValue, OperationError>(
    mut connection: Connection,
    mut handler: impl FnMut(&mut Connection, RequestValue) -> Result<ResponseValue, OperationError>,
    shutdown: impl FnOnce() -> Result<(), JsValue>,
) -> Result<(), JsValue>
where
    RequestValue: DeserializeOwned,
    ResponseValue: Serialize,
    OperationError: Serialize,
{
    let mut listener = Listener::new()?;
    listener.send(&Response::Ready)?;
    while let Some(message) = listener.messages.next().await {
        let request = message.and_then(|bytes| {
            postcard::from_bytes::<Request>(&bytes).map_err(|error| error.to_string())
        });
        match request {
            Ok(Request::Hello) => listener.send(&Response::Ready)?,
            Ok(Request::Call { id, payload }) => {
                let request = match postcard::from_bytes(&payload) {
                    Ok(request) => request,
                    Err(error) => {
                        listener.send(&Response::Failed(error.to_string()))?;
                        return Err(JsValue::from_str(&error.to_string()));
                    }
                };
                let result = handler(&mut connection, request);
                let payload = match postcard::to_allocvec(&result) {
                    Ok(payload) => payload,
                    Err(error) => {
                        listener.send(&Response::Failed(error.to_string()))?;
                        return Err(JsValue::from_str(&error.to_string()));
                    }
                };
                listener.send(&Response::Reply { id, payload })?;
            }
            Ok(Request::Close) => {
                let result = connection
                    .close()
                    .map_err(|(_, error)| error.to_string())
                    .and_then(|()| {
                        shutdown().map_err(|error| format!("database shutdown failed: {error:?}"))
                    });
                listener.send(&Response::Closed(result))?;
                return Ok(());
            }
            Err(message) => {
                listener.send(&Response::Failed(message.clone()))?;
                return Err(JsValue::from_str(&message));
            }
        }
    }
    Ok(())
}

/// Reports initialization failure to a caller connecting before or after it.
///
/// Use this when VFS, connection or schema initialization fails before `serve`.
/// The future retains its listener until the caller's handshake arrives. The
/// caller terminates the failed worker after receiving the failure.
pub async fn report_startup_error(message: String) -> Result<(), JsValue> {
    let mut listener = Listener::new()?;
    listener.send(&Response::StartupError(message.clone()))?;
    while let Some(Ok(bytes)) = listener.messages.next().await {
        if matches!(postcard::from_bytes::<Request>(&bytes), Ok(Request::Hello)) {
            listener.send(&Response::StartupError(message))?;
            return Ok(());
        }
    }
    Ok(())
}
