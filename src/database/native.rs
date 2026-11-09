use core::convert::Infallible;
use std::sync::{Arc, Mutex};

use futures_channel::oneshot;
use futures_util::{
    future::{BoxFuture, Shared},
    FutureExt,
};
use rusqlite::Connection;

use super::{CallError, DatabaseBackend};
use crate::{client, Client, Error};

type Handler<Request, Response, OperationError> =
    dyn Fn(&mut Connection, Request) -> Result<Response, OperationError> + Send + Sync;
type CloseResult = Shared<BoxFuture<'static, Result<(), String>>>;

enum Admission {
    Open,
    Closing(CloseResult),
    Closed,
}

impl Admission {
    fn refresh(&mut self) {
        if let Self::Closing(reply) = self {
            if let Some(result) = reply.clone().now_or_never() {
                *self = if result.is_ok() {
                    Self::Closed
                } else {
                    Self::Open
                };
            }
        }
    }
}

/// Executes typed requests on an existing native connection thread.
///
/// The handler owns transaction boundaries. All clones share admission and
/// shutdown; retain no other client handles that close or independently write
/// the connection when service-level ordering is required.
pub struct NativeDatabase<Request, Response, OperationError> {
    client: Client,
    handler: Arc<Handler<Request, Response, OperationError>>,
    admission: Arc<Mutex<Admission>>,
}

impl<Request, Response, OperationError> Clone
    for NativeDatabase<Request, Response, OperationError>
{
    fn clone(&self) -> Self {
        Self {
            client: self.client.clone(),
            handler: self.handler.clone(),
            admission: self.admission.clone(),
        }
    }
}

impl<Request, Response, OperationError> NativeDatabase<Request, Response, OperationError> {
    /// Attaches a request handler to an opened and initialized connection.
    ///
    /// Requests use the client's configured queue capacity. A full queue
    /// rejects the request before dispatch and does not run its handler.
    pub fn new(
        client: Client,
        handler: impl Fn(&mut Connection, Request) -> Result<Response, OperationError>
            + Send
            + Sync
            + 'static,
    ) -> Self {
        Self {
            client,
            handler: Arc::new(handler),
            admission: Arc::new(Mutex::new(Admission::Open)),
        }
    }
}

impl<Request, Response, OperationError> DatabaseBackend
    for NativeDatabase<Request, Response, OperationError>
where
    Request: Send + 'static,
    Response: Send + 'static,
    OperationError: Send + 'static,
{
    type Request = Request;
    type Response = Response;
    type Error = OperationError;

    fn call(&self, request: Request) -> BoxFuture<'_, Result<Response, CallError<OperationError>>> {
        Box::pin(async move {
            let reply = {
                let mut admission = self.admission.lock().expect("database admission poisoned");
                admission.refresh();
                if !matches!(*admission, Admission::Open) {
                    return Err(CallError::NotDispatched(
                        "database is closing or closed".into(),
                    ));
                }
                let handler = self.handler.clone();
                client::dispatch_owned(&self.client, move |connection| {
                    Ok(handler(connection, request))
                })
                .map_err(|error| CallError::NotDispatched(error.to_string()))?
            };
            match reply.await {
                Ok(Ok(result)) => result.map_err(CallError::Operation),
                Ok(Err(error)) => Err(CallError::OutcomeUnknown(error.to_string())),
                Err(error) => Err(CallError::OutcomeUnknown(error.to_string())),
            }
        })
    }

    fn close(&self) -> BoxFuture<'_, Result<(), CallError<Infallible>>> {
        Box::pin(async move {
            let reply = {
                let mut admission = self.admission.lock().expect("database admission poisoned");
                admission.refresh();
                match &mut *admission {
                    Admission::Closed => return Ok(()),
                    Admission::Closing(reply) => reply.clone(),
                    Admission::Open => {
                        let (complete, reply) = oneshot::channel();
                        let enqueued = client::close_owned(&self.client, move |result| {
                            let _ = complete.send(result);
                        });
                        match enqueued {
                            Ok(()) => {}
                            Err(Error::Closed) => {
                                *admission = Admission::Closed;
                                return Ok(());
                            }
                            Err(error) => {
                                return Err(CallError::NotDispatched(error.to_string()));
                            }
                        }
                        let reply = async move {
                            reply
                                .await
                                .map_err(|error| error.to_string())?
                                .map_err(|error| error.to_string())
                        }
                        .boxed()
                        .shared();
                        *admission = Admission::Closing(reply.clone());
                        reply
                    }
                }
            };
            reply.await.map_err(CallError::OutcomeUnknown)
        })
    }
}
