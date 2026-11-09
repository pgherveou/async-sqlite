use core::{convert::Infallible, marker::PhantomData};
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use futures_channel::{mpsc, oneshot};
use futures_util::{
    future::{select, BoxFuture, Either},
    StreamExt,
};
use js_sys::Uint8Array;
use serde::{de::DeserializeOwned, Serialize};
use wasm_bindgen::{closure::Closure, JsCast};
use web_sys::{ErrorEvent, Event, MessageEvent, Worker};

use super::wire::{Request, Response};
use crate::{CallError, DatabaseBackend};

type Reply = oneshot::Sender<Result<Vec<u8>, Failure>>;
type CloseReply = oneshot::Sender<Result<(), String>>;

enum Command {
    Call { payload: Vec<u8>, reply: Reply },
    Close,
    Terminate,
}

enum Failure {
    NotDispatched(String),
    Unknown(String),
}

impl Failure {
    fn into_call_error<OperationError>(self) -> CallError<OperationError> {
        match self {
            Self::NotDispatched(reason) => CallError::NotDispatched(reason),
            Self::Unknown(reason) => CallError::OutcomeUnknown(reason),
        }
    }
}

enum Phase {
    Open,
    Closing(Vec<CloseReply>),
    Closed(Result<(), String>),
}

enum WorkerEvent {
    Message(Vec<u8>),
    Failed(String),
}

struct Session {
    worker: Worker,
    _message: Closure<dyn FnMut(MessageEvent)>,
    _error: Closure<dyn FnMut(ErrorEvent)>,
    _message_error: Closure<dyn FnMut(Event)>,
}

impl Session {
    fn new(worker: Worker) -> (Self, mpsc::UnboundedReceiver<WorkerEvent>) {
        let (events, receiver) = mpsc::unbounded();
        let messages = events.clone();
        let message = Closure::new(move |event: MessageEvent| {
            let event = match event.data().dyn_into::<Uint8Array>() {
                Ok(bytes) => WorkerEvent::Message(bytes.to_vec()),
                Err(_) => WorkerEvent::Failed("database reply must contain a Uint8Array".into()),
            };
            let _ = messages.unbounded_send(event);
        });
        let errors = events.clone();
        let error = Closure::new(move |event: ErrorEvent| {
            event.prevent_default();
            let _ = errors.unbounded_send(WorkerEvent::Failed(event.message()));
        });
        let message_error = Closure::new(move |_: Event| {
            let _ = events.unbounded_send(WorkerEvent::Failed(
                "database reply could not be received".into(),
            ));
        });
        worker.set_onmessage(Some(message.as_ref().unchecked_ref()));
        worker.set_onerror(Some(error.as_ref().unchecked_ref()));
        worker.set_onmessageerror(Some(message_error.as_ref().unchecked_ref()));
        (
            Self {
                worker,
                _message: message,
                _error: error,
                _message_error: message_error,
            },
            receiver,
        )
    }

    fn send(&self, request: &Request) -> Result<(), String> {
        let bytes = postcard::to_allocvec(request).map_err(|error| error.to_string())?;
        self.worker
            .post_message(&Uint8Array::from(bytes.as_slice()))
            .map_err(|error| format!("database request could not be sent: {error:?}"))
    }
}

impl Drop for Session {
    fn drop(&mut self) {
        self.worker.set_onmessage(None);
        self.worker.set_onerror(None);
        self.worker.set_onmessageerror(None);
        self.worker.terminate();
    }
}

/// A typed request handle to an independently running database worker.
///
/// The handle contains only channels and admission state, so it can be cloned
/// and used by `Send` futures. Browser objects stay on the event loop that calls
/// `connect`. Requests use an unbounded queue and are never automatically retried.
/// Dropping a waiting future does not cancel an admitted operation.
/// Closing drains admitted requests, closes SQLite, and terminates the worker.
/// A close failure also terminates the worker and cannot be retried on this handle.
pub struct WorkerDatabase<RequestValue, ResponseValue, OperationError> {
    commands: mpsc::UnboundedSender<Command>,
    phase: Arc<Mutex<Phase>>,
    protocol: PhantomData<fn(RequestValue) -> Result<ResponseValue, OperationError>>,
}

impl<RequestValue, ResponseValue, OperationError> Clone
    for WorkerDatabase<RequestValue, ResponseValue, OperationError>
{
    fn clone(&self) -> Self {
        Self {
            commands: self.commands.clone(),
            phase: self.phase.clone(),
            protocol: PhantomData,
        }
    }
}

impl<RequestValue, ResponseValue, OperationError>
    WorkerDatabase<RequestValue, ResponseValue, OperationError>
{
    /// Takes ownership of a worker and waits for its database to be ready.
    ///
    /// The worker must call `serve` or `report_startup_error`. This method owns
    /// its message and error handlers and terminates it on startup failure or
    /// cancellation. A silent worker needs an application-owned startup timeout.
    pub async fn connect(worker: Worker) -> Result<Self, String> {
        let (session, mut events) = Session::new(worker);
        session.send(&Request::Hello)?;
        match events.next().await {
            Some(WorkerEvent::Message(bytes)) => match postcard::from_bytes(&bytes) {
                Ok(Response::Ready) => {}
                Ok(Response::StartupError(reason) | Response::Failed(reason)) => {
                    return Err(reason)
                }
                Ok(_) => return Err("database sent a reply before readiness".into()),
                Err(error) => return Err(error.to_string()),
            },
            Some(WorkerEvent::Failed(reason)) => return Err(reason),
            None => return Err("database worker disconnected during startup".into()),
        }
        let (commands, receiver) = mpsc::unbounded();
        let phase = Arc::new(Mutex::new(Phase::Open));
        let bridge = Bridge {
            session,
            events,
            commands: receiver,
            commands_open: true,
            phase: phase.clone(),
            pending: HashMap::new(),
            next_id: 0,
            close_sent: false,
        };
        wasm_bindgen_futures::spawn_local(bridge.run());
        Ok(Self {
            commands,
            phase,
            protocol: PhantomData,
        })
    }

    /// Stops admission and asks the owning event loop to terminate the worker.
    ///
    /// Dispatched operations without replies receive an unknown outcome. A
    /// transaction may already have committed. This method never retries it.
    pub fn terminate(&self) {
        let mut phase = self
            .phase
            .lock()
            .expect("database admission state poisoned");
        if matches!(*phase, Phase::Closed(_)) {
            return;
        }
        if matches!(*phase, Phase::Open) {
            *phase = Phase::Closing(Vec::new());
        }
        if self.commands.unbounded_send(Command::Terminate).is_err() {
            finish(&mut phase, Err("database worker disconnected".into()));
        }
    }

    fn admit(&self, payload: Vec<u8>, reply: Reply) -> Result<(), String> {
        let phase = self
            .phase
            .lock()
            .expect("database admission state poisoned");
        if !matches!(*phase, Phase::Open) {
            return Err("database is closing or closed".into());
        }
        self.commands
            .unbounded_send(Command::Call { payload, reply })
            .map_err(|_| "database worker disconnected".into())
    }

    fn begin_close(&self, reply: CloseReply) {
        let mut phase = self
            .phase
            .lock()
            .expect("database admission state poisoned");
        match &mut *phase {
            Phase::Open => {
                *phase = Phase::Closing(vec![reply]);
                if self.commands.unbounded_send(Command::Close).is_err() {
                    finish(&mut phase, Err("database worker disconnected".into()));
                }
            }
            Phase::Closing(replies) => replies.push(reply),
            Phase::Closed(result) => {
                let _ = reply.send(result.clone());
            }
        }
    }
}

impl<RequestValue, ResponseValue, OperationError> DatabaseBackend
    for WorkerDatabase<RequestValue, ResponseValue, OperationError>
where
    RequestValue: Serialize + Send + 'static,
    ResponseValue: DeserializeOwned + Send + 'static,
    OperationError: DeserializeOwned + Send + 'static,
{
    type Request = RequestValue;
    type Response = ResponseValue;
    type Error = OperationError;

    fn call(
        &self,
        request: RequestValue,
    ) -> BoxFuture<'_, Result<ResponseValue, CallError<OperationError>>> {
        Box::pin(async move {
            let payload = postcard::to_allocvec(&request)
                .map_err(|error| CallError::NotDispatched(error.to_string()))?;
            let (reply, result) = oneshot::channel();
            self.admit(payload, reply)
                .map_err(CallError::NotDispatched)?;
            let payload = result
                .await
                .map_err(|_| CallError::OutcomeUnknown("database worker disconnected".into()))?
                .map_err(Failure::into_call_error)?;
            let result: Result<ResponseValue, OperationError> = postcard::from_bytes(&payload)
                .map_err(|error| CallError::OutcomeUnknown(error.to_string()))?;
            result.map_err(CallError::Operation)
        })
    }

    fn close(&self) -> BoxFuture<'_, Result<(), CallError<Infallible>>> {
        Box::pin(async move {
            let (reply, result) = oneshot::channel();
            self.begin_close(reply);
            result
                .await
                .unwrap_or_else(|_| Err("database worker disconnected".into()))
                .map_err(CallError::OutcomeUnknown)
        })
    }
}

fn finish(phase: &mut Phase, result: Result<(), String>) {
    if let Phase::Closing(replies) = core::mem::replace(phase, Phase::Closed(result.clone())) {
        for reply in replies {
            let _ = reply.send(result.clone());
        }
    }
}

struct Bridge {
    session: Session,
    events: mpsc::UnboundedReceiver<WorkerEvent>,
    commands: mpsc::UnboundedReceiver<Command>,
    commands_open: bool,
    phase: Arc<Mutex<Phase>>,
    pending: HashMap<u64, Reply>,
    next_id: u64,
    close_sent: bool,
}

enum Incoming {
    Event(Option<WorkerEvent>),
    Command(Option<Command>),
}

impl Bridge {
    async fn run(mut self) {
        let result = self.process().await;
        let reason = result
            .as_ref()
            .err()
            .cloned()
            .unwrap_or_else(|| "database worker closed before replying".into());
        let mut phase = self
            .phase
            .lock()
            .expect("database admission state poisoned");
        finish(&mut phase, result);
        self.commands.close();
        for (_, reply) in self.pending.drain() {
            let _ = reply.send(Err(Failure::Unknown(reason.clone())));
        }
        while let Ok(command) = self.commands.try_recv() {
            if let Command::Call { reply, .. } = command {
                let _ = reply.send(Err(Failure::NotDispatched(reason.clone())));
            }
        }
    }

    async fn process(&mut self) -> Result<(), String> {
        loop {
            let command = if self.commands_open {
                Either::Left(self.commands.next())
            } else {
                Either::Right(core::future::pending())
            };
            let incoming = match select(self.events.next(), command).await {
                Either::Left((event, _)) => Incoming::Event(event),
                Either::Right((command, _)) => Incoming::Command(command),
            };
            match incoming {
                Incoming::Event(Some(WorkerEvent::Message(bytes))) => {
                    match postcard::from_bytes::<Response>(&bytes)
                        .map_err(|error| error.to_string())?
                    {
                        Response::Ready => {}
                        Response::Reply { id, payload } => {
                            let reply = self
                                .pending
                                .remove(&id)
                                .ok_or("database replied to an unknown request")?;
                            let _ = reply.send(Ok(payload));
                        }
                        Response::Closed(result) => {
                            if !self.close_sent || !self.pending.is_empty() {
                                return Err(
                                    "database closed before replying to accepted requests".into()
                                );
                            }
                            return result;
                        }
                        Response::StartupError(reason) | Response::Failed(reason) => {
                            return Err(reason)
                        }
                    }
                }
                Incoming::Event(Some(WorkerEvent::Failed(reason))) => return Err(reason),
                Incoming::Event(None) => return Err("database worker disconnected".into()),
                Incoming::Command(Some(Command::Call { payload, reply })) => {
                    let id = self.next_id;
                    self.next_id = match id.checked_add(1) {
                        Some(next) => next,
                        None => {
                            let _ = reply.send(Err(Failure::NotDispatched(
                                "database request IDs exhausted".into(),
                            )));
                            return Err("database request IDs exhausted".into());
                        }
                    };
                    match self.session.send(&Request::Call { id, payload }) {
                        Ok(()) => {
                            self.pending.insert(id, reply);
                        }
                        Err(reason) => {
                            let _ = reply.send(Err(Failure::NotDispatched(reason.clone())));
                            return Err(reason);
                        }
                    }
                }
                Incoming::Command(Some(Command::Close)) => self.send_close()?,
                Incoming::Command(Some(Command::Terminate)) => {
                    return Err("database worker terminated".into())
                }
                Incoming::Command(None) => {
                    self.commands_open = false;
                    self.send_close()?;
                }
            }
        }
    }

    fn send_close(&mut self) -> Result<(), String> {
        if !self.close_sent {
            self.session.send(&Request::Close)?;
            self.close_sent = true;
        }
        Ok(())
    }
}
