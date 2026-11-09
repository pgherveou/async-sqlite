use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use futures_channel::{mpsc, oneshot};
use futures_util::{
    future::{select, Either},
    StreamExt,
};
use js_sys::Uint8Array;
use wasm_bindgen::{closure::Closure, JsCast};
use web_sys::{ErrorEvent, Event, MessageEvent, Worker};

use super::super::sql;
use super::wire::{Request, Response};
use crate::Error;

type Reply = oneshot::Sender<Result<sql::Response, Error>>;
type CloseReply = oneshot::Sender<Result<(), String>>;

enum Command {
    Call { request: sql::Request, reply: Reply },
    Close,
    Terminate,
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

/// A channel handle to a database worker owned by the connecting event loop.
#[derive(Clone)]
pub struct Backend {
    commands: mpsc::UnboundedSender<Command>,
    phase: Arc<Mutex<Phase>>,
}

impl Backend {
    /// Takes ownership of a worker and waits for its database to be ready.
    ///
    /// The worker must call `serve` or `report_startup_error`. This method owns
    /// its message and error handlers and terminates it on startup failure or
    /// cancellation. A silent worker needs an application-owned startup timeout.
    pub async fn connect(worker: Worker) -> Result<Self, Error> {
        let (session, mut events) = Session::new(worker);
        let ready = async {
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
            Ok(())
        }
        .await;
        ready.map_err(|message| Error::NotDispatched { message })?;
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
        Ok(Self { commands, phase })
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

    fn admit(&self, request: sql::Request, reply: Reply) -> Result<(), String> {
        let phase = self
            .phase
            .lock()
            .expect("database admission state poisoned");
        if !matches!(*phase, Phase::Open) {
            return Err("database is closing or closed".into());
        }
        self.commands
            .unbounded_send(Command::Call { request, reply })
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

    /// Dispatches SQL without canceling admitted work when its caller stops waiting.
    pub async fn dispatch(&self, request: sql::Request) -> Result<sql::Response, Error> {
        let (reply, result) = oneshot::channel();
        self.admit(request, reply)
            .map_err(|message| Error::NotDispatched { message })?;
        result.await.map_err(|_| Error::OutcomeUnknown {
            message: "database worker disconnected".into(),
        })?
    }

    /// Drains accepted work and closes SQLite before terminating its worker.
    pub async fn close(&self) -> Result<(), Error> {
        let (reply, result) = oneshot::channel();
        self.begin_close(reply);
        result
            .await
            .unwrap_or_else(|_| Err("database worker disconnected".into()))
            .map_err(|message| Error::OutcomeUnknown { message })
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
            let _ = reply.send(Err(Error::OutcomeUnknown {
                message: reason.clone(),
            }));
        }
        while let Ok(command) = self.commands.try_recv() {
            if let Command::Call { reply, .. } = command {
                let _ = reply.send(Err(Error::NotDispatched {
                    message: reason.clone(),
                }));
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
                        Response::Reply { id, result } => {
                            let reply = self
                                .pending
                                .remove(&id)
                                .ok_or("database replied to an unknown request")?;
                            let _ = reply.send(result.map_err(Into::into));
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
                Incoming::Command(Some(Command::Call { request, reply })) => {
                    let id = self.next_id;
                    self.next_id = match id.checked_add(1) {
                        Some(next) => next,
                        None => {
                            let _ = reply.send(Err(Error::NotDispatched {
                                message: "database request IDs exhausted".into(),
                            }));
                            return Err("database request IDs exhausted".into());
                        }
                    };
                    match self.session.send(&Request::Call { id, request }) {
                        Ok(()) => {
                            self.pending.insert(id, reply);
                        }
                        Err(reason) => {
                            let _ = reply.send(Err(Error::NotDispatched {
                                message: reason.clone(),
                            }));
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
