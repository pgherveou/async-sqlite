use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
/// Caller messages, ordered by admission to one worker session.
pub enum Request {
    Hello,
    Call { id: u64, payload: Vec<u8> },
    Close,
}

#[derive(Serialize, Deserialize)]
/// Worker replies, with readiness and close bounding the operation replies.
pub enum Response {
    Ready,
    StartupError(String),
    Reply { id: u64, payload: Vec<u8> },
    Closed(Result<(), String>),
    Failed(String),
}
