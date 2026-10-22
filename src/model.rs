use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct Command {
    pub namespace: String,
    pub partition: String,
    pub action: String,
    pub payload: Value,
    pub request_id: String,
    pub now_ms: u64,
}

#[derive(Debug, thiserror::Error, Serialize, Deserialize, Clone, PartialEq, Eq)]
#[error("{code}: {message}")]
pub struct Error {
    pub code: String,
    pub message: String,
}

impl Error {
    pub fn new(code: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            code: code.into(),
            message: message.into(),
        }
    }
}

pub type Result<T> = std::result::Result<T, Error>;
