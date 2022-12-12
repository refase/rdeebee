use std::str::Utf8Error;

use thiserror::Error;

#[derive(Debug, Error)]
pub enum ClusterNodeError {
    #[error(transparent)]
    SerializationError(#[from] Box<bincode::ErrorKind>),
    #[error(transparent)]
    StringifyErrorJson(#[from] serde_json::Error),
    #[error(transparent)]
    StringifyError(#[from] Utf8Error),
    #[error(transparent)]
    EtcdError(#[from] etcd_client::Error),
    #[error("Error creating server (Node): {}", 0)]
    ServerCreationError(String),
    #[error("Attempting to run leader functions ({0}) as a non-leader")]
    InvalidFunctionAttempt(String),
    #[error("Invalid server state: {}", 0)]
    InvalidState(String),
}
