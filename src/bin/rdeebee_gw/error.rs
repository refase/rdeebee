use thiserror::Error;

#[derive(Error, Debug)]
pub enum CustomHttpErrors {
    #[error("Invalid length received")]
    InvalidLength,
    #[error("Could not fetch body: {0}")]
    InvalidBody(String),
    #[error("Could not copy response body: {0}")]
    IoCopyErr(String),
    #[error("Failed to flush the buffer: {0}")]
    FailedBufFlush(String),
}