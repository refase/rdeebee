use std::{path::PathBuf, time::SystemTimeError, num::ParseIntError, convert::Infallible};

use thiserror::Error;

#[derive(Debug, Error)]
pub enum StorageEngineError {
    #[error("No writer on SSTable: {0}")]
    InvalidSSTableWriter(PathBuf),
    #[error("Invalid SSTable File path: {0}")]
    InvalidSSTableFilePath(PathBuf),
    #[error("Invalid Wal File path: {0}")]
    InvalidWalFilePath(PathBuf),
    #[error("Invalid DB directory: {0}")]
    InvalidDbDir(PathBuf),
    #[error("Failed to create SSTable for epoch: {0}")]
    FailedSSTableCreation(u128),
    #[error("Failed to get MemTable")]
    InvalidMemTable,
    #[error(transparent)]
    TimeError(#[from] SystemTimeError),
    #[error(transparent)]
    EpochParseError(#[from] ParseIntError),
    #[error(transparent)]
    PathError(#[from] Infallible),
    #[error(transparent)]
    IOError(#[from] std::io::Error),
    #[error(transparent)]
    SerializationError(#[from] bincode::Error),
}
