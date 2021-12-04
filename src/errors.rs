/*!
This module contains error types specific to RainDB as well as wrappers and `From` implementations
for common errors to enable error propagation.
*/

use std::fmt;
use std::io;
use std::num::TryFromIntError;

use crate::compaction::CompactionWorkerError;
use crate::tables::errors::ReadError;
use crate::versioning;

// TODO: consider using snafu (https://docs.rs/snafu/0.6.10/snafu/guide/index.html) to have less boilerplate

pub type RainDBResult<T> = Result<T, RainDBError>;

/// Top-level database errors.
#[derive(Debug)]
pub enum RainDBError {
    /// Variant for errors stemming from top-level I/O operations.
    IO(io::Error),

    /// Variant for errors stemming from WAL operations.
    WAL(WALIOError),

    /// Variant for errors stemming from operating on the table cache.
    TableCache(String),

    /// Variant for errors stemming from reading table files.
    TableRead(ReadError),

    /// Variant for errors encountered while servicing a write request.
    Write(String),

    /// Variant for errors encountered while reading from a version.
    VersionRead(versioning::errors::ReadError),

    /// Variant for errors encountered during compaction.
    Compaction(CompactionWorkerError),

    /// Variant for lookup key parsing errors.
    KeyParsing(String),

    /// Variant used for one off situations. This should be used sparingly.
    Other(String),
}

impl std::error::Error for RainDBError {}

impl fmt::Display for RainDBError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RainDBError::IO(base_err) => write!(f, "{}", base_err),
            RainDBError::WAL(base_err) => write!(f, "{}", base_err),
            RainDBError::TableCache(base_err) => write!(f, "{}", base_err),
            RainDBError::TableRead(base_err) => write!(f, "{}", base_err),
            RainDBError::Write(base_err) => write!(f, "{}", base_err),
            RainDBError::VersionRead(base_err) => write!(f, "{}", base_err),
            RainDBError::Compaction(base_err) => write!(f, "{}", base_err),
            RainDBError::KeyParsing(base_err) => write!(f, "{}", base_err),
            RainDBError::Other(base_err) => write!(f, "{}", base_err),
        }
    }
}

impl From<io::Error> for RainDBError {
    fn from(err: io::Error) -> Self {
        RainDBError::IO(err)
    }
}

impl From<WALIOError> for RainDBError {
    fn from(err: WALIOError) -> Self {
        RainDBError::WAL(err)
    }
}

impl From<ReadError> for RainDBError {
    fn from(err: ReadError) -> Self {
        RainDBError::TableRead(err)
    }
}

/// Metadata describing the corruption detected in the WAL.
#[derive(Debug)]
pub struct WALCorruptionErrorMetadata {
    bytes_corrupted: u64,
    reason: String,
}

/// Errors related to writing to the write-ahead log (WAL).
#[derive(Debug)]
pub enum WALIOError {
    /**
    Variant for errors that are related to IO.
    */
    IO(io::Error),

    /**
    Variant for IO issues where the cause is malformed data on the file system.
    */
    Corruption(WALCorruptionErrorMetadata),

    /**
    Variant for parsing issues that arise specifically from deserializing data from the
    file system.
    */
    Seralization(WALSerializationErrorKind),
}

/**
Different kinds of errors that can arise from serialization and deserialization activities in the
WAL.
*/
#[derive(Debug)]
pub enum WALSerializationErrorKind {
    FromInt(TryFromIntError),
    Other(String),
}

impl std::error::Error for WALIOError {}

impl fmt::Display for WALIOError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            WALIOError::IO(base_err) => write!(f, "{}", base_err),
            WALIOError::Corruption(err_metadata) => write!(f, "{:?}", err_metadata),
            WALIOError::Seralization(err_metadata) => write!(f, "{:?}", err_metadata),
        }
    }
}

impl From<io::Error> for WALIOError {
    fn from(err: io::Error) -> Self {
        WALIOError::IO(err)
    }
}

impl From<TryFromIntError> for WALIOError {
    fn from(err: TryFromIntError) -> Self {
        WALIOError::Seralization(WALSerializationErrorKind::FromInt(err))
    }
}
