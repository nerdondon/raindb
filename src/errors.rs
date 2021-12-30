/*!
This module contains error types specific to RainDB as well as wrappers and [`From`] implementations
for common errors to enable error propagation.
*/

use std::fmt;
use std::io;
use std::num::TryFromIntError;

use crate::compaction::CompactionWorkerError;
use crate::tables::errors::BuilderError;
use crate::tables::errors::ReadError;
use crate::versioning;

// TODO: consider using snafu (https://docs.rs/snafu/0.6.10/snafu/guide/index.html) to have less boilerplate

pub type RainDBResult<T> = Result<T, RainDBError>;

/**
Top-level database errors.

Consider restructuring based on analysis in [this post] on Rust error handling in
[`std::io::Error`].

[this post]: https://matklad.github.io/2020/10/15/study-of-std-io-error.html
*/
#[derive(Clone, Debug)]
pub enum RainDBError {
    /// Variant for errors stemming from top-level I/O operations.
    IO(DBIOError),

    /// Variant for errors stemming from log file operations.
    Log(LogIOError),

    /// Variant for errors stemming from operating on the table cache.
    TableCache(String),

    /// Variant for errors stemming from reading table files.
    TableRead(ReadError),

    /// Variant for errors stemming from building table files.
    TableBuild(BuilderError),

    /// Variant for errors encountered while servicing a write request.
    Write(String),

    /// Variant for errors encountered while reading from a version.
    VersionRead(versioning::errors::ReadError),

    /// Variant for errors encountered during compaction.
    Compaction(CompactionWorkerError),

    /// Variant for internal key parsing errors.
    KeyParsing(String),

    /// Variant used for one off situations. This should be used sparingly.
    Other(String),
}

impl std::error::Error for RainDBError {}

impl fmt::Display for RainDBError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RainDBError::IO(base_err) => write!(f, "{}", base_err),
            RainDBError::Log(base_err) => write!(f, "{}", base_err),
            RainDBError::TableCache(base_err) => write!(f, "{}", base_err),
            RainDBError::TableRead(base_err) => write!(f, "{}", base_err),
            RainDBError::TableBuild(base_err) => write!(f, "{}", base_err),
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
        RainDBError::IO(err.into())
    }
}

impl From<LogIOError> for RainDBError {
    fn from(err: LogIOError) -> Self {
        RainDBError::Log(err)
    }
}

impl From<ReadError> for RainDBError {
    fn from(err: ReadError) -> Self {
        RainDBError::TableRead(err)
    }
}

impl From<BuilderError> for RainDBError {
    fn from(err: BuilderError) -> Self {
        RainDBError::TableBuild(err)
    }
}

impl From<CompactionWorkerError> for RainDBError {
    fn from(err: CompactionWorkerError) -> Self {
        RainDBError::Compaction(err)
    }
}

/// Metadata describing the corruption detected in a log file.
#[derive(Clone, Debug)]
pub struct LogCorruptionErrorMetadata {
    bytes_corrupted: u64,
    reason: String,
}

/// Errors related to writing to a log file.
#[derive(Clone, Debug)]
pub enum LogIOError {
    /**
    Variant for errors that are related to IO.
    */
    IO(DBIOError),

    /**
    Variant for IO issues where the cause is malformed data on the file system.
    */
    Corruption(LogCorruptionErrorMetadata),

    /**
    Variant for parsing issues that arise specifically from deserializing data from the
    file system.
    */
    Seralization(LogSerializationErrorKind),
}

/**
Different kinds of errors that can arise from serialization and deserialization activities in the
WAL.
*/
#[derive(Clone, Debug)]
pub enum LogSerializationErrorKind {
    FromInt(TryFromIntError),
    Other(String),
}

impl std::error::Error for LogIOError {}

impl fmt::Display for LogIOError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            LogIOError::IO(base_err) => write!(f, "{}", base_err),
            LogIOError::Corruption(err_metadata) => write!(f, "{:?}", err_metadata),
            LogIOError::Seralization(err_metadata) => write!(f, "{:?}", err_metadata),
        }
    }
}

impl From<io::Error> for LogIOError {
    fn from(err: io::Error) -> Self {
        LogIOError::IO(err.into())
    }
}

impl From<TryFromIntError> for LogIOError {
    fn from(err: TryFromIntError) -> Self {
        LogIOError::Seralization(LogSerializationErrorKind::FromInt(err))
    }
}

/// Wrapper for [`std::io::Error`] that implements [`Copy`].
#[derive(Clone, Debug)]
pub struct DBIOError {
    error_kind: io::ErrorKind,

    custom_message: &'static str,
}

/// Crate-only methods
impl DBIOError {
    pub(crate) fn new(error_kind: io::ErrorKind, custom_message: &'static str) -> Self {
        Self {
            error_kind,
            custom_message,
        }
    }
}

impl Copy for DBIOError {}

impl std::error::Error for DBIOError {}

impl fmt::Display for DBIOError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self)
    }
}

impl From<io::Error> for DBIOError {
    /**
    Coerce an [`std::io::Error`] to RainDB's wrapper.

    The custom message is set to the original error's display string because there is currently
    no way to get the custom message that is set on some [`std::io::Error`]'s.
    */
    fn from(io_err: io::Error) -> Self {
        DBIOError {
            error_kind: io_err.kind(),
            custom_message: format!("{}", io_err).as_str(),
        }
    }
}
