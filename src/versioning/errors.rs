/*!
This module contains error types specific to version operations as well as wrappers and `From`
implementations for common errors to enable error propagation.
*/

use std::fmt;

use crate::errors::{DBIOError, LogIOError};

/// Alias for a [`Result`] that wraps [`ReadError`].
pub type ReadResult<T> = Result<T, ReadError>;

/// Alias for a [`Result`] that wraps [`WriteError`].
pub type WriteResult<T> = Result<T, WriteError>;

/// Errors that can result from a read operation.
#[derive(Clone, Debug)]
pub enum ReadError {
    /// Variant for keys not found in the version.
    KeyNotFound,

    /// Variant for not having any versions.
    NoVersionsFound,
}

impl std::error::Error for ReadError {}

impl fmt::Display for ReadError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ReadError::KeyNotFound => {
                write!(f, "The key was not found in this version.")
            }
            ReadError::NoVersionsFound => {
                write!(f, "No versions were initialized in the version set.")
            }
        }
    }
}

/// Errors that can result from a write operation.
#[derive(Clone, Debug)]
pub enum WriteError {
    /// Variant for errors writing to the manifest.
    ManifestWrite(ManifestWriteErrorKind),
}

impl std::error::Error for WriteError {}

impl fmt::Display for WriteError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            WriteError::ManifestWrite(base_err) => write!(f, "{:?}", base_err),
        }
    }
}

/// Different errors that can occur when writing to a manifest file.
#[derive(Clone, Debug)]
pub enum ManifestWriteErrorKind {
    /// Variant for errors stemming from log I/O operations.
    LogIO(LogIOError),

    /// Variant for errors that occur swapping the *CURRENT* file.
    SwapCurrentFile(DBIOError),

    /**
    Variant for errors that occur cleaning up side effects after encountering a previous error
    writing to the manifest file.
    */
    ManifestErrorCleanup(DBIOError),
}

impl From<LogIOError> for WriteError {
    fn from(err: LogIOError) -> Self {
        WriteError::ManifestWrite(ManifestWriteErrorKind::LogIO(err))
    }
}
