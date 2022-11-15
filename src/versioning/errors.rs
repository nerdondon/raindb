// Copyright (c) 2021 Google LLC
//
// Use of this source code is governed by an MIT-style
// license that can be found in the LICENSE file or at
// https://opensource.org/licenses/MIT.

/*!
This module contains error types specific to version operations as well as wrappers and `From`
implementations for common errors to enable error propagation.
*/

use std::fmt;

use crate::errors::{DBIOError, LogIOError};
use crate::tables;

use super::version::SeekChargeMetadata;

/// Alias for a [`Result`] that wraps [`ReadError`].
pub(crate) type ReadResult<T> = Result<T, ReadError>;

/// Alias for a [`Result`] that wraps [`WriteError`].
pub type WriteResult<T> = Result<T, WriteError>;

/// Alias for a [`Result`] that wraps [`RecoverError`].
pub type RecoverResult<T> = Result<T, RecoverError>;

/// Errors that can result from a read operation.
#[derive(Clone, Debug)]
pub(crate) enum ReadError {
    /// Variant for errors that occur when attempting to read a table.
    TableRead((tables::errors::ReadError, Option<SeekChargeMetadata>)),
}

impl std::error::Error for ReadError {}

impl fmt::Display for ReadError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ReadError::TableRead((base_err, _)) => {
                write!(
                    f,
                    "The key was not found in this version. Table read error: {}",
                    base_err
                )
            }
        }
    }
}

/// Errors that can result from a write operation.
#[derive(Clone, Debug, PartialEq, Eq)]
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
#[derive(Clone, Debug, PartialEq, Eq)]
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

/// Errors that can result from a recovery operation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RecoverError {
    /// Variant for issues reading and parsing the CURRENT file.
    CurrentFileRead(CurrentFileReadErrorKind),

    /// Variant for issues reading the manifest file.
    ManifestRead(LogIOError),

    /**
    Variant for parsing issues with [`VersionChangeManifest`].

    [`VersionChangeManifest`]: super::version_manifest::VersionChangeManifest
    */
    ManifestParse(String),

    /// Variant for issues where manifest values are parsed correctly but do not have valid data.
    ManifestCorruption(String),
}

/// Enum to describe various kinds of errors when reading the CURRENT file
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CurrentFileReadErrorKind {
    /// Variant for IO errors reading the file from disk.
    IO(DBIOError),

    /// Variant for CURRENT file parsing errors.
    Parse(String),
}

impl std::error::Error for RecoverError {}

impl fmt::Display for RecoverError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RecoverError::CurrentFileRead(base_err) => {
                write!(f, "{base_err:?}", base_err = base_err)
            }
            RecoverError::ManifestRead(base_err) => {
                write!(f, "{base_err}", base_err = base_err)
            }
            RecoverError::ManifestParse(base_err) => {
                write!(
                    f,
                    "There was an error parsing the manifest record read from disk. Error: \
                    {base_err}",
                    base_err = base_err
                )
            }
            RecoverError::ManifestCorruption(base_err) => {
                write!(f, "{base_err}", base_err = base_err)
            }
        }
    }
}
