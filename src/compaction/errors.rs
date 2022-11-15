// Copyright (c) 2022 Google LLC
//
// Use of this source code is governed by an MIT-style
// license that can be found in the LICENSE file or at
// https://opensource.org/licenses/MIT.

use std::fmt;
use std::io;

use crate::errors::{DBIOError, RainDBError};
use crate::versioning::errors::WriteError;

/// Type alias for [`Result`]'s with [`CompactionWorkerError`]'s.
pub(crate) type CompactionWorkerResult<T> = Result<T, CompactionWorkerError>;

/// Errors that occur during compaction worker operations.
#[derive(Clone, Debug, PartialEq)]
pub enum CompactionWorkerError {
    /// Variant for IO errors encountered during worker operations.
    IO(DBIOError),

    /// Variant for issues that occur when writing a table file.
    WriteTable(Box<RainDBError>),

    /// Variant for issues that occur when persisting and applying a version change manifest.
    VersionManifestError(WriteError),

    /// Variant for halting compactions due to unexpected state.
    UnexpectedState(String),
}

impl std::error::Error for CompactionWorkerError {}

impl fmt::Display for CompactionWorkerError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CompactionWorkerError::IO(base_err) => write!(f, "{}", base_err),
            CompactionWorkerError::WriteTable(base_err) => write!(f, "{}", base_err),
            CompactionWorkerError::VersionManifestError(base_err) => write!(f, "{}", base_err),
            CompactionWorkerError::UnexpectedState(reason) => write!(f, "{}", reason),
        }
    }
}

impl From<io::Error> for CompactionWorkerError {
    fn from(err: io::Error) -> Self {
        CompactionWorkerError::IO(err.into())
    }
}

impl From<WriteError> for CompactionWorkerError {
    fn from(err: WriteError) -> Self {
        CompactionWorkerError::VersionManifestError(err)
    }
}
