/*!
This module contains error types specific to version operations as well as wrappers and `From`
implementations for common errors to enable error propagation.
*/

use std::fmt;

use super::version::SeekChargeMetadata;

pub type ReadResult<T> = Result<T, ReadError>;

/// Errors that can result from a read operation.
#[derive(Debug)]
pub enum ReadError {
    /// Variant for keys not found in the version.
    KeyNotFound(SeekChargeMetadata),

    /// Variant for not having any versions.
    NoVersionsFound,
}

impl std::error::Error for ReadError {}

impl fmt::Display for ReadError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ReadError::KeyNotFound(charge_metadata) => {
                write!(f, "The key was not found in this version.")
            }
            ReadError::NoVersionsFound => {
                write!(f, "No versions were initialized in the version set.")
            }
        }
    }
}
