/*!
This module contains error types specific to RainDB as well as wrappers and `From` implementations
for common errors to enable error propagation.
*/

use std::fmt;
use std::io;
use std::num::TryFromIntError;

// TODO: consider using snafu (https://docs.rs/snafu/0.6.10/snafu/guide/index.html) to have less boilerplate

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
    Variant for errors that are related to `try_from` conversions.
    */
    Parse(TryFromIntError),

    /**
    Variant for IO issues where the cause is malformed data on the file system.
    */
    Corruption(WALCorruptionErrorMetadata),

    /**
    Variant for parsing issues that arise specifically from deserializing data from the
    file system.
    */
    Serializer(bincode::Error),
}

impl std::error::Error for WALIOError {}

impl fmt::Display for WALIOError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            WALIOError::Parse(base_err) => write!(f, "{}", base_err),
            WALIOError::IO(base_err) => write!(f, "{}", base_err),
            WALIOError::Corruption(err_metadata) => write!(f, "{:?}", err_metadata),
            WALIOError::Serializer(base_err) => write!(f, "{:?}", base_err),
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
        WALIOError::Parse(err)
    }
}

impl From<bincode::Error> for WALIOError {
    fn from(err: bincode::Error) -> Self {
        WALIOError::Serializer(err)
    }
}
