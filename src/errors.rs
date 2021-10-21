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
struct WALCorruptionErrorMetadata {
    bytes_corrupted: u64,
    reason: String,
}

/**
Errors related to writing to the write-ahead log (WAL).
*/
#[derive(Debug)]
pub enum WALIOError {
    /**
    Variant for errors that are related to IO.
    */
    IOError(io::Error),

    /**
    Variant for errors that are related to `try_from` conversions.
    */
    ParseError(TryFromIntError),

    /**
    Variant for IO issues where the cause is malformed data on the file system.
    */
    CorruptionError(WALCorruptionErrorMetadata),

    /**
    Variant for parsing issues that arise specifically from deserializing data from the
    file system.
    */
    SerializerError(bincode::Error),
}

impl std::error::Error for WALIOError {}

impl fmt::Display for WALIOError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            WALIOError::ParseError(baseErr) => write!(f, "{}", baseErr),
            WALIOError::IOError(baseErr) => write!(f, "{}", baseErr),
            WALIOError::CorruptionError(errMetadata) => write!(f, "{:?}", errMetadata),
            WALIOError::SerializerError(baseErr) => write!(f, "{:?}", baseErr),
        }
    }
}

impl From<std::io::Error> for WALIOError {
    fn from(err: std::io::Error) -> Self {
        WALIOError::IOError(err)
    }
}

impl From<TryFromIntError> for WALIOError {
    fn from(err: TryFromIntError) -> Self {
        WALIOError::ParseError(err)
    }
}

impl From<bincode::Error> for WALIOError {
    fn from(err: bincode::Error) -> Self {
        WALIOError::SerializerError(err)
    }
}
