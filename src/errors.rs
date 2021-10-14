/*!
This module contains error types specific to RainDB as well as wrappers and `From` implementations
for common errors to enable error propagation.
*/

use std::fmt;
use std::io;
use std::num::TryFromIntError;

// TODO: consider using snafu (https://docs.rs/snafu/0.6.10/snafu/guide/index.html) to have less boilerplate

/**
Errors related to writing to the write-ahead log (WAL).
*/
#[derive(Debug)]
pub enum WALWriteError {
    IOError(io::Error),
    ParseError(TryFromIntError),
}

impl std::error::Error for WALWriteError {}

impl fmt::Display for WALWriteError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            WALWriteError::ParseError(baseErr) => write!(f, "{}", baseErr),
            WALWriteError::IOError(baseErr) => write!(f, "{}", baseErr),
        }
    }
}

impl From<std::io::Error> for WALWriteError {
    fn from(err: std::io::Error) -> Self {
        WALWriteError::IOError(err)
    }
}

impl From<TryFromIntError> for WALWriteError {
    fn from(err: TryFromIntError) -> Self {
        WALWriteError::ParseError(err)
    }
}
