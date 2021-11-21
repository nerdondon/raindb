/*!
This module contains error types specific to table operations as well as wrappers and `From`
implementations for common errors to enable error propagation.
*/

use std::{fmt, io};

use super::footer::SIZE_OF_FOOTER_BYTES;

/// Result that wraps [`ReadError`].
pub type TableResult<T> = Result<T, ReadError>;

/// Errors that can result from a read operation.
#[derive(Debug)]
pub enum ReadError {
    /// Variant for parsing errors.
    FailedToParse(String),
    /// Variant for footer serialiation errors where the value is the size of the serialized buffer.
    FooterSerialization(usize),
    /// Variant for block decompression issues.
    BlockDecompression(io::Error),
    /// Variant for IO errors.
    IO(io::Error),
}

impl std::error::Error for ReadError {}

impl fmt::Display for ReadError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ReadError::FailedToParse(msg) => {
                write!(f, "{}", msg)
            }
            ReadError::FooterSerialization(actual_buffer_size) => {
                write!(f, "Failed to serialize the footer. The length of the serialized buffer was expected to be {} but was {}", SIZE_OF_FOOTER_BYTES, actual_buffer_size)
            }
            ReadError::BlockDecompression(base_err) => {
                write!(
                    f,
                    "Failed to decompress a block from the file. The original error was {}",
                    base_err
                )
            }
            ReadError::IO(base_err) => write!(f, "{}", base_err),
        }
    }
}

impl From<io::Error> for ReadError {
    fn from(err: io::Error) -> Self {
        ReadError::IO(err)
    }
}
