/*!
This module contains error types specific to table operations as well as wrappers and `From`
implementations for common errors to enable error propagation.
*/

use std::{fmt, io};

use crate::errors::DBIOError;

use super::footer::SIZE_OF_FOOTER_BYTES;

/// Result that wraps [`ReadError`].
pub type TableReadResult<T> = Result<T, ReadError>;

/// Errors that can result from a read operation.
#[derive(Clone, Debug)]
pub enum ReadError {
    /// Variant for parsing errors.
    FailedToParse(String),

    /// Variant for errors that occur when handling table file footers.
    Footer(FooterError),

    /// Variant for block decompression issues.
    BlockDecompression(DBIOError),

    /// Variant for IO errors.
    IO(DBIOError),

    /// Variant for errors reading the filter block.
    FilterBlock(String),
}

impl std::error::Error for ReadError {}

impl fmt::Display for ReadError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ReadError::FailedToParse(msg) => {
                write!(f, "{}", msg)
            }
            ReadError::Footer(base_err) => {
                write!(f, "{}", base_err)
            }
            ReadError::BlockDecompression(base_err) => {
                write!(
                    f,
                    "Failed to decompress a block from the file. The original error was {}",
                    base_err
                )
            }
            ReadError::IO(base_err) => write!(f, "{}", base_err),
            ReadError::FilterBlock(base_err) => write!(f, "Failed attempting to read the filter block. Continuing without filters. Original error: {}", base_err),
        }
    }
}

impl From<io::Error> for ReadError {
    fn from(err: io::Error) -> Self {
        ReadError::IO(err.into())
    }
}

impl From<FooterError> for ReadError {
    fn from(err: FooterError) -> Self {
        ReadError::Footer(err)
    }
}

/// Errors that can result from operations involving the table file footer.
#[derive(Clone, Debug)]
pub enum FooterError {
    /// Variant for footer serialiation errors where the value is the size of the serialized buffer.
    FooterSerialization(usize),
}

impl std::error::Error for FooterError {}

impl fmt::Display for FooterError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            FooterError::FooterSerialization(actual_buffer_size) => {
                write!(f, "Failed to serialize the footer. The length of the serialized buffer was expected to be {} but was {}", SIZE_OF_FOOTER_BYTES, actual_buffer_size)
            }
        }
    }
}

/// Errors that can result from a table file builder operations.
#[derive(Clone, Debug)]
pub enum BuilderError {
    /// Variant for IO errors.
    IO(DBIOError),

    /// Variant for operations that were performed when the file was already closed.
    AlreadyClosed,

    /// Variant for attempting to add a key that is out of order.
    OutOfOrder,

    /// Variant for errors that occur when handling table file footers.
    Footer(FooterError),
}

impl std::error::Error for BuilderError {}

impl fmt::Display for BuilderError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            BuilderError::IO(base_err) => write!(f, "{}", base_err),
            BuilderError::AlreadyClosed => write!(
                f,
                "Attempted to perform an operation when the file had already been closed."
            ),
            BuilderError::OutOfOrder => {
                write!(f, "Attempted to add a key but it was out of order.")
            }
            BuilderError::Footer(base_err) => write!(f, "{}", base_err),
        }
    }
}

impl From<io::Error> for BuilderError {
    fn from(err: io::Error) -> Self {
        BuilderError::IO(err.into())
    }
}

impl From<FooterError> for BuilderError {
    fn from(err: FooterError) -> Self {
        BuilderError::Footer(err)
    }
}
