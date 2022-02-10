//! Contains helpers for structs that implement I/O traits.

use std::io::{self, Read, Write};

use integer_encoding::VarIntReader;
use integer_encoding::VarIntWriter;

use crate::config::MAX_NUM_LEVELS;

/**
Helpers that will be blanket implemented for structs that implement the [`std::io::Write`] trait.
*/
pub(crate) trait WriteHelpers {
    /**
    Write a slice to the buffer with the length of the slice prefixed with a varint-32 encoding.

    # Legacy

    This is synonomous to LevelDB's `leveldb::PutLengthPrefixedSlice` method.
    */
    fn write_length_prefixed_slice(&mut self, slice: &[u8]) -> io::Result<usize>;
}

impl<W: Write> WriteHelpers for W {
    fn write_length_prefixed_slice(&mut self, slice: &[u8]) -> io::Result<usize> {
        let mut bytes_written: usize = 0;
        bytes_written += self.write_varint(slice.len() as u32)?;
        self.write_all(slice)?;
        bytes_written += slice.len();

        Ok(bytes_written)
    }
}

/**
Helpers that will be blanket implemented for structs that implement the [`std::io::Read`] trait.
*/
pub(crate) trait ReadHelpers {
    /// Read and decode a stored varint32-encoded level.
    fn read_raindb_level(&mut self) -> io::Result<usize>;

    /**
    Read the value encoded as a varint32 length prefixed slice and return the value.

    # Legacy

    This is synonomous to LevelDB's `leveldb::GetLengthPrefixedSlice` method.
    */
    fn read_length_prefixed_slice(&mut self) -> io::Result<Vec<u8>>;
}

impl<R: Read> ReadHelpers for R {
    fn read_raindb_level(&mut self) -> io::Result<usize> {
        let level = self.read_varint::<u32>()? as usize;
        if level >= MAX_NUM_LEVELS {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("The level was invalid. Found {level}", level = level),
            ));
        }

        Ok(level)
    }

    fn read_length_prefixed_slice(&mut self) -> io::Result<Vec<u8>> {
        let length = self.read_varint::<u32>()? as usize;
        let mut buf = vec![0_u8; length];
        self.read_exact(&mut buf)?;

        Ok(buf)
    }
}
