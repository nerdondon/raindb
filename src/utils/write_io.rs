//! Contains helpers for structs that implement [`std::io::Write`].

use std::io::{self, Write};

use integer_encoding::VarIntWriter;

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
