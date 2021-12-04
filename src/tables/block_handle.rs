use integer_encoding::VarInt;
use std::convert::TryFrom;

use super::errors::{ReadError, TableResult};

/**
The maximum encoded size that a `BlockHandle` can be.

varint64 values are a maximum of 10 bytes.

A nice [explanation](https://carlmastrangelo.com/blog/lets-make-a-varint) of variable length
integers.
*/
pub(crate) const BLOCK_HANDLE_MAX_ENCODED_LENGTH_BYTES: u64 = 10 + 10;

/**
A block handle consists of two varint64 values representing the size and the offset of the block in
the file.
*/
#[derive(Debug)]
pub(crate) struct BlockHandle {
    /// The offset in the raw byte buffer that the block resides.
    offset: u64,

    /// The size of the block.
    size: u64,
}

impl BlockHandle {
    /// Create a new instance of [`BlockHandle`].
    pub fn new(offset: u64, size: u64) -> Self {
        Self { offset, size }
    }

    /// Get the offset to the block this handle is for.
    pub fn get_offset(&self) -> u64 {
        self.offset
    }

    /// Get the size of the block this handle is for.
    pub fn get_size(&self) -> u64 {
        self.offset
    }

    /**
    Deserialize a [`BlockHandle`] from a buffer.

    Returns a tuple of the deserialized block handle and the length of the range within the
    buffer that contained the block handle.
    */
    pub fn deserialize(buf: &[u8]) -> TableResult<(BlockHandle, usize)> {
        let maybe_decoded_offset = u64::decode_var(buf);
        if maybe_decoded_offset.is_none() {
            return Err(ReadError::FailedToParse(
                "Failed to deserialize the block handle offset.".to_string(),
            ));
        }

        let (offset, offset_occupied_length) = maybe_decoded_offset.unwrap();
        let maybe_decoded_size = u64::decode_var(&buf[offset_occupied_length..]);
        if maybe_decoded_size.is_none() {
            return Err(ReadError::FailedToParse(
                "Failed to deserialize the block handle offset.".to_string(),
            ));
        }

        let (size, size_occuppied_length) = maybe_decoded_size.unwrap();
        let bytes_read = offset_occupied_length + size_occuppied_length;

        Ok((BlockHandle::new(offset, size), bytes_read))
    }
}

impl TryFrom<&[u8]> for BlockHandle {
    type Error = ReadError;

    fn try_from(value: &[u8]) -> TableResult<BlockHandle> {
        let (handle, _bytes_read) = BlockHandle::deserialize(value)?;
        Ok(handle)
    }
}

impl TryFrom<&Vec<u8>> for BlockHandle {
    type Error = ReadError;

    fn try_from(value: &Vec<u8>) -> TableResult<BlockHandle> {
        let (handle, _bytes_read) = BlockHandle::deserialize(value)?;
        Ok(handle)
    }
}

impl From<&BlockHandle> for Vec<u8> {
    fn from(value: &BlockHandle) -> Vec<u8> {
        let encoded_offset = value.offset.encode_var_vec();
        let encoded_size = value.size.encode_var_vec();
        [encoded_offset, encoded_size].concat()
    }
}
