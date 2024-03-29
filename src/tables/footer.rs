// Copyright (c) 2021 Google LLC
//
// Use of this source code is governed by an MIT-style
// license that can be found in the LICENSE file or at
// https://opensource.org/licenses/MIT.

use integer_encoding::FixedInt;
use std::convert::TryFrom;

use super::block_handle::BlockHandle;
use super::errors::{FooterError, ReadError, TableReadResult};

/// Result that wraps [`FooterError`].
type FooterResult<T> = Result<T, FooterError>;

/**
The fixed size of a footer.

Refer to the [`Footer`] documentation for how this number is calculated.
*/
pub(crate) const SIZE_OF_FOOTER_BYTES: usize = 48;

/**
A magic number to provide padding.

The magic number was picked summing the [`u8`] representation of each charcter in the string
"batmann and robin".
*/
#[cfg(not(feature = "strict"))]
const TABLE_MAGIC_NUMBER: u64 = 1646;

/**
A magic number to provide padding.

The magic number was picked by running `echo http://code.google.com/p/leveldb/ | sha1sum` and
taking the leading 64 bits.
*/
#[cfg(feature = "strict")]
const TABLE_MAGIC_NUMBER: u64 = 0xdb4775248b80fb57;

/**
The footer of a table file.

A table file's footer consists of the following parts:
- A block handle for the metaindex
- A block handle for the index
- 40 zeroed bytes - (size of metaindex block handle) - (size of index block handle) to make the
  footer a fixed length
    - 40 comes from (2 * [`crate::block_handle::BLOCK_HANDLE_MAX_ENCODED_LENGTH`])
- 8-byte `TABLE_MAGIC_NUMBER`
*/
#[derive(Debug, Eq, PartialEq)]
pub(crate) struct Footer {
    metaindex_handle: BlockHandle,
    index_handle: BlockHandle,
}

/// Public methods
impl Footer {
    /// Create a new instance of [`Footer`].
    pub fn new(metaindex_handle: BlockHandle, index_handle: BlockHandle) -> Self {
        Self {
            metaindex_handle,
            index_handle,
        }
    }

    /// Get a reference to the metaindex block handle.
    pub fn get_metaindex_handle(&self) -> &BlockHandle {
        &self.metaindex_handle
    }

    /// Get a reference to the index block handle.
    pub fn get_index_handle(&self) -> &BlockHandle {
        &self.index_handle
    }
}

impl TryFrom<&Vec<u8>> for Footer {
    type Error = ReadError;

    fn try_from(value: &Vec<u8>) -> TableReadResult<Footer> {
        if value.len() != SIZE_OF_FOOTER_BYTES {
            return Err(ReadError::FailedToParse(
                "The length of the buffer was not equal to the fixed size of a footer.".to_string(),
            ));
        }

        let magic_number_ptr = value.len() - 8;
        let magic_number = u64::decode_fixed(&value[magic_number_ptr..]);
        if magic_number != TABLE_MAGIC_NUMBER {
            return Err(ReadError::FailedToParse(
                "The magic number was incorrect. This is not a table file.".to_string(),
            ));
        }

        let (metaindex_handle, bytes_read) = BlockHandle::deserialize(value)?;
        let (index_handle, _bytes_read) = BlockHandle::deserialize(&value[bytes_read..])?;

        Ok(Footer::new(metaindex_handle, index_handle))
    }
}

impl TryFrom<&Footer> for Vec<u8> {
    type Error = FooterError;

    fn try_from(value: &Footer) -> FooterResult<Vec<u8>> {
        let mut buf: Vec<u8> = vec![];
        let mut serialized_metaindex_handle = Vec::<u8>::from(&value.metaindex_handle);
        let mut serialized_index_handle = Vec::<u8>::from(&value.index_handle);
        let length_of_padding =
            40 - serialized_metaindex_handle.len() - serialized_index_handle.len();
        let mut zero_padding: Vec<u8> = vec![0; length_of_padding];

        buf.append(&mut serialized_metaindex_handle);
        buf.append(&mut serialized_index_handle);
        buf.append(&mut zero_padding);
        buf.append(&mut TABLE_MAGIC_NUMBER.encode_fixed_vec());

        if buf.len() != SIZE_OF_FOOTER_BYTES {
            return Err(FooterError::FooterSerialization(buf.len()));
        }

        Ok(buf)
    }
}

#[cfg(test)]
mod tests {
    use pretty_assertions::assert_eq;

    use super::*;

    #[test]
    fn footer_can_be_serialized_and_deserialized() {
        let metaindex_handle = BlockHandle::new(80, 20);
        let index_handle = BlockHandle::new(100, 20);
        let footer = Footer::new(metaindex_handle, index_handle);

        let serialized = Vec::<u8>::try_from(&footer).unwrap();
        let deserialized = Footer::try_from(&serialized).unwrap();

        assert_eq!(footer, deserialized);
    }
}
