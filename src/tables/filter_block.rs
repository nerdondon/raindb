use integer_encoding::FixedInt;
use std::sync::Arc;

use crate::filter_policy::FilterPolicy;

use super::errors::{ReadError, TableResult};

/**
A compressed size for the amount of data covered by a filter.

The currently set range size for filters is 2 KiB (2048 bytes) and the compression is achieved by
taking the base 2 logarithm of 2048 to arrive at 11. The result of a logarithm is an exponent, hence
the name of the constant.
*/
const FILTER_RANGE_SIZE_EXPONENT: u8 = 11;

/// The uncompressed range size in bytes.
const FILTER_RANGE_SIZE_BYTES: u32 = 1 << FILTER_RANGE_SIZE_EXPONENT;

/**
A reader for filter blocks.

# Layout

Filter blocks are layed out in the following format:

1. A sequence of filters
1. A sequence of 4 byte offsets to the filters
1. A 4 byte value representing the byte offset to the start of the offsets vector
1. A 1 byte value representing log(range_size). `range_size` is the size in KiB of the block ranges
   covered by one filter. The log is base 2.
*/
pub struct FilterBlockReader {
    /// A vector of filters.
    filters: Vec<Vec<u8>>,

    /// The filter policy used by the database.
    filter_policy: Arc<Box<dyn FilterPolicy>>,

    /// Offsets of actual data blocks for keys within the filter.
    offsets: Vec<u32>,

    /**
    The range size stored in the file.

    This does not necessarily have to be the same as the range size constant.
    */
    encoded_range_size_exponent: u8,
}

/// Public methods
impl FilterBlockReader {
    /// Create a new instance of a [`FilterBlockReader`].
    pub fn new(
        filter_policy: Arc<Box<dyn FilterPolicy>>,
        filter_data: Vec<u8>,
    ) -> TableResult<Self> {
        if filter_data.len() < 5 {
            // There should be at least a 1 byte for the range size exponent and 4 bytes for the
            // serialized offset vector
            return Err(ReadError::FailedToParse(
                "Failed to parse the filter block.".to_string(),
            ));
        }

        // Get exponent which will be the last element in the data
        let encoded_range_size_exponent = filter_data.pop().unwrap();

        // Get the byte offset to the start of the offsets array. This resides in the 4 bytes right
        // before the range size exponent.
        let raw_offsets_start = &filter_data[filter_data.len() - 4..];
        let offsets_start_index = u32::decode_fixed(raw_offsets_start) as usize;
        let offsets = FilterBlockReader::deserialize_offsets(&filter_data[offsets_start_index..])?;

        let raw_filters = &filter_data[..&filter_data.len() - 4].to_vec();
        let filters =
            FilterBlockReader::deserialize_filters(encoded_range_size_exponent, raw_filters)?;

        Ok(Self {
            filter_policy: Arc::clone(&filter_policy),
            encoded_range_size_exponent,
            offsets,
        })
    }

    pub fn key_may_match(&self, block_offset: u64, key: &[u8]) {
        /*
        LevelDB uses `block_offset >> self.encoded_range_size_exponent` as shorthand for the below.
        We expand it here in an effort to increase readability. Intuitively, a right shift is
        arithmetically equivalent to division by a power of 2 i.e.
        `block_offset / (2 ^ range_size_exponent)`
        */
        let range_size: u64 = 1 << self.encoded_range_size_exponent;
        let filter_index = block_offset / range_size;
    }
}

/// Private methods
impl FilterBlockReader {
    fn deserialize_offsets(raw_offsets: &[u8]) -> TableResult<Vec<u32>> {
        if raw_offsets.len() % 4 != 0 {
            return Err(ReadError::FailedToParse(
                "Failed to parse the filter block. The offset array was malformed.".to_string(),
            ));
        }

        let offsets = raw_offsets
            .chunks(4)
            .map(|chunk| u32::decode_fixed(chunk))
            .collect();

        Ok(offsets)
    }

    fn deserialize_filters(
        range_size_exponent: u32,
        offsets: &[u32],
        raw_filters: &[u8],
    ) -> Vec<Vec<u8>> {
    }
}
