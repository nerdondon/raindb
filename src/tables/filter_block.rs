use integer_encoding::FixedInt;
use std::sync::Arc;

use crate::filter_policy::FilterPolicy;

use super::errors::{ReadError, TableReadResult};

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
    filter_policy: Arc<dyn FilterPolicy>,

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
        filter_policy: Arc<dyn FilterPolicy>,
        mut filter_data: Vec<u8>,
    ) -> TableReadResult<Self> {
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

        // Split up the filters according to the deserialized offets for more straight-forward
        // checking later
        let raw_filters = &filter_data[..&filter_data.len() - 4].to_vec();
        let filters = FilterBlockReader::split_filters_with_offset(&offsets, raw_filters);

        Ok(Self {
            filter_policy: Arc::clone(&filter_policy),
            filters,
            encoded_range_size_exponent,
            offsets,
        })
    }

    /// Returns true if the `key` is in the filter.
    pub fn key_may_match(&self, block_offset: u64, key: &[u8]) -> bool {
        if self.filters.is_empty() {
            // There are no filters so just return false
            return false;
        }

        /*
        LevelDB uses `block_offset >> self.encoded_range_size_exponent` as shorthand for the below.
        We expand it here in an effort to increase readability. Intuitively, a right shift is
        arithmetically equivalent to division by a power of 2 i.e.
        `block_offset / (2 ^ range_size_exponent)`
        */
        let range_size: u64 = 1 << self.encoded_range_size_exponent;
        let filter_index = (block_offset / range_size) as usize;
        if filter_index >= self.filters.len() {
            // Like LevelDB we ignore a filter matching error and force a disk seek when one is
            // encountered
            log::warn!("The provided block offset ({}) could not be used to index the filter array. Ignoring the error and returning true so that a disk seek is done.", block_offset);
        } else {
            if self.filters[filter_index].is_empty() {
                // Empty filters do not match any keys
                return false;
            }

            match (*self.filter_policy).key_may_match(key, &self.filters[filter_index]) {
                Err(error) => {
                    log::warn!("There was an error checking the filter for a match. Ignoring the error and forcing a disk seek. Original error: {}", error);
                }
                Ok(filter_result) => return filter_result,
            }
        }

        true
    }
}

/// Private methods
impl FilterBlockReader {
    /// Deserialize the offsets array.
    fn deserialize_offsets(raw_offsets: &[u8]) -> TableReadResult<Vec<u32>> {
        if raw_offsets.len() % 4 != 0 {
            return Err(ReadError::FailedToParse(
                "Failed to parse the filter block. The offset array was malformed.".to_string(),
            ));
        }

        // Offsets are u32 values stored in a fixed-length encoding (4 bytes).
        let offsets = raw_offsets
            .chunks(4)
            .map(|chunk| u32::decode_fixed(chunk))
            .collect();

        Ok(offsets)
    }

    /**
    Get filters by the provided offsets.

    Filters are stored end to end in the byte buffer. This method uses the provided offsets to
    find the ranges representing individual filters and splits the filters up accordingly.
    */
    fn split_filters_with_offset(offsets: &[u32], raw_filters: &[u8]) -> Vec<Vec<u8>> {
        let mut filters: Vec<Vec<u8>> = vec![];

        for offset_index in 0..(offsets.len() - 1) {
            let next_offset_index = offset_index + 1;
            let filter_start_index = offsets[offset_index] as usize;

            if next_offset_index >= offsets.len() {
                // This is the only or the last filter so just push the rest of the range
                filters.push(raw_filters[filter_start_index..].to_vec());
            } else {
                let filter_end_index = (offsets[next_offset_index] - 1) as usize;
                filters.push(raw_filters[filter_start_index..filter_end_index].to_vec())
            }
        }

        filters
    }
}
