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
        let offsets = FilterBlockReader::deserialize_offsets(
            &filter_data[offsets_start_index..(filter_data.len() - 4)],
        )?;

        // Split up the filters according to the deserialized offets for more straight-forward
        // checking later
        let raw_filters = &filter_data[..offsets_start_index].to_vec();
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
            // There are no filters so return true and force a disk seek
            return true;
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
            log::warn!(
                "The provided block offset ({}) could not be used to index the filter array. \
                Ignoring the error and returning true so that a disk seek is done.",
                block_offset
            );
        } else {
            if self.filters[filter_index].is_empty() {
                // Empty filters do not match any keys
                return false;
            }

            match (*self.filter_policy).key_may_match(key, &self.filters[filter_index]) {
                Err(error) => {
                    log::warn!(
                        "There was an error checking the filter for a match. Ignoring the error \
                        and forcing a disk seek. Original error: {}",
                        error
                    );
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
        let offsets = raw_offsets.chunks(4).map(u32::decode_fixed).collect();

        Ok(offsets)
    }

    /**
    Get filters by the provided offsets.

    Filters are stored end to end in the byte buffer. This method uses the provided offsets to
    find the ranges representing individual filters and splits the filters up accordingly.
    */
    fn split_filters_with_offset(offsets: &[u32], raw_filters: &[u8]) -> Vec<Vec<u8>> {
        let mut filters: Vec<Vec<u8>> = vec![];

        for offset_index in 0..offsets.len() {
            let next_offset_index = offset_index + 1;
            let filter_start_index = offsets[offset_index] as usize;

            if next_offset_index >= offsets.len() {
                // This is the only or the last filter so just push the rest of the range
                filters.push(raw_filters[filter_start_index..].to_vec());
            } else {
                let next_filter_start_index = offsets[next_offset_index] as usize;
                filters.push(raw_filters[filter_start_index..next_filter_start_index].to_vec())
            }
        }

        filters
    }
}

#[cfg(test)]
mod tests {
    use pretty_assertions::assert_eq;

    use crate::key::{InternalKey, RainDbKeyType};
    use crate::tables::filter_block_builder::FilterBlockBuilder;
    use crate::{BloomFilterPolicy, Operation};

    use super::*;

    #[test]
    fn filter_block_reader_can_be_created_from_serialized_filter_block() {
        let filter_policy: Arc<dyn FilterPolicy> = Arc::new(BloomFilterPolicy::new(10));
        let mut filter_block =
            create_filter_builder_with_num_data_blocks(Arc::clone(&filter_policy), 3);
        let filters = filter_block.finalize();
        let filter_reader = FilterBlockReader::new(filter_policy, filters).unwrap();

        assert_eq!(
            filter_reader.filters.len(),
            6,
            "With default options there should be twice as many filter blocks as data blocks."
        );
    }

    #[test]
    fn filter_block_reader_forces_disk_seeks_for_filter_blocks_without_filters() {
        let filter_policy: Arc<dyn FilterPolicy> = Arc::new(BloomFilterPolicy::new(10));
        let mut filter_block = FilterBlockBuilder::new(Arc::clone(&filter_policy));
        let filters = filter_block.finalize();
        let filter_reader = FilterBlockReader::new(filter_policy, filters).unwrap();

        assert!(
            filter_reader.key_may_match(
                0,
                &InternalKey::new(b"something".to_vec(), 1, Operation::Put).as_bytes()
            ),
            "Should force for all queries if there are no filters."
        );
        assert!(
            filter_reader.key_may_match(
                100_000,
                &InternalKey::new(b"something".to_vec(), 1, Operation::Put).as_bytes()
            ),
            "Should force for all queries if there are no filters."
        );
    }

    #[test]
    fn filter_block_reader_correctly_detects_keys_when_a_single_filter_covers_keys_for_multiple_data_blocks(
    ) {
        let filter_policy: Arc<dyn FilterPolicy> = Arc::new(BloomFilterPolicy::new(10));
        let mut filter_block = FilterBlockBuilder::new(Arc::clone(&filter_policy));
        filter_block.notify_new_data_block(100);
        filter_block.add_key(InternalKey::new(b"foo".to_vec(), 1, Operation::Put).as_bytes());
        filter_block.add_key(InternalKey::new(b"bar".to_vec(), 1, Operation::Put).as_bytes());
        filter_block.add_key(InternalKey::new(b"box".to_vec(), 1, Operation::Put).as_bytes());
        filter_block.notify_new_data_block(200);
        filter_block.add_key(InternalKey::new(b"box".to_vec(), 1, Operation::Put).as_bytes());
        filter_block.notify_new_data_block(300);
        filter_block.add_key(InternalKey::new(b"hello".to_vec(), 1, Operation::Put).as_bytes());
        let filters = filter_block.finalize();

        let filter_reader = FilterBlockReader::new(filter_policy, filters).unwrap();

        assert!(filter_reader.key_may_match(
            100,
            &InternalKey::new(b"foo".to_vec(), 1, Operation::Put).as_bytes()
        ));
        assert!(filter_reader.key_may_match(
            100,
            &InternalKey::new(b"bar".to_vec(), 1, Operation::Put).as_bytes()
        ));
        assert!(filter_reader.key_may_match(
            100,
            &InternalKey::new(b"box".to_vec(), 1, Operation::Put).as_bytes()
        ));
        assert!(filter_reader.key_may_match(
            100,
            &InternalKey::new(b"hello".to_vec(), 1, Operation::Put).as_bytes()
        ));
        assert!(!filter_reader.key_may_match(
            100,
            &InternalKey::new(b"missing".to_vec(), 1, Operation::Put).as_bytes()
        ));
        assert!(!filter_reader.key_may_match(
            100,
            &InternalKey::new(b"another missing".to_vec(), 1, Operation::Put).as_bytes()
        ));
    }

    #[test]
    fn filter_block_reader_correctly_detects_keys_when_there_are_multiple_filters_covering_multiple_data_blocks(
    ) {
        let filter_policy: Arc<dyn FilterPolicy> = Arc::new(BloomFilterPolicy::new(10));
        let mut filter_block = FilterBlockBuilder::new(Arc::clone(&filter_policy));

        // First filter
        filter_block.notify_new_data_block(0);
        filter_block.add_key(InternalKey::new(b"foo".to_vec(), 1, Operation::Put).as_bytes());
        filter_block.notify_new_data_block(2000);
        filter_block.add_key(InternalKey::new(b"bar".to_vec(), 1, Operation::Put).as_bytes());

        // Second filter
        filter_block.notify_new_data_block(3100);
        filter_block.add_key(InternalKey::new(b"box".to_vec(), 1, Operation::Put).as_bytes());

        // Third filter is kept empty by notifying with a much larger data block offset
        // Fourth filter
        filter_block.notify_new_data_block(9000);
        filter_block.add_key(InternalKey::new(b"box".to_vec(), 1, Operation::Put).as_bytes());
        filter_block.add_key(InternalKey::new(b"hello".to_vec(), 1, Operation::Put).as_bytes());
        let filters = filter_block.finalize();

        let filter_reader = FilterBlockReader::new(filter_policy, filters).unwrap();

        // Check first filter
        assert!(filter_reader.key_may_match(
            0,
            &InternalKey::new(b"foo".to_vec(), 1, Operation::Put).as_bytes()
        ));
        assert!(filter_reader.key_may_match(
            2000,
            &InternalKey::new(b"bar".to_vec(), 1, Operation::Put).as_bytes()
        ));
        assert!(!filter_reader.key_may_match(
            0,
            &InternalKey::new(b"box".to_vec(), 1, Operation::Put).as_bytes()
        ));
        assert!(!filter_reader.key_may_match(
            0,
            &InternalKey::new(b"hello".to_vec(), 1, Operation::Put).as_bytes()
        ));

        // Check second filter
        assert!(filter_reader.key_may_match(
            3100,
            &InternalKey::new(b"box".to_vec(), 1, Operation::Put).as_bytes()
        ));
        assert!(!filter_reader.key_may_match(
            3100,
            &InternalKey::new(b"foo".to_vec(), 1, Operation::Put).as_bytes()
        ));
        assert!(!filter_reader.key_may_match(
            3100,
            &InternalKey::new(b"bar".to_vec(), 1, Operation::Put).as_bytes()
        ));
        assert!(!filter_reader.key_may_match(
            3100,
            &InternalKey::new(b"hello".to_vec(), 1, Operation::Put).as_bytes()
        ));

        // Empty filter should not match any keys
        assert!(!filter_reader.key_may_match(
            4100,
            &InternalKey::new(b"foo".to_vec(), 1, Operation::Put).as_bytes()
        ));
        assert!(!filter_reader.key_may_match(
            4100,
            &InternalKey::new(b"bar".to_vec(), 1, Operation::Put).as_bytes()
        ));
        assert!(!filter_reader.key_may_match(
            4100,
            &InternalKey::new(b"box".to_vec(), 1, Operation::Put).as_bytes()
        ));
        assert!(!filter_reader.key_may_match(
            4100,
            &InternalKey::new(b"hello".to_vec(), 1, Operation::Put).as_bytes()
        ));

        // Check fourth filter
        assert!(filter_reader.key_may_match(
            9000,
            &InternalKey::new(b"box".to_vec(), 1, Operation::Put).as_bytes()
        ));
        assert!(filter_reader.key_may_match(
            9000,
            &InternalKey::new(b"hello".to_vec(), 1, Operation::Put).as_bytes()
        ));
        assert!(!filter_reader.key_may_match(
            9000,
            &InternalKey::new(b"foo".to_vec(), 1, Operation::Put).as_bytes()
        ));
        assert!(!filter_reader.key_may_match(
            9000,
            &InternalKey::new(b"bar".to_vec(), 1, Operation::Put).as_bytes()
        ));
    }

    /// Create a [`FilterBlockBuilder`] with the specified number of data blocks added to it.
    fn create_filter_builder_with_num_data_blocks(
        filter_policy: Arc<dyn FilterPolicy>,
        num_data_blocks: usize,
    ) -> FilterBlockBuilder {
        let mut filter_block = FilterBlockBuilder::new(filter_policy);

        for idx in 0..num_data_blocks {
            let keys = generate_num_keys(30);
            for key in keys {
                filter_block.add_key(key.as_bytes());
            }

            filter_block.notify_new_data_block((idx + 1) * 4096);
        }

        filter_block
    }

    /// Generate a specified number of [`InternalKey`]'s.
    fn generate_num_keys(num_keys_to_generate: usize) -> Vec<InternalKey> {
        let mut keys = Vec::with_capacity(num_keys_to_generate);
        for idx in 0..num_keys_to_generate {
            let num = idx + 100_000;
            let key = InternalKey::new(
                num.to_string().as_bytes().to_vec(),
                idx as u64,
                Operation::Put,
            );
            keys.push(key);
        }

        keys
    }
}
