// Copyright (c) 2021 Google LLC
//
// Use of this source code is governed by an MIT-style
// license that can be found in the LICENSE file or at
// https://opensource.org/licenses/MIT.

use std::sync::Arc;

use integer_encoding::FixedInt;

use crate::config::SIZE_OF_U32_BYTES;
use crate::FilterPolicy;

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
Construct the filters for a table.

A filter is generated every 2 KiB of data. This means that filters can span multiple blocks and can
also apply to one partial block. Filters are generated eargerly as new data blocks are created.

Callers should call the filter block builder methods in the following order:

1. `notify_new_data_block` when new data blocks are added. This will potentially finalize a filter
   if the size threshold is reached.
1. Add the keys that will be used for generating a filter with `add_key`.
1. `finalize` once all data blocks have been processed.

In regex, the pattern would look like: `(notify_new_data_block add_key*)* finalize`.
*/
pub(crate) struct FilterBlockBuilder {
    /// The filter policy to use when generating the filter blocks.
    filter_policy: Arc<dyn FilterPolicy>,

    /// The keys (as bytes) to turn into a filter.
    keys: Vec<Vec<u8>>,

    /// Filters that have been generated.
    filters: Vec<Vec<u8>>,
}

/// Crate-only methods
impl FilterBlockBuilder {
    /// Create a new instance of [`FilterBlockBuilder`].
    pub(crate) fn new(filter_policy: Arc<dyn FilterPolicy>) -> Self {
        Self {
            filter_policy,
            keys: vec![],
            filters: vec![],
        }
    }

    /**
    Notify builder that a new data block is being written.

    This initiates checks for the filter size threshold and will generate filters if the threshold
    is reached.

    # Legacy

    This is synonomous to LevelDB's `FilterBlockBuilder::StartBlock` method.
    */
    pub(crate) fn notify_new_data_block(&mut self, block_offset: usize) {
        let filter_index = block_offset / (FILTER_RANGE_SIZE_BYTES as usize);

        // The data block was written at an offset further than the number filters that have been
        // generated so far. This means that we need to generate more filters for any pending keys.
        while filter_index > self.filters.len() {
            self.generate_filter();
        }
    }

    /// Add the provided key to the filter.
    pub(crate) fn add_key(&mut self, key: Vec<u8>) {
        self.keys.push(key);
    }

    /// Return a byte buffer of the serialized filters and their metadata.
    pub(crate) fn finalize(&mut self) -> Vec<u8> {
        if !self.keys.is_empty() {
            self.generate_filter();
        }

        // Flatten the filters and get an array of serialized per-filter offsets
        let mut results: Vec<u8> = vec![];
        let mut serialized_offsets: Vec<u8> =
            Vec::with_capacity(self.filters.len() * SIZE_OF_U32_BYTES);
        let mut curr_filter_offset = 0;
        for filter in &self.filters {
            serialized_offsets.append(&mut u32::encode_fixed_vec(curr_filter_offset));
            results.extend_from_slice(filter);

            curr_filter_offset += filter.len() as u32;
        }

        // Append offsets next
        // The array of filter offsets come after the filters. Record this offset for serialization.
        let offset_of_offsets = results.len();
        results.append(&mut serialized_offsets);

        // Add the offset to the array of filter offsets
        results.append(&mut u32::encode_fixed_vec(offset_of_offsets as u32));

        // Save the filter range size exponent
        results.push(FILTER_RANGE_SIZE_EXPONENT);

        results
    }
}

/// Private methods
impl FilterBlockBuilder {
    /// Generate a filter from any pending keys.
    fn generate_filter(&mut self) {
        if self.keys.is_empty() {
            // Fast path if there are no keys pending filter generation
            self.filters.push(vec![]);
            return;
        }

        let filter = self.filter_policy.create_filter(&self.keys);
        self.filters.push(filter);

        self.keys.clear();
    }
}

#[cfg(test)]
mod tests {
    use pretty_assertions::assert_eq;

    use crate::key::{InternalKey, RainDbKeyType};
    use crate::{BloomFilterPolicy, Operation};

    use super::*;

    #[test]
    fn can_generate_filter_blocks_based_on_a_filter_policy() {
        let mut filter_block = FilterBlockBuilder::new(Arc::new(BloomFilterPolicy::new(10)));
        let keys = generate_num_keys(30);
        for key in keys {
            filter_block.add_key(key.as_bytes());
        }

        filter_block.notify_new_data_block(4096);

        assert_eq!(filter_block.filters.len(), 2);

        let keys = generate_num_keys(40);
        for key in keys {
            filter_block.add_key(key.as_bytes());
        }

        filter_block.notify_new_data_block(8192);

        assert_eq!(filter_block.filters.len(), 4);
    }

    #[test]
    fn finalizing_a_filter_block_builder_returns_a_serialized_representation() {
        let mut filter_block = FilterBlockBuilder::new(Arc::new(BloomFilterPolicy::new(10)));
        let keys = generate_num_keys(30);
        for key in keys {
            filter_block.add_key(key.as_bytes());
        }
        filter_block.notify_new_data_block(4096);

        let keys = generate_num_keys(40);
        for key in keys {
            filter_block.add_key(key.as_bytes());
        }
        filter_block.notify_new_data_block(8192);

        assert!(
            !filter_block.finalize().is_empty(),
            "The serialized represenation of a set of filters should not be empty."
        );
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
