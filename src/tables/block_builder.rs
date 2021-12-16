use std::cmp;

use integer_encoding::{FixedInt, VarInt};

use crate::config::{BLOCK_RESTART_INTERVAL, SIZE_OF_U32_BYTES};
use crate::key::LookupKey;
use crate::tables::errors::BuilderError;
use crate::DbOptions;

/**
Build table file blocks where the keys are prefix-compressed.

# Format

[LevelDB's `BlockBuilder` docs] provide a good summary of the serialization format of blocks:

```text
When we store a key, we drop the prefix shared with the previous string. This helps reduce the
space requirement significantly. Furthermore, once every K keys, we do not apply the prefix
compression and store the entire key. We call this a "restart point". The tail end of the block
stores the offsets of all of the restart points, and can be used to do a binary search when looking
for a particular key. Values are stored as-is (without compression) immediately following the
corresponding key.
```

See the RainDB docs for a more in-depth discussion of the block format.

[LevelDB's `BlockBuilder` docs]: https://github.com/google/leveldb/blob/e426c83e88c4babc785098d905c2dcb4f4e884af/table/block_builder.cc#L5-L27
*/
pub(crate) struct BlockBuilder<'b> {
    /// Options for configuring the operation of the database.
    options: &'b DbOptions,

    /// A buffer holding the data for the block.
    buffer: Vec<u8>,

    /// The offsets to the restart points within the block.
    restart_points: Vec<u32>,

    /// The current number of keys that have been prefix compressed since the last restart point.
    curr_compressed_count: u32,

    /// True if the block has been finalized, i.e. [`BlockBuilder::finalize`] was called.
    block_finalized: bool,

    /// The last key that was added to the block as a byte buffer.
    last_key_added_bytes: Vec<u8>,
}

/// Crate-only methods
impl<'b> BlockBuilder<'b> {
    /// Create a new [`BlockBuilder`] instance.
    pub(crate) fn new(options: &'b DbOptions) -> Self {
        let restart_points = vec![];
        // The first restart point is at offset 0 i.e. the first key is not compressed
        restart_points.push(0);

        Self {
            options,
            buffer: vec![],
            restart_points,
            curr_compressed_count: 0,
            block_finalized: false,
            last_key_added_bytes: vec![],
        }
    }

    /**
    Add a key-value pair to the block.

    # Panics

    The following invariants must be maintained:

    1. [`BlockBuilder::finalize`] has not been called since the last call to
       [`BlockBuilder::reset`].
    1. The provided key is larger than any previously provided key.

    # Legacy

    This method was just called `Add` in LevelDB.
    */
    pub(crate) fn add_entry(&mut self, key: &LookupKey, value: &Vec<u8>) {
        // Panic if our invariants are not maintained. This is a bug.
        assert!(
            !self.block_finalized,
            "Attempted to add a key-value pair to a finalized block."
        );
        assert!(
            self.buffer.is_empty() || self.last_key_added_bytes < &key,
            "{}",
            BuilderError::<'static>::OutOfOrder(&key, self.last_key_added_bytes.as_ref().unwrap())
        );
        assert!(
            self.curr_compressed_count <= BLOCK_RESTART_INTERVAL,
            "Attempted to add too many consecutive compressed entries."
        );

        let key_bytes = key.as_bytes();
        let mut shared_prefix_size: usize = 0;
        if self.curr_compressed_count < BLOCK_RESTART_INTERVAL {
            // Compare to the previously added key to see how much the current key can be compressed
            let min_prefix_length = cmp::min(self.last_key_added_bytes.len(), key_bytes.len());
            while shared_prefix_size < min_prefix_length
                && self.last_key_added_bytes[shared_prefix_size] == key_bytes[shared_prefix_size]
            {
                shared_prefix_size += 1;
            }
        } else {
            // Restart compression
            self.restart_points.push(self.buffer.len());
            self.curr_compressed_count = 0;
        }

        let non_shared_bytes = key_bytes - shared_prefix_size;

        // Write number of shared bytes for the key
        self.buffer
            .extend_from_slice(u32::encode_var_vec(shared_prefix_size));

        // Write number of non-shared bytes
        self.buffer
            .extend_from_slice(u32::encode_var_vec(non_shared_bytes));

        // Write length of the value
        self.buffer
            .extend_from_slice(u32::encode_var_vec(value.len()));

        // Write non-shared part of the key i.e. the prefix-compressed key
        self.buffer.extend_from_slice(key_bytes[non_shared_bytes..]);

        // Write the value
        self.buffer.extend_from_slice(value);

        // Update builder state
        self.last_key_added_bytes = key.as_bytes();
        self.curr_compressed_count += 1;
    }

    /**
    Reset fields to their initial states i.e. as if the builder was just constructed.

    # Legacy

    This is behavior taken from LevelDB. Presumably, this is done to save on allocations.
    */
    pub(crate) fn reset(&mut self) {
        self.restart_points.clear();
        // The first restart point is at offset 0 i.e. the first key is not compressed
        self.restart_points.push(0);

        self.buffer.clear();
        self.curr_compressed_count = 0;
        self.block_finalized = false;
        self.last_key_added_bytes.clear();
    }

    /**
    Finalize the block and return a reference to the block contents.

    The contents slice will be valid for the lifetime of the builder or until the builder is reset.

    # Legacy

    This was called `BlockBuilder::Finish` in LevelDB.
    */
    pub(crate) fn finalize(&mut self) -> &'b [u8] {
        // Append the restart points
        for point in self.restart_points.iter() {
            self.buffer.extend_from_slice(u32::encode_fixed_vec(point));
        }

        // Append the number of restart points
        self.buffer
            .extend_from_slice(u32::encode_fixed_vec(self.restart_points.len()));

        self.block_finalized = true;

        &self.buffer[..]
    }

    /**
    Get the approximate size of the block in bytes.

    # Legacy

    This is synonomous to LevelDB's `BlockBuilder::CurrentSizeEstimate`.
    */
    pub(crate) fn approximate_size(&self) -> usize {
        let size_of_data = self.buffer.len();
        let size_of_restart_points = self.restart_points.len() * SIZE_OF_U32_BYTES;
        let size_of_restart_points_length = SIZE_OF_U32_BYTES;

        size_of_data + size_of_restart_points + size_of_restart_points_length
    }

    /// Return true if the block does not have any entries.
    pub(crate) fn is_empty(&self) {
        self.buffer.is_empty()
    }
}
