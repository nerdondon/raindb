use std::cmp;
use std::rc::Rc;

use integer_encoding::{FixedInt, VarInt};

use crate::config::SIZE_OF_U32_BYTES;
use crate::key::RainDbKeyType;
use crate::tables::errors::BuilderError;

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
pub(crate) struct BlockBuilder<K>
where
    K: RainDbKeyType,
{
    /// The number of keys to perform prefix compression on before restarting with a full key.
    prefix_compression_restart_interval: usize,

    /// A buffer holding the data for the block.
    buffer: Vec<u8>,

    /// The offsets to the restart points within the block.
    restart_points: Vec<u32>,

    /// The current number of keys that have been prefix compressed since the last restart point.
    curr_compressed_count: usize,

    /// True if the block has been finalized, i.e. [`BlockBuilder::finalize`] was called.
    block_finalized: bool,

    /// The last key that was added to the block.
    maybe_last_key_added: Option<Rc<K>>,
}

/// Crate-only methods
impl<K> BlockBuilder<K>
where
    K: RainDbKeyType,
{
    /**
    Create a new [`BlockBuilder`] instance.

    # Panics

    Panics if a `prefix_compression_restart_interval` of zero is specified.
    */
    pub(crate) fn new(prefix_compression_restart_interval: usize) -> Self {
        assert!(prefix_compression_restart_interval > 0, "Attempted to create a block builder with a prefix compression restart interval of 0. Only values > 1 are accepted.");

        // The first restart point is at offset 0 i.e. the first key is not compressed
        let restart_points = vec![0];

        Self {
            prefix_compression_restart_interval,
            buffer: vec![],
            restart_points,
            curr_compressed_count: 0,
            block_finalized: false,
            maybe_last_key_added: None,
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
    pub(crate) fn add_entry(&mut self, key: Rc<K>, value: &[u8]) {
        let key_bytes = key.as_bytes();

        // Panic if our invariants are not maintained. This is a bug.
        assert!(
            !self.block_finalized,
            "Attempted to add a key-value pair to a finalized block."
        );
        assert!(
            self.buffer.is_empty() || self.maybe_last_key_added.as_ref().unwrap() < &key,
            "{}",
            BuilderError::OutOfOrder
        );
        assert!(
            self.curr_compressed_count <= self.prefix_compression_restart_interval,
            "Attempted to add too many consecutive compressed entries."
        );

        let mut shared_prefix_size: usize = 0;
        if self.curr_compressed_count < self.prefix_compression_restart_interval {
            if let Some(last_key_added) = self.maybe_last_key_added.as_ref() {
                // Compare to the previously added key to see how much the current key can be compressed
                let last_key_added_bytes = last_key_added.as_bytes();
                let min_prefix_length = cmp::min(last_key_added_bytes.len(), key_bytes.len());

                while shared_prefix_size < min_prefix_length
                    && last_key_added_bytes[shared_prefix_size] == key_bytes[shared_prefix_size]
                {
                    shared_prefix_size += 1;
                }
            }
        } else {
            // Restart compression
            self.restart_points.push(self.buffer.len() as u32);
            self.curr_compressed_count = 0;
        }

        let non_shared_bytes = (key_bytes.len() - shared_prefix_size) as u32;

        // Write number of shared bytes for the key
        self.buffer
            .extend(u32::encode_var_vec(shared_prefix_size as u32));

        // Write number of non-shared bytes
        self.buffer.extend(u32::encode_var_vec(non_shared_bytes));

        // Write length of the value
        self.buffer.extend(u32::encode_var_vec(value.len() as u32));

        // Write non-shared part of the key i.e. the prefix-compressed key
        self.buffer
            .extend_from_slice(&key_bytes[(non_shared_bytes as usize)..]);

        // Write the value
        self.buffer.extend_from_slice(value);

        // Update builder state
        self.maybe_last_key_added = Some(key);
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
        self.maybe_last_key_added = None;
    }

    /**
    Finalize the block and return a reference to the block contents.

    The contents slice will be valid for the lifetime of the builder or until the builder is reset.

    # Legacy

    This was called `BlockBuilder::Finish` in LevelDB.
    */
    pub(crate) fn finalize(&mut self) -> Vec<u8> {
        // Append the restart points
        for point in self.restart_points.iter() {
            self.buffer.extend(u32::encode_fixed_vec(*point));
        }

        // Append the number of restart points
        self.buffer
            .extend(u32::encode_fixed_vec(self.restart_points.len() as u32));

        self.block_finalized = true;

        // TODO: Make this a consuming a method so that we can avoid cloning. Would also remove the need for `block_finalized` field.
        self.buffer.clone()
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
    pub(crate) fn is_empty(&self) -> bool {
        self.buffer.is_empty()
    }
}
