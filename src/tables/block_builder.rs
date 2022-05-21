use std::cmp;
use std::fmt::Debug;
use std::marker::PhantomData;
use std::rc::Rc;

use integer_encoding::{FixedInt, VarInt};

use crate::config::SIZE_OF_U32_BYTES;
use crate::key::{InternalKey, RainDbKeyType};
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

    /// The last key that was added in bytes.
    last_key_bytes: Vec<u8>,

    /**
    Marker to allow the specification of the type of keys this block stores.

    This is meant to ensure that only a single type of key is added to the block rather than
    any key that implements the [`RainDbKeyType`] trait.
    */
    key_type_marker: PhantomData<K>,
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
            last_key_bytes: vec![],
            key_type_marker: PhantomData,
        }
    }

    /**
    Add a key-value pair to the block.

    # Panics

    The following invariants must be maintained:

    1. [`BlockBuilder::finalize`] has not been called since the last call to
       [`BlockBuilder::reset`].
    1. The provided key is larger than any previously provided key. This requirement is maintained
       by the fact that blocks are created by tables and tables are created by adding entries that
       are already sorted from the memtable.
    1. Key compression and reconstruction must be inverse functions.

    # Legacy

    This method was just called `Add` in LevelDB.
    */
    pub(crate) fn add_entry(&mut self, key: Rc<K>, value: &[u8]) {
        let key_bytes = key.as_bytes();

        // Panic if our invariants are not maintained. If a panic occurred, there is a definite bug.
        assert!(
            !self.block_finalized,
            "Attempted to add a key-value pair to a finalized block."
        );
        assert!(
            self.curr_compressed_count <= self.prefix_compression_restart_interval,
            "Attempted to add too many consecutive compressed entries."
        );

        /*
        This check does a conversion to an InternalKey even if the block builder key type is
        not an InternalKey. This works because the serialized format of the InternalKey has
        very simple layout (e.g. there are not length prefixed slices) that lends itself to be
        extracted from any byte buffer of a sufficient length. This behavior is carried over from
        LevelDB.
        */
        assert!(
            self.buffer.is_empty()
                || InternalKey::try_from(self.last_key_bytes.clone()).unwrap()
                    < InternalKey::try_from(key_bytes.clone()).unwrap(),
            "{}",
            BuilderError::OutOfOrder
        );

        let mut shared_prefix_size: usize = 0;
        if self.curr_compressed_count < self.prefix_compression_restart_interval {
            // Compare to the current key prefix to see how much the new key can be compressed
            let min_prefix_length = cmp::min(self.last_key_bytes.len(), key_bytes.len());

            while shared_prefix_size < min_prefix_length
                && self.last_key_bytes[shared_prefix_size] == key_bytes[shared_prefix_size]
            {
                shared_prefix_size += 1;
            }
        } else {
            // Restart compression
            self.restart_points.push(self.buffer.len() as u32);
            self.curr_compressed_count = 0;
        }

        let num_non_shared_bytes = (key_bytes.len() - shared_prefix_size) as u32;

        // Write number of shared bytes for the key
        self.buffer
            .extend(u32::encode_var_vec(shared_prefix_size as u32));

        // Write number of non-shared bytes
        self.buffer
            .extend(u32::encode_var_vec(num_non_shared_bytes));

        // Write length of the value
        self.buffer.extend(u32::encode_var_vec(value.len() as u32));

        // Write non-shared part of the key i.e. the prefix-compressed key
        self.buffer
            .extend_from_slice(&key_bytes[shared_prefix_size..]);

        // Write the value
        self.buffer.extend_from_slice(value);

        // Update builder state
        self.last_key_bytes.truncate(shared_prefix_size);
        self.last_key_bytes
            .extend_from_slice(&key_bytes[shared_prefix_size..]);
        assert!(
            self.last_key_bytes == key_bytes,
            "The reconstructed key was not the same as the key being added."
        );

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
        self.last_key_bytes.clear();
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

impl<K> Debug for BlockBuilder<K>
where
    K: RainDbKeyType,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BlockBuilder")
            .field(
                "prefix_compression_restart_interval",
                &self.prefix_compression_restart_interval,
            )
            .field(
                "buffer",
                &format!("buffer[current len={}", self.buffer.len()),
            )
            .field("restart_points", &self.restart_points)
            .field("curr_compressed_count", &self.curr_compressed_count)
            .field("block_finalized", &self.block_finalized)
            .field("key_type_marker", &self.key_type_marker)
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use pretty_assertions::assert_eq;

    use crate::config::PREFIX_COMPRESSION_RESTART_INTERVAL;
    use crate::key::InternalKey;
    use crate::Operation;

    use super::*;

    #[test]
    fn can_add_entries_as_expected() {
        let mut block_builder: BlockBuilder<InternalKey> =
            BlockBuilder::new(PREFIX_COMPRESSION_RESTART_INTERVAL);

        assert!(block_builder.is_empty());
        assert_eq!(block_builder.restart_points.len(), 1);

        for idx in 0..PREFIX_COMPRESSION_RESTART_INTERVAL {
            let num = idx + 100_000;
            let key = InternalKey::new(
                num.to_string().as_bytes().to_vec(),
                idx as u64,
                Operation::Put,
            );
            block_builder.add_entry(Rc::new(key), &u64::encode_fixed_vec(num as u64));
        }

        assert_eq!(block_builder.restart_points.len(), 1);

        let reset_prefix_key = InternalKey::new(
            200_000_u32.to_string().as_bytes().to_vec(),
            200_u64,
            Operation::Put,
        );
        block_builder.add_entry(Rc::new(reset_prefix_key), &u64::encode_fixed_vec(200_u64));

        assert_eq!(
            block_builder.restart_points.len(),
            2,
            "A new restart point should have been created"
        );

        let key_after_reset = InternalKey::new(
            201_000_u32.to_string().as_bytes().to_vec(),
            201_u64,
            Operation::Put,
        );
        block_builder.add_entry(Rc::new(key_after_reset), &u64::encode_fixed_vec(201_u64));

        assert_eq!(
            block_builder.restart_points.len(),
            2,
            "There should not be another reset point"
        );

        assert!(
            block_builder.approximate_size() >= block_builder.buffer.len(),
            "The approximate size should be for the finalized size of the block"
        );
    }

    #[test]
    #[should_panic(expected = "Attempted to add a key but it was out of order.")]
    fn panics_if_entries_are_not_added_in_order() {
        let mut block_builder: BlockBuilder<InternalKey> =
            BlockBuilder::new(PREFIX_COMPRESSION_RESTART_INTERVAL);

        block_builder.add_entry(
            Rc::new(InternalKey::new(b"def".to_vec(), 399, Operation::Put)),
            b"123",
        );
        block_builder.add_entry(
            Rc::new(InternalKey::new(b"abc".to_vec(), 400, Operation::Put)),
            b"456",
        );
    }

    #[test]
    fn finalize_works_as_expected() {
        let mut block_builder: BlockBuilder<InternalKey> =
            BlockBuilder::new(PREFIX_COMPRESSION_RESTART_INTERVAL);

        for idx in 0..(PREFIX_COMPRESSION_RESTART_INTERVAL + 2) {
            let num = idx + 100_000;
            let key = InternalKey::new(
                num.to_string().as_bytes().to_vec(),
                idx as u64,
                Operation::Put,
            );
            block_builder.add_entry(Rc::new(key), &u64::encode_fixed_vec(num as u64));
        }

        let finalized_block = block_builder.finalize();
        assert!(
            finalized_block.len() <= block_builder.approximate_size(),
            "The buffer should be larger after calling `finalize` because of the addition of the \
            serialized restart points."
        );
    }

    #[test]
    fn given_a_prefix_compression_restart_interval_of_one_can_build_a_block_as_expected() {
        // This test case is particularly applicable to index blocks
        let mut block_builder: BlockBuilder<InternalKey> = BlockBuilder::new(1);
        for idx in 0..30_usize {
            let num = idx + 100_000;
            let key = InternalKey::new(
                num.to_string().as_bytes().to_vec(),
                idx as u64,
                Operation::Put,
            );
            block_builder.add_entry(Rc::new(key), &u64::encode_fixed_vec(num as u64));
        }

        assert_eq!(block_builder.restart_points.len(), 30);
    }
}
