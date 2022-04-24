use integer_encoding::{FixedInt, VarInt};
use std::cmp::Ordering;
use std::convert::TryFrom;
use std::sync::Arc;

use crate::config::SIZE_OF_U32_BYTES;
use crate::iterator::RainDbIterator;
use crate::key::{InternalKey, RainDbKeyType};

use super::errors::{ReadError, TableReadResult};

/// A block where the keys of entries are RainDB internal keys.
pub type DataBlockReader = BlockReader<InternalKey>;

/// A block where the keys of entries are metaindex keys.
pub(crate) type MetaIndexBlockReader = BlockReader<MetaIndexKey>;

/// An entry in a block.
#[derive(Debug)]
pub struct BlockEntry<K> {
    /// The offset that this block entry is at in the parent block.
    block_offset: usize,

    /**
    The number of bytes this key's prefix shares with a parent restart point.

    This is zero if the key is a restart point.
    */
    key_num_shared_bytes: u32,

    /// The number of bytes unique to this key i.e. bytes not shared with a parent restart point.
    key_num_unshared_bytes: u32,

    /// The length of the value.
    value_length: u32,

    /**
    The unique parts of the key.

    This necessarily has the same length as `key_num_shared_bytes`.
    */
    key_delta: Vec<u8>,

    /// The full, deserialized key.
    key: K,

    /// The value of the entry.
    value: Vec<u8>,
}

/// Reader for deserializing a block from the table file and iterating its entries.
#[derive(Debug)]
pub struct BlockReader<K>
where
    K: RainDbKeyType,
{
    /// The unserialized data contained within the block.
    raw_data: Vec<u8>,

    /// The number of restart points that the block has.
    num_restart_points: u32,

    /// The offset within the raw data where the restart points are listed.
    restart_offset: usize,

    /**
    The deserialized entries within a block.

    The entries are wrapped in an [`Arc`] so that a cheap reference can be given to iterators
    without tying the lifetime of the iterator to the block reader.
    */
    block_entries: Arc<Vec<BlockEntry<K>>>,

    /// The indexes within `block_entries` that contain restart points.
    restart_point_indexes: Vec<usize>,
}

// Crate-only methods
impl<K> BlockReader<K>
where
    K: RainDbKeyType,
{
    /// Create a new instance of a [`BlockReader`].
    pub(crate) fn new(raw_data: Vec<u8>) -> TableReadResult<Self> {
        if raw_data.len() < SIZE_OF_U32_BYTES {
            return Err(ReadError::FailedToParse(
                "Failed to parse restart points. The buffer is too small.".to_string(),
            ));
        }

        // The number of restart points is in the last 4 bytes of the block when serialized.
        let num_restart_points_offset = raw_data.len() - SIZE_OF_U32_BYTES;
        let num_restart_points = u32::decode_fixed(&raw_data[num_restart_points_offset..]);

        // The array of restart points is right before the number of restart points when serialized.
        let restart_offset =
            raw_data.len() - (1 + (num_restart_points as usize)) * SIZE_OF_U32_BYTES;
        let restart_point_offsets = BlockReader::<K>::deserialize_restart_offsets(
            &raw_data[restart_offset..(raw_data.len() - SIZE_OF_U32_BYTES)],
            num_restart_points,
        )?;

        let (block_entries, restart_point_indexes) =
            BlockReader::deserialize_entries(&raw_data[..restart_offset], restart_point_offsets)?;

        Ok(Self {
            raw_data,
            num_restart_points,
            restart_offset,
            block_entries: Arc::new(block_entries),
            restart_point_indexes,
        })
    }

    /// Get the size of the block.
    pub(crate) fn size(&self) -> usize {
        self.raw_data.len()
    }

    /// Get a [`crate::iterator::RainDbIterator`] to the block entries.
    pub(crate) fn iter(&self) -> BlockIter<K> {
        BlockIter {
            current_index: 0,
            block_entries: Arc::clone(&self.block_entries),
        }
    }
}

// Private methods
impl<K> BlockReader<K>
where
    K: RainDbKeyType,
{
    /**
    Deserialize block entries.

    `restart_point_offsets` are the offsets to the restart points within the raw buffer.

    A block is laid out in the following format on disk:

    * key_num_shared_bytes: varint32
    * key_num_unshared_bytes: varint32
    * value_length: varint32
    * key_delta: Vec<u8> where this is a buffer the size of `key_num_unshared_bytes`
    * value: Vec<u8>

    Returns a tuple where the first element is the deserialized block entries and the second element
    is a vector of the indexes of the block entries that are also restart points.
    */
    fn deserialize_entries(
        buf: &[u8],
        restart_point_offsets: Vec<u32>,
    ) -> TableReadResult<(Vec<BlockEntry<K>>, Vec<usize>)> {
        let mut block_entries: Vec<BlockEntry<K>> = vec![];
        let mut restart_point_indexes: Vec<usize> = vec![];
        let mut restart_offsets_index = 0;
        let mut current_offset = 0;
        let mut current_full_key_buffer: Vec<u8> = vec![];

        while current_offset < buf.len() {
            let block_offset = current_offset;

            let maybe_key_shared_bytes = u32::decode_var(&buf[current_offset..]);
            if maybe_key_shared_bytes.is_none() {
                return Err(ReadError::FailedToParse(format!(
                    "Failed to parse number of shared bytes from buffer of size {} at offset {}",
                    buf.len(),
                    current_offset
                )));
            }
            let (key_num_shared_bytes, bytes_read) = maybe_key_shared_bytes.unwrap();
            current_offset += bytes_read;

            let maybe_key_unshared_bytes = u32::decode_var(&buf[current_offset..]);
            if maybe_key_unshared_bytes.is_none() {
                return Err(ReadError::FailedToParse(format!(
                    "Failed to parse number of unshared bytes from buffer of size {} at offset {}",
                    buf.len(),
                    current_offset
                )));
            }
            let (key_num_unshared_bytes, bytes_read) = maybe_key_unshared_bytes.unwrap();
            current_offset += bytes_read;

            let maybe_value_length = u32::decode_var(&buf[current_offset..]);
            if maybe_value_length.is_none() {
                return Err(ReadError::FailedToParse(format!(
                    "Failed to parse the value length from buffer of size {} at offset {}",
                    buf.len(),
                    current_offset
                )));
            }
            let (value_length, bytes_read) = maybe_value_length.unwrap();
            current_offset += bytes_read;

            // Parse key information
            let key_end_offset = current_offset + (key_num_unshared_bytes as usize);
            let key_delta = buf[current_offset..key_end_offset].to_vec();

            // Combine the two parts of the keys and try to deserialize
            current_full_key_buffer.truncate(key_num_shared_bytes as usize);
            current_full_key_buffer.extend_from_slice(&key_delta);
            let maybe_key = K::try_from(current_full_key_buffer.clone());
            let key: K = match maybe_key {
                Err(_base_err) => {
                    return Err(ReadError::FailedToParse(
                        "Failed to fully deserialize an entry. The key may be corrupted."
                            .to_string(),
                    ));
                }
                Ok(k) => k,
            };
            current_offset = key_end_offset;

            // Parse value
            let value_end_offset = current_offset + (value_length as usize);
            let value = buf[current_offset..value_end_offset].to_vec();
            current_offset = value_end_offset;

            // Check if the block offset is also the offset of a restart point.
            // Also, do a check to ensure that the number of shared bytes is zero since restart
            // points do not have any prefix compression. This second check should be largely
            // redundant but here just to be extra safe and verify all invariants.
            if restart_offsets_index < restart_point_offsets.len()
                && block_offset == (restart_point_offsets[restart_offsets_index] as usize)
                && key_num_shared_bytes == 0
            {
                restart_point_indexes.push(block_entries.len());
                restart_offsets_index += 1;
            }

            block_entries.push(BlockEntry {
                block_offset,
                key,
                key_num_shared_bytes,
                key_num_unshared_bytes,
                value_length,
                key_delta,
                value,
            })
        }

        if restart_point_indexes.len() != restart_point_offsets.len() {
            return Err(ReadError::FailedToParse(format!(
                "Failed to match restart point offsets to deserialized vector indexes. There were {} raw offsets but there are {} matched block entries.",
                restart_point_offsets.len(),
                restart_point_indexes.len()
            )));
        }

        Ok((block_entries, restart_point_indexes))
    }

    /**
    Deserialize the offsets to the block's restart points.

    The restart point offsets are serialized as a sequence of fixed-length `u32` values.
    */
    fn deserialize_restart_offsets(
        buf: &[u8],
        expected_num_restart_points: u32,
    ) -> TableReadResult<Vec<u32>> {
        if (buf.len() % SIZE_OF_U32_BYTES) != 0 {
            let error_msg = format!(
                "Failed to parse the restart point offsets in the block. The buffer is not evenly divisible into {} bytes.",
                SIZE_OF_U32_BYTES
            );
            return Err(ReadError::FailedToParse(error_msg));
        }

        let mut restart_points: Vec<u32> = vec![];
        let mut current_offset = 0;
        while current_offset < buf.len() {
            let restart_point =
                u32::decode_fixed(&buf[current_offset..(current_offset + SIZE_OF_U32_BYTES)]);
            restart_points.push(restart_point);
            current_offset += SIZE_OF_U32_BYTES;
        }

        if restart_points.len() != (expected_num_restart_points as usize) {
            let error_msg = format!(
                "Failed to parse the restart point offsets in the block. Expected {} restart points but only found {}.",
                expected_num_restart_points,
                restart_points.len(),
            );
            return Err(ReadError::FailedToParse(error_msg));
        }

        Ok(restart_points)
    }
}

/// Iterator adapter used to maintain iteration state.
pub(crate) struct BlockIter<K>
where
    K: RainDbKeyType,
{
    /// The index to the current entry in the `block_entries` vector.
    current_index: usize,

    /// The deserialized entries within a block.
    block_entries: Arc<Vec<BlockEntry<K>>>,
}

/// Private methods
impl<K> BlockIter<K>
where
    K: RainDbKeyType,
{
    /// Update iterator adapter state to point at the entry at the specified restart point.
    fn seek_to_restart_point(&mut self, restart_point_index: usize) {
        self.current_index = restart_point_index;
    }
}

impl<K> RainDbIterator for BlockIter<K>
where
    K: RainDbKeyType,
{
    type Key = K;
    type Error = ReadError;

    fn is_valid(&self) -> bool {
        self.current_index < self.block_entries.len()
    }

    fn seek(&mut self, target: &K) -> Result<(), Self::Error> {
        // Check if the cursor is already at the target
        if self.is_valid() {
            let (current_key, _) = self.current().unwrap();
            if current_key.eq(target) {
                // The cursor is already at the target
                return Ok(());
            }
        }

        // Binary search to find a key <= `target` since block entries are sorted
        let mut left = 0;
        let mut right = self.block_entries.len();
        while left < right {
            let mid = (left + right) / 2;
            let mid_entry = &self.block_entries[mid];

            match mid_entry.key.cmp(target) {
                Ordering::Less => {
                    // The key at `mid` is smaller than `target`. So, shift the search space right
                    // to see if we can find a larger key that is smaller than `target`. Our aim is
                    // to get as close as we can to the `target`.
                    left = mid + 1;
                }
                Ordering::Greater | Ordering::Equal => {
                    // The key at `mid` is >= the `target`. So, shift the search space left to see
                    // if we can find a key that is smaller than `target` in the case that the
                    // `target` does not exist.
                    right = mid;
                }
            }
        }

        self.current_index = left;

        Ok(())
    }

    fn seek_to_first(&mut self) -> Result<(), Self::Error> {
        self.current_index = 0;

        Ok(())
    }

    fn seek_to_last(&mut self) -> Result<(), Self::Error> {
        self.current_index = self.block_entries.len() - 1;

        Ok(())
    }

    fn next(&mut self) -> Option<(&K, &Vec<u8>)> {
        // Don't do anything if we are just past the last element
        if self.current_index == self.block_entries.len() {
            return None;
        }

        self.current_index += 1;

        // Return the next item if the iterator is still valid
        if self.is_valid() {
            return self.current();
        }

        None
    }

    fn prev(&mut self) -> Option<(&K, &Vec<u8>)> {
        // Don't do anything if we are the first element
        if self.current_index == 0 {
            return None;
        }

        self.current_index -= 1;

        // Return the previous item if the iterator is still valid
        if self.is_valid() {
            return self.current();
        }

        None
    }

    fn current(&self) -> Option<(&K, &Vec<u8>)> {
        if !self.is_valid() {
            return None;
        }

        let current_entry = &self.block_entries[self.current_index];
        Some((&current_entry.key, &current_entry.value))
    }
}

/**
A newtype that represents a metaindex and meta block key.

The newtype is necessary so that we can implement the `TryFrom` trait to deserialize a byte array
to the string value.
*/
#[derive(Eq, Ord, PartialEq, PartialOrd)]
pub(crate) struct MetaIndexKey(String);

/// Crate-only
impl MetaIndexKey {
    /// Create a new instance of [`MetaIndexKey`].
    pub(crate) fn new(key: String) -> Self {
        Self(key)
    }
}

impl RainDbKeyType for MetaIndexKey {
    fn as_bytes(&self) -> Vec<u8> {
        String::as_bytes(&self.0).to_vec()
    }
}

impl TryFrom<Vec<u8>> for MetaIndexKey {
    type Error = ReadError;

    fn try_from(value: Vec<u8>) -> Result<Self, Self::Error> {
        let maybe_key = std::str::from_utf8(&value);
        match maybe_key {
            Err(_base_err) => Err(ReadError::FailedToParse(
                "Failed to parse the string for the metaindex key.".to_string(),
            )),
            Ok(key) => Ok(MetaIndexKey(key.to_string())),
        }
    }
}

#[cfg(test)]
mod tests {
    use pretty_assertions::assert_eq;
    use std::rc::Rc;

    use crate::config::PREFIX_COMPRESSION_RESTART_INTERVAL;
    use crate::tables::block_builder::BlockBuilder;
    use crate::Operation;

    use super::*;

    #[test]
    fn block_reader_can_deserialize_a_block() {
        let mut block_builder: BlockBuilder<InternalKey> =
            BlockBuilder::new(PREFIX_COMPRESSION_RESTART_INTERVAL);
        for idx in 0..2_000_usize {
            let num = idx + 100_000;
            let key = InternalKey::new(
                num.to_string().as_bytes().to_vec(),
                idx as u64,
                Operation::Put,
            );
            block_builder.add_entry(Rc::new(key), &u64::encode_fixed_vec(num as u64));
        }
        let finalized_block = block_builder.finalize();

        let block_reader: BlockReader<InternalKey> = BlockReader::new(finalized_block).unwrap();
        assert!(
            block_builder.approximate_size() >= block_reader.size(),
            "The size of the deserialized block should not be understated by the builder's \
            approximation."
        );
        assert_eq!(
            block_reader.block_entries.len(),
            2_000,
            "There should be the same number of entries as the block builder that created the \
            block being deserialized."
        );
    }

    #[test]
    fn block_iterator_can_iterate_the_block() {
        let mut block_builder: BlockBuilder<InternalKey> =
            BlockBuilder::new(PREFIX_COMPRESSION_RESTART_INTERVAL);
        for idx in 0..2_000_usize {
            let num = idx + 100_000;
            let key = InternalKey::new(
                num.to_string().as_bytes().to_vec(),
                idx as u64,
                Operation::Put,
            );
            block_builder.add_entry(Rc::new(key), &u64::encode_fixed_vec(num as u64));
        }
        let finalized_block = block_builder.finalize();

        let block_reader: BlockReader<InternalKey> = BlockReader::new(finalized_block).unwrap();
        let mut block_iter = block_reader.iter();
        let mut idx: usize = 0;
        while block_iter.is_valid() && idx < 2_000 {
            let num = idx + 100_000;
            let expected_key = InternalKey::new(
                num.to_string().as_bytes().to_vec(),
                idx as u64,
                Operation::Put,
            );
            let expected_val = u64::encode_fixed_vec(num as u64);

            let (curr_key, curr_val) = block_iter.current().unwrap();
            assert_eq!(&expected_key, curr_key);
            assert_eq!(&expected_val, curr_val);

            // Move to the next entry
            idx += 1;
            block_iter.next();
        }

        assert!(
            block_iter.next().is_none() && idx == 2_000,
            "Arrived at the last element early (index {idx}). Expected last element at iteration \
            2,000."
        );
    }

    #[test]
    fn block_iterator_can_seek_to_last() {
        let mut block_builder: BlockBuilder<InternalKey> =
            BlockBuilder::new(PREFIX_COMPRESSION_RESTART_INTERVAL);
        for idx in 0..2_000_usize {
            let num = idx + 100_000;
            let key = InternalKey::new(
                num.to_string().as_bytes().to_vec(),
                idx as u64,
                Operation::Put,
            );
            block_builder.add_entry(Rc::new(key), &u64::encode_fixed_vec(num as u64));
        }
        let finalized_block = block_builder.finalize();

        let block_reader: BlockReader<InternalKey> = BlockReader::new(finalized_block).unwrap();
        let mut block_iter = block_reader.iter();

        block_iter.seek_to_last().unwrap();
        let (last_key, last_val) = block_iter.current().unwrap();
        assert_eq!(
            last_key,
            &InternalKey::new(
                101_999_usize.to_string().as_bytes().to_vec(),
                1_999,
                Operation::Put,
            ),
            "Found an incorrect last key"
        );
        assert_eq!(
            last_val,
            &u64::encode_fixed_vec(101_999),
            "Found an incorrect last value"
        );
        assert!(block_iter.next().is_none());
        assert!(block_iter.prev().is_some());
    }

    #[test]
    fn block_iterator_can_seek_to_targets() {
        let mut block_builder: BlockBuilder<InternalKey> =
            BlockBuilder::new(PREFIX_COMPRESSION_RESTART_INTERVAL);
        for idx in 0..2_000_usize {
            let num = idx + 100_000;
            let key = InternalKey::new(
                num.to_string().as_bytes().to_vec(),
                idx as u64,
                Operation::Put,
            );
            block_builder.add_entry(Rc::new(key), &u64::encode_fixed_vec(num as u64));
        }
        let finalized_block = block_builder.finalize();

        let block_reader: BlockReader<InternalKey> = BlockReader::new(finalized_block).unwrap();
        let mut block_iter = block_reader.iter();

        // Start the iterator at an arbitrary position
        block_iter.seek_to_last().unwrap();

        // Seek key that exists
        block_iter
            .seek(&InternalKey::new(
                100_117_usize.to_string().as_bytes().to_vec(),
                117,
                Operation::Put,
            ))
            .unwrap();
        let (target_key, target_val) = block_iter.current().unwrap();

        assert_eq!(
            target_key,
            &InternalKey::new(
                100_117_usize.to_string().as_bytes().to_vec(),
                117,
                Operation::Put,
            ),
            "Found an incorrect key"
        );
        assert_eq!(
            target_val,
            &u64::encode_fixed_vec(100_117),
            "Found an incorrect value"
        );

        // Seeking a key that does not exist should end at an entry less than the target
        block_iter
            .seek(&InternalKey::new(
                100_117_usize.to_string().as_bytes().to_vec(),
                // Sequence numbers are sorted in descending order so this is greater than what
                // exists in the block
                118,
                Operation::Put,
            ))
            .unwrap();
        let (target_key, target_val) = block_iter.current().unwrap();

        assert_eq!(
            target_key,
            &InternalKey::new(
                100_117_usize.to_string().as_bytes().to_vec(),
                117,
                Operation::Put,
            ),
            "Found an incorrect key. Should have found a key less than the target."
        );
        assert_eq!(
            target_val,
            &u64::encode_fixed_vec(100_117),
            "Found an incorrect key. Should have found a key less than the target."
        );
    }

    #[test]
    fn block_reader_given_a_compression_restart_interval_of_one_can_deserialize_a_block() {
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
        let finalized_block = block_builder.finalize();

        let block_reader: BlockReader<InternalKey> = BlockReader::new(finalized_block).unwrap();
        assert_eq!(
            block_reader.block_entries.len(),
            30,
            "There should be the same number of entries as the block builder that created the \
            block being deserialized."
        );
    }

    #[test]
    fn block_iterator_given_a_compression_restart_interval_of_one_can_seek_to_targets() {
        let mut block_builder: BlockBuilder<InternalKey> = BlockBuilder::new(1);
        for idx in 0..2_000_usize {
            let num = idx + 100_000;
            let key = InternalKey::new(
                num.to_string().as_bytes().to_vec(),
                idx as u64,
                Operation::Put,
            );
            block_builder.add_entry(Rc::new(key), &u64::encode_fixed_vec(num as u64));
        }
        let finalized_block = block_builder.finalize();

        let block_reader: BlockReader<InternalKey> = BlockReader::new(finalized_block).unwrap();
        let mut block_iter = block_reader.iter();

        // Start the iterator at an arbitrary position
        block_iter.seek_to_last().unwrap();

        // Seek key that exists
        block_iter
            .seek(&InternalKey::new(
                100_117_usize.to_string().as_bytes().to_vec(),
                117,
                Operation::Put,
            ))
            .unwrap();
        let (target_key, target_val) = block_iter.current().unwrap();

        assert_eq!(
            target_key,
            &InternalKey::new(
                100_117_usize.to_string().as_bytes().to_vec(),
                117,
                Operation::Put,
            ),
            "Found an incorrect key"
        );
        assert_eq!(
            target_val,
            &u64::encode_fixed_vec(100_117),
            "Found an incorrect value"
        );

        // Seeking a key that does not exist should end at an entry less than the target
        block_iter
            .seek(&InternalKey::new(
                100_117_usize.to_string().as_bytes().to_vec(),
                // Sequence numbers are sorted in descending order so this is greater than what
                // exists in the block
                118,
                Operation::Put,
            ))
            .unwrap();
        let (target_key, target_val) = block_iter.current().unwrap();

        assert_eq!(
            target_key,
            &InternalKey::new(
                100_117_usize.to_string().as_bytes().to_vec(),
                117,
                Operation::Put,
            ),
            "Found an incorrect key. Should have found a key less than the target."
        );
        assert_eq!(
            target_val,
            &u64::encode_fixed_vec(100_117),
            "Found an incorrect value. Should have found a value less than the target."
        );
    }
}
