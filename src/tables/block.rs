use integer_encoding::{FixedInt, VarInt};
use std::cmp::Ordering;
use std::convert::TryFrom;

use crate::config::SIZE_OF_U32_BYTES;
use crate::iterator::RainDbIterator;
use crate::key::{LookupKey, RainDbKeyType};

use super::errors::{ReadError, TableResult};

/// A block where the keys of entries are RainDB lookup keys.
pub type DataBlockReader = BlockReader<LookupKey>;

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

    /// The deserialized entries within a block.
    block_entries: Vec<BlockEntry<K>>,

    /// The indexes within `block_entries` that contain restart points.
    restart_point_indexes: Vec<usize>,
}

// Public methods
impl<K> BlockReader<K>
where
    K: RainDbKeyType,
{
    /// Create a new instance of a [`BlockReader`].
    pub fn new(raw_data: Vec<u8>) -> TableResult<Self> {
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
            BlockReader::deserialize_entries(&raw_data[0..restart_offset], restart_point_offsets)?;

        Ok(Self {
            raw_data,
            num_restart_points,
            restart_offset,
            block_entries,
            restart_point_indexes,
        })
    }

    /// Get the size of the block.
    pub fn size(&self) -> usize {
        self.raw_data.len()
    }

    /// Get a [`crate::iterator::RainDbIterator`] to the block entries.
    pub fn iter(&self) -> BlockIter<'_, K> {
        BlockIter {
            current_index: 0,
            block_entries: &self.block_entries,
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

    A block is layed out in the following format on disk:

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
    ) -> TableResult<(Vec<BlockEntry<K>>, Vec<usize>)> {
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
            let key: K;
            match maybe_key {
                Err(_base_err) => {
                    return Err(ReadError::FailedToParse(
                        "Failed to fully deserialize an entry. The key may be corrupted."
                            .to_string(),
                    ));
                }
                Ok(k) => key = k,
            }
            current_offset = key_end_offset;

            // Parse value
            let value_end_offset = current_offset + (value_length as usize);
            let value = buf[current_offset..value_end_offset].to_vec();
            current_offset = value_end_offset;

            // Check if the block offset is also the offset of a restart point.
            // Also do a check to ensure that the number of shared bytes is zero since restart
            // points do not have any prefix compression. This second check should be largely
            // redundant but here just to be extra safe and verify all invariants.
            if block_offset == (restart_point_offsets[restart_offsets_index] as usize)
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
    ) -> TableResult<Vec<u32>> {
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
            current_offset += current_offset + SIZE_OF_U32_BYTES;
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
pub(crate) struct BlockIter<'a, K>
where
    K: RainDbKeyType,
{
    /// The index to the current entry in the `block_entries` vector.
    current_index: usize,

    /// The deserialized entries within a block.
    block_entries: &'a [BlockEntry<K>],
}

/// Private methods
impl<'a, K> BlockIter<'a, K>
where
    K: RainDbKeyType,
{
    /// Update iterator adapter state to point at the entry at the specified restart point.
    fn seek_to_restart_point(&self, restart_point_index: usize) {
        self.current_index = restart_point_index;
    }
}

impl<'a, K> RainDbIterator<'_> for BlockIter<'a, K>
where
    K: RainDbKeyType,
{
    type Key = K;
    type Error = ReadError;

    fn is_valid(&self) -> bool {
        self.current_index >= 0 && self.current_index < self.block_entries.len()
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
        let mut right = self.block_entries.len() - 1;
        while left < right {
            let mid = (left + right + 1) / 2;
            let mid_entry = &self.block_entries[mid];

            match mid_entry.key.cmp(target) {
                Ordering::Less => {
                    // The key at `mid` is smaller than `target`. So, shift the search space right
                    // to see if we can find a larger key that is smaller than `target`. Our aim is
                    // to get as close as we can to the `target`.
                    left = mid;
                }
                Ordering::Greater | Ordering::Equal => {
                    // The key at `mid` is >= the `target`. So, shift the search space left to see
                    // if we can find a key that is smaller than `target`.
                    right = mid - 1;
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
        // The iterator is not valid, don't do anything
        if !self.is_valid() {
            return None;
        }

        self.current_index += 1;

        // Return the next item if the iterator is still valid
        if self.is_valid() {
            return self.current();
        }

        return None;
    }

    fn prev(&mut self) -> Option<(&K, &Vec<u8>)> {
        // The iterator is not valid, don't do anything
        if !self.is_valid() {
            return None;
        }

        self.current_index -= 1;

        // Return the previous item if the iterator is still valid
        if self.is_valid() {
            return self.current();
        }

        return None;
    }

    fn current(&self) -> Option<(&K, &Vec<u8>)> {
        if !self.is_valid() {
            return None;
        }

        let current_entry = self.block_entries[self.current_index];
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

/// Public methods
impl MetaIndexKey {
    /// Create a new instance of [`MetaIndexKey`].
    pub fn new(key: String) -> Self {
        Self(key)
    }
}

impl RainDbKeyType for MetaIndexKey {}

impl TryFrom<Vec<u8>> for MetaIndexKey {
    type Error = ReadError;

    fn try_from(value: Vec<u8>) -> Result<Self, Self::Error> {
        let maybe_key = std::str::from_utf8(&value);
        match maybe_key {
            Err(_base_err) => {
                return Err(ReadError::FailedToParse(
                    "Failed to parse the string for the metaindex key.".to_string(),
                ));
            }
            Ok(key) => return Ok(MetaIndexKey(key.to_string())),
        }
    }
}
