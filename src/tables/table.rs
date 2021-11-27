use crc::{Crc, CRC_32_ISCSI};
use integer_encoding::FixedInt;
use snap::read::FrameDecoder;
use std::convert::TryFrom;
use std::fmt;
use std::io::{self, Read};

use crate::config::{TableFileCompressionType, SIZE_OF_U32_BYTES};
use crate::filter_policy::get_filter_block_name;
use crate::fs::ReadonlyRandomAccessFile;
use crate::iterator::RainDbIterator;
use crate::key::{LookupKey, Operation, RainDbKeyType};
use crate::utils::cache::CacheEntry;
use crate::{DbOptions, ReadOptions};

use super::block::{BlockReader, DataBlockReader, MetaIndexBlockReader, MetaIndexKey};
use super::block_handle::BlockHandle;
use super::errors::{ReadError, TableResult};
use super::filter_block::FilterBlockReader;
use super::footer::{Footer, SIZE_OF_FOOTER_BYTES};

/**
The size of a block descriptor in bytes.

This is a 1-byte enum + a 32-bit CRC (4 bytes).
*/
const BLOCK_DESCRIPTOR_SIZE_BYTES: usize = 1 + 4;

/**
CRC calculator using the iSCSI polynomial.

LevelDB uses the [google/crc32c](https://github.com/google/crc32c) CRC implementation. This
implementation specifies using the iSCSI polynomial so that is what we use here as well.
*/
const CRC_CALCULATOR: Crc<u32> = Crc::<u32>::new(&CRC_32_ISCSI);

/// Type alias for block cache entries.
type DataBlockCacheEntry = Box<dyn CacheEntry<DataBlockReader>>;

/**
An immutable, sorted map of strings to strings.

# Concurrency
A table is thread-safe.
*/
pub(crate) struct Table {
    /// Database options to refer to when reading the table file.
    options: DbOptions,

    /// The underlying file holding the table data.
    file: Box<dyn ReadonlyRandomAccessFile>,

    /**
    The partition ID to use when reading from the block cache.

    A block cache may be used by multiple clients and this ID is used to partition the cache to
    differentiate values cached by different clients.
    */
    cache_partition_id: u64,

    /**
    The footer of the table file.

    The footer contains handles to the metaindex and index blocks.
    */
    footer: Footer,

    /// Block containing the index.
    index_block: DataBlockReader,

    /// Optional block containing filters defined by the database filter policy.
    maybe_filter_block: Option<FilterBlockReader>,
}

/// Public methods
impl Table {
    pub fn open(options: DbOptions, file: Box<dyn ReadonlyRandomAccessFile>) -> TableResult<Table> {
        let file_length = file.len()?;
        if file_length < (SIZE_OF_FOOTER_BYTES as u64) {
            return Err(ReadError::FailedToParse(format!(
                "Failed to open the table file. The file length ({:?}) is invalid.",
                file_length
            )));
        }

        let cache_partition_id = (*options.block_cache()).new_id();

        log::debug!("Reading and parsing the table file footer");
        let mut footer_buf: Vec<u8> = vec![0; SIZE_OF_FOOTER_BYTES];
        file.read_from(
            &mut footer_buf,
            (file_length as usize) - SIZE_OF_FOOTER_BYTES,
        )?;
        let footer = Footer::try_from(&footer_buf)?;

        log::debug!("Reading and parsing the index block");
        let index_block: DataBlockReader =
            Table::get_data_block_reader(&file, footer.get_index_handle())?;

        log::debug!("Reading and parsing the metaindex block");
        let metaindex_block: MetaIndexBlockReader =
            Table::get_data_block_reader(&file, footer.get_metaindex_handle())?;

        log::debug!("Reading and parsing the filter block");
        let maybe_filter_block: Option<FilterBlockReader> =
            Table::read_filter_meta_block(&options, &file, &metaindex_block);

        Ok(Table {
            options,
            cache_partition_id,
            file,
            footer,
            index_block,
            maybe_filter_block,
        })
    }

    /**
    Get the value for the given seek key stored in the table file.

    This method corresponds to the publicly exposed `Db::get` operation so will ignore deleted
    values.

    Returns an non-deleted value if the key is in the table. Otherwise, return `None`.
    */
    pub fn get(&self, read_options: ReadOptions, key: &LookupKey) -> TableResult<Option<Vec<u8>>> {
        // Search the index block for the offset of a block that may or may not contain the key we
        // are looking for
        let index_block_iter = self.index_block.iter();
        index_block_iter.seek(key)?;
        let maybe_raw_handle = index_block_iter.current();
        if maybe_raw_handle.is_none() {
            // Offset to the key does not exist in the index so the key is not stored in this file
            return Ok(None);
        }

        let (_key, raw_handle) = maybe_raw_handle.unwrap();
        let block_handle = BlockHandle::try_from(raw_handle)?;
        let serialized_key = Vec::<u8>::from(key);

        // First check the filter to see if the key is actually in the block that was returned
        if self.maybe_filter_block.is_some()
            && !self
                .maybe_filter_block
                .as_ref()
                .unwrap()
                .key_may_match(block_handle.get_offset(), &serialized_key)
        {
            // The key was not found in the filter so it is not in the block and hence not in the
            // table
            return Ok(None);
        }

        // Search the block itself for the key by first checking the cache for the block
        let maybe_cached_block_entry = self.get_block_reader_from_cache(&block_handle)?;

        /*
        This is a kind of roundabout but the cache only returns `CacheEntry` objects which returns
        references to block readers (i.e. `&DataBlockReader`) while getting a fresh block reader
        returns the whole instance (i.e. `DataBlockReader`).
        */
        let maybe_new_block_reader: Option<DataBlockReader> = if maybe_cached_block_entry.is_none()
        {
            let reader: DataBlockReader = Table::get_data_block_reader(&self.file, &block_handle)?;
            if read_options.fill_cache {
                self.cache_block_reader(reader, &block_handle);
            }

            Some(reader)
        } else {
            None
        };

        let block_reader: &DataBlockReader = match maybe_cached_block_entry.as_ref() {
            Some(cache_entry) => cache_entry.get_value(),
            None => maybe_new_block_reader.as_ref().unwrap(),
        };

        let block_reader_iter = block_reader.iter();
        block_reader_iter.seek(key);
        match block_reader_iter.current() {
            Some((key, value)) => {
                if *key.get_operation() == Operation::Delete {
                    return Ok(None);
                }

                Ok(Some(value.clone()))
            }
            None => Ok(None),
        }
    }
}

// Private methods
impl Table {
    /// Return a reader for the data block at the specified handle.
    fn get_data_block_reader<K: RainDbKeyType>(
        file: &Box<dyn ReadonlyRandomAccessFile>,
        block_handle: &BlockHandle,
    ) -> TableResult<BlockReader<K>> {
        let block_data = Table::read_block_from_disk(file, block_handle)?;

        BlockReader::new(block_data)
    }

    /// Return decompressed byte buffer representing the block at the specified block handle.
    fn read_block_from_disk(
        file: &Box<dyn ReadonlyRandomAccessFile>,
        block_handle: &BlockHandle,
    ) -> TableResult<Vec<u8>> {
        // Handy alias to the block size as a `usize`
        let block_data_size: usize = block_handle.get_size() as usize;
        // The total block size is the size on disk plus the descriptor size
        let total_block_size: usize = block_data_size + BLOCK_DESCRIPTOR_SIZE_BYTES;
        let raw_block_data: Vec<u8> = vec![0; total_block_size];
        let bytes_read = file.read_from(&mut raw_block_data, block_handle.get_offset() as usize)?;
        if bytes_read != total_block_size {
            return Err(ReadError::IO(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "Could not read the entire block into the buffer.".to_string(),
            )));
        }

        // Check the CRC of the type and the block contents
        // The block checksum is stored at the end of the buffer as a 32-bit int
        let offset_to_checksum = total_block_size - SIZE_OF_U32_BYTES;
        let checksum_on_disk = u32::decode_fixed(&raw_block_data[offset_to_checksum..]);
        let calculated_block_checksum =
            CRC_CALCULATOR.checksum(&raw_block_data[0..offset_to_checksum]);
        if checksum_on_disk != calculated_block_checksum {
            return Err(ReadError::FailedToParse(
                "Failed to parse the block. There was a mismatch in the checksum".to_string(),
            ));
        }

        // Check the block compression type and decompress the block if necessary
        let compression_type_offset = total_block_size - BLOCK_DESCRIPTOR_SIZE_BYTES;
        let compression_type: TableFileCompressionType;
        match bincode::deserialize(&raw_block_data[compression_type_offset..]) {
            Err(error) => {
                return Err(ReadError::BlockDecompression(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "Failed to get the compression type for the block.".to_string(),
                )));
            }
            Ok(encoded_compression_type) => compression_type = encoded_compression_type,
        };

        match compression_type {
            TableFileCompressionType::None => {
                return Ok(raw_block_data[0..compression_type_offset].to_vec());
            }
            Snappy => {
                /*
                TODO: I would love to not allocate a Vec here from the range but I'm not sure how
                to get a slice. I need something that implements the `Read` trait
                */
                let snappy_reader = FrameDecoder::new(
                    raw_block_data[0..compression_type_offset]
                        .to_vec()
                        .as_slice(),
                );
                let mut decompressed_data: Vec<u8> = vec![];

                match snappy_reader.read_exact(&mut decompressed_data) {
                    Err(error) => {
                        return Err(ReadError::BlockDecompression(error));
                    }
                    Ok(_) => {
                        return Ok(decompressed_data);
                    }
                };
            }
        }
    }

    /// Return a reader for the filter meta block at the specified block handle.
    fn read_filter_meta_block(
        options: &DbOptions,
        file: &Box<dyn ReadonlyRandomAccessFile>,
        metaindex_block: &MetaIndexBlockReader,
    ) -> Option<FilterBlockReader> {
        let filter_block_name = get_filter_block_name(options.filter_policy());
        let metaindex_block_iter = metaindex_block.iter();
        // Seek to the filter meta block
        match metaindex_block_iter.seek(&MetaIndexKey::new(filter_block_name)) {
            Err(error) => {
                /*
                RainDB can continue to operate without filter blocks, albeit with possible
                degraded performance. Just log a warning and continue as if there was no filter
                block.
                */
                log::warn!("Encountered an error attempting to read the filter block. Continuing without filters. Original error: {}", error);
                return None;
            }
            _ => {}
        }

        match metaindex_block_iter.current() {
            Some((_key, filter)) => {
                match FilterBlockReader::new(options.filter_policy(), filter.to_vec()) {
                    Err(error) => {
                        log::warn!("Encountered an error getting the filter block reader. Continuing without filters. Original error: {}", error);
                        return None;
                    }
                    Ok(reader) => return Some(reader),
                }
            }
            None => return None,
        }
    }

    /**
    Check the block cache for the block at the specified block handle.

    Return the block reader if found. Otherwise, return [`None`].
    */
    fn get_block_reader_from_cache(
        &self,
        block_handle: &BlockHandle,
    ) -> TableResult<Option<DataBlockCacheEntry>> {
        let block_cache = self.options.block_cache();
        let block_cache_key =
            BlockCacheKey::new(self.cache_partition_id, block_handle.get_offset());
        let maybe_cache_entry = block_cache.get(&block_cache_key);

        Ok(maybe_cache_entry)
    }

    /// Cache the specified block reader in the block cache.
    fn cache_block_reader(
        &self,
        block_reader: DataBlockReader,
        block_handle: &BlockHandle,
    ) -> DataBlockCacheEntry {
        let block_cache = self.options.block_cache();
        let block_cache_key =
            BlockCacheKey::new(self.cache_partition_id, block_handle.get_offset());

        block_cache.insert(block_cache_key, block_reader)
    }
}

impl fmt::Debug for Table {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Table")
            .field("cache_partition_id", &self.cache_partition_id)
            .field("footer", &self.footer)
            .finish()
    }
}

/**
The key used to index into the block cache.

# Serialization

The key is serialized as a 16 byte value with the cache partition ID in the first byte and the
block offset in the second byte.
*/

#[derive(Debug, Eq, Hash, PartialEq)]
pub struct BlockCacheKey {
    /// The cache partition ID that the parent table is using for storage.
    cache_partition_id: u64,

    /// The offset to the block in the table file.
    block_offset: u64,
}

/// Public methods
impl BlockCacheKey {
    /// Create a new instance of [`BlockCacheKey`].
    pub fn new(cache_partition_id: u64, block_offset: u64) -> Self {
        Self {
            cache_partition_id,
            block_offset,
        }
    }
}

impl From<&BlockCacheKey> for Vec<u8> {
    fn from(value: &BlockCacheKey) -> Self {
        // We should only need 8 bytes for the partition id + 8 bytes for the offset = 16 bytes.
        let serialized_value = Vec::with_capacity(8 + 8);
        serialized_value.append(&mut u64::encode_fixed_vec(value.cache_partition_id));
        serialized_value.append(&mut u64::encode_fixed_vec(value.block_offset));

        serialized_value
    }
}
