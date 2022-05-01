use integer_encoding::FixedInt;
use snap::read::FrameDecoder;
use std::convert::{TryFrom, TryInto};
use std::fmt;
use std::io::{self, Read};
use std::sync::Arc;

use crate::config::{TableFileCompressionType, SIZE_OF_U32_BYTES};
use crate::errors::{DBIOError, RainDBError, RainDBResult};
use crate::filter_policy;
use crate::fs::ReadonlyRandomAccessFile;
use crate::iterator::RainDbIterator;
use crate::key::{InternalKey, Operation, RainDbKeyType};
use crate::utils::cache::CacheEntry;
use crate::utils::crc;
use crate::{DbOptions, ReadOptions};

use super::block::{BlockIter, BlockReader, DataBlockReader, MetaIndexBlockReader, MetaIndexKey};
use super::block_handle::BlockHandle;
use super::constants::{BLOCK_DESCRIPTOR_SIZE_BYTES, CRC_CALCULATOR};
use super::errors::{ReadError, TableReadResult};
use super::filter_block::FilterBlockReader;
use super::footer::{Footer, SIZE_OF_FOOTER_BYTES};

/// Type alias for block cache entries.
type DataBlockCacheEntry = Box<dyn CacheEntry<Arc<DataBlockReader>>>;

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
    /// Open a table file and parse some initial information for iterating the file.
    pub fn open(
        options: DbOptions,
        file: Box<dyn ReadonlyRandomAccessFile>,
    ) -> TableReadResult<Table> {
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
            Table::get_data_block_reader_from_disk(&*file, footer.get_index_handle())?;

        log::debug!("Reading and parsing the metaindex block");
        let metaindex_block: MetaIndexBlockReader =
            Table::get_data_block_reader_from_disk(&*file, footer.get_metaindex_handle())?;

        log::debug!("Reading and parsing the filter block");
        let maybe_filter_block: Option<FilterBlockReader> =
            match Table::read_filter_meta_block(&options, &*file, &metaindex_block) {
                Ok(filter_block) => filter_block,
                Err(error) => {
                    /*
                    RainDB can continue to operate without filter blocks, albeit with possible
                    degraded performance. Just log a warning and continue as if there was no filter
                    block.
                    */
                    log::warn!("{}", error);

                    None
                }
            };

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

    Returns a non-deleted value if the key is in the table. Otherwise, return `None`.
    */
    pub fn get(
        &self,
        read_options: &ReadOptions,
        key: &InternalKey,
    ) -> TableReadResult<Option<Vec<u8>>> {
        // Search the index block for the offset of a block that may or may not contain the key we
        // are looking for
        let mut index_block_iter = self.index_block.iter();
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
            return Err(ReadError::KeyNotFound);
        }

        let block_reader = self.get_block_reader(read_options, &block_handle)?;

        // We have the block reader, now use the iterator to try to find the value for the key.
        let mut block_reader_iter = block_reader.iter();
        block_reader_iter.seek(key)?;
        match block_reader_iter.current() {
            Some((found_key, found_value)) => {
                if found_key.get_user_key() != key.get_user_key() {
                    return Err(ReadError::KeyNotFound);
                }

                if found_key.get_operation() == Operation::Delete {
                    return Ok(None);
                }

                Ok(Some(found_value.clone()))
            }
            None => Err(ReadError::KeyNotFound),
        }
    }

    /**
    Get an iterator for the table.

    In the terminology set by LevelDB, this is a two level iterator. We invert the lifetime
    relationship of the iterator object and the table unlike common Rust iterator objects. This is
    because of the requirement to keep a list of table iterators in single iterator that merges
    all of the table data. There was an effort to use [`std::borrow::Cow`] and [`std::ops::Deref`]
    in RainDB but things got a little hairy and the effort was not worth it at the time to come up
    with a more generic solution to allow a method that accepts either a table reference or an
    owned value.
    */
    pub fn iter_with(table: Arc<Table>, read_options: ReadOptions) -> TwoLevelIterator {
        TwoLevelIterator::new(table, read_options)
    }
}

// Private methods
impl Table {
    /// Return a reader from disk for the data block at the specified handle.
    fn get_data_block_reader_from_disk<K: RainDbKeyType>(
        file: &dyn ReadonlyRandomAccessFile,
        block_handle: &BlockHandle,
    ) -> TableReadResult<BlockReader<K>> {
        let block_data = Table::read_block_from_disk(file, block_handle)?;

        BlockReader::new(block_data)
    }

    /**
    Return decompressed byte buffer from disk representing the block at the specified
    block handle.
    */
    fn read_block_from_disk(
        file: &dyn ReadonlyRandomAccessFile,
        block_handle: &BlockHandle,
    ) -> TableReadResult<Vec<u8>> {
        // Handy alias to the block size as a `usize`
        let block_data_size: usize = block_handle.get_size() as usize;
        // The total block size is the size on disk plus the descriptor size
        let total_block_size: usize = block_data_size + BLOCK_DESCRIPTOR_SIZE_BYTES;
        let mut raw_block_data: Vec<u8> = vec![0; total_block_size];
        let bytes_read = file.read_from(&mut raw_block_data, block_handle.get_offset() as usize)?;
        if bytes_read != total_block_size {
            return Err(ReadError::IO(DBIOError::new(
                io::ErrorKind::UnexpectedEof,
                "Could not read the entire block into the buffer.".to_string(),
            )));
        }

        // Check the CRC of the type and the block contents
        // The block checksum is stored at the end of the buffer as a 32-bit int
        let offset_to_checksum = total_block_size - SIZE_OF_U32_BYTES;
        let checksum_on_disk = u32::decode_fixed(&raw_block_data[offset_to_checksum..]);
        let unmasked_stored_checksum = crc::unmask_checksum(checksum_on_disk);
        let calculated_block_checksum =
            CRC_CALCULATOR.checksum(&raw_block_data[0..offset_to_checksum]);
        if unmasked_stored_checksum != calculated_block_checksum {
            return Err(ReadError::FailedToParse(
                "Failed to parse the block. There was a mismatch in the checksum".to_string(),
            ));
        }

        // Check the block compression type and decompress the block if necessary
        let compression_type_offset = total_block_size - BLOCK_DESCRIPTOR_SIZE_BYTES;
        let maybe_compression_type: RainDBResult<TableFileCompressionType> =
            raw_block_data[compression_type_offset].try_into();
        let compression_type: TableFileCompressionType;
        match maybe_compression_type {
            Err(error) => {
                return Err(ReadError::BlockDecompression(DBIOError::new(
                    io::ErrorKind::InvalidData,
                    error.to_string(),
                )));
            }
            Ok(encoded_compression_type) => compression_type = encoded_compression_type,
        };

        let raw_block_contents = &raw_block_data[..compression_type_offset];
        match compression_type {
            TableFileCompressionType::None => Ok(raw_block_contents.to_vec()),
            TableFileCompressionType::Snappy => {
                let mut snappy_reader = FrameDecoder::new(raw_block_contents);
                // We do not know the decompressed size of the data but pre-allocating a ballpark
                // can still help
                let mut decompressed_data: Vec<u8> = Vec::with_capacity(raw_block_contents.len());

                match snappy_reader.read_to_end(&mut decompressed_data) {
                    Err(error) => Err(ReadError::BlockDecompression(error.into())),
                    Ok(_) => Ok(decompressed_data),
                }
            }
        }
    }

    /// Return a reader for the filter meta block at the specified block handle.
    fn read_filter_meta_block(
        options: &DbOptions,
        file: &dyn ReadonlyRandomAccessFile,
        metaindex_block: &MetaIndexBlockReader,
    ) -> TableReadResult<Option<FilterBlockReader>> {
        let filter_block_name = filter_policy::get_filter_block_name(options.filter_policy());
        let mut metaindex_block_iter = metaindex_block.iter();
        // Seek to the filter meta block
        if let Err(error) = metaindex_block_iter.seek(&MetaIndexKey::new(filter_block_name)) {
            return Err(ReadError::FilterBlock(format!("{}", error)));
        }

        match metaindex_block_iter.current() {
            Some((_key, raw_contents)) => {
                let filter_block_handle = BlockHandle::try_from(raw_contents)?;
                let raw_filter_block = Table::read_block_from_disk(file, &filter_block_handle)?;

                match FilterBlockReader::new(options.filter_policy(), raw_filter_block) {
                    Err(error) => return Err(ReadError::FilterBlock(format!("{}", error))),
                    Ok(reader) => Ok(Some(reader)),
                }
            }
            None => Ok(None),
        }
    }

    /**
    Get a block reader by checking the block cache first and reading it from disk if there's a
    cache miss.
    */
    fn get_block_reader(
        &self,
        read_options: &ReadOptions,
        block_handle: &BlockHandle,
    ) -> TableReadResult<Arc<DataBlockReader>> {
        // Search the block itself for the key by first checking the cache for the block
        match self.get_block_reader_from_cache(block_handle) {
            Some(cache_entry) => Ok(Arc::clone(&cache_entry.get_value())),
            None => {
                let reader: DataBlockReader =
                    Table::get_data_block_reader_from_disk(&*self.file, block_handle)?;
                if read_options.fill_cache {
                    let cache_entry = self.cache_block_reader(reader, &block_handle);
                    return Ok(Arc::clone(&cache_entry.get_value()));
                }

                Ok(Arc::new(reader))
            }
        }
    }

    /**
    Check the block cache for the block at the specified block handle.

    Return the block reader if found. Otherwise, return [`None`].
    */
    fn get_block_reader_from_cache(
        &self,
        block_handle: &BlockHandle,
    ) -> Option<DataBlockCacheEntry> {
        let block_cache = self.options.block_cache();
        let block_cache_key =
            BlockCacheKey::new(self.cache_partition_id, block_handle.get_offset());

        block_cache.get(&block_cache_key)
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

        block_cache.insert(block_cache_key, Arc::new(block_reader))
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

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
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
        let mut serialized_value = Vec::with_capacity(8 + 8);
        serialized_value.append(&mut u64::encode_fixed_vec(value.cache_partition_id));
        serialized_value.append(&mut u64::encode_fixed_vec(value.block_offset));

        serialized_value
    }
}

/**
A two-level iterator that first iterates the index block and then iterates a corresponding data
block.

This iterator yields the concatenation of all key-value pairs in a sequence of blocks (e.g. in a
table file).
*/
pub struct TwoLevelIterator {
    /// The table the iterator is for.
    table: Arc<Table>,

    /// Options for configuring the behavior of reads done by the iterator.
    read_options: ReadOptions,

    /// Iterator for the index block.
    index_block_iter: BlockIter<InternalKey>,

    /// Iterator for a data block.
    maybe_data_block_iter: Option<BlockIter<InternalKey>>,

    /// The block handle used to get the data block in the [`TwoLevelIterator::data_block`] field.
    data_block_handle: Option<BlockHandle>,
}

/// Private methods
impl TwoLevelIterator {
    /// Create a new instance of [`TwoLevelIterator`].
    fn new(table: Arc<Table>, read_options: ReadOptions) -> Self {
        let index_block_iter = table.index_block.iter();

        Self {
            table,
            read_options,
            index_block_iter,
            maybe_data_block_iter: None,
            data_block_handle: None,
        }
    }

    fn init_data_block(&mut self) -> TableReadResult<()> {
        if !self.index_block_iter.is_valid() {
            self.maybe_data_block_iter = None;
            self.data_block_handle = None;
        } else {
            let data_block_handle_bytes = self.index_block_iter.current().unwrap().1;
            let block_handle = BlockHandle::try_from(data_block_handle_bytes)?;

            if self.data_block_handle.is_some()
                && self.data_block_handle.as_ref().unwrap() == &block_handle
            {
                // Don't need to do anything since the data block is already stored
            } else {
                let data_block =
                    (*self.table).get_block_reader(&self.read_options, &block_handle)?;

                self.maybe_data_block_iter = Some(data_block.iter());
                self.data_block_handle = Some(block_handle);
            }
        }

        Ok(())
    }

    /// Move the index iterator and data iterator forward until we find a non-empty block.
    fn skip_empty_data_blocks_forward(&mut self) -> TableReadResult<()> {
        while self.maybe_data_block_iter.is_none()
            || !self.maybe_data_block_iter.as_mut().unwrap().is_valid()
        {
            if !self.index_block_iter.is_valid() {
                // We've reached the end of the index block so there are no more data blocks
                self.maybe_data_block_iter = None;
                self.data_block_handle = None;
                return Ok(());
            }

            // Move index iterator to check for the next data block handle
            self.index_block_iter.next();
            self.init_data_block()?;

            if self.maybe_data_block_iter.is_some() {
                self.maybe_data_block_iter
                    .as_mut()
                    .unwrap()
                    .seek_to_first()?;
            }
        }

        Ok(())
    }

    /**
    Move the index iterator and data iterator backward until we find a non-empty block.

    If a data block is found, this will set the data block iterator to the last entry of the
    data block.
    */
    fn skip_empty_data_blocks_backward(&mut self) -> TableReadResult<()> {
        while self.maybe_data_block_iter.is_none()
            || !self.maybe_data_block_iter.as_ref().unwrap().is_valid()
        {
            if !self.index_block_iter.is_valid() {
                // We've reached the end of the index block so there are no more data blocks
                self.maybe_data_block_iter = None;
                self.data_block_handle = None;
                return Ok(());
            }

            // Move index iterator to check for the previous data block handle
            self.index_block_iter.prev();
            self.init_data_block()?;

            if self.maybe_data_block_iter.is_some() {
                self.maybe_data_block_iter
                    .as_mut()
                    .unwrap()
                    .seek_to_last()?;
            }
        }

        Ok(())
    }
}

impl RainDbIterator for TwoLevelIterator {
    type Key = InternalKey;
    type Error = RainDBError;

    fn is_valid(&self) -> bool {
        self.maybe_data_block_iter.is_some()
            && self.maybe_data_block_iter.as_ref().unwrap().is_valid()
    }

    fn seek(&mut self, target: &Self::Key) -> Result<(), Self::Error> {
        self.index_block_iter.seek(target)?;
        self.init_data_block()?;

        if self.maybe_data_block_iter.is_some() {
            self.maybe_data_block_iter.as_mut().unwrap().seek(target)?;
        }

        self.skip_empty_data_blocks_forward()?;

        Ok(())
    }

    fn seek_to_first(&mut self) -> Result<(), Self::Error> {
        self.index_block_iter.seek_to_first()?;
        self.init_data_block()?;

        if self.maybe_data_block_iter.is_some() {
            self.maybe_data_block_iter
                .as_mut()
                .unwrap()
                .seek_to_first()?;
        }

        self.skip_empty_data_blocks_forward()?;

        Ok(())
    }

    fn seek_to_last(&mut self) -> Result<(), Self::Error> {
        self.index_block_iter.seek_to_last()?;
        self.init_data_block()?;

        if self.maybe_data_block_iter.is_some() {
            self.maybe_data_block_iter
                .as_mut()
                .unwrap()
                .seek_to_last()?;
        }

        self.skip_empty_data_blocks_backward()?;

        Ok(())
    }

    fn next(&mut self) -> Option<(&Self::Key, &Vec<u8>)> {
        if !self.is_valid() {
            return None;
        }

        if self
            .maybe_data_block_iter
            .as_mut()
            .unwrap()
            .next()
            .is_none()
        {
            if let Err(error) = self.skip_empty_data_blocks_forward() {
                log::error!(
                    "There was an error skipping forward in a two-level iterator. Original \
                    error: {}",
                    error
                );
                return None;
            }
        }

        // Check if there is still a data block after skipping forward
        if let Some(data_block) = self.maybe_data_block_iter.as_ref() {
            return data_block.current();
        }

        None
    }

    fn prev(&mut self) -> Option<(&Self::Key, &Vec<u8>)> {
        if !self.is_valid() {
            return None;
        }

        if self
            .maybe_data_block_iter
            .as_mut()
            .unwrap()
            .prev()
            .is_none()
        {
            if let Err(error) = self.skip_empty_data_blocks_backward() {
                log::error!(
                    "There was an error skipping backward in a two-level iterator. Original \
                    error: {}",
                    error
                );
                return None;
            }
        }

        // Check if there is still a data block after skipping backwards
        if let Some(data_block) = self.maybe_data_block_iter.as_ref() {
            return data_block.current();
        }

        None
    }

    fn current(&self) -> Option<(&Self::Key, &Vec<u8>)> {
        if !self.is_valid() {
            return None;
        }

        self.maybe_data_block_iter.as_ref().unwrap().current()
    }
}

#[cfg(test)]
mod table_tests {
    use std::rc::Rc;

    use pretty_assertions::assert_eq;

    use crate::file_names::FileNameHandler;
    use crate::tables::TableBuilder;

    use super::*;

    fn setup() {
        let _ = env_logger::builder()
            // Include all events in tests
            .filter_level(log::LevelFilter::max())
            // Ensure events are captured by `cargo test`
            .is_test(true)
            // Ignore errors initializing the logger if tests race to configure it
            .try_init();
    }

    #[test]
    fn table_with_one_entry_can_be_opened() {
        setup();

        const MAX_BLOCK_SIZE_BYTES: usize = 256;
        let mut options = DbOptions::with_memory_env();
        // Use smaller block size to exercise block boundary conditions more often
        options.max_block_size = MAX_BLOCK_SIZE_BYTES;

        let mut table_builder = TableBuilder::new(options.clone(), 55).unwrap();
        let num: u64 = 100_000;
        let key = InternalKey::new(num.to_string().as_bytes().to_vec(), 30, Operation::Put);
        table_builder
            .add_entry(Rc::new(key), &u64::encode_fixed_vec(num))
            .unwrap();
        table_builder.finalize().unwrap();
        drop(table_builder);

        let file_path = FileNameHandler::new(options.db_path().to_string()).get_table_file_path(55);
        let file = options.filesystem_provider().open_file(&file_path).unwrap();

        let _ = Table::open(options.clone(), file).unwrap();
    }

    #[test]
    fn table_with_multiple_entries_can_be_opened() {
        setup();

        const MAX_BLOCK_SIZE_BYTES: usize = 256;
        let mut options = DbOptions::with_memory_env();
        // Use smaller block size to exercise block boundary conditions more often
        options.max_block_size = MAX_BLOCK_SIZE_BYTES;

        let mut table_builder = TableBuilder::new(options.clone(), 55).unwrap();
        for idx in 0..400_usize {
            let num = idx + 100_000;
            let key = InternalKey::new(
                num.to_string().as_bytes().to_vec(),
                idx as u64,
                Operation::Put,
            );
            table_builder
                .add_entry(Rc::new(key), &u64::encode_fixed_vec(num as u64))
                .unwrap();
        }
        table_builder.finalize().unwrap();
        drop(table_builder);

        let file_path = FileNameHandler::new(options.db_path().to_string()).get_table_file_path(55);
        let file = options.filesystem_provider().open_file(&file_path).unwrap();

        let _ = Table::open(options.clone(), file).unwrap();
    }

    #[test]
    fn given_a_table_with_a_single_entry_the_reader_can_get_a_value() {
        setup();

        const MAX_BLOCK_SIZE_BYTES: usize = 256;
        let mut options = DbOptions::with_memory_env();
        // Use smaller block size to exercise block boundary conditions more often
        options.max_block_size = MAX_BLOCK_SIZE_BYTES;

        let mut table_builder = TableBuilder::new(options.clone(), 55).unwrap();
        let num: u64 = 100_000;
        let key = InternalKey::new(num.to_string().as_bytes().to_vec(), 30, Operation::Put);
        table_builder
            .add_entry(Rc::new(key), &u64::encode_fixed_vec(num))
            .unwrap();
        table_builder.finalize().unwrap();
        drop(table_builder);

        let file_path = FileNameHandler::new(options.db_path().to_string()).get_table_file_path(55);
        let file = options.filesystem_provider().open_file(&file_path).unwrap();

        let table = Table::open(options.clone(), file).unwrap();

        let actual = table
            .get(
                &ReadOptions::default(),
                &InternalKey::new_for_seeking(num.to_string().as_bytes().to_vec(), 30),
            )
            .unwrap()
            .unwrap();

        assert_eq!(actual, u64::encode_fixed_vec(100_000));
    }

    #[test]
    fn given_a_table_with_multiple_entries_the_table_iterator_yields_expected_values_when_iterated()
    {
        setup();

        const MAX_BLOCK_SIZE_BYTES: usize = 256;
        let mut options = DbOptions::with_memory_env();
        // Use smaller block size to exercise block boundary conditions more often
        options.max_block_size = MAX_BLOCK_SIZE_BYTES;

        let mut table_builder = TableBuilder::new(options.clone(), 55).unwrap();
        for idx in 0..2000_usize {
            let num = idx + 100_000;
            let key = InternalKey::new(
                num.to_string().as_bytes().to_vec(),
                idx as u64,
                Operation::Put,
            );
            table_builder
                .add_entry(Rc::new(key), &u64::encode_fixed_vec(num as u64))
                .unwrap();
        }
        table_builder.finalize().unwrap();
        drop(table_builder);

        let file_path = FileNameHandler::new(options.db_path().to_string()).get_table_file_path(55);
        let file = options.filesystem_provider().open_file(&file_path).unwrap();
        let table = Table::open(options.clone(), file).unwrap();
        let mut table_iter = Table::iter_with(Arc::new(table), ReadOptions::default());
        table_iter.seek_to_first().unwrap();

        let mut idx: usize = 0;
        while table_iter.is_valid() && idx < 2000 {
            let num = idx + 100_000;
            let expected_key = InternalKey::new(
                num.to_string().as_bytes().to_vec(),
                idx as u64,
                Operation::Put,
            );
            let expected_val = u64::encode_fixed_vec(num as u64);

            let (curr_key, curr_val) = table_iter.current().unwrap();
            assert_eq!(&expected_key, curr_key);
            assert_eq!(&expected_val, curr_val);

            // Move to the next entry
            idx += 1;
            table_iter.next();
        }

        assert!(
            table_iter.next().is_none() && idx == 2000,
            "Arrived at the last element early (index {idx}). Expected last element at iteration \
            2000."
        );
    }

    #[test]
    fn given_a_table_with_a_multiple_entries_the_table_iterator_can_seek_to_targets() {
        setup();

        const MAX_BLOCK_SIZE_BYTES: usize = 256;
        let mut options = DbOptions::with_memory_env();
        // Use smaller block size to exercise block boundary conditions more often
        options.max_block_size = MAX_BLOCK_SIZE_BYTES;

        let mut table_builder = TableBuilder::new(options.clone(), 55).unwrap();
        for idx in 0..2000_usize {
            let num = idx + 100_000;
            let key = InternalKey::new(
                num.to_string().as_bytes().to_vec(),
                idx as u64,
                Operation::Put,
            );
            table_builder
                .add_entry(Rc::new(key), &u64::encode_fixed_vec(num as u64))
                .unwrap();
        }
        table_builder.finalize().unwrap();
        drop(table_builder);

        let file_path = FileNameHandler::new(options.db_path().to_string()).get_table_file_path(55);
        let file = options.filesystem_provider().open_file(&file_path).unwrap();
        let table = Table::open(options.clone(), file).unwrap();
        let mut table_iter = Table::iter_with(Arc::new(table), ReadOptions::default());

        // Start the iterator at an arbitrary position
        table_iter.seek_to_last().unwrap();

        let (last_key, last_val) = table_iter.current().unwrap();
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

        // Seek key that exists
        table_iter
            .seek(&InternalKey::new(
                101_117_usize.to_string().as_bytes().to_vec(),
                1117,
                Operation::Put,
            ))
            .unwrap();
        let (actual_key, actual_val) = table_iter.current().unwrap();

        assert_eq!(
            actual_key,
            &InternalKey::new(
                101_117_usize.to_string().as_bytes().to_vec(),
                1117,
                Operation::Put,
            ),
            "Found an incorrect key"
        );
        assert_eq!(
            actual_val,
            &u64::encode_fixed_vec(101_117),
            "Found an incorrect value"
        );

        // Seeking a key that does not exist should end at an entry less than the target
        table_iter
            .seek(&InternalKey::new(
                101_117_usize.to_string().as_bytes().to_vec(),
                // Sequence numbers are sorted in descending order so this is greater than what
                // exists in the block
                1118,
                Operation::Put,
            ))
            .unwrap();
        let (actual_key, actual_val) = table_iter.current().unwrap();
        assert_eq!(
            actual_key,
            &InternalKey::new(
                101_117_usize.to_string().as_bytes().to_vec(),
                1117,
                Operation::Put,
            ),
            "Found an incorrect key. Should have found a key less than the target."
        );
        assert_eq!(
            actual_val,
            &u64::encode_fixed_vec(101_117),
            "Found an incorrect value. Should have found a value less than the target."
        );
    }

    #[test]
    fn given_a_table_with_one_block_get_can_filter_out_deleted_values() {
        setup();

        const MAX_BLOCK_SIZE_BYTES: usize = 256;
        let mut options = DbOptions::with_memory_env();
        // Use smaller block size to exercise block boundary conditions more often
        options.max_block_size = MAX_BLOCK_SIZE_BYTES;

        let keys = [
            InternalKey::new(b"batmann".to_vec(), 1, Operation::Put),
            InternalKey::new(b"robin".to_vec(), 3, Operation::Put),
            InternalKey::new(b"robin".to_vec(), 2, Operation::Delete),
            InternalKey::new(b"tumtum".to_vec(), 5, Operation::Delete),
            InternalKey::new(b"tumtum".to_vec(), 4, Operation::Put),
            InternalKey::new(b"tumtum".to_vec(), 1, Operation::Put),
        ];
        let mut table_builder = TableBuilder::new(options.clone(), 55).unwrap();

        for key in keys {
            let val = key.get_sequence_number();
            table_builder
                .add_entry(Rc::new(key), val.to_string().as_bytes())
                .unwrap();
        }

        table_builder.finalize().unwrap();
        drop(table_builder);

        let file_path = FileNameHandler::new(options.db_path().to_string()).get_table_file_path(55);
        let file = options.filesystem_provider().open_file(&file_path).unwrap();
        let table = Table::open(options.clone(), file).unwrap();

        let found_value = table
            .get(
                &ReadOptions::default(),
                &InternalKey::new_for_seeking(b"robin".to_vec(), 3),
            )
            .unwrap()
            .unwrap();
        assert_eq!(b"3".to_vec(), found_value);

        let found_value = table
            .get(
                &ReadOptions::default(),
                &InternalKey::new(b"robin".to_vec(), 2, Operation::Delete),
            )
            .unwrap();
        assert_eq!(
            None, found_value,
            "Looking exactly for a deleted key should find the key."
        );

        let found_value = table
            .get(
                &ReadOptions::default(),
                &InternalKey::new_for_seeking(b"robin".to_vec(), 2),
            )
            .err();
        assert_eq!(
            Some(ReadError::KeyNotFound),
            found_value,
            "Using a seeking key should not find a deleted key in the table"
        );

        let found_value = table
            .get(
                &ReadOptions::default(),
                &InternalKey::new_for_seeking(b"robin".to_vec(), 1),
            )
            .err();
        assert_eq!(Some(ReadError::KeyNotFound), found_value);
    }

    #[test]
    fn given_a_table_with_multiple_blocks_get_can_filter_out_deleted_values() {
        setup();

        const MAX_BLOCK_SIZE_BYTES: usize = 256;
        let mut options = DbOptions::with_memory_env();
        // Use smaller block size to exercise block boundary conditions more often
        options.max_block_size = MAX_BLOCK_SIZE_BYTES;

        let mut table_builder = TableBuilder::new(options.clone(), 55).unwrap();
        for idx in 0..400_usize {
            let num = idx + 100_000;
            let key = InternalKey::new(
                num.to_string().as_bytes().to_vec(),
                idx as u64,
                Operation::Put,
            );
            table_builder
                .add_entry(Rc::new(key), &u64::encode_fixed_vec(num as u64))
                .unwrap();
        }

        // Put a record with multiple versions
        let user_key_to_find = 100_404_usize.to_string().as_bytes().to_vec();
        table_builder
            .add_entry(
                Rc::new(InternalKey::new(
                    user_key_to_find.clone(),
                    403,
                    Operation::Delete,
                )),
                &Vec::new(),
            )
            .unwrap();
        table_builder
            .add_entry(
                Rc::new(InternalKey::new(
                    user_key_to_find.clone(),
                    402,
                    Operation::Put,
                )),
                b"v3",
            )
            .unwrap();
        table_builder
            .add_entry(
                Rc::new(InternalKey::new(
                    user_key_to_find.clone(),
                    401,
                    Operation::Put,
                )),
                b"v2",
            )
            .unwrap();
        table_builder
            .add_entry(
                Rc::new(InternalKey::new(
                    user_key_to_find.clone(),
                    400,
                    Operation::Put,
                )),
                b"original",
            )
            .unwrap();

        // Add some more values to sandwich
        for idx in 0..400_usize {
            let num = idx + 100_000 + 405;
            let key = InternalKey::new(
                num.to_string().as_bytes().to_vec(),
                idx as u64 + 405,
                Operation::Put,
            );
            table_builder
                .add_entry(Rc::new(key), &u64::encode_fixed_vec(num as u64))
                .unwrap();
        }

        table_builder.finalize().unwrap();
        drop(table_builder);

        let file_path = FileNameHandler::new(options.db_path().to_string()).get_table_file_path(55);
        let file = options.filesystem_provider().open_file(&file_path).unwrap();
        let table = Table::open(options.clone(), file).unwrap();

        let found_value = table
            .get(
                &ReadOptions::default(),
                &InternalKey::new_for_seeking(user_key_to_find.clone(), 401),
            )
            .unwrap()
            .unwrap();
        assert_eq!(b"v2".to_vec(), found_value);

        let found_value = table
            .get(
                &ReadOptions::default(),
                &InternalKey::new_for_seeking(user_key_to_find.clone(), 402),
            )
            .unwrap()
            .unwrap();
        assert_eq!(b"v3".to_vec(), found_value);

        let found_value = table
            .get(
                &ReadOptions::default(),
                &InternalKey::new(user_key_to_find.clone(), 403, Operation::Delete),
            )
            .unwrap();
        assert_eq!(
            None, found_value,
            "Looking exactly for a deleted key should find the key."
        );

        let found_value = table
            .get(
                &ReadOptions::default(),
                &InternalKey::new_for_seeking(user_key_to_find, 403),
            )
            .err();
        assert_eq!(
            Some(ReadError::KeyNotFound),
            found_value,
            "Using a seeking key should not find a deleted key in the table"
        );
    }
}
