// Copyright (c) 2021 Google LLC
//
// Use of this source code is governed by an MIT-style
// license that can be found in the LICENSE file or at
// https://opensource.org/licenses/MIT.

use std::fmt::Debug;
use std::io::Write;
use std::rc::Rc;

use integer_encoding::FixedInt;
use snap::write::FrameEncoder;

use crate::config::{TableFileCompressionType, PREFIX_COMPRESSION_RESTART_INTERVAL};
use crate::file_names::FileNameHandler;
use crate::filter_policy;
use crate::fs::RandomAccessFile;
use crate::key::InternalKey;
use crate::utils::bytes::BinarySeparable;
use crate::utils::crc;
use crate::DbOptions;

use super::block::MetaIndexKey;
use super::block_handle::BlockHandle;
use super::constants::{BLOCK_DESCRIPTOR_SIZE_BYTES, CRC_CALCULATOR};
use super::errors::BuilderError;
use super::footer::Footer;
use super::BlockBuilder;
use super::FilterBlockBuilder;

/// Type alias for a [`Result`] that returns [`BuilderError`]'s.
type TableBuildResult<T> = Result<T, BuilderError>;

/**
Builds and outputs a table file (an immutable map and sorted map from keys to values).

# Format

A table file has the following format:

1. A series of data blocks
1. A series of meta blocks (blocks that contain table metadata e.g. a Bloom filter for reducing
   disk read costs)
1. A metadata index block (referred to as metaindex block) that provide offsets to meta blocks
1. An index block that provides offets to data blocks
1. A fixed-length footer providing offests to the index blocks

# Concurrency

The table builder cannot currently be passed between threads because it uses primitives that do not
implement [`Sync`] (e.g. [`Rc`]). This should not be a problem since a table builder is currently
only constructed and used locally in a single thread.
*/
pub(crate) struct TableBuilder {
    /// Options for configuring the operation of the database.
    options: DbOptions,

    /**
    Set to true when the file contents have been finalized or when abandoning the table
    generation.
    */
    file_closed: bool,

    /// The physical table file that is being written to.
    file: Box<dyn RandomAccessFile>,

    /// The file number that is being used to identify this file.
    file_number: u64,

    /// The current offset in the file where data is being appended.
    current_offset: u64,

    /// A block builder for data entries.
    data_block_builder: BlockBuilder<InternalKey>,

    /// A block builder for index entries.
    index_block_builder: BlockBuilder<InternalKey>,

    /// A builder for the table file's filter block.
    filter_block_builder: FilterBlockBuilder,

    /// The current number of entries that have been added to the table.
    num_entries: usize,

    /// The last key that was added to the table.
    maybe_last_key_added: Option<Rc<InternalKey>>,
}

/// Public methods
impl TableBuilder {
    /// Create a new instance of [`TableBuilder`].
    pub fn new(options: DbOptions, file_number: u64) -> TableBuildResult<Self> {
        let file_name_handler = FileNameHandler::new(options.db_path().to_string());
        let table_file_name = file_name_handler.get_table_file_path(file_number);
        let file = options
            .filesystem_provider()
            .create_file(&table_file_name, false)?;
        let filter_block_builder = FilterBlockBuilder::new(options.filter_policy());

        Ok(Self {
            options,
            file_number,
            file,
            file_closed: false,
            num_entries: 0,
            maybe_last_key_added: None,
            data_block_builder: BlockBuilder::new(PREFIX_COMPRESSION_RESTART_INTERVAL),
            index_block_builder: BlockBuilder::new(1),
            filter_block_builder,
            current_offset: 0,
        })
    }

    /**
    Add a key-value pair to the table being constructed.

    # Panics

    The following invariants must be maintained:

    1. The table must not have been finalized or abandoned.
    1. The provided key is ordered after any previously provided key. For example, if two keys have
       the same value, then they are always added with sequence numbers already in descending order.
       This requirement makes sense because tables are created from memtables which keeps records
       sorted by key already.

    # Legacy

    This method is synonomous to LevelDB's `TableBuilder::Add`. The RainDB implementation here
    differs from the LevelDB implementation in that it delays the timing of writing the data block.
    LevelDB will immediately write a block to disk once it hits the size threshold and will keep
    some state fields for the written block. Only when the next key-value pair is added, will it
    build up an index entry from the saved state and add the entry to the index.

    RainDB eschews maintaining this state and will delay both the emission of the data block
    as well as the insertion of the index entry until the next key is seen. Because table building
    generally occurs from a pre-existing collection of data, there isn't much of a delay until the
    next entry is added or the entire table is finalized and flushed to disk.
    */
    pub fn add_entry(&mut self, key: Rc<InternalKey>, value: &[u8]) -> TableBuildResult<()> {
        // Panic if our invariants are not maintained. This is a bug.
        assert!(!self.file_closed, "{}", BuilderError::AlreadyClosed);
        assert!(
            self.maybe_last_key_added
                .as_ref()
                .map_or(true, |last_key| last_key < &key),
            "{}",
            BuilderError::OutOfOrder
        );

        if self.data_block_builder.approximate_size() >= self.options.max_block_size() {
            let maybe_block_handle = self.flush_data_block()?;

            if let Some(block_handle) = maybe_block_handle {
                // Insert an index entry if a block was written
                let last_key_added = self.maybe_last_key_added.as_ref().unwrap();
                let key_separator =
                    BinarySeparable::find_shortest_separator(last_key_added.as_ref(), key.as_ref());
                let deserialized_separator = Rc::new(InternalKey::try_from(key_separator).unwrap());
                self.index_block_builder.add_entry(
                    Rc::clone(&deserialized_separator),
                    &Vec::from(&block_handle),
                );
            }
        }

        self.maybe_last_key_added = Some(key.clone());
        self.num_entries += 1;
        self.data_block_builder.add_entry(key.clone(), value);

        // Add entry to the filter block
        self.filter_block_builder
            .add_key(key.get_user_key().to_vec());

        Ok(())
    }

    /**
    Finish building the table. Any pending data is flushed to disk and final metadata is appended.

    # Panics

    The table should not have been finalized or abandoned already.
    */
    pub fn finalize(&mut self) -> TableBuildResult<()> {
        assert!(!self.file_closed, "{}", BuilderError::AlreadyClosed);

        // Flush pending data to disk
        let maybe_block_handle = self.flush_data_block()?;
        if let Some(block_handle) = maybe_block_handle {
            // Insert an index entry if a block was written
            let last_key_added = self.maybe_last_key_added.as_ref().unwrap();
            let key_separator = BinarySeparable::find_shortest_successor(last_key_added.as_ref());
            let serialized_separator = Rc::new(InternalKey::try_from(key_separator).unwrap());
            self.index_block_builder
                .add_entry(Rc::clone(&serialized_separator), &Vec::from(&block_handle));
        }

        self.file_closed = true;

        // Write the filter block
        let filter_block_offset = self.current_offset;
        let filter_block_contents = self.filter_block_builder.finalize();
        self.emit_block_to_disk(&filter_block_contents, TableFileCompressionType::None)?;
        let filter_block_handle =
            BlockHandle::new(filter_block_offset, filter_block_contents.len() as u64);

        // Write the metaindex block
        let mut metaindex_builder: BlockBuilder<MetaIndexKey> =
            BlockBuilder::new(PREFIX_COMPRESSION_RESTART_INTERVAL);
        let filter_block_key = MetaIndexKey::new(filter_policy::get_filter_block_name(
            self.options.filter_policy(),
        ));
        metaindex_builder.add_entry(Rc::new(filter_block_key), &Vec::from(&filter_block_handle));
        let metaindex_contents = metaindex_builder.finalize();
        let metaindex_handle = self.write_block(&metaindex_contents)?;
        metaindex_builder.reset();

        // Write the index block
        let index_contents = self.index_block_builder.finalize();
        let index_block_handle = self.write_block(&index_contents)?;
        self.index_block_builder.reset();

        // Write the footer
        let footer = Footer::new(metaindex_handle, index_block_handle);
        let serialized_footer = Vec::<u8>::try_from(&footer)?;
        self.file.write_all(&serialized_footer)?;
        self.current_offset += serialized_footer.len() as u64;

        Ok(())
    }

    /**
    Indicates that the contents of the builder should be abandoned.

    If the creator of the table does not call [`TableBuilder::finalize`], it must call
    `TableBuilder::abandon` (i.e this function).

    # Panics

    The table should have been finalized or abandoned already.
    */
    pub fn abandon(&mut self) {
        assert!(!self.file_closed, "{}", BuilderError::AlreadyClosed);
        self.file_closed = true;
    }

    /// Get the current size of the table file.
    pub fn file_size(&self) -> u64 {
        self.current_offset
    }

    /// Get the number of entries in the table file.
    pub fn get_num_entries(&self) -> usize {
        self.num_entries
    }
}

/// Private methods
impl TableBuilder {
    /// Write a data block to disk.
    fn flush_data_block(&mut self) -> TableBuildResult<Option<BlockHandle>> {
        if self.data_block_builder.is_empty() {
            return Ok(None);
        }

        let block_contents = self.data_block_builder.finalize();
        let block_handle = self.write_block(&block_contents)?;
        self.data_block_builder.reset();
        self.file.flush()?;

        // Signal filter block builder that a new block was written
        self.filter_block_builder
            .notify_new_data_block(self.current_offset as usize);

        Ok(Some(block_handle))
    }

    /**
    Prepares a block and its metadata and writes them to disk.

    Returns the a [`BlockHandle`] to the newly written block.
    */
    fn write_block(&mut self, block_contents: &[u8]) -> TableBuildResult<BlockHandle> {
        let start_offset = self.current_offset;

        // For now, compression is always on and always uses Snappy
        // TODO: Support turning compression on and off and also more compression algorithms
        let mut compressed_block: Vec<u8> = vec![];
        let mut snappy_encoder = FrameEncoder::new(compressed_block);
        snappy_encoder.write_all(block_contents)?;
        snappy_encoder.flush()?;
        compressed_block = snappy_encoder.into_inner().unwrap();

        // Emit the raw block if the compression ratio is less than 12.5% (1/8)
        let actual_block_size =
            if compressed_block.len() < (block_contents.len() - (block_contents.len() / 8)) {
                self.emit_block_to_disk(
                    compressed_block.as_slice(),
                    TableFileCompressionType::Snappy,
                )?;
                compressed_block.len()
            } else {
                self.emit_block_to_disk(block_contents, TableFileCompressionType::None)?;
                block_contents.len()
            };

        Ok(BlockHandle::new(start_offset, actual_block_size as u64))
    }

    /// Actually performs the operations to write a block to disk.
    fn emit_block_to_disk(
        &mut self,
        block_contents: &[u8],
        compression_type: TableFileCompressionType,
    ) -> TableBuildResult<()> {
        // Write the block contents
        self.file.write_all(block_contents)?;

        // Get the checksum of the data and the compression type
        let mut digest = CRC_CALCULATOR.digest();
        digest.update(block_contents);
        digest.update(&[compression_type as u8]);
        let checksum = digest.finalize();

        // Write block descriptor
        self.file.write_all(&[compression_type as u8])?;
        let masked_checksum = crc::mask_checksum(checksum);
        self.file
            .write_all(&u32::encode_fixed_vec(masked_checksum))?;

        self.current_offset += (block_contents.len() + BLOCK_DESCRIPTOR_SIZE_BYTES) as u64;

        Ok(())
    }
}

impl Debug for TableBuilder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TableBuilder")
            .field("options", &self.options)
            .field("file_closed", &self.file_closed)
            .field("file_number", &self.file_number)
            .field("current_offset", &self.current_offset)
            .field("num_entries", &self.num_entries)
            .field("maybe_last_key_added", &self.maybe_last_key_added)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use pretty_assertions::assert_eq;

    use crate::Operation;

    use super::*;

    #[test]
    fn table_builder_can_write_data_blocks_to_disk() {
        const MAX_BLOCK_SIZE_BYTES: usize = 256;
        let mut options = DbOptions::with_memory_env();
        // Use smaller block size to exercise block boundary conditions more often
        options.max_block_size = MAX_BLOCK_SIZE_BYTES;

        let mut table_builder = TableBuilder::new(options, 55).unwrap();
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

        assert_eq!(table_builder.get_num_entries(), 400);

        assert!(
            table_builder.file_size() >= (2 * MAX_BLOCK_SIZE_BYTES as u64),
            "The current file size should be at least as large as two blocks given our test input."
        );

        let pre_finalize_file_size = table_builder.file_size();
        table_builder.finalize().unwrap();

        assert!(table_builder.file_size() > pre_finalize_file_size);
    }

    #[test]
    #[should_panic(
        expected = "Attempted to perform an operation when the file had already been closed."
    )]
    fn table_builder_finalizing_an_abandoned_builder_should_panic() {
        const MAX_BLOCK_SIZE_BYTES: usize = 256;
        let mut options = DbOptions::with_memory_env();
        // Use smaller block size to exercise block boundary conditions more often
        options.max_block_size = MAX_BLOCK_SIZE_BYTES;

        let mut table_builder = TableBuilder::new(options, 55).unwrap();
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
        table_builder.abandon();

        table_builder.finalize().unwrap();
    }

    #[test]
    fn table_builder_succeeds_when_keys_are_added_in_sorted_order() {
        const MAX_BLOCK_SIZE_BYTES: usize = 256;
        let mut options = DbOptions::with_memory_env();
        // Use smaller block size to exercise block boundary conditions more often
        options.max_block_size = MAX_BLOCK_SIZE_BYTES;

        let keys = [
            InternalKey::new(b"batmann".to_vec(), 1, Operation::Put),
            InternalKey::new(b"robin".to_vec(), 3, Operation::Put),
            InternalKey::new(b"robin".to_vec(), 2, Operation::Delete),
            InternalKey::new(b"tumtum".to_vec(), 5, Operation::Put),
            InternalKey::new(b"tumtum".to_vec(), 1, Operation::Put),
        ];

        let mut table_builder = TableBuilder::new(options, 55).unwrap();

        for key in keys {
            table_builder.add_entry(Rc::new(key), b"random").unwrap();
        }
    }

    #[test]
    #[should_panic(expected = "Attempted to add a key but it was out of order.")]
    fn table_builder_panics_if_user_keys_are_not_added_in_sorted_order() {
        const MAX_BLOCK_SIZE_BYTES: usize = 256;
        let mut options = DbOptions::with_memory_env();
        // Use smaller block size to exercise block boundary conditions more often
        options.max_block_size = MAX_BLOCK_SIZE_BYTES;

        let mut table_builder = TableBuilder::new(options, 55).unwrap();

        table_builder
            .add_entry(
                Rc::new(InternalKey::new(b"def".to_vec(), 399, Operation::Put)),
                b"123",
            )
            .unwrap();
        table_builder
            .add_entry(
                Rc::new(InternalKey::new(b"abc".to_vec(), 400, Operation::Put)),
                b"456",
            )
            .unwrap();
    }

    #[test]
    #[should_panic(expected = "Attempted to add a key but it was out of order.")]
    fn table_builder_panics_when_user_keys_are_sorted_but_sequence_numbers_are_not_added_in_sorted_order(
    ) {
        const MAX_BLOCK_SIZE_BYTES: usize = 256;
        let mut options = DbOptions::with_memory_env();
        // Use smaller block size to exercise block boundary conditions more often
        options.max_block_size = MAX_BLOCK_SIZE_BYTES;

        let mut table_builder = TableBuilder::new(options, 55).unwrap();

        table_builder
            .add_entry(
                Rc::new(InternalKey::new(b"abc".to_vec(), 399, Operation::Put)),
                b"123",
            )
            .unwrap();
        table_builder
            .add_entry(
                Rc::new(InternalKey::new(b"def".to_vec(), 400, Operation::Put)),
                b"456",
            )
            .unwrap();
        table_builder
            .add_entry(
                Rc::new(InternalKey::new(b"ghi".to_vec(), 401, Operation::Put)),
                b"original",
            )
            .unwrap();
        table_builder
            .add_entry(
                Rc::new(InternalKey::new(b"ghi".to_vec(), 402, Operation::Put)),
                b"most up to date",
            )
            .unwrap();
    }
}
