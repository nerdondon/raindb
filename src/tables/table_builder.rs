use std::io::Write;

use integer_encoding::FixedInt;
use snap::write::FrameEncoder;

use crate::config::{
    TableFileCompressionType, MAX_BLOCK_DATA_SIZE, PREFIX_COMPRESSION_RESTART_INTERVAL,
};
use crate::file_names::FileNameHandler;
use crate::fs::RandomAccessFile;
use crate::key::{LookupKey, RainDbKeyType};
use crate::tables::block::MetaIndexKey;
use crate::utils::bytes::BinarySeparable;
use crate::utils::crc::mask_checksum;
use crate::DbOptions;

use super::block_handle::BlockHandle;
use super::constants::{BLOCK_DESCRIPTOR_SIZE_BYTES, CRC_CALCULATOR};
use super::errors::BuilderError;
use super::BlockBuilder;
use super::FilterBlockBuilder;

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
*/
pub(crate) struct TableBuilder<'t> {
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
    data_block_builder: BlockBuilder<'t, LookupKey>,

    /// A block builder for index entries.
    index_block_builder: BlockBuilder<'t, LookupKey>,

    /// A builder for the table file's filter block.
    filter_block_builder: FilterBlockBuilder,

    /// The current number of entries that have been added to the table.
    num_entries: usize,

    /// The last key that was added to the table.
    maybe_last_key_added: Option<LookupKey>,
}

/// Public methods
impl<'t> TableBuilder<'t> {
    /// Create a new instance of [`TableBuilder`].
    pub fn new(options: DbOptions, file_number: u64) -> TableBuildResult<Self> {
        let file_name_handler = FileNameHandler::new(options.db_path().to_string());
        let table_file_name = file_name_handler.get_table_file_name(file_number);
        let file = options
            .filesystem_provider()
            .create_file(&table_file_name)?;

        Ok(Self {
            options,
            file_number,
            file,
            file_closed: false,
            num_entries: 0,
            maybe_last_key_added: None,
            data_block_builder: BlockBuilder::new(PREFIX_COMPRESSION_RESTART_INTERVAL),
            index_block_builder: BlockBuilder::new(1),
            filter_block_builder: FilterBlockBuilder::new(options.filter_policy()),
            current_offset: 0,
        })
    }

    /**
    Add a key-value pair to the table being constructed.

    # Panics

    The following invariants must be maintained:

    1. The table must not have been finalized or abandoned.
    1. The provided key is larger than any previously provided key.

    # Legacy

    This method is synonomous to LevelDB's `TableBuilder::Add`. The RainDB implementation here
    differs from the LevelDB implementation in that it delays the timing of writing the data block.
    LevelDB will immediately write a block to disk once it hits the size threshold and will keep
    some state fields for the written block. Only when the next key-value pair is added, will it
    build up an index entry from the saved state and add the entry to the index.

    RainDB eschews maintaining this state and will delay both the emission of the data block
    as well as the insertion of the index entry until the next key is seen. Because table building
    generall occurs from a pre-existing collection of data, there isn't much of a delay until the
    next entry is added or the entire table is finalized and flushed to disk.
    */
    pub fn add_entry(&mut self, key: &'t LookupKey, value: &Vec<u8>) -> TableBuildResult<()> {
        // Panic if our invariants are not maintained. This is a bug.
        assert!(!self.file_closed, "{}", BuilderError::AlreadyClosed);
        assert!(
            self.maybe_last_key_added
                .as_ref()
                .map_or(true, |last_key| last_key < key),
            "{}",
            BuilderError::OutOfOrder
        );

        if self.data_block_builder.approximate_size() >= MAX_BLOCK_DATA_SIZE {
            let starting_offset = self.current_offset;
            self.write_block()?;

            // Create a block handle to the data block and add as an index entry
            let last_key_added = self.maybe_last_key_added.as_ref().unwrap();
            let key_separator = BinarySeparable::find_shortest_separator(last_key_added, key);
            let block_handle =
                BlockHandle::new(self.current_offset, self.current_offset - starting_offset);
            self.index_block_builder.add_entry(
                &LookupKey::try_from(key_separator).unwrap(),
                &Vec::from(&block_handle),
            );
        }

        self.maybe_last_key_added = Some(key.clone());
        self.num_entries += 1;
        self.data_block_builder.add_entry(key, value);

        // Add entry to the filter block
        self.filter_block_builder.add_key(key.as_bytes());

        Ok(())
    }

    /**
    Finish building the table. Flushes all pending blocks to disk and adds final metadata.

    # Panics

    The table should have been finalized or abandoned already.
    */
    pub fn finalize(&mut self) -> TableBuildResult<()> {
        assert!(!self.file_closed, "{}", BuilderError::AlreadyClosed);

        // Flush pending data to disk
        if let Some(last_key_added) = self.maybe_last_key_added.as_ref() {
            if !self.data_block_builder.is_empty() {
                let starting_offset = self.current_offset;
                self.write_block()?;

                // Create a block handle to the data block and add as an index entry
                let key_separator = BinarySeparable::find_shortest_successor(last_key_added);
                let block_handle =
                    BlockHandle::new(self.current_offset, self.current_offset - starting_offset);
                self.index_block_builder.add_entry(
                    &LookupKey::try_from(key_separator).unwrap(),
                    &Vec::from(&block_handle),
                );
            }
        }

        self.file_closed = true;

        // Write the filter block
        let filter_block_offset = self.current_offset;
        let filter_block_contents = self.filter_block_builder.finalize();
        self.emit_block_to_disk(&filter_block_contents, TableFileCompressionType::None)?;
        let filter_block_handle =
            BlockHandle::new(filter_block_offset, filter_block_contents.len() as u64);

        // Write the metaindex block
        let metaindex_offset = self.current_offset;
        let metaindex_builder: BlockBuilder<'_, MetaIndexKey> =
            BlockBuilder::new(PREFIX_COMPRESSION_RESTART_INTERVAL);

        // Write the index block

        // Write the footer

        Ok(())
    }

    /**
    Indicates that the contents of the builder should be abandoned.

    If the creator of the table does not call [`TableBuilder::finalize`], it must call
    [`TableBuilder::abandon`].

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
}

/// Private methods
impl TableBuilder<'_> {
    /// Prepares a block and its metadata and writes them to disk.
    fn write_block(&mut self) -> TableBuildResult<()> {
        // Finalize data block
        let block_contents = self.data_block_builder.finalize();

        // For now, compression is always on and always uses Snappy
        // TODO: Support turning compression on and off and also more compression algorithms
        let compressed_block: Vec<u8> = vec![];
        let snappy_encoder = FrameEncoder::new(compressed_block);
        snappy_encoder.write_all(block_contents)?;
        snappy_encoder.flush()?;

        // Emit the raw block if the compression ratio is less than 12.5% (1/8)
        if compressed_block.len() < (block_contents.len() - (block_contents.len() / 8)) {
            self.emit_block_to_disk(block_contents, TableFileCompressionType::None)?;
        } else {
            self.emit_block_to_disk(
                compressed_block.as_slice(),
                TableFileCompressionType::Snappy,
            )?;
        }

        // Signal filter block builder that a new block was written
        self.filter_block_builder
            .notify_new_data_block(self.current_offset as usize);

        self.data_block_builder.reset();
        Ok(())
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
        let digest = CRC_CALCULATOR.digest();
        digest.update(block_contents);
        digest.update(&[compression_type as u8]);
        let checksum = digest.finalize();

        // Write block descriptor
        self.file.write_all(&[compression_type as u8])?;
        self.file
            .write_all(&u32::encode_fixed_vec(mask_checksum(checksum)))?;

        self.current_offset += (block_contents.len() + BLOCK_DESCRIPTOR_SIZE_BYTES) as u64;
        self.file.flush()?;

        Ok(())
    }
}
