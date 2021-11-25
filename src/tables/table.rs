use crc::{Crc, CRC_32_ISCSI};
use integer_encoding::FixedInt;
use snap::read::FrameDecoder;
use std::convert::TryFrom;
use std::io::{self, Read};

use crate::config::{TableFileCompressionType, SIZE_OF_U32_BYTES};
use crate::db::DbOptions;
use crate::filter_policy::get_filter_block_name;
use crate::fs::ReadonlyRandomAccessFile;
use crate::iterator::RainDbIterator;
use crate::key::RainDbKeyType;

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
    pub fn open(
        &self,
        options: DbOptions,
        file: Box<dyn ReadonlyRandomAccessFile>,
    ) -> TableResult<Table> {
        if file.len()? < (SIZE_OF_FOOTER_BYTES as u64) {
            return Err(ReadError::FailedToParse(format!(
                "Failed to open the table file. The file length ({:?}) is invalid.",
                file.len()
            )));
        }

        let cache_partition_id = (*options.block_cache()).new_id();

        log::debug!("Reading and parsing the table file footer");
        let mut footer_buf: Vec<u8> = vec![0; SIZE_OF_FOOTER_BYTES];
        file.read_exact(&mut footer_buf)?;
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
}
}
