/*!
The write-ahead log (WAL) persists writes to disk to enable recovery of in-memory information in
the event of a crash.

The log file contents are series of 32 KiB blocks.

The current header of a block is 3 bytes and consists of a 2 byte u16 length and a 1 byte record
type.

A record never starts within the last 2 bytes of a block (since it won't fit). Any leftover bytes
here form the trailer, which must consist entirely of zero bytes and must be skipped by readers.

Aside: if exactly three bytes are left in the current block, and a new non-zero length record is
added, the writer must emit a `[BlockType::First](BlockType::First)` record (which contains zero
bytes of user data) to fill up the trailing three bytes of the block and then emit all of the user
data in subsequent blocks.
*/

use bincode::Options;
use serde::{Deserialize, Serialize};
use std::convert::TryFrom;
use std::io::Write;
use std::path::Path;

use crate::errors::WALWriteError;
use crate::file_names::FileNameHandler;
use crate::fs::{FileSystem, RandomAccessFile};

const HEADER_LENGTH_BYTES: usize = 2 + 1;

const BLOCK_SIZE_BYTES: usize = 32 * 1024;

const BLOCK_SIZE_MASK: usize = BLOCK_SIZE_BYTES - 1;

type WALWriteResult<T> = Result<T, WALWriteError>;

/**
Block record types denote whether the data contained in the block is split across multiple
blocks or if they contain all of the data for a single user record.

Note, the use of record is overloaded here. Be aware of the distinction between a block record
and the actual user record.
*/
#[repr(u8)]
#[derive(Debug, Deserialize, Serialize)]
pub(crate) enum BlockType {
    /// Denotes that the block contains the entirety of a user record.
    Full = 0,
    /// Denotes the first fragment of a user record.
    First,
    /// Denotes the interior fragments of a user record.
    Middle,
    /// Denotes the last fragment of a user record.
    Last,
}

/**
A record that is stored in a particular block. It is potentially only a fragment of a full user
record.
*/
#[derive(Debug, Deserialize, Serialize)]
pub(crate) struct BlockRecord {
    /// The size of the data within the block.
    length: u16,

    /// The [`BlockType`] of the block.
    block_type: BlockType,

    /// User data to be stored in a block.
    data: Vec<u8>,
}

impl From<&BlockRecord> for Vec<u8> {
    fn from(value: &BlockRecord) -> Self {
        bincode::DefaultOptions::new()
            .with_fixint_encoding()
            .serialize(value)
            .unwrap()
    }
}

/** Handles all write activity to the write-ahead log. */
pub struct WALWriter<'fs> {
    /** A wrapper around a particular file system to use. */
    fs: &'fs Box<dyn FileSystem>,

    /** The underlying file representing the WAL.  */
    wal_file: Box<dyn RandomAccessFile>,

    /** Handler for databases filenames. */
    file_name_handler: FileNameHandler,

    /**
    The offset within the last block that was written to or zero for the start of a new block.
    */
    current_block_offset: usize,
}

/// Public methods.
impl<'fs> WALWriter<'fs> {
    /**
    Construct a new `WALWriter`.

    * `fs`- The wrapped file system to use for I/O.
    * `db_path` - The absolute path where the database files reside.
    */
    pub fn new<P: AsRef<Path>>(fs: &'fs Box<dyn FileSystem>, db_path: P) -> WALWriteResult<Self> {
        let file_name_handler =
            FileNameHandler::new(db_path.as_ref().to_str().unwrap().to_string());
        let wal_path = file_name_handler.get_wal_path();

        log::info!("Creating WAL file at {}", wal_path.as_path().display());
        let wal_file = fs.create_file(wal_path.as_path())?;

        let mut block_offset = 0;
        let wal_file_size = fs.get_file_size(&wal_path)? as usize;
        if wal_file_size > 0 {
            block_offset = wal_file_size % BLOCK_SIZE_BYTES;
        }

        Ok(WALWriter {
            fs,
            file_name_handler,
            wal_file,
            current_block_offset: block_offset,
        })
    }

    /// Append `data` to the log.
    pub fn append(&mut self, data: &[u8]) -> WALWriteResult<()> {
        let mut data_to_write = &data[..];
        let mut is_first_data_chunk = true;

        while !data_to_write.is_empty() {
            let mut block_available_space = BLOCK_SIZE_BYTES - self.current_block_offset;

            if block_available_space < HEADER_LENGTH_BYTES {
                log::debug!(
                    "There is not enough remaining space in the current block for the header. \
                    Filling it with zeroes."
                );
                self.wal_file
                    .write_all(&vec![0, 0, 0, 0][0..block_available_space])?;
                self.current_block_offset = 0;
                block_available_space = BLOCK_SIZE_BYTES;
            }

            let space_available_for_data = block_available_space - HEADER_LENGTH_BYTES;
            let block_data_chunk_length = if data_to_write.len() < space_available_for_data {
                data_to_write.len()
            } else {
                space_available_for_data
            };

            let is_last_data_chunk = block_available_space == space_available_for_data;
            let block_type = if is_first_data_chunk && is_last_data_chunk {
                BlockType::Full
            } else if is_first_data_chunk {
                BlockType::First
            } else if is_last_data_chunk {
                BlockType::Last
            } else {
                BlockType::Middle
            };

            self.emit_block(block_type, &data_to_write[0..block_data_chunk_length])?;
            // Remove chunk that was written from the front
            data_to_write = data_to_write.split_at(block_data_chunk_length).1;
            is_first_data_chunk = false;
        }

        Ok(())
    }
}

/// Private methods.
impl<'fs> WALWriter<'fs> {
    /// Write the block out to the underlying medium.
    fn emit_block(&mut self, block_type: BlockType, data_chunk: &[u8]) -> WALWriteResult<()> {
        // Convert `usize` to `u16` so that it fits in our header format.
        let data_length = u16::try_from(data_chunk.len())?;
        let block = BlockRecord {
            length: data_length,
            block_type,
            data: data_chunk.to_vec(),
        };

        log::info!(
            "Writing new record to WAL with length {} and block type {:?}.",
            data_length,
            block_type
        );
        self.wal_file
            .write_all(Vec::<u8>::from(&block).as_slice())?;
        self.wal_file.flush()?;

        let bytes_written = HEADER_LENGTH_BYTES + data_chunk.len();
        self.current_block_offset += bytes_written;
        log::info!("Wrote {} bytes to the WAL.", bytes_written);
        Ok(())
    }
}
