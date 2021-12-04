/*!
The write-ahead log (WAL) persists writes to disk to enable recovery of in-memory information in
the event of a crash.

The log file contents are series of 32 KiB blocks.

The current header of a block is 3 bytes and consists of a 2 byte u16 length and a 1 byte record
type.

A record never starts within the last 2 bytes of a block (since it won't fit). Any leftover bytes
here form the trailer, which must consist entirely of zero bytes and must be skipped by readers.

Note that if exactly three bytes are left in the current block, and a new non-zero length record is
added, the writer must emit a `[BlockType::First](BlockType::First)` record (which contains zero
bytes of user data) to fill up the trailing three bytes of the block and then emit all of the user
data in subsequent blocks.
*/

use integer_encoding::FixedInt;
use std::convert::{TryFrom, TryInto};
use std::io::{self, ErrorKind, SeekFrom, Write};
use std::path::Path;
use std::sync::Arc;

use crate::errors::{WALIOError, WALSerializationErrorKind};
use crate::file_names::FileNameHandler;
use crate::fs::{FileSystem, RandomAccessFile, ReadonlyRandomAccessFile};

/**
The length of block headers.

This is 3 bytes.
*/
const HEADER_LENGTH_BYTES: usize = 2 + 1;

/**
The size of blocks in the write-ahead log.

This is set at 32 KiB.
*/
const BLOCK_SIZE_BYTES: usize = 32 * 1024;

type WALIOResult<T> = Result<T, WALIOError>;

/**
Block record types denote whether the data contained in the block is split across multiple
blocks or if they contain all of the data for a single user record.

Note, the use of record is overloaded here. Be aware of the distinction between a block record
and the actual user record.
*/
#[repr(u8)]
#[derive(Clone, Copy, Debug)]
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

impl TryFrom<u8> for BlockType {
    type Error = WALIOError;

    fn try_from(value: u8) -> WALIOResult<BlockType> {
        let operation = match value {
            0 => BlockType::Full,
            1 => BlockType::First,
            2 => BlockType::Middle,
            3 => BlockType::Last,
            _ => {
                return Err(WALIOError::Seralization(WALSerializationErrorKind::Other(
                    format!(
                        "There was an problem parsing the block type. The value received was {}",
                        value
                    ),
                )))
            }
        };

        Ok(operation)
    }
}

/**
A record that is stored in a particular block. It is potentially only a fragment of a full user
record.

# Serialization

When serialized to disk the block record will have the following format:

1. The length as a 2-byte integer with a fixed-size encoding
1. The block type converted to a 1 byte integer with a fixed-size encoding
1. The data

*/
#[derive(Debug)]
pub(crate) struct BlockRecord {
    /// The size of the data within the block.
    length: u16,

    /// The [`BlockType`] of the block.
    block_type: BlockType,

    /// User data to be stored in a block.
    data: Vec<u8>,
}

impl From<&BlockRecord> for Vec<u8> {
    fn from(record: &BlockRecord) -> Self {
        let initial_capacity = HEADER_LENGTH_BYTES + record.data.len();
        let mut buf: Vec<u8> = Vec::with_capacity(initial_capacity);
        buf.extend_from_slice(&u16::encode_fixed_vec(record.length));
        buf.extend_from_slice(&[record.block_type as u8]);
        buf.extend_from_slice(&record.data);

        buf
    }
}

impl TryFrom<&Vec<u8>> for BlockRecord {
    type Error = WALIOError;

    fn try_from(buf: &Vec<u8>) -> WALIOResult<BlockRecord> {
        if buf.len() < HEADER_LENGTH_BYTES {
            let error_msg = format!(
                "Failed to deserialize the provided buffer to a WAL record. The buffer was expected to be at least the size of the header ({} bytes) but was {}.",
                HEADER_LENGTH_BYTES, buf.len()
            );
            return Err(WALIOError::Seralization(WALSerializationErrorKind::Other(
                error_msg,
            )));
        }

        // The first two bytes are the length of the data
        let data_length = u16::decode_fixed(&buf[0..2]);

        // The third byte should be the block type
        let block_type: BlockType = buf[2].try_into()?;

        Ok(BlockRecord {
            length: data_length,
            block_type,
            data: buf[3..].to_vec(),
        })
    }
}

/** Handles all write activity to the write-ahead log. */
pub(crate) struct WALWriter {
    /** A wrapper around a particular file system to use. */
    fs: Arc<Box<dyn FileSystem>>,

    /** The underlying file representing the WAL.  */
    wal_file: Box<dyn RandomAccessFile>,

    /** Handler for file names used by the database. */
    file_name_handler: FileNameHandler,

    /**
    The byte offset within the file.

    This cursor position is not necessarily aligned to a block i.e. it can be in the middle of a
    lock during a write operation.
    */
    current_cursor_position: usize,
}

/// Public methods.
impl WALWriter {
    /**
    Construct a new `WALWriter`.

    * `fs`- The wrapped file system to use for I/O.
    * `db_path` - The absolute path where the database files reside.
    * `file_number` - The file number of the write-ahead log.
    */
    pub fn new<P: AsRef<Path>>(
        fs: Arc<Box<dyn FileSystem>>,
        db_path: P,
        file_number: u64,
    ) -> WALIOResult<Self> {
        let file_name_handler =
            FileNameHandler::new(db_path.as_ref().to_str().unwrap().to_string());
        let wal_path = file_name_handler.get_wal_path(file_number);

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
            current_cursor_position: block_offset,
        })
    }

    /// Append `data` to the log.
    pub fn append(&mut self, data: &[u8]) -> WALIOResult<()> {
        let mut data_to_write = &data[..];
        let mut is_first_data_chunk = true;

        while !data_to_write.is_empty() {
            let mut block_available_space = BLOCK_SIZE_BYTES - self.current_cursor_position;

            if block_available_space < HEADER_LENGTH_BYTES {
                log::debug!(
                    "There is not enough remaining space in the current block for the header. \
                    Filling it with zeroes."
                );
                self.wal_file
                    .write_all(&vec![0, 0, 0][0..block_available_space])?;
                self.current_cursor_position = 0;
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
impl WALWriter {
    /// Write the block out to the underlying medium.
    fn emit_block(&mut self, block_type: BlockType, data_chunk: &[u8]) -> WALIOResult<()> {
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
            block.block_type
        );
        self.wal_file
            .write_all(Vec::<u8>::from(&block).as_slice())?;
        self.wal_file.flush()?;

        let bytes_written = HEADER_LENGTH_BYTES + data_chunk.len();
        self.current_cursor_position += bytes_written;
        log::info!("Wrote {} bytes to the WAL.", bytes_written);
        Ok(())
    }
}

/** Handles all read activity to the write-ahead log. */
pub(crate) struct WALReader {
    /** A wrapper around a particular file system to use. */
    fs: Arc<Box<dyn FileSystem>>,

    /** The underlying file representing the WAL.  */
    wal_file: Box<dyn ReadonlyRandomAccessFile>,

    /** Handler for databases filenames. */
    file_name_handler: FileNameHandler,

    /**
    An initial offset to start reading the WAL from.

    This can be a byte offset and is not necessarily aligned the start of a block.
    */
    initial_offset: usize,

    /**
    The offset to the byte being read.

    This should usually be aligned to the start of a block.
    */
    current_cursor_position: usize,
}

/// Public methods.
impl WALReader {
    /**
    Construct a new `WALReader`.

    * `fs`- The wrapped file system to use for I/O.
    * `db_path` - The absolute path where the database files reside.
    * `file_number` - The file number of the write-ahead log.
    * `initial_block_offset` - An initial offset to start reading the WAL from.
    */
    pub fn new<P: AsRef<Path>>(
        fs: Arc<Box<dyn FileSystem>>,
        db_path: P,
        file_number: u64,
        initial_block_offset: usize,
    ) -> WALIOResult<Self> {
        let file_name_handler =
            FileNameHandler::new(db_path.as_ref().to_str().unwrap().to_string());
        let wal_path = file_name_handler.get_wal_path(file_number);

        log::info!("Reading the WAL file at {}", wal_path.as_path().display());
        let wal_file = fs.open_file(wal_path.as_path())?;

        let reader = Self {
            fs,
            wal_file,
            file_name_handler,
            initial_offset: initial_block_offset,
            current_cursor_position: initial_block_offset,
        };

        Ok(reader)
    }

    /// Read a record from the WAL.
    pub fn read_record(&mut self) -> WALIOResult<Vec<u8>> {
        self.current_cursor_position += self.seek_to_initial_block()? as usize;
        // A buffer consolidating all of the fragments retrieved from the WAL file.
        let mut data_buffer: Vec<u8> = vec![];

        loop {
            let record = self.read_physical_record()?;
            data_buffer.extend(record.data);

            match record.block_type {
                BlockType::Full => {
                    return Ok(data_buffer);
                }
                BlockType::First => {}
                BlockType::Middle => {}
                BlockType::Last => {
                    return Ok(data_buffer);
                }
            }
        }
    }
}

/// Private methods.
impl WALReader {
    /**
    Seek to the first block on or before the initial offset that was provided.

    The initial offset may land in the middle of a block's byte range. We will look backwards to
    find the start of the block if this happens. Corrupted data will be logged and skipped.

    Returns the number of bytes the cursor was moved.
    */
    fn seek_to_initial_block(&mut self) -> WALIOResult<u64> {
        let offset_in_block = self.initial_offset % BLOCK_SIZE_BYTES;
        let mut block_start_position = self.initial_offset - offset_in_block;

        // Skip ahead if we landed in the trailer.
        if offset_in_block > BLOCK_SIZE_BYTES - 2 {
            block_start_position += BLOCK_SIZE_BYTES;
        }

        // Move the file's underlying cursor to the start of the first block
        if block_start_position > 0 {
            return Ok(self
                .wal_file
                .seek(SeekFrom::Start(block_start_position as u64))?);
        }

        Ok(0)
    }

    /**
    Read the physical record from the file system and parse it into a [`BlockRecord`](BlockRecord).

    Returns the parsed [`BlockRecord`](BlockRecord).
    */
    fn read_physical_record(&mut self) -> WALIOResult<BlockRecord> {
        // Read the header
        let mut header_buffer = [0; 3];
        let header_bytes_read = self.wal_file.read(&mut header_buffer)?;
        if header_bytes_read < HEADER_LENGTH_BYTES {
            // The end of the file was reached before we were able to read a full header. This
            // can occur if the log writer died in the middle of writing the record.
            return Err(WALIOError::IO(io::Error::new(
                ErrorKind::UnexpectedEof,
                "Unexpectedly reached the end of the file while attempting to read a header."
                    .to_string(),
            )));
        }

        let data_length = u16::decode_fixed(&header_buffer[0..2]);

        // Read the payload
        let mut data_buffer = Vec::with_capacity(data_length as usize);
        let data_bytes_read = self.wal_file.read(&mut data_buffer)?;

        if data_bytes_read < (data_length as usize) {
            // The end of the file was reached before we were able to read a full data chunk. This
            // can occur if the log writer died in the middle of writing the record.
            return Err(WALIOError::IO(io::Error::new(
                ErrorKind::UnexpectedEof,
                "Unexpectedly reached the end of the file while attempting to read the data chunk."
                    .to_string(),
            )));
        }

        // Parse the payload
        let serialized_block = [header_buffer.to_vec(), data_buffer].concat();
        let block_record: BlockRecord = BlockRecord::try_from(&serialized_block)?;
        self.current_cursor_position += header_buffer.len() + data_bytes_read;

        Ok(block_record)
    }

    /// Log bytes dropped with the provided reason.
    fn log_drop(num_bytes_dropped: u64, reason: String) {
        log::error!(
            "Skipped reading {} bytes. Reason: {}",
            num_bytes_dropped,
            &reason
        );
    }

    /// Log bytes dropped because corruption was detected.
    fn log_corruption(num_bytes_corrupted: u64) {
        WALReader::log_drop(num_bytes_corrupted, "Detected corruption.".to_owned())
    }
}
