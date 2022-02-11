/*!
The log file format is used by both write-ahead logs and manifest files (a.k.a. descriptor logs).

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
use std::fmt;
use std::io::{ErrorKind, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::errors::{DBIOError, LogIOError, LogSerializationErrorKind};
use crate::fs::{FileSystem, RandomAccessFile, ReadonlyRandomAccessFile};

/**
The length of block headers.

This is 3 bytes.
*/
const HEADER_LENGTH_BYTES: usize = 2 + 1;

/**
The size of blocks in the log file format.

This is set at 32 KiB.
*/
const BLOCK_SIZE_BYTES: usize = 32 * 1024;

/// Alias for a [`Result`] that wraps a [`LogIOError`].
type LogIOResult<T> = Result<T, LogIOError>;

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
    type Error = LogIOError;

    fn try_from(value: u8) -> LogIOResult<BlockType> {
        let operation = match value {
            0 => BlockType::Full,
            1 => BlockType::First,
            2 => BlockType::Middle,
            3 => BlockType::Last,
            _ => {
                return Err(LogIOError::Seralization(LogSerializationErrorKind::Other(
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
    type Error = LogIOError;

    fn try_from(buf: &Vec<u8>) -> LogIOResult<BlockRecord> {
        if buf.len() < HEADER_LENGTH_BYTES {
            let error_msg = format!(
                "Failed to deserialize the provided buffer to a log block record. The buffer was \
                expected to be at least the size of the header ({} bytes) but was {}.",
                HEADER_LENGTH_BYTES,
                buf.len()
            );
            return Err(LogIOError::Seralization(LogSerializationErrorKind::Other(
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

/** Handles all write activity to a log file. */
pub(crate) struct LogWriter {
    /** A wrapper around a particular file system to use. */
    fs: Arc<dyn FileSystem>,

    /// The path to the log file.
    log_file_path: PathBuf,

    /** The underlying file representing the log.  */
    log_file: Box<dyn RandomAccessFile>,

    /**
    The byte offset within the file.

    This cursor position is not necessarily aligned to a block i.e. it can be in the middle of a
    lock during a write operation.
    */
    current_cursor_position: usize,
}

/// Public methods
impl LogWriter {
    /// Construct a new [`LogWriter`].
    pub fn new<P: AsRef<Path>>(
        fs: Arc<dyn FileSystem>,
        log_file_path: P,
        is_appending: bool,
    ) -> LogIOResult<Self> {
        log::info!(
            "Creating/appending to a log file at {}",
            log_file_path.as_ref().to_string_lossy()
        );
        let log_file = fs.create_file(log_file_path.as_ref(), is_appending)?;

        let mut block_offset = 0;
        let log_file_size = fs.get_file_size(log_file_path.as_ref())? as usize;
        if log_file_size > 0 {
            block_offset = log_file_size % BLOCK_SIZE_BYTES;
        }

        Ok(LogWriter {
            fs,
            log_file_path: log_file_path.as_ref().to_path_buf(),
            log_file,
            current_cursor_position: block_offset,
        })
    }

    /// Append `data` to the log.
    pub fn append(&mut self, data: &[u8]) -> LogIOResult<()> {
        let mut data_to_write = data;
        let mut is_first_data_chunk = true;

        while !data_to_write.is_empty() {
            let mut block_available_space = BLOCK_SIZE_BYTES - self.current_cursor_position;

            if block_available_space < HEADER_LENGTH_BYTES {
                log::debug!(
                    "Log file {:?}. There is not enough remaining space in the current block \
                    for the header. Filling it with zeroes.",
                    self.log_file_path
                );
                self.log_file
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

/// Private methods
impl LogWriter {
    /// Write the block out to the underlying medium.
    fn emit_block(&mut self, block_type: BlockType, data_chunk: &[u8]) -> LogIOResult<()> {
        // Convert `usize` to `u16` so that it fits in our header format.
        let data_length = u16::try_from(data_chunk.len())?;
        let block = BlockRecord {
            length: data_length,
            block_type,
            data: data_chunk.to_vec(),
        };

        log::info!(
            "Writing new record to log file at {:?} with length {} and block type {:?}.",
            self.log_file_path,
            data_length,
            block.block_type
        );
        self.log_file
            .write_all(Vec::<u8>::from(&block).as_slice())?;
        self.log_file.flush()?;

        let bytes_written = HEADER_LENGTH_BYTES + data_chunk.len();
        self.current_cursor_position += bytes_written;
        log::info!("Wrote {} bytes to the log file.", bytes_written);
        Ok(())
    }
}

impl fmt::Debug for LogWriter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("LogWriter")
            .field("log_file_path", &self.log_file_path)
            .finish()
    }
}

/** Handles all read activity to a log file. */
pub(crate) struct LogReader {
    /** A wrapper around a particular file system to use. */
    fs: Arc<dyn FileSystem>,

    /** The underlying file representing the log. */
    log_file: Box<dyn ReadonlyRandomAccessFile>,

    /// The path to the log file.
    log_file_path: PathBuf,

    /** The underlying file representing the log.  */

    /**
    An initial offset to start reading the log file from.

    This can be a byte offset and is not necessarily aligned the start of a block.
    */
    initial_offset: usize,

    /**
    The offset to the byte being read.

    This should usually be aligned to the start of a block.
    */
    current_cursor_position: usize,
}

/// Public methods
impl LogReader {
    /**
    Construct a new [`LogReader`].

    * `fs`- The wrapped file system to use for I/O.
    * `log_file_path` - The absolute path to the log file.
    * `initial_block_offset` - An initial offset to start reading the log file from.
    */
    pub fn new<P: AsRef<Path>>(
        fs: Arc<dyn FileSystem>,
        log_file_path: P,
        initial_block_offset: usize,
    ) -> LogIOResult<Self> {
        log::info!("Reading the log file at {:?}", log_file_path.as_ref());
        let log_file = fs.open_file(log_file_path.as_ref())?;

        let reader = Self {
            fs,
            log_file,
            log_file_path: log_file_path.as_ref().to_path_buf(),
            initial_offset: initial_block_offset,
            current_cursor_position: initial_block_offset,
        };

        Ok(reader)
    }

    /**
    Read a record from the log file.

    If the end of the log file has been reached, this method will return an empty [`Vec`].
    */
    pub fn read_record(&mut self) -> LogIOResult<Vec<u8>> {
        if self.current_cursor_position < self.initial_offset {
            self.current_cursor_position += self.seek_to_initial_block()? as usize;
        }

        // A buffer consolidating all of the fragments retrieved from the log file.
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
impl LogReader {
    /**
    Seek to the first block on or before the initial offset that was provided.

    The initial offset may land in the middle of a block's byte range. We will look backwards to
    find the start of the block if this happens. Corrupted data will be logged and skipped.

    Returns the number of bytes the cursor was moved.
    */
    fn seek_to_initial_block(&mut self) -> LogIOResult<u64> {
        let offset_in_block = self.initial_offset % BLOCK_SIZE_BYTES;
        let mut block_start_position = self.initial_offset - offset_in_block;

        // Skip ahead if we landed in the trailer.
        if offset_in_block > BLOCK_SIZE_BYTES - 2 {
            block_start_position += BLOCK_SIZE_BYTES;
        }

        // Move the file's underlying cursor to the start of the first block
        if block_start_position > 0 {
            return Ok(self
                .log_file
                .seek(SeekFrom::Start(block_start_position as u64))?);
        }

        Ok(0)
    }

    /**
    Read the physical record from the file system and parse it into a [`BlockRecord`](BlockRecord).

    Returns the parsed [`BlockRecord`](BlockRecord).
    */
    fn read_physical_record(&mut self) -> LogIOResult<BlockRecord> {
        // Read the header
        let mut header_buffer = [0; 3];
        let header_bytes_read = self.log_file.read(&mut header_buffer)?;
        if header_bytes_read < HEADER_LENGTH_BYTES {
            // The end of the file was reached before we were able to read a full header. This
            // can occur if the log writer died in the middle of writing the record.
            return Err(LogIOError::IO(DBIOError::new(
                ErrorKind::UnexpectedEof,
                "Unexpectedly reached the end of the file while attempting to read a header."
                    .to_string(),
            )));
        }

        let data_length = u16::decode_fixed(&header_buffer[0..2]);

        // Read the payload
        let mut data_buffer = Vec::with_capacity(data_length as usize);
        let data_bytes_read = self.log_file.read(&mut data_buffer)?;

        if data_bytes_read < (data_length as usize) {
            // The end of the file was reached before we were able to read a full data chunk. This
            // can occur if the log writer died in the middle of writing the record.
            return Err(LogIOError::IO(DBIOError::new(
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
        LogReader::log_drop(num_bytes_corrupted, "Detected corruption.".to_owned())
    }
}
