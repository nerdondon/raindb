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

use crc::{Crc, CRC_32_ISCSI};
use integer_encoding::FixedInt;
use std::convert::{TryFrom, TryInto};
use std::fmt;
use std::io::{ErrorKind, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::errors::{DBIOError, LogIOError, LogSerializationErrorKind};
use crate::fs::{FileSystem, RandomAccessFile, ReadonlyRandomAccessFile};
use crate::utils::crc::{mask_checksum, unmask_checksum};

/**
The length of block headers.

This is 7 bytes.
*/
const HEADER_LENGTH_BYTES: usize = 4 + 2 + 1;

/**
The size of blocks in the log file format.

This is set at 32 KiB.
*/
const BLOCK_SIZE_BYTES: usize = 32 * 1024;

/**
CRC calculator using the iSCSI polynomial.

LevelDB uses the [google/crc32c](https://github.com/google/crc32c) CRC implementation. This
implementation specifies using the iSCSI polynomial so that is what we use here as well.
*/
const CRC_CALCULATOR: Crc<u32> = Crc::<u32>::new(&CRC_32_ISCSI);

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

1. A 32-bit checksum of the data
1. The length as a 2-byte integer with a fixed-size encoding
1. The block type converted to a 1 byte integer with a fixed-size encoding
1. The data

*/
#[derive(Debug)]
pub(crate) struct BlockRecord {
    /// A checksum of the data in this block.
    checksum: u32,

    /// The size of the data within the block.
    length: u16,

    /// The [`BlockType`] of the block.
    block_type: BlockType,

    /// User data to be stored in a block.
    data: Vec<u8>,
}

/// Crate-only methods
impl BlockRecord {
    pub(crate) fn new(length: u16, block_type: BlockType, data: Vec<u8>) -> Self {
        let checksum = CRC_CALCULATOR.checksum(&data);

        Self {
            checksum,
            length,
            block_type,
            data,
        }
    }
}

impl From<&BlockRecord> for Vec<u8> {
    fn from(record: &BlockRecord) -> Self {
        let initial_capacity = HEADER_LENGTH_BYTES + record.data.len();
        let mut buf: Vec<u8> = Vec::with_capacity(initial_capacity);
        // Mask the checksum before storage in case there are other checksums being done
        buf.extend_from_slice(&u32::encode_fixed_vec(mask_checksum(record.checksum)));
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

        // The first four bytes are the length of the data
        let checksum = u32::decode_fixed(&buf[0..4]);
        let unmasked_checksum = unmask_checksum(checksum);

        // The next two bytes are the length of the data
        let data_length = u16::decode_fixed(&buf[4..6]);

        // The last byte should be the block type
        let block_type: BlockType = buf[6].try_into()?;

        // Get data and check the integrity of the data
        let data = buf[HEADER_LENGTH_BYTES..].to_vec();
        let calculated_checksum = CRC_CALCULATOR.checksum(&data);
        if calculated_checksum != unmasked_checksum {
            return Err(LogIOError::Seralization(LogSerializationErrorKind::Other(
                format!(
                    "The checksums of the data did not match. Expected {unmasked_checksum} but \
                    got {calculated_checksum}"
                ),
            )));
        }

        Ok(BlockRecord::new(data_length, block_type, data))
    }
}

/** Handles all write activity to a log file. */
pub(crate) struct LogWriter {
    /// The path to the log file.
    log_file_path: PathBuf,

    /// The underlying file representing the log.
    log_file: Box<dyn RandomAccessFile>,

    /**
    The offset in the current block being written to.

    This position is not necessarily aligned to a block i.e. it can be in the middle of a block
    during a write operation.
    */
    current_block_offset: usize,
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
        let log_file_size = log_file.len()? as usize;
        if log_file_size > 0 {
            block_offset = log_file_size % BLOCK_SIZE_BYTES;
        }

        Ok(LogWriter {
            log_file_path: log_file_path.as_ref().to_path_buf(),
            log_file,
            current_block_offset: block_offset,
        })
    }

    /// Append `data` to the log.
    pub fn append(&mut self, data: &[u8]) -> LogIOResult<()> {
        let mut data_to_write = data;
        let mut is_first_data_chunk = true;

        loop {
            let block_available_space = BLOCK_SIZE_BYTES - self.current_block_offset;
            if block_available_space < HEADER_LENGTH_BYTES {
                if block_available_space > 0 {
                    log::debug!(
                        "Log file {:?}. There is not enough remaining space in the current block \
                    for the header. Filling it with zeroes.",
                        self.log_file_path
                    );
                    self.log_file
                        .write_all(&vec![0; HEADER_LENGTH_BYTES - 1][0..block_available_space])?;
                }

                // Switch to a new block
                self.current_block_offset = 0;
            }

            let space_available_for_data =
                BLOCK_SIZE_BYTES - self.current_block_offset - HEADER_LENGTH_BYTES;

            // The length available for the next data chunk a.k.a. how much of the buffer can
            // actually be written
            let block_data_chunk_length = if data_to_write.len() < space_available_for_data {
                data_to_write.len()
            } else {
                space_available_for_data
            };

            let is_last_data_chunk = data_to_write.len() == block_data_chunk_length;
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

            if data_to_write.is_empty() {
                // Use a do-while loop formulation so that we emit a zero-length block if asked to
                // to append an empty buffer (same as in LevelDB)
                break;
            }
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
        let block = BlockRecord::new(data_length, block_type, data_chunk.to_vec());

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
        self.current_block_offset += bytes_written;
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

    # Legacy

    This is synonomous to LevelDB's `log::Reader::end_of_buffer_offset_`.
    */
    current_cursor_position: usize,

    /**
    The offset in the current block being written to.

    # Legacy

    This is synonomous to whenever LevelDB calls `log::Reader::buffer_.size()`. LevelDB uses a this
    private `buffer_` field when reading from the physical file backing the log reader. For
    efficiency, LevelDB reads the physical file a block at a time e.g. 32 KiB at time. RainDB
    attempts to simplify matters by just reading what we need. Reading a block at a time helps with
    efficiency because the log is usually read in a sequential manner and all entries are
    processed (e.g. during database recoveries).
    */
    current_block_offset: usize,
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
            log_file,
            log_file_path: log_file_path.as_ref().to_path_buf(),
            initial_offset: initial_block_offset,
            current_cursor_position: initial_block_offset,
            current_block_offset: 0,
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

        if self.current_cursor_position > 0
            && (self.current_cursor_position as u64) == self.log_file.len()?
        {
            return Ok(vec![]);
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
        if offset_in_block > (BLOCK_SIZE_BYTES - (HEADER_LENGTH_BYTES - 1)) {
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
    Read the physical record from the file system and parse it into a [`BlockRecord`].

    Returns the parsed [`BlockRecord`].
    */
    fn read_physical_record(&mut self) -> LogIOResult<BlockRecord> {
        // Read the header
        let mut header_buffer = [0; HEADER_LENGTH_BYTES];
        let header_bytes_read = self.log_file.read(&mut header_buffer)?;
        if header_bytes_read < HEADER_LENGTH_BYTES {
            // The end of the file was reached before we were able to read a full header. This
            // can occur if the log writer died in the middle of writing the record.
            let err_msg = format!(
                "Unexpectedly reached the end of the log file at {log_file_path:?} while \
                attempting to read a header.",
                log_file_path = self.log_file_path
            );
            return Err(LogIOError::IO(DBIOError::new(
                ErrorKind::UnexpectedEof,
                err_msg,
            )));
        }
        self.current_block_offset += header_bytes_read;

        let data_length = u16::decode_fixed(&header_buffer[4..6]) as usize;

        // Read the payload
        let mut data_buffer = vec![0; data_length];
        let data_bytes_read = self.log_file.read(&mut data_buffer)?;

        if data_bytes_read < data_length {
            // The end of the file was reached before we were able to read a full data chunk. This
            // can occur if the log writer died in the middle of writing the record.
            let err_msg = format!(
                "Unexpectedly reached the end of the log file at {log_file_path:?} while \
                attempting to read the data chunk.",
                log_file_path = self.log_file_path
            );
            return Err(LogIOError::IO(DBIOError::new(
                ErrorKind::UnexpectedEof,
                err_msg,
            )));
        }

        // Parse the payload
        let serialized_block = [header_buffer.to_vec(), data_buffer].concat();
        let block_record: BlockRecord = BlockRecord::try_from(&serialized_block)?;
        self.current_cursor_position += header_buffer.len() + data_bytes_read;
        self.current_block_offset =
            (self.current_block_offset + data_bytes_read) % BLOCK_SIZE_BYTES;

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
