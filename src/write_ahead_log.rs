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
use std::fs::File;
use std::io::{Result, Write};
use std::path::Path;

use crate::{file_names::FileNameHandler, fs::FileSystem};

const HEADER_LENGTH_BYTES: usize = 2 + 1;

const BLOCK_SIZE_BYTES: usize = 32 * 1024;

const BLOCK_SIZE_MASK: usize = BLOCK_SIZE_BYTES - 1;

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

    /** The underlying file representing  */
    wal_file: File,

    /** Handler for databases filenames. */
    file_name_handler: FileNameHandler,
}

impl<'fs> WALWriter<'fs> {
    /**
    Construct a new `WALWriter`.

    * `fs`- The wrapped file system to use for I/O.
    */
    pub fn new<P: AsRef<Path>>(fs: &'fs Box<dyn FileSystem>, db_path: P) -> Result<Self> {
        let file_name_handler =
            FileNameHandler::new(db_path.as_ref().to_str().unwrap().to_string());
        let wal_path = file_name_handler.get_wal_path();

        log::info!("Creating WAL file at {}", wal_path.as_path().display());
        let wal_file = fs.create_file(wal_path.as_path())?;

        Ok(WALWriter {
            fs,
            file_name_handler,
            wal_file,
        })
    }
}
