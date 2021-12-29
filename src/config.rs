/*!
This module contains global configuration constants for RainDB.

These values usually correspond to configurable options for LevelDB (in its `options.h` file). To
get to an MVP and iterate, we keep static values here. These may be made configurable in future
versions.
*/

use std::convert::TryFrom;

use crate::errors::{RainDBError, RainDBResult};

/// The size of a `u32` in bytes.
pub(crate) const SIZE_OF_U32_BYTES: usize = 4;

/**
The approximate maximum size of user data that is allowed to be packed into a block of a table
file.

The data considered here is uncompressed data. The actual size of the data on disk may be smaller
due to compression.

In LevelDB this is configurable and has a default size of 4 KiB.
*/
pub(crate) const MAX_BLOCK_DATA_SIZE: usize = 4 * 1024;

/**
The number of keys between restart points when prefix compressing keys.

# Legacy

This is configurable in LevelDB as part of the options object because LevelDB tends to pass all
options around even if only a couple of the fields are required. RainDB is taking a more tactical
approach and so this value is not configurable.
*/
pub(crate) const PREFIX_COMPRESSION_RESTART_INTERVAL: usize = 16;

/// The maximum number of SSTable levels that is allowed.
pub(crate) const MAX_NUM_LEVELS: usize = 7;

/// Level-0 compaction is started when we hit this many files.
pub(crate) const L0_COMPACTION_TRIGGER: usize = 4;

/**
Soft limit on the number of level-0 files.

We slow down writes at this point.
*/
pub(crate) const L0_SLOWDOWN_WRITES_TRIGGER: usize = 8;

/**
Maximum number of level-0 files.

We stop writes at this point.
*/
pub(crate) const L0_STOP_WRITES_TRIGGER: usize = 12;

/**
The overall maximum group commit batch size.

This is set at 1 MiB.

This limit is so that doing a group commit gives better latency on average but does not affect the
latency of any single write too much.
*/
pub(crate) const MAX_GROUP_COMMIT_SIZE_BYTES: usize = 1 * 1024 * 1024;

/**
The upper threshold for a write to be considered a small write.

This is set at 128 KiB.
*/
pub(crate) const GROUP_COMMIT_SMALL_WRITE_THRESHOLD_BYTES: usize = 128 * 1024;

/**
The allowable additional bytes to add to a group commit where the first writer is doing a small
write.

This is set at 128 KiB.

If the initial writer of a group commit has a small write
(<= [`GROUP_COMMIT_SMALL_WRITE_THRESHOLD_BYTES`]), then limit the growth of the group commit so that
the small write is not impacted too much.
*/
pub(crate) const SMALL_WRITE_ADDITIONAL_GROUP_COMMIT_SIZE_BYTES: usize = 128 * 1024;

/**
Maximum level to which a newly compacted memtable is pushed if it does not create an overlap in
keys.

We try to push to level 2 to avoid the relatively expensive level 0 to level 1 compactions and to
avoid some expensive manifest file operations. We do not push all the way to the largest level since
that can generate a lot of wasted disk space if the same key space is being repeatedly overwritten.
*/
pub(crate) const MAX_MEM_COMPACT_LEVEL: usize = 2;

/**
The number of kibibytes that are allowed for a single seek of a table file.

Per LevelDB:
We arrange to automatically compact this file after a certain number of seeks. Let's assume:
    (1) One seek costs 10ms
    (2) Writing or reading 1MB costs 10ms (100MB/s)
    (3) A compaction of 1MB does 25MB of IO:
        1MB read from this level
        10-12MB read from next level (boundaries may be misaligned)
        10-12MB written to next level

This implies that 25 seeks cost the same as the compaction of 1MB of data. I.e., one seek costs
approximately the same as the compaction of 40KB of data. We are a little conservative and allow
approximately one seek for every 16KB of data before triggering a compaction.

TODO: Make allowed_seeks tunable (https://github.com/google/leveldb/issues/229)
*/
pub(crate) const SEEK_DATA_SIZE_THRESHOLD_KIB: u64 = 16 * 1024;

/**
The compression types available for blocks within a table file.

# LevelDB's analysis

Typical speeds of Snappy compression on an Intel(R) Core(TM)2 2.4GHz:
   ~200-500MB/s compression
   ~400-800MB/s decompression
Note that these speeds are significantly faster than most persistent storage speeds, and
therefore it is typically never worth switching to kNoCompression. Even if the input data is
incompressible, the Snappy compression implementation will efficiently detect that and will switch
to uncompressed mode.
*/
#[repr(u8)]
#[derive(Copy, Clone, Debug)]
pub enum TableFileCompressionType {
    /// No compression.
    None = 0,
    /// Snappy compression.
    Snappy,
}

impl TryFrom<u8> for TableFileCompressionType {
    type Error = RainDBError;

    fn try_from(value: u8) -> RainDBResult<TableFileCompressionType> {
        let compression_type = match value {
            0 => TableFileCompressionType::None,
            1 => TableFileCompressionType::Snappy,
            _ => {
                return Err(RainDBError::Other(
                    format!(
                        "There was an problem parsing the table file compression type. The value received was {}.",
                        value
                    ),
                ))
            }
        };

        Ok(compression_type)
    }
}

/**
The compression type to use for blocks within a table file.

The default is [`TableFileCompressionType::Snappy`].
*/
pub(crate) const TABLE_FILE_COMPRESSION_TYPE: TableFileCompressionType =
    TableFileCompressionType::Snappy;
