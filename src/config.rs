/*!
This module contains global configuration constants for RainDB.

These values usually correspond to configurable options for LevelDB (in its `options.h` file). To
get to an MVP and iterate, we keep static values here. These may be made configurable in future
versions.
*/

/**
The approximate maximum size of user data that is allowed to be packed into a block of a table
file.

The data considered here is uncompressed data. The actual size of the data on disk may be smaller
due to compression.

In LevelDB this is configurable and has a default size of 4 KiB.
*/
pub(crate) const MAX_BLOCK_DATA_SIZE: usize = 4 * 1024;

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
Maximum level to which a new compacted memtable is pushed if it does not create overlap in keys.

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
