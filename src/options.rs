// Copyright (c) 2021 Google LLC
//
// Use of this source code is governed by an MIT-style
// license that can be found in the LICENSE file or at
// https://opensource.org/licenses/MIT.

/*!
This module holds the various option structures that can be passed to RainDB operations
*/

use std::sync::Arc;

use crate::filter_policy::{BloomFilterPolicy, FilterPolicy};
use crate::fs::{FileSystem, InMemoryFileSystem, OsFileSystem};
use crate::tables::block::DataBlockReader;
use crate::tables::BlockCacheKey;
use crate::utils::cache::LRUCache;
use crate::{Cache, Snapshot};

/**
Holds options to control database behavior.

There is a mix of options to configure here that are remniscent of those configurable in
LevelDB and RocksDB.
*/
#[derive(Clone, Debug)]
pub struct DbOptions {
    // TODO: Add option for users to set a custom comparator.
    /**
    The path of the director to use for the database's operations.

    **This defaults to the current working directory.**
    */
    pub db_path: String,

    /**
    The maximum size that the memtable can reach before it is flushed to disk.

    Up to two memtables can reside in memory at a time, one actively serving reads and writes
    and a second one in the process of being flushed to disk.

    This option corresponds to `write_buffer_size` in LevelDB. We feel the RainDB name is more clear
    as to what function this option performs.

    **This defaults to 4 MiB.**
    */
    pub max_memtable_size: usize,

    /**
    This amount of bytes will be written to a file before switching to a new one.

    Most clients should leave this parameter alone. However if your filesystem is more efficient
    with larger files, you could consider increasing the value. The downside will be longer
    compactions and hence longer latency/performance hiccups. Another reason to increase this
    parameter might be when you are initially populating a large database.

    **This defaults to 2 MiB.**
    */
    pub max_file_size: u64,

    /**
    The approximate maximum size of user data that is allowed to be packed into a block of a table
    file.

    The data considered here is uncompressed data. The actual size of the data on disk may be smaller
    due to compression.

    In LevelDB this is configurable and has a default size of 4 KiB.
    */
    pub max_block_size: usize,

    /**
    A wrapper around a particular file system to use.

    **This defaults to [`OsFileSystem`](crate::fs::OsFileSystem).**
    */
    pub filesystem_provider: Arc<dyn FileSystem>,

    /**
    The filter policy to use for filtering requests to table files to reduce disk seeks.

    **This defaults to [`BloomFilterPolicy`](crate::filter_policy::BloomFilterPolicy).**
    */
    pub filter_policy: Arc<dyn FilterPolicy>,

    /**
    Cache used to store blocks read from disk in-memory to save on disk reads.

    This cache store uncompressed data so--if changed--the cache size should be appropriate to the
    application using the database.

    **This defaults to an 8 MiB internal cache if not set.**
    */
    pub block_cache: Arc<dyn Cache<BlockCacheKey, Arc<DataBlockReader>>>,

    /// If true, the database will be created if it is missing.
    pub create_if_missing: bool,

    /// If true, an error is raised if the database already exists.
    pub error_if_exists: bool,

    /**
    If true, append to existing manifest and write-ahead logs when opening a database.

    **This defaults to true (unlike in LevelDB that defaults to false).**
    */
    pub reuse_log_files: bool,
}

/// Public methods
impl DbOptions {
    /// Get [`DbOptions`] with an in-memory file system.
    pub fn with_memory_env() -> DbOptions {
        DbOptions {
            filesystem_provider: Arc::new(InMemoryFileSystem::new()),
            ..DbOptions::default()
        }
    }

    /// Get the database path.
    pub fn db_path(&self) -> &str {
        self.db_path.as_str()
    }

    /// Get the write buffer size.
    pub fn max_memtable_size(&self) -> usize {
        self.max_memtable_size
    }

    /// Get the database's maximum table file size.
    pub fn max_file_size(&self) -> u64 {
        self.max_file_size
    }

    /// Get the maximum block size.
    pub fn max_block_size(&self) -> usize {
        self.max_block_size
    }

    /// Get the whether the database is reusing log files.
    pub fn reuse_log_files(&self) -> bool {
        self.reuse_log_files
    }

    /// Get a strong reference to the file system provider.
    pub fn filesystem_provider(&self) -> Arc<dyn FileSystem> {
        Arc::clone(&self.filesystem_provider)
    }

    /// Get a strong reference to the filter policy.
    pub fn filter_policy(&self) -> Arc<dyn FilterPolicy> {
        Arc::clone(&self.filter_policy)
    }

    /// Get a strong reference to the block cache.
    pub fn block_cache(&self) -> Arc<dyn Cache<BlockCacheKey, Arc<DataBlockReader>>> {
        Arc::clone(&self.block_cache)
    }

    /// Returns true if we should create the database if it is missing.
    pub fn create_if_missing(&self) -> bool {
        self.create_if_missing
    }

    /// Get a reference to the db options's error if exists.
    pub fn error_if_exists(&self) -> bool {
        self.error_if_exists
    }
}

impl Default for DbOptions {
    fn default() -> Self {
        DbOptions {
            db_path: std::env::current_dir()
                .unwrap()
                .to_str()
                .unwrap()
                .to_owned(),
            max_memtable_size: 4 * 1024 * 1024,
            max_file_size: 2 * 1024 * 1024,
            max_block_size: 4 * 1024,
            filesystem_provider: Arc::new(OsFileSystem::new()),
            filter_policy: Arc::new(BloomFilterPolicy::new(10)),
            block_cache: Arc::new(LRUCache::<BlockCacheKey, Arc<DataBlockReader>>::new(
                8 * 1024 * 1024,
            )),
            create_if_missing: false,
            error_if_exists: false,
            reuse_log_files: true,
        }
    }
}

/// Options for read operations.
#[derive(Clone, Debug)]
pub struct ReadOptions {
    /**
    Cache data read as a result of a read operation.

    Callers may want to set this to false for bulk scans.

    **Defaults to true.**
    */
    pub fill_cache: bool,

    /**
    Configure the read operation to read as of the state of the supplied snapshot. If [`None`], the
    read will be performed at the state of the database when the request was received.

    This snapshot must have been received by the database it is being passed to and must not have
    been released yet.
    */
    pub snapshot: Option<Snapshot>,
}

impl Default for ReadOptions {
    fn default() -> Self {
        Self {
            fill_cache: true,
            snapshot: None,
        }
    }
}

/// Options for write operations.
#[derive(Clone, Debug)]
pub struct WriteOptions {
    /**
    Whether or not to perform the write operation synchronously.

    If true, the program will try to ensure that the write is flusehd completely to disk before it
    considers the write complete. This extra check means that writes with this flag on will be
    slower.

    If false and the machine crashes, some recent writes might be lost. Note that, if it is just the
    program that crashes (i.e. the machine does not reboot), then no writes will be lost.

    In other words, writes with this flag `false` has the same semantics as just a `write()` system
    call. A write with the flag as `true` has the semantics of a `write()` followed by an `fsync()`.

    **Defaults to false.**
    */
    pub synchronous: bool,
}

impl Default for WriteOptions {
    fn default() -> Self {
        Self { synchronous: false }
    }
}
