/*!
The database module contains the primary API for interacting with the key-value store.
*/

use std::path::PathBuf;
use std::sync::Arc;

use crate::errors::RainDBResult;
use crate::file_names::{DATA_DIR, WAL_DIR};
use crate::filter_policy::{BloomFilterPolicy, FilterPolicy};
use crate::fs::{FileSystem, OsFileSystem};
use crate::memtable::{MemTable, SkipListMemTable};
use crate::tables::block::DataBlockReader;
use crate::tables::BlockCacheKey;
use crate::utils::cache::LRUCache;
use crate::write_ahead_log::WALWriter;
use crate::Cache;

/**
Holds options to control database behavior.

There is a mix of options to configure here that are remniscent of those configurable in
LevelDB and RocksDB.
*/
#[derive(Debug)]
pub struct DbOptions {
    // TODO: Add option for users to set a custom comparator.
    /**
    The path of the director to use for the database's operations.

    **This defaults to the current working directory.**
    */
    db_path: String,

    /**
    The maximum size that the memtable can reach before it is flushed to disk.

    Up to two memtables can reside in memory at a time, one actively serving reads and writes
    and a second one in the process of being flushed to disk.

    **This defaults to 4 MiB.**
    */
    write_buffer_size: usize,

    /**
    This amount of bytes will be written to a file before switching to a new one.

    Most clients should leave this parameter alone. However if your filesystem is more efficient
    with larger files, you could consider increasing the value. The downside will be longer
    compactions and hence longer latency/performance hiccups. Another reason to increase this
    parameter might be when you are initially populating a large database.

    **This defaults to 2 MiB.**
    */
    max_file_size: usize,

    /**
    A wrapper around a particular file system to use.

    **This defaults to [`OsFileSystem`](crate::fs::OsFileSystem).**
    */
    filesystem_provider: Arc<Box<dyn FileSystem>>,

    /**
    The filter policy to use for filtering requests to table files to reduce disk seeks.

    **This defaults to [`BloomFilterPolicy`](crate::filter_policy::BloomFilterPolicy).**
    */
    filter_policy: Arc<Box<dyn FilterPolicy>>,

    /**
    Cache used to store blocks read from disk in-memory to save on disk reads.

    This cache store uncompressed data so--if changed--the cache size should be appropriate to the
    application using the database.

    **This defaults to an 8 MiB internal cache if not set.**
    */
    block_cache: Arc<Box<dyn Cache<BlockCacheKey, DataBlockReader>>>,
}

/// Public methods
impl DbOptions {
    /// Get the database path.
    pub fn db_path(&self) -> &str {
        self.db_path.as_str()
    }

    /// Get the write buffer size.
    pub fn write_buffer_size(&self) -> usize {
        self.write_buffer_size
    }

    /// Get the databases maximum table file size.
    pub fn max_file_size(&self) -> &usize {
        &self.max_file_size
    }

    /// Get a strong reference to the file system provider.
    pub fn filesystem_provider(&self) -> Arc<Box<dyn FileSystem>> {
        Arc::clone(&self.filesystem_provider)
    }

    /// Get a strong reference to the filter policy.
    pub fn filter_policy(&self) -> Arc<Box<dyn FilterPolicy>> {
        Arc::clone(&self.filter_policy)
    }

    /// Get a strong reference to the block cache.
    pub fn block_cache(&self) -> Arc<Box<dyn Cache<BlockCacheKey, DataBlockReader>>> {
        Arc::clone(&self.block_cache)
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
            write_buffer_size: 4 * 1024 * 1024,
            max_file_size: 2 * 1024 * 1024,
            filesystem_provider: Arc::new(Box::new(OsFileSystem::new())),
            filter_policy: Arc::new(Box::new(BloomFilterPolicy::new(10))),
            block_cache: Arc::new(Box::new(LRUCache::<BlockCacheKey, DataBlockReader>::new(
                8 * 1024 * 1024,
            ))),
        }
    }
}

pub struct DB {
    options: DbOptions,
    memtable: Box<dyn MemTable>,
    wal: WALWriter,
}

impl DB {
    pub fn open(options: DbOptions) -> RainDBResult<DB> {
        log::info!(
            "Initializing raindb with the following options {:#?}",
            options
        );

        let fs = options.filesystem_provider();
        let db_path = options.db_path();

        // Create DB root directory
        log::info!("Creating DB root directory at {}.", &db_path);
        let root_path = PathBuf::from(&db_path);
        let mut wal_path = PathBuf::from(&db_path);
        wal_path.push(WAL_DIR);
        let mut data_path = PathBuf::from(&db_path);
        data_path.push(DATA_DIR);

        fs.create_dir_all(&root_path)?;
        fs.create_dir(&wal_path)?;
        fs.create_dir(&data_path)?;

        // Create WAL
        let wal = WALWriter::new(Arc::clone(&fs), wal_path.to_str().unwrap())?;

        // Create memtable
        let memtable = Box::new(SkipListMemTable::new());

        // Start compaction service

        Ok(DB {
            options,
            wal,
            memtable,
        })
    }

    pub fn get(&self) {
        todo!("working on it!")
    }

    pub fn put(&self) {
        todo!("working on it!")
    }

    pub fn delete(&self) {
        todo!("working on it!")
    }

    pub fn close(&self) {
        todo!("working on it!")
    }
}
