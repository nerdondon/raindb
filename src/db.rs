/*!
The database module contains the primary API for interacting with the key-value store.
*/

use crate::fs::{FileSystem, OsFileSystem};
use crate::memtable::MemTable;
use std::io;

/**
Holds options to control database behavior.

There is a mix of options to configure here that are remniscent of those configurable in
LevelDb and RocksDb.
*/
#[derive(Debug)]
pub struct DbOptions {
    // TODO: Add option for users to set a custom comparator.
    /**
    The path of the director to use for the database's operations.
    */
    pub db_path: String,

    /**
    The maximum size that the memtable can reach before it is flushed to disk.

    Up to two memtables can reside in memory at a time, one actively serving reads and writes
    and a second one in the process of being flushed to disk.

    **This defaults to 4 MiB.**
    */
    pub write_buffer_size: usize,

    /**
    This amount of bytes will be written to a file before switching to a new one.

    Most clients should leave this parameter alone. However if your filesystem is more efficient
    with larger files, you could consider increasing the value. The downside will be longer
    compactions and hence longer latency/performance hiccups. Another reason to increase this
    parameter might be when you are initially populating a large database.

    **This defaults to 2 MiB.**
    */
    pub max_file_size: usize,

    /**
    A wrapper around a particular file system to use.

    **This defaults to [`OsFileSystem`](crate::fs::OsFileSystem)**
    */
    filesystem_provider: Box<dyn FileSystem>,
}

impl Default for DbOptions {
    fn default() -> Self {
        DbOptions {
            db_path: Default::default(),
            write_buffer_size: 4 * 1024 * 1024,
            max_file_size: 2 * 1024 * 1024,
            filesystem_provider: Box::new(OsFileSystem::new()),
        }
    }
}

pub struct DB {
    options: DbOptions,
    memtable: MemTable,
}

impl DB {
    pub fn open(options: DbOptions) -> Result<DB, io::Error> {
        // Create WAL

        // Create memtable

        // Start compaction service

        DB { options }
    }

    pub fn get() {}

    pub fn put() {}

    pub fn delete() {}

    pub fn close() {}
}
