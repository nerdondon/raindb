/*!
The database module contains the primary API for interacting with the key-value store.
*/

use std::io;
use std::path::PathBuf;

use crate::file_names::{DATA_DIR, WAL_DIR};
use crate::fs::{FileSystem, OsFileSystem};
use crate::memtable::MemTable;
use crate::write_ahead_log::WALWriter;

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

    **This defaults to the current working directory.**
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
            db_path: std::env::current_dir()
                .unwrap()
                .to_str()
                .unwrap()
                .to_owned(),
            write_buffer_size: 4 * 1024 * 1024,
            max_file_size: 2 * 1024 * 1024,
            filesystem_provider: Box::new(OsFileSystem::new()),
        }
    }
}

pub struct DB<'fs> {
    options: DbOptions,
    memtable: MemTable,
    wal: WALWriter<'fs>,
}

impl DB<'_> {
    pub fn open<'fs>(options: DbOptions) -> Result<DB<'fs>, io::Error> {
        log::info!(
            "Initializing raindb with the following options {:#?}",
            options
        );

        let DbOptions {
            filesystem_provider: fs,
            db_path,
            ..
        } = options;

        // Create DB root directory
        log::info!("Creating DB root directory at {}.", &db_path);
        let root_path = PathBuf::from(&db_path);
        let wal_path = PathBuf::from(&db_path);
        wal_path.push(WAL_DIR);
        let data_path = PathBuf::from(&db_path);
        data_path.push(DATA_DIR);

        fs.create_dir_all(&root_path);
        fs.create_dir(&wal_path);
        fs.create_dir(&data_path);

        // Create WAL
        let wal = WALWriter::new(&fs, wal_path.to_str().unwrap())?;

        // Create memtable

        // Start compaction service

        Ok(DB { options, wal })
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
