/*!
The database module contains the primary API for interacting with the key-value store.
*/

use parking_lot::{Condvar, RwLock};
use std::path::PathBuf;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;

use crate::batch::Batch;
use crate::errors::RainDBResult;
use crate::file_names::{DATA_DIR, WAL_DIR};
use crate::memtable::{MemTable, SkipListMemTable};
use crate::table_cache::TableCache;
use crate::versioning::version_set::VersionSet;
use crate::write_ahead_log::WALWriter;
use crate::DbOptions;

/**
A thread requesting a write operation.

When multiple threads request a write operation, RainDB will queue up the threads so that the
writes occur serially. Threads waiting in the queue are parked and then signalled to wake up when it
is their turn to perform the requested operation.
*/
struct Writer {
    /// The batch of operations this writer is requesting to be performed.
    batch: Batch,

    /// Whether the write operations should be synchronously flushed to disk.
    synchronous_write: bool,

    /// Whether the requested operation was completed regardless of whether if failed or succeeded.
    operation_completed: bool,

    /**
    A condition variable to signal the thread to park or wake-up to perform it's requested
    operation.
    */
    thread_signaller: Condvar,

    /**
    The set of versions currently representing the database.

    The current version represents the most up to date values in the database. Other versions are
    kept to support a consistent view for live iterators.
    */
    version_set: VersionSet,
}

/// Struct holding database fields that need a lock before accessing.
struct GuardedDbFields {
    /// The current write-ahead log (WAL) number.
    curr_wal_file_number: u64,

    /**
    A condidtion variable used to notify parked threads that background work (e.g. compaction) has
    finished.
    */
    background_work_finished_signal: Condvar,

    /// Queued threads waiting to perform write operations.
    writer_queue: Vec<Writer>,
}

/// The primary database object that exposes the public API.
pub struct DB {
    /// Options for the operation of the database.
    options: DbOptions,

    /**
    An in-memory table of key-value pairs to support quick access to recently changed values.

    All operations (reads and writes) go through this in-memory representation first.
    */
    memtable: Box<dyn MemTable>,

    /// The writer for the current write-ahead log file.
    wal: WALWriter,

    /// A cache of table files.
    table_cache: Arc<TableCache>,

    /// Database fields that require a lock for accesses (reads and writes).
    guarded_fields: RwLock<GuardedDbFields>,

    /// Field indicating if the database is shutting down.
    is_shutting_down: AtomicBool,
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
