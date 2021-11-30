/*!
The database module contains the primary API for interacting with the key-value store.
*/

use parking_lot::{Condvar, Mutex, MutexGuard};
use std::path::PathBuf;
use std::ptr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;
use std::time;

use crate::batch::Batch;
use crate::config::{L0_SLOWDOWN_WRITES_TRIGGER, L0_STOP_WRITES_TRIGGER};
use crate::errors::{RainDBError, RainDBResult};
use crate::file_names::FileNameHandler;
use crate::file_names::{DATA_DIR, WAL_DIR};
use crate::memtable::{MemTable, SkipListMemTable};
use crate::table_cache::TableCache;
use crate::versioning::version_set::VersionSet;
use crate::write_ahead_log::WALWriter;
use crate::{DbOptions, WriteOptions};

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
    pub operation_completed: bool,

    /**
    The result of the operation.

    This field must be populated if `operation_completed` was set to `true`. This field is mostly
    used to indicate status to a writer if its operation was part of a group commit.
    */
    pub operation_result: Option<RainDBResult<()>>,

    /**
    A condition variable to signal the thread to park or wake-up to perform it's requested
    operation.
    */
    thread_signaller: Condvar,
}

impl PartialEq for Writer {
    fn eq(&self, other: &Self) -> bool {
        self.batch == other.batch
            && self.synchronous_write == other.synchronous_write
            && self.operation_completed == other.operation_completed
    }
}

impl Writer {
    /// Create a new instance of [`Writer`].
    fn new(batch: Batch, synchronous_write: bool) -> Self {
        Self {
            batch,
            synchronous_write,
            operation_completed: false,
            operation_result: None,
            thread_signaller: Condvar::new(),
        }
    }

    /// Whether the writer should perform synchronous writes.
    fn is_synchronous_write(&self) -> bool {
        self.synchronous_write
    }

    /// Get a reference to the operations this writer needs to perform.
    fn batch(&self) -> &Batch {
        &self.batch
    }

    /// Parks the thread while it waits for its turn to perform its operation.
    fn wait_for_turn(&self, database_mutex_guard: MutexGuard<GuardedDbFields>) {
        self.thread_signaller.wait(&mut database_mutex_guard)
    }

    /**
    Notify that writer that it is potentially it's turn to perform its operation.

    If the operation was already completed as part of a group commit, the thread will return.
    */
    fn notify_writer(&self) -> bool {
        self.thread_signaller.notify_one()
    }
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
    writer_queue: Vec<Arc<Writer>>,

    /// Holds the immutable table i.e. the memtable currently undergoing compaction.
    maybe_immutable_memtable: Option<Box<dyn MemTable>>,

    /**
    The set of versions currently representing the database.

    The current version represents the most up to date values in the database. Other versions are
    kept to support a consistent view for live iterators.
    */
    version_set: VersionSet,

    /**
    An error that was encountered when performing background operations (e.g. when compacting a
    memtable).
    */
    maybe_background_error: Option<RainDBError>,
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

    /// Current write-ahead log file number.
    curr_wal_file_number: u64,

    /// A cache of table files.
    table_cache: Arc<TableCache>,

    /// Database fields that require a lock for accesses (reads and writes).
    guarded_fields: Arc<Mutex<GuardedDbFields>>,

    /// Handler for file names used by the database.
    file_name_handler: FileNameHandler,

    /// Field indicating if the database is shutting down.
    is_shutting_down: AtomicBool,

    /**
    Field indicating if there is an immutable memtable.

    An memtable is made immutable when it is undergoing the compaction process.
    */
    has_immutable_memtable: AtomicBool,
}

/// Public methods
impl DB {
    pub fn open(options: DbOptions) -> RainDBResult<DB> {
        log::info!(
            "Initializing raindb with the following options {:#?}",
            options
        );

        let fs = options.filesystem_provider();
        let db_path = options.db_path();
        let file_name_handler = FileNameHandler::new(db_path.to_string());

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
        let wal_file_number = 0;
        let wal = WALWriter::new(Arc::clone(&fs), wal_path.to_str().unwrap(), wal_file_number)?;

        // Create memtable
        let memtable = Box::new(SkipListMemTable::new());

        // Start compaction service

        Ok(DB {
            options,
            wal,
            memtable,
            file_name_handler,
            curr_wal_file_number: wal_file_number,
        })
    }

    pub fn get(&self) {
        todo!("working on it!")
    }

    pub fn put(&self, write_options: WriteOptions, key: Vec<u8>, value: Vec<u8>) {
        todo!("working on it!")
    }

    pub fn delete(&self, write_options: WriteOptions, key: Vec<u8>) {
        todo!("working on it!")
    }

    pub fn close(&self) {
        todo!("working on it!")
    }
}

/// Private methods
impl DB {
    /**
    Apply changes contained in the write batch. The requesting thread is queued if there are
    multiple write requests.

    # Group commits

    Like LevelDB, RainDB may perform an extra level of batching on top of the `batch` already
    specified. If there are multiple threads making write requests, RainDB will queue the threads
    so that write operations are performed serially. In order to reduce request latency, RainDB will
    group batch requests on the queue up to a certain size limit and perform the requested writes
    together as if they were in the same [`Batch`]. We call this extra level of batching a group
    commit per the [commit that added it] in LevelDB.

    [commit that added it]: https://github.com/google/leveldb/commit/d79762e27369365a7ffe1f2e3a5c64b0632079e1
    */
    fn apply_changes(&self, write_options: WriteOptions, batch: Batch) -> RainDBResult<()> {
        let writer = Arc::new(Writer::new(batch, write_options.synchronous));
        let locked_fields = self.guarded_fields.lock();
        locked_fields.writer_queue.push(Arc::clone(&writer));

        let maybe_first_writer = locked_fields.writer_queue.first().map(|wrapped_writer| {
            /*
            We want to compare the shallow identity of writer objects so we need to dereference
            to the writer object. The first dereference is for the `&Arc<Writer>` and gives us
            `Arc<Writer>`. The second dereference is for going through the `Arc` smart pointer to
            give us just `Writer`.
            */
            **wrapped_writer
        });
        let is_first_writer =
            maybe_first_writer.is_some() && ptr::eq(&*writer, &maybe_first_writer.unwrap());

        while !writer.operation_completed && !is_first_writer {
            writer.wait_for_turn(locked_fields);
        }

        // Check if the work for this thread was already completed as part of a group commit and
        // return the result if it was
        if writer.operation_completed {
            return writer.operation_result.unwrap();
        }

        Ok(())
    }

    /**
    Ensures that there is room in the memtable for more writes and triggers a compaction if
    necessary.

    - `force_compaction` - This should usually be false. When true, this will force a compaction
      of the memtable.

    # Concurrency

    The calling thread must be holding a lock to the guarded fields and the calling thread must be
    at the front of the writer queue. During the course of this method the lock may be released and
    reacquired.
    */
    fn make_room_for_write(
        &self,
        mutex_guard: MutexGuard<GuardedDbFields>,
        force_compaction: bool,
    ) -> RainDBResult<()> {
        let allow_write_delay = !force_compaction;

        loop {
            let num_level_zero_files = match mutex_guard.version_set.num_files_at_level(0) {
                Ok(num_files) => num_files,
                Err(error) => return Err(RainDBError::VersionRead(error)),
            };

            if mutex_guard.maybe_background_error.is_some() {
                // We encountered an issue with a background task. Return with the error.
                let error = mutex_guard.maybe_background_error.unwrap();
                log::error!("Stopping compaction check because there may be a relevant background error. Error: {}", error);
                return Err(error);
            } else if allow_write_delay && num_level_zero_files >= L0_SLOWDOWN_WRITES_TRIGGER {
                /*
                We are getting close to hitting the hard limit on the number level 0 files. Rather
                than delaying a single write by several seconds when we hit the hard limit, start
                delaying each individual write by 1ms to reduce latency variance. Also, this delay
                hands over some CPU to the compaction thread in case it is sharing the same core as
                the writer.
                */
                log::info!("Slowing down write's to allow some some time for compaction");
                let one_millis = time::Duration::from_millis(1);
                parking_lot::MutexGuard::<'_, GuardedDbFields>::unlocked(&mut mutex_guard, || {
                    thread::sleep(one_millis);
                    // Do not delay a single write more than once
                    allow_write_delay = false;
                });
            } else if !force_compaction
                && (self.memtable.approximate_memory_usage() <= self.options.max_memtable_size())
            {
                // There is still room in the memtable
                log::info!(
                    "There is still room in the memtable for writes. Proceeding with write."
                );
                return Ok(());
            } else if mutex_guard.maybe_immutable_memtable.is_some() {
                /*
                We have filled up the current memtable, but the previous one is still being
                compacted, so we wait.
                */
                log::info!(
                    "Current memtable is full but the previous memtable is still compacting. \
                    Waiting before attempting to compact current memtable."
                );
                mutex_guard
                    .background_work_finished_signal
                    .wait(&mut mutex_guard);
            } else if num_level_zero_files >= L0_STOP_WRITES_TRIGGER {
                log::info!(
                    "Too many level 0 files. Waiting for compaction before proceeding with write \
                    operations."
                );
                mutex_guard
                    .background_work_finished_signal
                    .wait(&mut mutex_guard);
            } else {
                if mutex_guard.version_set.maybe_prev_wal_number().is_some() {
                    let error_msg =
                        "Detected that the memtable is already undergoing compaction (possibly by \
                        another thread) while the current thread is attempting to start a \
                        compaction.";
                    log::error!("{}", error_msg);
                    return Err(RainDBError::BackgroundTask(error_msg.to_string()));
                }

                log::info!("Memtable is full. Attempting compaction.");

                // First create a new WAL file.
                let new_wal_number = mutex_guard.version_set.get_new_file_number();
                let maybe_wal_writer = WALWriter::new(
                    self.options.filesystem_provider(),
                    self.options.db_path(),
                    new_wal_number,
                );

                if maybe_wal_writer.is_err() {
                    let error = maybe_wal_writer.err().unwrap();
                    log::error!(
                        "Encountered an error while trying to create a new WAL file. Error: {}",
                        &error
                    );

                    log::debug!(
                        "Set the WAL number we requested for reuse so we don't use up the file \
                        number space. Number to reuse: {}",
                        new_wal_number
                    );
                    mutex_guard.version_set.reuse_file_number(new_wal_number);

                    // Re-throw the error
                    return Err(RainDBError::WAL(error));
                }

                // Replace old WAL state fields with new WAL values
                let wal_writer = maybe_wal_writer.unwrap();
                self.wal = wal_writer;
                mutex_guard.curr_wal_file_number = new_wal_number;

                log::info!("Move the current memtable to the immutable memtable field.");
                mutex_guard.maybe_immutable_memtable = Some(self.memtable);
                self.has_immutable_memtable.store(true, Ordering::Release);

                log::info!("Create the new memtable.");
                self.memtable = Box::new(SkipListMemTable::new());

                // Do not force another compaction since we have room
                force_compaction = false;

                // Schedule compaction of the immutable memtable.
                self.maybe_schedule_compaction();
            }
        }

        Ok(())
    }
}
