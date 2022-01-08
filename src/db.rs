/*!
The database module contains the primary API for interacting with the key-value store.
*/

use arc_swap::ArcSwap;
use parking_lot::{Condvar, Mutex, MutexGuard};
use std::collections::{HashSet, VecDeque};
use std::path::PathBuf;
use std::rc::Rc;
use std::sync::atomic::{AtomicBool, AtomicPtr, Ordering};
use std::sync::Arc;
use std::time::{self, Instant};
use std::{io, thread};

use crate::batch::Batch;
use crate::compaction::{CompactionWorker, LevelCompactionStats, ManualCompaction, TaskKind};
use crate::config::{
    GROUP_COMMIT_SMALL_WRITE_THRESHOLD_BYTES, L0_SLOWDOWN_WRITES_TRIGGER, L0_STOP_WRITES_TRIGGER,
    MAX_GROUP_COMMIT_SIZE_BYTES, MAX_NUM_LEVELS, SMALL_WRITE_ADDITIONAL_GROUP_COMMIT_SIZE_BYTES,
};
use crate::errors::{RainDBError, RainDBResult};
use crate::file_names::FileNameHandler;
use crate::file_names::{DATA_DIR, WAL_DIR};
use crate::fs::FileSystem;
use crate::key::InternalKey;
use crate::logs::LogWriter;
use crate::memtable::{MemTable, SkipListMemTable};
use crate::table_cache::TableCache;
use crate::tables::TableBuilder;
use crate::utils::linked_list::SharedNode;
use crate::versioning::file_metadata::FileMetadata;
use crate::versioning::version::Version;
use crate::versioning::{VersionChangeManifest, VersionSet};
use crate::writers::Writer;
use crate::{DbOptions, RainDbIterator, ReadOptions, WriteOptions};

/**
Database state property bag that can be shared via closures and sent to other threds.

The state stored in here is intended to be passed around to objects and closures that cannot
take a direct dependency on the main [`DB`] object e.g. the compaction worker thread.
*/
#[derive(Clone)]
pub(crate) struct PortableDatabaseState {
    /**
    Clone of options for configuring the operation of the database.

    This disconnects the options from the data but this should be okay since options is readonly
    after initialization.
    */
    pub(crate) options: DbOptions,

    /// Handler for file names used by the database.
    file_name_handler: Arc<FileNameHandler>,

    /**
    Database state that require a lock to be held before being read or written to.

    In the course of database operation, this is usually instantiated in and obtained from [`DB`].

    [`DB`]: crate::db::DB
    */
    pub(crate) guarded_db_fields: Arc<Mutex<GuardedDbFields>>,

    /// A cache of table files.
    pub(crate) table_cache: Arc<TableCache>,

    /// Field indicating if the database is shutting down.
    pub(crate) is_shutting_down: Arc<AtomicBool>,

    /**
    Field indicating if there is an immutable memtable.

    An memtable is made immutable when it is undergoing the compaction process.
    */
    pub(crate) has_immutable_memtable: Arc<AtomicBool>,

    /**
    A condition variable used to notify parked threads that background work (e.g. compaction) has
    finished.
    */
    pub(crate) background_work_finished_signal: Arc<Condvar>,
}

/// Struct holding database fields that need a lock before accessing.
pub(crate) struct GuardedDbFields {
    /**
    The current write-ahead log (WAL) number.

    This field is synonomous with the `leveldb::DBImpl::logfile_number_` field.
    */
    pub(crate) curr_wal_file_number: u64,

    /// A background compaction was scheduled.
    pub(crate) background_compaction_scheduled: bool,

    /**
    An error that was encountered when performing write operations or background operations that may
    have put the database in a bad state.

    This error is sticky and essentially stops all future writes to the database when it is set. An
    example of when this may occur, is when a write to the write-ahead log fails.

    # Legacy

    This is synonomous to `DBImpl::bg_error_` in LevelDB.
    */
    pub(crate) maybe_bad_database_state: Option<RainDBError>,

    /**
    Stores state information for a manual compaction. `None` if there is no manual compaction in
    progress or if no manual compaction was requested.
    */
    maybe_manual_compaction: Option<ManualCompaction>,

    /// Queued threads waiting to perform write operations.
    writer_queue: VecDeque<Arc<Writer>>,

    /// Holds the immutable table i.e. the memtable currently undergoing compaction.
    pub(crate) maybe_immutable_memtable: Option<Arc<Box<dyn MemTable>>>,

    /**
    The set of versions currently representing the database.

    The current version represents the most up to date values in the database. Other versions are
    kept to support a consistent view for live iterators.
    */
    pub(crate) version_set: VersionSet,

    /// The ongoing compaction statistics per level.
    pub(crate) compaction_stats: [LevelCompactionStats; MAX_NUM_LEVELS],

    /**
    Set of tables to protect from deletion because they are part of ongoing compactions.

    The tables are identified by their file numbers.

    # Legacy

    This is synonomous to the `DBImpl::pending_outputs_` field in LevelDB.
    */
    pub(crate) tables_in_use: HashSet<u64>,
}

/// The primary database object that exposes the public API.
pub struct DB {
    /// Options for configuring the operation of the database.
    options: DbOptions,

    /**
    An in-memory table of key-value pairs to support quick access to recently changed values.

    All operations (reads and writes) go through this in-memory representation first.

    # Concurrency

    We use [`ArcSwap`] because we need a combination of an [`AtomicPtr`] and an [`Arc`]. Putting an
    `Arc` into an `AtomicPtr` doesn't work because storing/loading through the `AtomicPtr` does not
    change the `Arc`'s reference counts.

    [`AtomicPtr`]: std::sync::atomic::AtomicPtr
    */
    memtable_ptr: ArcSwap<Box<dyn MemTable>>,

    /// The writer for the current write-ahead log file.
    wal: AtomicPtr<LogWriter>,

    /// A cache of table files.
    table_cache: Arc<TableCache>,

    /// Database fields that require a lock for accesses (reads and writes).
    guarded_fields: Arc<Mutex<GuardedDbFields>>,

    /// Handler for file names used by the database.
    file_name_handler: Arc<FileNameHandler>,

    /// Field indicating if the database is shutting down.
    is_shutting_down: Arc<AtomicBool>,

    /**
    Field indicating if there is an immutable memtable.

    An memtable is made immutable when it is undergoing the compaction process.
    */
    has_immutable_memtable: Arc<AtomicBool>,

    /**
    The worker managing the compaction thread.

    This is used to schedule compaction related tasks on a background thread.
    */
    compaction_worker: CompactionWorker,

    /**
    A condition variable used to notify parked threads that background work (e.g. compaction) has
    finished.
    */
    background_work_finished_signal: Arc<Condvar>,

    /// Accumulator for compaction operation statistics/metrics per level.
    compaction_stats: [LevelCompactionStats; MAX_NUM_LEVELS],
}

/// Public methods
impl DB {
    pub fn open(options: DbOptions) -> RainDBResult<()> {
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
        let wal_file_path = file_name_handler.get_wal_path(wal_file_number);
        let wal = LogWriter::new(Arc::clone(&fs), wal_file_path)?;

        // Create memtable
        let memtable = Box::new(SkipListMemTable::new());

        // Start compaction service

        /* Ok(DB {
            options,
            wal,
            memtable,
            file_name_handler,
        }) */
        Ok(())
    }

    pub fn get(&self, read_options: ReadOptions, key: &Vec<u8>) {
        todo!("working on it!")
    }

    pub fn put(
        &self,
        write_options: WriteOptions,
        key: Vec<u8>,
        value: Vec<u8>,
    ) -> RainDBResult<()> {
        todo!("working on it!")
    }

    pub fn delete(&self, write_options: WriteOptions, key: Vec<u8>) -> RainDBResult<()> {
        todo!("working on it!")
    }

    /**
    Atomically apply a batch of changes to the database. The requesting thread is queued if there
    are multiple write requests.

    This is the public API to the underlying [`DB::apply_changes`] method.
    */
    pub fn apply(&self, write_options: WriteOptions, write_batch: Batch) -> RainDBResult<()> {
        self.apply_changes(write_options, Some(write_batch))
    }

    pub fn close(&self) {
        todo!("working on it!")
    }
}

/// Private methods
impl DB {
    /**
    Get a mutable reference to the write-ahead log.

    # Safety

    RainDB guarantees that there is only one thread that accesses the WAL log writer so giving out
    a mutable reference is fine.
    */
    fn wal_mut(&self) -> &mut LogWriter {
        unsafe { &mut *self.wal.load(Ordering::Acquire) }
    }

    /// Get a shared reference to the memtable.
    fn memtable(&self) -> Arc<Box<dyn MemTable>> {
        self.memtable_ptr.load_full()
    }

    /// Generate portable database state.
    fn generate_portable_state(&self) -> PortableDatabaseState {
        PortableDatabaseState {
            options: self.options.clone(),
            file_name_handler: Arc::clone(&self.file_name_handler),
            table_cache: Arc::clone(&self.table_cache),
            guarded_db_fields: Arc::clone(&self.guarded_fields),
            is_shutting_down: Arc::clone(&self.is_shutting_down),
            has_immutable_memtable: Arc::clone(&self.has_immutable_memtable),
            background_work_finished_signal: Arc::clone(&self.background_work_finished_signal),
        }
    }

    /**
    Apply changes contained in the write batch. The requesting thread is queued if there are
    multiple write requests.

    # Concurrency

    All write activity should be coordinated through this thread. Any existing thread workers
    (e.g. [`CompactionWorker`]) or future thread worker types should not apply writes to the WAL or
    to the memtable. We should try to lock this down somehow but this is a design choice inherited
    from LevelDB.

    # Group commits

    Like LevelDB, RainDB may perform an extra level of batching on top of the `batch` already
    specified. If there are multiple threads making write requests, RainDB will queue the threads
    so that write operations are performed serially. In order to reduce request latency, RainDB will
    group batch requests on the queue up to a certain size limit and perform the requested writes
    together as if they were in the same [`Batch`]. We call this extra level of batching a group
    commit per the [commit that added it] in LevelDB.

    # Legacy

    This method is synonymous with [`leveldb::DBImpl::Write`] in LevelDB.

    [commit that added it]: https://github.com/google/leveldb/commit/d79762e27369365a7ffe1f2e3a5c64b0632079e1
    [`DBImpl::Write`]: https://github.com/google/leveldb/blob/e426c83e88c4babc785098d905c2dcb4f4e884af/db/db_impl.cc#L1200
    */
    fn apply_changes(
        &self,
        write_options: WriteOptions,
        maybe_batch: Option<Batch>,
    ) -> RainDBResult<()> {
        // Create a new writer to represent this thread and push it into the back of the writer
        // queue
        let force_compaction = maybe_batch.is_none();
        let writer = Arc::new(Writer::new(maybe_batch, write_options.synchronous));
        let mut fields_mutex_guard = self.guarded_fields.lock();
        fields_mutex_guard
            .writer_queue
            .push_back(Arc::clone(&writer));

        let is_first_writer = self.is_first_writer(&mut fields_mutex_guard, &writer);
        // Wait until it is the current writer's turn to write
        while !writer.is_operation_complete() && !is_first_writer {
            writer.wait_for_turn(&mut fields_mutex_guard);
        }

        // Check if the work for this thread was already completed as part of a group commit and
        // return the result if it was
        if writer.is_operation_complete() {
            return writer.get_operation_result().unwrap();
        }

        let mut write_result = self.make_room_for_write(&mut fields_mutex_guard, force_compaction);
        let prev_sequence_number = fields_mutex_guard.version_set.get_prev_sequence_number();
        let mut last_writer = Arc::clone(&writer);

        if write_result.is_ok() && !force_compaction {
            // Attempt to create a group commit batch
            let (mut write_batch, last_writer_in_batch) =
                self.build_group_commit_batch(&mut fields_mutex_guard)?;
            last_writer = last_writer_in_batch;
            write_batch.set_starting_seq_number(prev_sequence_number + 1);
            let sequence_number_after_write = prev_sequence_number + (write_batch.len() as u64);

            // Add to the write-ahead log and apply changes to the memtable
            write_result = parking_lot::MutexGuard::<'_, GuardedDbFields>::unlocked_fair(
                &mut fields_mutex_guard,
                || -> RainDBResult<()> {
                    /*
                    We can release the lock during this phase since `writer` is currently the
                    only awake writer thread. This means it was soley responsible writing to the
                    WAL and to the memtable.
                    */

                    // Write the changes to the write-ahead log first
                    self.wal_mut().append(&Vec::<u8>::from(&write_batch))?;

                    // Write the changes to the memtable
                    self.apply_batch_to_memtable(&write_batch);

                    Ok(())
                },
            );

            if write_result.is_err() {
                // There was an error writing the write-ahead log and the log itself may
                // even be in a bad state that could show up if the database is re-opened.
                // So we force the database into a mode where all future write fail.
                DB::set_bad_database_state(
                    &self.generate_portable_state(),
                    &mut fields_mutex_guard,
                    write_result.clone().unwrap_err().into(),
                );
            }

            fields_mutex_guard
                .version_set
                .set_prev_sequence_number(sequence_number_after_write);
        }

        loop {
            // We can unwrap immediately after popping because we know that at least the current
            // writer is in the queue and the loop takes care to never iterate on an empty queue.
            let first_writer = fields_mutex_guard.writer_queue.pop_front().unwrap();

            // We want to check shallow/reference equality of the writers so we use `Arc::ptr_eq`.
            if !Arc::ptr_eq(&first_writer, &writer) {
                /*
                The writer that was first is not the current writer. This means that the first
                writer had its write operation performed as part of a group commit. Mark it as
                done.
                */
                first_writer.set_operation_completed(true);
                first_writer.set_operation_result(write_result.clone());
                first_writer.notify_writer();
            }

            if Arc::ptr_eq(&first_writer, &last_writer) {
                break;
            }
        }

        // Notify the new head of the write queue
        if !fields_mutex_guard.writer_queue.is_empty() {
            fields_mutex_guard
                .writer_queue
                .front()
                .unwrap()
                .notify_writer();
        }

        Ok(())
    }

    /// Check if the provided writer is the first writer in the writer queue.
    fn is_first_writer(
        &self,
        mutex_guard: &mut MutexGuard<GuardedDbFields>,
        writer: &Arc<Writer>,
    ) -> bool {
        // We want to check shallow/reference equality of the writers so we use `Arc::ptr_eq`.
        let maybe_first_writer = mutex_guard.writer_queue.front();
        maybe_first_writer.is_some() && Arc::ptr_eq(writer, maybe_first_writer.unwrap())
    }

    /**
    Ensures that there is room in the memtable for more writes and triggers a compaction if
    necessary.

    * `force_compaction` - This should usually be false. When true, this will force a
      compaction check of the memtable.

    # Concurrency

    The calling thread must be holding a lock to the guarded fields and the calling thread must be
    at the front of the writer queue. During the course of this method the lock may be released and
    reacquired.
    */
    fn make_room_for_write(
        &self,
        mutex_guard: &mut MutexGuard<GuardedDbFields>,
        mut force_compaction: bool,
    ) -> RainDBResult<()> {
        let mut allow_write_delay = !force_compaction;

        loop {
            let num_level_zero_files = mutex_guard.version_set.num_files_at_level(0);

            if mutex_guard.maybe_bad_database_state.is_some() {
                // We encountered an issue with a background task. Return with the error.
                let error = mutex_guard.maybe_bad_database_state.clone().unwrap();
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
                parking_lot::MutexGuard::<'_, GuardedDbFields>::unlocked_fair(mutex_guard, || {
                    thread::sleep(one_millis);
                    // Do not delay a single write more than once
                    allow_write_delay = false;
                });
            } else if !force_compaction
                && (self.memtable().approximate_memory_usage() <= self.options.max_memtable_size())
            {
                log::info!("There is room in the memtable for writes. Proceeding with write.");
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
                self.background_work_finished_signal.wait(mutex_guard);
            } else if num_level_zero_files >= L0_STOP_WRITES_TRIGGER {
                log::info!(
                    "Too many level 0 files. Waiting for compaction before proceeding with write \
                    operations."
                );
                self.background_work_finished_signal.wait(mutex_guard);
            } else {
                if mutex_guard.version_set.maybe_prev_wal_number().is_some() {
                    let error_msg =
                        "Detected that the memtable is already undergoing compaction (possibly by \
                        another thread) while the current thread is attempting to start a \
                        compaction.";
                    log::error!("{}", error_msg);
                    return Err(RainDBError::Write(error_msg.to_string()));
                }

                log::info!("Memtable is full. Attempting compaction.");

                // First create a new WAL file.
                let new_wal_number = mutex_guard.version_set.get_new_file_number();
                let wal_file_path = self.file_name_handler.get_wal_path(new_wal_number);
                let maybe_wal_writer =
                    LogWriter::new(self.options.filesystem_provider(), wal_file_path);

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
                    return Err(RainDBError::Log(error));
                }

                // Replace old WAL state fields with new WAL values
                let mut wal_writer = maybe_wal_writer.unwrap();
                self.wal.store(&mut wal_writer as *mut _, Ordering::Release);
                mutex_guard.curr_wal_file_number = new_wal_number;

                log::info!("Create a new memtable and make it active.");
                let new_memtable: Arc<Box<dyn MemTable>> =
                    Arc::new(Box::new(SkipListMemTable::new()));
                // RainDB enforces that only one thread can trigger a memtable compaction at a time
                // so we just `swap` instead of `compare_and_swap`
                let old_memtable = self.memtable_ptr.swap(new_memtable);
                log::info!("Move the current memtable to the immutable memtable field.");
                mutex_guard.maybe_immutable_memtable = Some(Arc::clone(&old_memtable));
                self.has_immutable_memtable.store(true, Ordering::Release);

                // Do not force another compaction since we have room
                force_compaction = false;

                log::info!("Attempt to schedule a compaction of the immutable memtable");
                self.maybe_schedule_compaction(mutex_guard);
            }
        }
    }

    /**
    Schedule a compaction if possible.

    Various conditions are checked to see if a compaction is scheduled. For example, if the
    database is shutting down, a compaction will not be scheduled.

    # Concurrency

    This method requires the caller to have a lock on the guarded fields.
    */
    fn maybe_schedule_compaction(&self, mutex_guard: &mut MutexGuard<GuardedDbFields>) {
        if mutex_guard.background_compaction_scheduled {
            log::info!("A background compaction was already initiated.");
            return;
        }

        if self.is_shutting_down.load(Ordering::Acquire) {
            log::info!("The database is shutting down. Will not schedule a background compaction.");
            return;
        }

        if mutex_guard.maybe_bad_database_state.is_some() {
            let background_error = mutex_guard.maybe_bad_database_state.as_ref().unwrap();
            log::error!(
                "Detected a sticky background error (potentially from previous compaction \
                attempts). Error: {}",
                background_error
            );

            return;
        }

        if mutex_guard.maybe_immutable_memtable.is_none()
            && mutex_guard.maybe_manual_compaction.is_none()
            && !mutex_guard.version_set.needs_compaction()
        {
            log::info!("No compaction work detected.");
            return;
        }

        log::info!("Determined that compaction is necessary. Scheduling compaction task.");
        mutex_guard.background_compaction_scheduled = true;
        self.compaction_worker.schedule_task(TaskKind::Compaction);
    }

    /**
    Build a [`Batch`] to execute as part of a group commit.

    This method will return an error if the writer queue is empty or if the first writer does
    not have a batch. The first writer must have a batch because this method is for performing
    actual writes and we do not want to force a compaction and impact the latency of other writers
    in the batch.
    */
    fn build_group_commit_batch(
        &self,
        mutex_guard: &mut MutexGuard<GuardedDbFields>,
    ) -> RainDBResult<(Batch, Arc<Writer>)> {
        // The writer queue must be non-empty. It should at least contain the writer representing
        // the current thread.
        if mutex_guard.writer_queue.is_empty() {
            let error_msg =
                "Found an empty writer queue while attempting to create a group commit batch.";
            log::error!("{}", error_msg);
            return Err(RainDBError::Write(error_msg.to_string()));
        }

        let first_writer = mutex_guard.writer_queue.front().unwrap();
        if first_writer.maybe_batch().is_none() {
            let error_msg =
                "The first writer to be processed for a group commit had an empty batch.";
            log::error!("{}", error_msg);
            return Err(RainDBError::Write(error_msg.to_string()));
        }

        let mut batch_size = first_writer.maybe_batch().unwrap().get_approximate_size();

        /*
        Allow the group to grow to a maximum size so we don't impact the write latency of an
        individual write too much. If the original write is small, limit the growth also so that
        we do not slow down the small write too much.
        */
        let max_size: usize = if batch_size <= GROUP_COMMIT_SMALL_WRITE_THRESHOLD_BYTES {
            batch_size + SMALL_WRITE_ADDITIONAL_GROUP_COMMIT_SIZE_BYTES
        } else {
            MAX_GROUP_COMMIT_SIZE_BYTES
        };

        let mut group_commit_batch = Batch::new();
        group_commit_batch.append_batch(first_writer.maybe_batch().unwrap());

        let mut last_writer = first_writer;
        let mut writer_iter = mutex_guard.writer_queue.iter();
        // Skip the first writer since we used it to bootstrap the group commit.
        writer_iter.next();
        for writer in writer_iter {
            if writer.is_synchronous_write() && !first_writer.is_synchronous_write() {
                // Do not include a synchronous write into a batch handled by a non-synchronous
                // writer.
                break;
            }

            if writer.maybe_batch().is_none() {
                // Do not include writers servicing force compaction requests in the group commit.
                last_writer = writer;
                break;
            }

            let curr_writer_batch = writer.maybe_batch().unwrap();
            batch_size += curr_writer_batch.get_approximate_size();
            if batch_size > max_size {
                // Adding this writer to the group commit would make it too large. Stop adding
                // writers;
                break;
            }

            group_commit_batch.append_batch(curr_writer_batch);
            last_writer = writer;
        }

        Ok((group_commit_batch, Arc::clone(last_writer)))
    }

    /// Apply the changes in the provided batch to the memtable.
    fn apply_batch_to_memtable(&self, batch: &Batch) {
        let mut curr_sequence_num = batch.get_starting_seq_number().unwrap();
        for batch_element in batch.iter() {
            let internal_key = InternalKey::new(
                batch_element.get_key().to_vec(),
                curr_sequence_num,
                batch_element.get_operation(),
            );
            let value = batch_element.get_value().map_or(vec![], |val| val.to_vec());
            self.memtable().insert(internal_key, value);

            curr_sequence_num += 1;
        }
    }

    /**
    Build a table file from the contents of a [`RainDbIterator`].

    The generated table file will be named after the provided table number. Upon successful
    table file generation, relevant fields of the the passed in [`FileMetadata`] will be filled in
    will metadata from the generated file.

    If the passed in iterator is empty, a table file will **not** be generated and the file size
    field of the metadata struct will be set to zero.

    [`RainDbIterator`]: crate::RainDbIterator
    */
    fn build_table_from_iterator(
        options: &DbOptions,
        metadata: &mut FileMetadata,
        iterator: Box<dyn RainDbIterator<Key = InternalKey, Error = RainDBError>>,
        table_cache: &Arc<TableCache>,
    ) -> RainDBResult<()> {
        let file_name_handler = FileNameHandler::new(options.db_path().to_string());
        let table_file_name = file_name_handler.get_table_file_name(metadata.file_number());
        iterator.seek_to_first()?;

        if iterator.is_valid() {
            let table_builder = TableBuilder::new(options.clone(), metadata.file_number())?;
            metadata.set_smallest_key(Some(iterator.current().unwrap().0.clone()));

            // Iterate the memtable and add the entries to a table
            let mut larget_key_seen: Option<&InternalKey> = None;
            while let Some((key, value)) = iterator.current() {
                larget_key_seen = Some(key);
                table_builder.add_entry(Rc::new(key.clone()), value);
                iterator.next();
            }

            // The iterator is valid so we should just be able to unwrap
            metadata.set_largest_key(Some(larget_key_seen.unwrap().clone()));
            table_builder.finalize()?;
            metadata.set_file_size(table_builder.file_size());

            // Verify that the table file is usable by attempting to create an iterator from it
            match table_cache.find_table(metadata.file_number()) {
                Ok(table_cache_entry) => {
                    let table = table_cache_entry.get_value();
                    let _table_iter = table.two_level_iter(ReadOptions::default());
                }
                Err(error) => {
                    log::error!(
                        "There was an issue attempting to open the table file with number {} after \
                        creating it. Deleting file. Original error: {}.",
                        metadata.file_number(),
                        error
                    );
                    // Delete the table file if there was an error
                    options
                        .filesystem_provider()
                        .remove_file(&table_file_name)?;

                    return Err(error);
                }
            }

            if metadata.get_file_size() < 1 {
                // Delete the file if it is empty
                options
                    .filesystem_provider()
                    .remove_file(&table_file_name)?;
            }
        }

        Ok(())
    }
}

/// Crate-only methods
impl DB {
    /**
    Set field indicating that the database is in bad state and should not be written to.

    # Legacy

    This is synonomous to `DBImpl::RecordBackgroundError` in LevelDB.
    */
    pub(crate) fn set_bad_database_state(
        db_state: &PortableDatabaseState,
        mutex_guard: &mut MutexGuard<GuardedDbFields>,
        catastrophic_error: RainDBError,
    ) {
        if mutex_guard.maybe_bad_database_state.is_some() {
            return;
        }

        mutex_guard.maybe_bad_database_state = Some(catastrophic_error);
        db_state.background_work_finished_signal.notify_all();
    }

    /// Convert the immutable memtable to a table file.
    pub(crate) fn write_level0_table(
        db_state: &PortableDatabaseState,
        db_fields_guard: &mut MutexGuard<GuardedDbFields>,
        memtable: &'static Box<dyn MemTable>,
        base_version: SharedNode<Version>,
        change_manifest: &mut VersionChangeManifest,
    ) -> RainDBResult<()> {
        // Actual work starts here so get a timer for metric gathering purposes
        let compaction_instant = Instant::now();
        let file_number = db_fields_guard.version_set.get_new_file_number();
        let file_metadata = FileMetadata::new(file_number);
        db_fields_guard.tables_in_use.insert(file_number);

        log::info!(
            "Starting to build level-0 table file with file number {}.",
            file_number
        );
        parking_lot::MutexGuard::<'_, GuardedDbFields>::unlocked_fair(
            &mut db_fields_guard,
            || -> RainDBResult<()> {
                DB::build_table_from_iterator(
                    &db_state.options,
                    &mut file_metadata,
                    memtable.iter(),
                    &db_state.table_cache,
                )
            },
        )?;

        log::info!(
            "Level-0 table file {} created with a file size of {}.",
            file_number,
            file_metadata.get_file_size()
        );
        db_fields_guard.tables_in_use.remove(&file_number);

        // If the file size is zero, that means that the file was deleted and should not be added
        // to the manifest.
        let mut file_level: usize = 0;
        if file_metadata.get_file_size() > 0 {
            let smallest_user_key = file_metadata.smallest_key().get_user_key();
            let largest_user_key = file_metadata.largest_key().get_user_key();
            file_level = base_version
                .read()
                .element
                .pick_level_for_memtable_output(smallest_user_key, largest_user_key);
            // TODO: Just clone the file metadata and pass that in. make a From method for the upcoming Compaction metadata struct to file metadata so the method is reusable.
            change_manifest.add_file(
                file_level,
                file_metadata.file_number(),
                file_metadata.get_file_size(),
                file_metadata.smallest_key().clone()..file_metadata.largest_key().clone(),
            );
        }
        log::info!(
            "Level-0 table file {} will be placed at level {}.",
            file_number,
            file_level
        );

        let stats = LevelCompactionStats {
            compaction_duration: compaction_instant.elapsed(),
            bytes_written: file_metadata.get_file_size(),
            ..LevelCompactionStats::default()
        };
        db_fields_guard.compaction_stats[file_level] += stats;

        Ok(())
    }

    /// Set a new `CURRENT` file.
    pub(crate) fn set_current_file(
        filesystem_provider: Arc<Box<dyn FileSystem>>,
        file_name_handler: &FileNameHandler,
        manifest_file_number: u64,
    ) -> io::Result<()> {
        let manifest_file_path = file_name_handler.get_manifest_file_name(manifest_file_number);
        let manifest_file_name = manifest_file_path.file_name().unwrap();
        let temp_file_path = file_name_handler.get_temp_file_name(manifest_file_number);
        let mut temp_file = filesystem_provider.create_file(&temp_file_path)?;
        let contents = [
            manifest_file_name.to_string_lossy().as_bytes(),
            "\n".as_bytes(),
        ]
        .concat();

        let temp_file_write_result = temp_file.append(&contents);
        if temp_file_write_result.is_err() {
            log::error!(
                "Creating a new CURRENT file failed at writing the manifest file name ({:?}) to \
                the temp file at {:?}.",
                manifest_file_name,
                &temp_file_path
            );

            filesystem_provider.remove_file(&temp_file_path)?;
            return Err(temp_file_write_result.err().unwrap());
        }

        let current_file_path = file_name_handler.get_current_file_path();
        let rename_result = filesystem_provider.rename(&temp_file_path, &current_file_path);
        if rename_result.is_err() {
            log::error!(
                "Creating a new CURRENT file failed at renaming the temp file ({:?}) to the \
                CURRENT file.",
                &temp_file_path
            );

            filesystem_provider.remove_file(&temp_file_path)?;
            return Err(rename_result.err().unwrap());
        }

        Ok(())
    }
}
