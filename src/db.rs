/*!
The database module contains the primary API for interacting with the key-value store.
*/

use arc_swap::ArcSwap;
use parking_lot::{Condvar, Mutex, MutexGuard};
use std::cell::UnsafeCell;
use std::collections::{HashSet, VecDeque};
use std::fmt::Write;
use std::ops::Range;
use std::path::PathBuf;
use std::rc::Rc;
use std::sync::atomic::{AtomicBool, AtomicPtr, Ordering};
use std::sync::Arc;
use std::time::{self, Instant};
use std::{io, panic, ptr, thread};

use crate::batch::Batch;
use crate::compaction::{
    CompactionWorker, LevelCompactionStats, ManualCompactionConfiguration, TaskKind,
};
use crate::config::{
    GROUP_COMMIT_SMALL_WRITE_THRESHOLD_BYTES, L0_SLOWDOWN_WRITES_TRIGGER, L0_STOP_WRITES_TRIGGER,
    MAX_GROUP_COMMIT_SIZE_BYTES, MAX_NUM_LEVELS, SMALL_WRITE_ADDITIONAL_GROUP_COMMIT_SIZE_BYTES,
};
use crate::errors::{RainDBError, RainDBResult};
use crate::file_names::{FileNameHandler, ParsedFileType};
use crate::fs::{FileLock, FileSystem};
use crate::iterator::DatabaseIterator;
use crate::key::{InternalKey, MAX_SEQUENCE_NUMBER};
use crate::logs::{LogReader, LogWriter};
use crate::memtable::{MemTable, SkipListMemTable};
use crate::snapshots::SnapshotList;
use crate::table_cache::TableCache;
use crate::tables::{Table, TableBuilder};
use crate::utils::linked_list::SharedNode;
use crate::versioning::file_iterators::MergingIterator;
use crate::versioning::file_metadata::FileMetadata;
use crate::versioning::version::{SeekChargeMetadata, Version};
use crate::versioning::{self, VersionChangeManifest, VersionSet};
use crate::writers::Writer;
use crate::{DbOptions, Operation, RainDbIterator, ReadOptions, Snapshot, WriteOptions};

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
    pub(crate) file_name_handler: Arc<FileNameHandler>,

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

    # Legacy

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

    There is an extra layer of `Arc`/`Mutex` because the configuration is shared by the thread
    requesting the compaction and the thread processing the compaction. It is an invariant of
    LevelDB--so by extension RainDB--that only a holder of the main database lock (i.e.
    [`DB::guarded_fields`]) can mutate the configuration value. The only way to inform Rust of this
    is either through raw pointers or via another mutex. We choose to just use a mutex since it is
    easier to deal with.
    */
    pub(crate) maybe_manual_compaction: Option<Arc<Mutex<ManualCompactionConfiguration>>>,

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

    /// Accumulator for compaction operation metrics and statistics per level.
    pub(crate) compaction_stats: [LevelCompactionStats; MAX_NUM_LEVELS],

    /**
    Set of tables to protect from deletion because they are part of ongoing compactions.

    The tables are identified by their file numbers.

    # Legacy

    This is synonomous to the `DBImpl::pending_outputs_` field in LevelDB.
    */
    pub(crate) tables_in_use: HashSet<u64>,

    /// A list of snapshots currently in use.
    pub(crate) snapshots: SnapshotList,

    /// Seed used for determining when to send read samples for statistics updates.
    read_sampling_seed: u64,
}

/// The primary database object that exposes the public API.
pub struct DB {
    /// Options for configuring the operation of the database.
    options: DbOptions,

    /// A lock over the persistent (i.e. on disk) state of the database.
    db_lock: Option<FileLock>,

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
    wal: AtomicPtr<UnsafeCell<LogWriter>>,

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
    compaction_worker: Arc<CompactionWorker>,

    /**
    A condition variable used to notify parked threads that background work (e.g. compaction) has
    finished.
    */
    background_work_finished_signal: Arc<Condvar>,
}

/// Public methods
impl DB {
    /// Open a database with the specified options.
    pub fn open(options: DbOptions) -> RainDBResult<DB> {
        log::info!(
            "Initializing raindb with the following options {:#?}",
            options
        );

        let fs = options.filesystem_provider();
        let db_path = options.db_path();
        let file_name_handler = Arc::new(FileNameHandler::new(db_path.to_string()));

        log::info!("Creating DB root directory at {}.", db_path);
        fs.create_dir_all(&file_name_handler.get_db_path())?;

        log::info!("Creating supporting database paths.");
        fs.create_dir(&file_name_handler.get_wal_dir())?;
        fs.create_dir(&file_name_handler.get_data_dir())?;

        // Create WAL
        let wal_file_number = 0;
        let wal_ptr = AtomicPtr::new(ptr::null_mut());

        // Create memtable
        let memtable: Arc<Box<dyn MemTable>> = Arc::new(Box::new(SkipListMemTable::new()));
        let memtable_ptr = ArcSwap::from(memtable);

        // Create table cache
        let table_cache = Arc::new(TableCache::new(options.clone(), 1000));

        // Initialize guarded fields
        let guarded_fields = Arc::new(Mutex::new(GuardedDbFields {
            curr_wal_file_number: wal_file_number,
            background_compaction_scheduled: false,
            maybe_bad_database_state: None,
            maybe_manual_compaction: None,
            writer_queue: VecDeque::new(),
            maybe_immutable_memtable: None,
            compaction_stats: Default::default(),
            version_set: VersionSet::new(options.clone(), Arc::clone(&table_cache)),
            tables_in_use: HashSet::new(),
            snapshots: SnapshotList::new(),
            read_sampling_seed: 0,
        }));

        // Set up database fields that should also be portable
        let is_shutting_down = Arc::new(AtomicBool::new(false));
        let has_immutable_memtable = Arc::new(AtomicBool::new(false));
        let background_work_finished_signal = Arc::new(Condvar::new());

        // Start compaction service
        let portable_state = PortableDatabaseState {
            options: options.clone(),
            file_name_handler: Arc::clone(&file_name_handler),
            table_cache: Arc::clone(&table_cache),
            guarded_db_fields: Arc::clone(&guarded_fields),
            is_shutting_down: Arc::clone(&is_shutting_down),
            has_immutable_memtable: Arc::clone(&has_immutable_memtable),
            background_work_finished_signal: Arc::clone(&background_work_finished_signal),
        };
        let compaction_worker = Arc::new(CompactionWorker::new(portable_state)?);

        let db = DB {
            options,
            db_lock: None,
            memtable_ptr,
            wal: wal_ptr,
            table_cache,
            guarded_fields,
            file_name_handler,
            is_shutting_down,
            has_immutable_memtable,
            compaction_worker,
            background_work_finished_signal,
        };

        // Check if records need to be recovered or initialize this as a new database
        let mut db_fields_guard = db.guarded_fields.lock();
        let (mut version_change_manifest, create_new_snapshot) =
            db.recover(&mut db_fields_guard)?;
        if db.wal.load(Ordering::Acquire).is_null() {
            assert!(
                db.memtable().is_empty(),
                "The memtable must be empty if there is no backing WAL"
            );
            log::info!("Creating a new WAL and memtable for the database.");

            let new_wal_number = db_fields_guard.version_set.get_new_file_number();
            let new_wal_path = db.file_name_handler.get_wal_file_path(new_wal_number);
            let new_wal_writer =
                LogWriter::new(db.options.filesystem_provider(), new_wal_path, false)?;
            version_change_manifest.wal_file_number = Some(new_wal_number);
            db.set_wal(&mut db_fields_guard, new_wal_number, new_wal_writer);
        }

        if create_new_snapshot {
            // Older logs are no longer relevant after recovery operations
            version_change_manifest.prev_wal_file_number = None;
            version_change_manifest.wal_file_number = Some(db_fields_guard.curr_wal_file_number);
            if let Err(version_write_err) =
                VersionSet::log_and_apply(&mut db_fields_guard, &mut version_change_manifest)
            {
                log::error!(
                    "There was an error applying changes from recovery operations. Error: {err}",
                    err = &version_write_err
                );
                return Err(RainDBError::Recovery(version_write_err.to_string()));
            }
        }

        DB::remove_obsolete_files(
            &mut db_fields_guard,
            db.options.filesystem_provider(),
            &db.file_name_handler,
            &*db.table_cache,
        );
        if DB::should_schedule_compaction(&db.generate_portable_state(), &mut db_fields_guard) {
            log::info!("Determined that compaction is necessary. Scheduling compaction task.");
            db.compaction_worker.schedule_task(TaskKind::Compaction);
        }

        MutexGuard::unlock_fair(db_fields_guard);

        Ok(db)
    }

    /**
    Get a handle to the current state of the database.

    Get requests and iterators created with this snapshot will have a stable view of the database
    state. Callers *must* call [`DB::release_snapshot`] when the snapshot is no longer needed.
    */
    pub fn get_snapshot(&self) -> Snapshot {
        let mut db_fields_guard = self.guarded_fields.lock();
        let latest_sequence_num = db_fields_guard.version_set.get_prev_sequence_number();
        db_fields_guard.snapshots.new_snapshot(latest_sequence_num)
    }

    /// Release a previously acquired snapshot.
    pub fn release_snapshot(&self, snapshot: Snapshot) {
        let mut db_fields_guard = self.guarded_fields.lock();
        db_fields_guard.snapshots.delete_snapshot(snapshot)
    }

    /**
    Return the value stored at the specified `key` if it exists. Otherwise returns
    [`RainDBError::KeyNotFound`].
    */
    pub fn get(&self, read_options: ReadOptions, key: &[u8]) -> RainDBResult<Vec<u8>> {
        let mut db_fields_guard = self.guarded_fields.lock();
        let snapshot: u64 = if let Some(snapshot_handle) = read_options.snapshot.as_ref() {
            snapshot_handle.sequence_number()
        } else {
            db_fields_guard.version_set.get_prev_sequence_number()
        };
        let maybe_immutable_memtable = db_fields_guard.maybe_immutable_memtable.clone();
        let current_version = db_fields_guard.version_set.get_current_version();

        // Unlock mutex while reading from memtable or files
        let mut maybe_seek_charge: Option<SeekChargeMetadata> = None;
        let get_result = parking_lot::MutexGuard::unlocked_fair(
            &mut db_fields_guard,
            || -> RainDBResult<Option<Vec<u8>>> {
                let internal_key = InternalKey::new_for_seeking(key.to_vec(), snapshot);

                // Check the memtable first
                if let Ok(maybe_value) = self.memtable().get(&internal_key) {
                    match maybe_value {
                        Some(value) => return Ok(Some(value.clone())),
                        None => {
                            // The value was deleted, as opposed to not found, so we stop
                            // processing
                            return Ok(None);
                        }
                    }
                }

                // Check the immutable memtable (i.e. the memtable pending compaction) if there is
                // one
                if let Some(immutable_memtable) = maybe_immutable_memtable {
                    if let Ok(maybe_value) = immutable_memtable.get(&internal_key) {
                        match maybe_value {
                            Some(value) => return Ok(Some(value.clone())),
                            None => {
                                // The value was deleted, as opposed to not found, so we stop
                                // processing
                                return Ok(None);
                            }
                        }
                    }
                }

                // Check table files on disk
                match current_version
                    .read()
                    .element
                    .get(&read_options, &internal_key)
                {
                    Ok(get_response) => {
                        maybe_seek_charge = Some(get_response.charge_metadata);

                        Ok(get_response.value)
                    }
                    Err(versioning::errors::ReadError::TableRead((base_err, seek_charge))) => {
                        maybe_seek_charge = seek_charge;

                        Err(base_err.into())
                    }
                }
            },
        );

        if let Some(seek_charge) = maybe_seek_charge {
            let is_ready_for_compaction =
                current_version.write().element.update_stats(&seek_charge);
            let db_state = self.generate_portable_state();
            if is_ready_for_compaction
                && DB::should_schedule_compaction(&db_state, &mut db_fields_guard)
            {
                log::info!(
                    "Determined that a version is ready for compaction after too many seeks were \
                    charged to file number {file_num} at level {level}. The file's allowed seeks \
                    was at {allowed_seeks}.",
                    file_num = seek_charge.seek_file().unwrap().file_number(),
                    level = seek_charge.seek_file_level(),
                    allowed_seeks = seek_charge.seek_file().unwrap().allowed_seeks()
                );
                self.compaction_worker.schedule_task(TaskKind::Compaction);
            }
        }

        db_fields_guard.version_set.release_version(current_version);

        match get_result {
            Ok(Some(value)) => Ok(value),
            Ok(None) => Err(RainDBError::KeyNotFound),
            Err(read_err) => Err(read_err),
        }
    }

    /// Set the provided `key` to the specified `value`.
    pub fn put(
        &self,
        write_options: WriteOptions,
        key: Vec<u8>,
        value: Vec<u8>,
    ) -> RainDBResult<()> {
        let mut batch = Batch::new();
        batch.add_put(key, value);
        self.apply(write_options, batch)
    }

    /**
    Delete the specified `key` from the database.

    The operation is considered successful even if the key does not exist in the database.
    */
    pub fn delete(&self, write_options: WriteOptions, key: Vec<u8>) -> RainDBResult<()> {
        let mut batch = Batch::new();
        batch.add_delete(key);
        self.apply(write_options, batch)
    }

    /**
    Atomically apply a batch of changes to the database. The requesting thread is queued if there
    are multiple write requests.

    This is the public API to the underlying [`DB::apply_changes`] method.
    */
    pub fn apply(&self, write_options: WriteOptions, write_batch: Batch) -> RainDBResult<()> {
        self.apply_changes(write_options, Some(write_batch))
    }

    /// Returns an iterator over the contents of the database.
    pub fn new_iterator(&self, read_options: ReadOptions) -> RainDBResult<DatabaseIterator> {
        let mut db_fields_guard = self.guarded_fields.lock();
        let snapshot: u64 = if let Some(snapshot_handle) = read_options.snapshot.as_ref() {
            snapshot_handle.sequence_number()
        } else {
            db_fields_guard.version_set.get_prev_sequence_number()
        };

        // Collect child iterators that represent the complete state of the database
        let mut db_iterators: Vec<Box<dyn RainDbIterator<Key = InternalKey, Error = RainDBError>>> =
            vec![];
        let memtable_iter = self.memtable().iter();
        db_iterators.push(memtable_iter);

        if db_fields_guard.maybe_immutable_memtable.is_some() {
            let immutable_memtable_iter = db_fields_guard
                .maybe_immutable_memtable
                .as_ref()
                .unwrap()
                .iter();
            db_iterators.push(immutable_memtable_iter);
        }

        let current_version = db_fields_guard.version_set.get_current_version();
        let mut version_iterators = current_version
            .read()
            .element
            .get_representative_iterators(&read_options)?;
        db_iterators.append(&mut version_iterators);

        let mut db_state_iterator = MergingIterator::new(db_iterators);
        let db_state = self.generate_portable_state();
        db_state_iterator.register_cleanup_method(Box::new(move || {
            // Ensure that the version is released from the version set after use
            db_state
                .guarded_db_fields
                .lock()
                .version_set
                .release_version(current_version);
        }));

        let read_sampling_seed = db_fields_guard.read_sampling_seed;
        db_fields_guard.read_sampling_seed += 1;

        let client_iter = DatabaseIterator::new(
            self.generate_portable_state(),
            db_state_iterator,
            snapshot,
            read_sampling_seed,
            Arc::clone(&self.compaction_worker),
        );

        Ok(client_iter)
    }

    /// Destroy the contents of the database. **Be very careful using this method.**
    pub fn destroy_database(options: DbOptions) -> RainDBResult<()> {
        let fs = options.filesystem_provider();
        let db_path = options.db_path();
        let file_name_handler = FileNameHandler::new(db_path.to_string());
        log::info!("Destroying the database at {db_path}.", db_path = db_path);

        log::info!("Checking access to database folder.");
        if let Err(io_err) = fs.list_dir(&file_name_handler.get_db_path()) {
            log::error!(
                "There was an error listing files in the database folder. Error: {io_err}",
                io_err = io_err
            );

            return Err(RainDBError::Destruction(io_err.to_string()));
        }

        log::info!("Attempting to acquire database lock.");
        let lock_file_path = file_name_handler.get_lock_file_path();
        let db_lock = match fs.lock_file(&lock_file_path) {
            Ok(lock) => lock,
            Err(err) => {
                log::error!(
                    "There was an error acquiring a lock on the database. Error: {err}",
                    err = err
                );

                return Err(RainDBError::Destruction(err.to_string()));
            }
        };

        log::info!("Deleting the WAL directory.");
        if let Err(io_err) = fs.remove_dir_all(&file_name_handler.get_wal_dir()) {
            log::error!(
                "There was an error deleting the write-ahead log directory at {wal_dir_path:?}. \
                Error: {io_err}",
                wal_dir_path = file_name_handler.get_wal_dir(),
                io_err = io_err
            );

            return Err(RainDBError::Destruction(io_err.to_string()));
        }

        log::info!("Deleting the table file directory.");
        if let Err(io_err) = fs.remove_dir_all(&file_name_handler.get_data_dir()) {
            log::error!(
                "There was an error deleting the table file directory at {table_dir_path:?}. \
                Error: {io_err}",
                table_dir_path = file_name_handler.get_data_dir(),
                io_err = io_err
            );

            return Err(RainDBError::Destruction(io_err.to_string()));
        }

        log::info!(
            "Deleting all files except the database lock file in the database root directory."
        );
        let mut maybe_deletion_err: Option<io::Error> = None;
        for file_path in fs.list_dir(&file_name_handler.get_db_path())? {
            match FileNameHandler::get_file_type_from_name(&file_path) {
                Ok(ParsedFileType::DBLockFile) => {
                    log::debug!("Ignoring database lock file for now.");
                }
                Ok(_) => {
                    log::debug!(
                        "Attempting to delete file at {file_path:?}",
                        file_path = file_path
                    );
                    if let Err(io_err) = fs.remove_file(&file_path) {
                        log::error!(
                            "Encountered an error deleting {file_path:?}. Ignoring the error. \
                            Error: {io_err}",
                            file_path = file_path,
                            io_err = &io_err
                        );
                        maybe_deletion_err = Some(io_err);
                    }
                }
                Err(parse_err) => {
                    log::error!(
                        "Could not parse the file at {file_path:?}. Ignoring it. Error: \
                        {parse_err}",
                        file_path = file_path,
                        parse_err = parse_err
                    );
                }
            }
        }

        drop(db_lock);

        log::info!("Deleting database lock file.");
        if let Err(io_err) = fs.remove_file(&file_name_handler.get_lock_file_path()) {
            log::error!(
                "There was an error deleting the database lock file. Error: {io_err}",
                io_err = io_err
            );

            return Err(RainDBError::Destruction(io_err.to_string()));
        }

        if let Some(deletion_err) = maybe_deletion_err {
            return Err(RainDBError::Destruction(deletion_err.to_string()));
        } else if let Err(io_err) = fs.remove_dir(&file_name_handler.get_db_path()) {
            log::error!(
                "There was an error deleting the database root directory. Error: {io_err}",
                io_err = io_err
            );

            return Err(RainDBError::Destruction(io_err.to_string()));
        }

        Ok(())
    }

    /**
    Compact the underlying storage for the key range specified.

    This operation will remove deleted or overwritten versions for a key and will rearrange how
    data is stored in order to reduce the cost of operations for accessing the data.

    `None` on either end of the key range will signify an open end to the range e.g. `None` at the
    start of the range will signify intent to compact all keys from the start of the database's
    key range.
    */
    pub fn compact_range(&self, key_range: Range<Option<&[u8]>>) {
        let mut max_level_with_files_for_compaction: usize = 1;
        {
            let db_fields_guard = self.guarded_fields.lock();
            let current_version = db_fields_guard.version_set.get_current_version();
            for level in 1..MAX_NUM_LEVELS {
                if current_version.read().element.has_overlap_in_level(
                    level,
                    key_range.start,
                    key_range.end,
                ) {
                    max_level_with_files_for_compaction = level;
                }
            }
        }

        if let Err(compaction_err) = self.force_memtable_compaction() {
            log::warn!(
                "There was a background error detected during forced memtable compaction. Error: \
                {compaction_err}"
            );
        }

        for level in 0..max_level_with_files_for_compaction {
            self.force_level_compaction(level, &key_range);
        }
    }

    /**
    Get a string describing the requested descriptor.

    # Legacy

    This is synonomous to LevelDB's `DB::GetProperty`.
    */
    pub fn get_descriptor(&self, descriptor: DatabaseDescriptor) -> RainDBResult<String> {
        let db_fields_guard = self.guarded_fields.lock();
        match descriptor {
            DatabaseDescriptor::NumFilesAtLevel(level) => {
                if level >= MAX_NUM_LEVELS {
                    let err_msg = format!(
                        "Could not process the NumFilesAtLevel descriptor because the specified \
                        level {level} was not valid."
                    );
                    log::error!("{err_msg}", err_msg = &err_msg);
                    return Err(RainDBError::Other(err_msg));
                }

                let num_files = db_fields_guard.version_set.num_files_at_level(level);
                Ok(num_files.to_string())
            }
            DatabaseDescriptor::Stats => {
                let db_stats = self.summarize_compaction_stats();
                Ok(db_stats)
            }
            DatabaseDescriptor::SSTables => {
                let table_stats = db_fields_guard
                    .version_set
                    .get_current_version()
                    .read()
                    .element
                    .debug_summary();
                Ok(table_stats)
            }
        }
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
    fn wal(&self) -> &UnsafeCell<LogWriter> {
        unsafe { &*self.wal.load(Ordering::Acquire) }
    }

    /// Set WAL state fields to provided values.
    fn set_wal(
        &self,
        db_fields_guard: &mut MutexGuard<GuardedDbFields>,
        new_wal_number: u64,
        wal_writer: LogWriter,
    ) {
        let new_wal_writer = Box::new(UnsafeCell::new(wal_writer));
        let old_wal_ptr = self
            .wal
            .swap(Box::into_raw(new_wal_writer), Ordering::AcqRel);

        if !old_wal_ptr.is_null() {
            // Box the old WAL reader again to drop the memory
            unsafe {
                // SAFETY: We do a null check before loading and dropping the memory reference.
                Box::from_raw(old_wal_ptr)
            };
        }

        db_fields_guard.curr_wal_file_number = new_wal_number;
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
    Recover database state from persistent storage. This method should only be called on database
    initialization.

    This may do a significant amount of work to recover recently logged updates (e.g. in the WAL or
    a manifest file).

    This method returns a tuple of a [`VersionChangeManifest`] with changes recovered from disk and
    a boolean set to true if recovery operations have changes to be saved.

    # Panics

    This method panics if this client has already obtained a database lock.
    */
    fn recover(
        &self,
        db_fields_guard: &mut MutexGuard<GuardedDbFields>,
    ) -> RainDBResult<(VersionChangeManifest, bool)> {
        assert!(self.db_lock.is_none());

        log::info!(
            "Checking for uncommitted entries of persistent log files as part of initialization."
        );
        log::info!("Attempting to acquire database lock.");
        let filesystem = self.options.filesystem_provider();
        let lock_file_path = self.file_name_handler.get_lock_file_path();
        filesystem.lock_file(&lock_file_path)?;

        let current_file_path = self.file_name_handler.get_current_file_path();
        match filesystem.open_file(&current_file_path) {
            Err(io_error) => {
                if let io::ErrorKind::NotFound = io_error.kind() {
                    if self.options.create_if_missing() {
                        log::info!(
                            "Performing new database steps since there was no existing database \
                            the provided path."
                        );
                        self.initialize_as_new_db()?;
                    } else {
                        let error_message =
                            "Did not find the current manifest file. Ending initialization \
                            because `DbOptions::create_if_missing` is false.";
                        log::error!(
                            "{msg} Error: {error}",
                            msg = error_message,
                            error = &io_error
                        );
                        return Err(io::Error::new(io_error.kind(), error_message).into());
                    }
                } else {
                    let error_message =
                            "There was an error checking for database existence. Ending initialization \
                            because `DbOptions::create_if_missing` is false.";
                    log::error!(
                        "{msg} Error: {error}",
                        msg = error_message,
                        error = &io_error
                    );
                    return Err(io::Error::new(io_error.kind(), error_message).into());
                }
            }
            Ok(_file) => {
                if self.options.error_if_exists() {
                    let error_message = format!(
                        "The database already exists at the provided path: {db_path}. Ending \
                        initialization because `DbOptions::error_if_exists` was set to true.",
                        db_path = self.options.db_path()
                    );
                    log::error!("{msg}", msg = &error_message);
                    return Err(io::Error::new(io::ErrorKind::AlreadyExists, error_message).into());
                }
            }
        }

        /*
        A new manifest file and version would need to be created if any of the following conditions
        occurred:

        1. The version set recovery operation did NOT reuse an existing manifest file
        2. Recovery operations caused new files to be added to the database (e.g. compactions
           occurred during recovery)
        */
        let mut create_new_snapshot = !db_fields_guard.version_set.recover()?;

        // Recover any unrecorded WAL files
        let (version_manifest, should_create_new_snapshot) =
            self.recover_unrecorded_logs(db_fields_guard)?;
        create_new_snapshot = create_new_snapshot || should_create_new_snapshot;

        Ok((version_manifest, create_new_snapshot))
    }

    /**
    Initialize fields and database structures for a new database.

    # Legacy

    This is synonomous to LevelDB's `DBImpl::NewDB`.
    */
    fn initialize_as_new_db(&self) -> RainDBResult<()> {
        let new_db_manifest = VersionChangeManifest {
            wal_file_number: Some(0),
            curr_file_number: Some(1),
            prev_sequence_number: Some(0),
            ..VersionChangeManifest::default()
        };

        let manifest_file_number = 1;
        let manifest_path = self
            .file_name_handler
            .get_manifest_file_path(manifest_file_number);
        let mut manifest_writer = LogWriter::new(
            self.options.filesystem_provider(),
            manifest_path.clone(),
            false,
        )?;
        match manifest_writer.append(&Vec::<u8>::from(&new_db_manifest)) {
            Ok(_) => {
                DB::set_current_file(
                    self.options.filesystem_provider(),
                    &self.file_name_handler,
                    manifest_file_number,
                )?;
            }
            Err(error) => {
                log::error!(
                    "There was an error writing database state to the manifest file at \
                    {manifest_path:?}. Stopping database initialization and attempting to clean \
                    up. Error: {error}",
                    manifest_path = &manifest_path,
                    error = &error
                );

                let cleanup_result = self
                    .options
                    .filesystem_provider()
                    .remove_file(&manifest_path);
                if let Err(cleanup_err) = cleanup_result {
                    log::error!(
                        "Failed to clean up manifest file. Ignoring and returning original \
                        manifest log error. The error encountered during clean-up was \
                        {cleanup_err}",
                        cleanup_err = cleanup_err
                    );
                }

                return Err(error.into());
            }
        }

        Ok(())
    }

    /**
    Recover state from any WAL files that were not recorded to the manifest yet.

    Newer WAL files may have been added in previous runs of the database without having been
    registered in the manifest file yet.

    This method returns a tuple of a [`VersionChangeManifest`] with changes recovered from disk and
    a boolean set to true if recovery operations have changes to be saved.
    */
    fn recover_unrecorded_logs(
        &self,
        db_fields_guard: &mut MutexGuard<GuardedDbFields>,
    ) -> RainDBResult<(VersionChangeManifest, bool)> {
        let min_log_num = db_fields_guard.version_set.get_curr_wal_number();
        let potential_log_files = self.get_all_db_files()?;

        let mut expected_files = db_fields_guard.version_set.get_live_files();
        let mut logs_to_recover: Vec<u64> = Vec::with_capacity(expected_files.len());
        for file_path in potential_log_files {
            match FileNameHandler::get_file_type_from_name(&file_path) {
                Ok(file_kind) => match file_kind {
                    ParsedFileType::CurrentFile | ParsedFileType::DBLockFile => continue,
                    ParsedFileType::TableFile(file_num)
                    | ParsedFileType::ManifestFile(file_num)
                    | ParsedFileType::TempFile(file_num) => {
                        expected_files.remove(&file_num);
                    }
                    ParsedFileType::WriteAheadLog(file_num) => {
                        expected_files.remove(&file_num);
                        if file_num >= min_log_num {
                            logs_to_recover.push(file_num);
                        }
                    }
                },
                Err(parse_err) => {
                    log::warn!(
                        "Encountered an unexpected file type when finding WAL files to \
                        recover. Error: {err}",
                        err = parse_err
                    );
                }
            }
        }

        if !expected_files.is_empty() {
            let err_msg = format!(
                "During recovery, we found that there are {num_missing_files} missing files \
                (i.e. not recorded in the version set's live file state). For example, \
                {missing_file_path:?}",
                num_missing_files = expected_files.len(),
                missing_file_path = self
                    .file_name_handler
                    .get_table_file_path(*expected_files.iter().next().unwrap())
            );
            log::error!("{}", &err_msg);
            return Err(RainDBError::Recovery(err_msg));
        }

        // Recover the logs in chronological order i.e. smallest file num to largest
        logs_to_recover.sort_unstable();

        // Aggregate changes to apply to the version set later
        let mut version_change_manifest = VersionChangeManifest::default();
        let num_wal_files = logs_to_recover.len();
        let mut use_new_manifest = false;
        let mut max_sequence_num_seen: u64 = 0;
        for (log_idx, log_num) in logs_to_recover.into_iter().enumerate() {
            let is_last_wal = log_idx == (num_wal_files - 1);
            let (new_manifest, last_sequence_seen) = self.recover_wal_records(
                db_fields_guard,
                log_num,
                is_last_wal,
                &mut version_change_manifest,
            )?;
            use_new_manifest = use_new_manifest || new_manifest;

            if last_sequence_seen > max_sequence_num_seen {
                max_sequence_num_seen = last_sequence_seen;
            }

            // The previous database instance may not have updated the manifest file after
            // allocating this log number, so we manaully update the file number counter in the
            // version set.
            db_fields_guard.version_set.mark_file_number_used(log_num);
        }

        if db_fields_guard.version_set.get_prev_sequence_number() < max_sequence_num_seen {
            db_fields_guard
                .version_set
                .set_prev_sequence_number(max_sequence_num_seen);
        }

        Ok((version_change_manifest, use_new_manifest))
    }

    /**
    Read and apply transactions recorded in the WAL to the database.

    If `is_last_wal` is true, the method will attempt to reuse the WAL file.

    This method will a tuple with a boolean set to true if recovery operations have changes to be
    saved and the maximum sequence number seen during log recovery.

    # Legacy

    This is synonomous to LevelDB's `DBImpl::RecoverLogFile`.
    */
    fn recover_wal_records(
        &self,
        db_fields_guard: &mut MutexGuard<GuardedDbFields>,
        wal_number: u64,
        is_last_wal: bool,
        change_manifest: &mut VersionChangeManifest,
    ) -> RainDBResult<(bool, u64)> {
        // Get a reader for the log file to recover
        let wal_path = self.file_name_handler.get_wal_file_path(wal_number);
        let mut wal_reader = LogReader::new(self.options.filesystem_provider(), &wal_path, 0)?;

        // Read all WAL records and add to a memtable
        log::info!(
            "Recovering operations from WAL {wal_path:?}.",
            wal_path = &wal_path
        );
        let mut num_compactions: usize = 0;
        let mut memtable: Arc<Box<dyn MemTable>> = Arc::new(Box::new(SkipListMemTable::new()));
        let mut last_sequence_number: u64 = 0;
        let mut use_new_manifest = false;
        let db_state = self.generate_portable_state();

        let (mut wal_record, mut is_eof) = wal_reader.read_record()?;
        while !is_eof {
            if wal_record.len() < 12 {
                // Ignore transactions that do not contain any operations
                log::warn!(
                    "During recovery, found a log record that was too small. Size {record_size}",
                    record_size = wal_record.len()
                );
            }

            let transaction = Batch::try_from(wal_record.as_slice())?;
            DB::apply_batch_to_memtable(&**memtable, &transaction);
            let last_transaction_seq_num =
                transaction.get_starting_seq_number().unwrap() + (transaction.len() as u64) - 1;
            if last_transaction_seq_num > last_sequence_number {
                last_sequence_number = last_transaction_seq_num;
            }

            // Compact the memtable if it gets filled up
            if memtable.approximate_memory_usage() >= self.options.max_memtable_size() {
                num_compactions += 1;
                use_new_manifest = true;
                DB::convert_memtable_to_file(
                    &db_state,
                    db_fields_guard,
                    Arc::clone(&memtable),
                    None,
                    change_manifest,
                )?;
                memtable = Arc::new(Box::new(SkipListMemTable::new()));
            }

            (wal_record, is_eof) = wal_reader.read_record()?;
        }

        let mut was_memtable_reused = false;
        if is_last_wal && num_compactions == 0 {
            log::info!("Reusing WAL file: {wal_path:?}.", wal_path = &wal_path);
            drop(wal_reader);
            if let Ok(wal_writer) =
                LogWriter::new(self.options.filesystem_provider(), wal_path, true)
            {
                self.set_wal(db_fields_guard, wal_number, wal_writer);

                if !memtable.is_empty() {
                    self.memtable_ptr.store(memtable);
                    was_memtable_reused = true;
                    memtable = Arc::new(Box::new(SkipListMemTable::new()));
                }
            }
        }

        if !was_memtable_reused {
            // The memtable was not reused so compact it
            use_new_manifest = true;
            DB::convert_memtable_to_file(
                &db_state,
                db_fields_guard,
                memtable,
                None,
                change_manifest,
            )?;
        }

        Ok((use_new_manifest, last_sequence_number))
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

    This method is synonymous with [`DBImpl::Write`] in LevelDB.

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
                    unsafe {
                        // SAFETY: RainDB only allows one writer thread at a time.
                        (*self.wal().get()).append(&Vec::<u8>::from(&write_batch))?;
                    }

                    // Write the changes to the memtable
                    DB::apply_batch_to_memtable(&**self.memtable(), &write_batch);

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
                    write_result.clone().unwrap_err(),
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

                if force_compaction {
                    log::info!(
                        "Received the `force_compaction` flag, forcing a memtable compaction."
                    );
                } else {
                    log::info!("Memtable is full. Attempting compaction.");
                }

                // First create a new WAL file.
                let new_wal_number = mutex_guard.version_set.get_new_file_number();
                let wal_file_path = self.file_name_handler.get_wal_file_path(new_wal_number);
                let maybe_wal_writer =
                    LogWriter::new(self.options.filesystem_provider(), wal_file_path, false);

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
                self.set_wal(mutex_guard, new_wal_number, maybe_wal_writer.unwrap());

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
                if DB::should_schedule_compaction(&self.generate_portable_state(), mutex_guard) {
                    log::info!(
                        "Determined that compaction is necessary. Scheduling compaction task."
                    );
                    self.compaction_worker.schedule_task(TaskKind::Compaction);
                }
            }
        }
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
    fn apply_batch_to_memtable(memtable: &dyn MemTable, batch: &Batch) {
        let mut curr_sequence_num = batch.get_starting_seq_number().unwrap();
        for batch_element in batch.iter() {
            let internal_key = InternalKey::new(
                batch_element.get_key().to_vec(),
                curr_sequence_num,
                batch_element.get_operation(),
            );
            let value = batch_element.get_value().map_or(vec![], |val| val.to_vec());
            memtable.insert(internal_key, value);

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
    fn build_table_from_iterator<'m>(
        options: &DbOptions,
        metadata: &mut FileMetadata,
        mut iterator: Box<dyn RainDbIterator<Key = InternalKey, Error = RainDBError> + 'm>,
        table_cache: &Arc<TableCache>,
    ) -> RainDBResult<()> {
        let file_name_handler = FileNameHandler::new(options.db_path().to_string());
        let table_file_name = file_name_handler.get_table_file_path(metadata.file_number());
        iterator.seek_to_first()?;

        if iterator.is_valid() {
            let mut table_builder = TableBuilder::new(options.clone(), metadata.file_number())?;
            metadata.set_smallest_key(Some(iterator.current().unwrap().0.clone()));

            // Iterate the memtable and add the entries to a table
            let mut larget_key_seen: Option<InternalKey> = None;
            while let Some((key, value)) = iterator.current() {
                larget_key_seen = Some(key.clone());
                table_builder.add_entry(Rc::new(key.clone()), value)?;
                iterator.next();
            }

            // The iterator is valid so we should just be able to unwrap
            metadata.set_largest_key(Some(larget_key_seen.unwrap()));
            table_builder.finalize()?;
            metadata.set_file_size(table_builder.file_size());

            // Verify that the table file is usable by attempting to create an iterator from it
            match table_cache.find_table(metadata.file_number()) {
                Ok(table) => {
                    let _table_iter = Table::iter_with(table, ReadOptions::default());
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

                    return Err(error.into());
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

    /**
    Return true if a compaction should be scheduled.

    Various conditions are checked to see if a compaction is scheduled. For example, if the
    database is shutting down, a compaction will not be scheduled.

    # Legacy

    This is synonomous with LevelDB's `DBImpl::MaybeScheduleCompaction` except that it only checks
    if a compaction should be scheduled. The caller will handle scheduling the compaction
    themselves.
    */
    pub(crate) fn should_schedule_compaction(
        db_state: &PortableDatabaseState,
        mutex_guard: &mut MutexGuard<GuardedDbFields>,
    ) -> bool {
        if mutex_guard.background_compaction_scheduled {
            log::info!("A background compaction was already initiated.");
            return false;
        }

        if db_state.is_shutting_down.load(Ordering::Acquire) {
            log::info!("The database is shutting down. Will not schedule a background compaction.");
            return false;
        }

        if mutex_guard.maybe_bad_database_state.is_some() {
            let background_error = mutex_guard.maybe_bad_database_state.as_ref().unwrap();
            log::error!(
                "Detected a sticky background error (potentially from previous compaction \
                attempts). Error: {}",
                background_error
            );

            return false;
        }

        if mutex_guard.maybe_immutable_memtable.is_none()
            && mutex_guard.maybe_manual_compaction.is_none()
            && !mutex_guard.version_set.needs_compaction()
        {
            log::info!("No compaction work detected.");
            return false;
        }

        mutex_guard.background_compaction_scheduled = true;
        true
    }

    /**
    Convert the memtable to a table file.

    # Legacy

    This method is synonomous with LevelDB's `DBImpl::WriteLevel0Table`. This was renamed to be
    more specific to its actual function of converting memtables to table files. It does not always
    place the generated file at level 0.
    */
    pub(crate) fn convert_memtable_to_file(
        db_state: &PortableDatabaseState,
        db_fields_guard: &mut MutexGuard<GuardedDbFields>,
        memtable: Arc<Box<dyn MemTable>>,
        maybe_base_version: Option<&SharedNode<Version>>,
        change_manifest: &mut VersionChangeManifest,
    ) -> RainDBResult<()> {
        // Actual work starts here so get a timer for metric gathering purposes
        let compaction_instant = Instant::now();
        let file_number = db_fields_guard.version_set.get_new_file_number();
        let mut file_metadata = FileMetadata::new(file_number);
        db_fields_guard.tables_in_use.insert(file_number);

        log::info!(
            "Starting to build convert memtable to a table file with file number {}.",
            file_number
        );
        parking_lot::MutexGuard::<'_, GuardedDbFields>::unlocked_fair(
            db_fields_guard,
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
            "Memtable table file {} created with a file size of {}.",
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
            if let Some(base_version) = maybe_base_version.as_ref() {
                file_level = base_version
                    .read()
                    .element
                    .pick_level_for_memtable_output(smallest_user_key, largest_user_key)
            };

            // TODO: Just clone the file metadata and pass that in. make a From method for the upcoming Compaction metadata struct to file metadata so the method is reusable.
            change_manifest.add_file(
                file_level,
                file_metadata.file_number(),
                file_metadata.get_file_size(),
                file_metadata.smallest_key().clone()..file_metadata.largest_key().clone(),
            );
        }
        log::info!(
            "Memtable table file {} will be placed at level {}.",
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
        filesystem_provider: Arc<dyn FileSystem>,
        file_name_handler: &FileNameHandler,
        manifest_file_number: u64,
    ) -> io::Result<()> {
        let manifest_file_path = file_name_handler.get_manifest_file_path(manifest_file_number);
        let manifest_file_name = manifest_file_path.file_name().unwrap();
        let temp_file_path = file_name_handler.get_temp_file_path(manifest_file_number);
        let mut temp_file = filesystem_provider.create_file(&temp_file_path, false)?;
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

    /// Remove files that are no longer in use.
    pub(crate) fn remove_obsolete_files(
        db_fields_guard: &mut MutexGuard<GuardedDbFields>,
        filesystem_provider: Arc<dyn FileSystem>,
        file_name_handler: &FileNameHandler,
        table_cache: &TableCache,
    ) {
        if db_fields_guard.maybe_bad_database_state.is_some() {
            // After a background error, we don't know whether a new version may or may not have
            // been committed so we cannot safely garbage collect.
            log::error!(
                "Encountered a background error while attempting to remove obsolete \
                files. Aborting file removal. Error: {}",
                db_fields_guard.maybe_bad_database_state.clone().unwrap()
            );
            return;
        }

        let mut live_files: HashSet<u64> = db_fields_guard.tables_in_use.clone();
        for file in db_fields_guard.version_set.get_live_files() {
            live_files.insert(file);
        }
        log::info!(
            "Found {} live files. Proceeding with database path discovery and file pruning.",
            live_files.len()
        );

        // While doing checks we ignore errors from `list_dir` since it may be transient and
        // subsequent, successful runs will remove the stale files also
        let mut files_to_delete: Vec<PathBuf> = vec![];

        // Check WAL file directory for stale files
        if let Ok(wal_files) = filesystem_provider.list_dir(&file_name_handler.get_wal_dir()) {
            for file in wal_files {
                match filesystem_provider.is_dir(&file) {
                    Ok(is_dir) => {
                        if is_dir {
                            log::warn!(
                                "Found an unexpected directory in the WAL folder. Skipping it. \
                                Path: {:?}",
                                &file
                            );
                            continue;
                        }
                    }
                    Err(error) => {
                        log::warn!(
                            "Encountered an error checking if a path pointed at a directory in the \
                            WAL database folder. Path: {:?}. Error: {}",
                            &file,
                            error
                        );
                        continue;
                    }
                }

                match FileNameHandler::get_file_type_from_name(file.as_path()) {
                    Ok(file_type) => {
                        if let ParsedFileType::WriteAheadLog(wal_number) = file_type {
                            // Delete the file if the WAL number is less than the currently live WAL
                            // and it is not currently being compacted
                            let is_live_wal: bool =
                                wal_number >= db_fields_guard.version_set.get_curr_wal_number();
                            let is_being_compacted: bool =
                                match db_fields_guard.version_set.maybe_prev_wal_number() {
                                    Some(prev_wal_number) => wal_number == prev_wal_number,
                                    None => false,
                                };

                            if !is_live_wal && !is_being_compacted {
                                log::debug!("Marking WAL file {:?} for deletion.", wal_number);
                                files_to_delete.push(file);
                            }
                        } else {
                            log::warn!(
                                "Found an unexpected file in the WAL folder. Skipping it. \
                                Path: {:?}",
                                &file
                            );
                        }
                    }
                    Err(parse_err) => {
                        log::warn!(
                            "Encountered an error parsing a file path in the WAL folder. \
                            Path: {:?}. Error: {}",
                            &file,
                            parse_err
                        );
                    }
                }
            }
        }

        // Check data directory for stale table files
        if let Ok(data_files) = filesystem_provider.list_dir(&file_name_handler.get_data_dir()) {
            for file in data_files {
                match filesystem_provider.is_dir(&file) {
                    Ok(is_dir) => {
                        if is_dir {
                            log::warn!(
                                "Found an unexpected directory in the data folder. Skipping it. \
                                Path: {:?}",
                                &file
                            );
                            continue;
                        }
                    }
                    Err(error) => {
                        log::warn!(
                            "Encountered an error checking if a path pointed at a directory in the \
                            data folder. Path: {:?}. Error: {}",
                            &file,
                            error
                        );
                        continue;
                    }
                }

                match FileNameHandler::get_file_type_from_name(file.as_path()) {
                    Ok(file_type) => {
                        if let ParsedFileType::TableFile(table_number) = file_type {
                            if !live_files.contains(&table_number) {
                                log::debug!(
                                    "Evicting table file from cache and marking for deletion. \
                                    Table number: {:?}",
                                    &table_number
                                );

                                table_cache.remove(table_number);
                                files_to_delete.push(file);
                            }
                        } else {
                            log::warn!(
                                "Found an unexpected file in the data folder. Skipping it. \
                                Path: {:?}",
                                &file
                            );
                        }
                    }
                    Err(parse_err) => {
                        log::warn!(
                            "Encountered an error parsing a file path in the data folder. \
                            Path: {:?}. Error: {}",
                            &file,
                            parse_err
                        );
                    }
                }
            }
        }

        // Check main directory for stale files
        if let Ok(files) = filesystem_provider.list_dir(&file_name_handler.get_db_path()) {
            for file in files {
                match filesystem_provider.is_dir(&file) {
                    Ok(is_dir) => {
                        if is_dir {
                            continue;
                        }
                    }
                    Err(error) => {
                        log::warn!(
                            "Encountered an error checking if a path pointed at a directory in \
                            the main database folder. Path: {:?}. Error: {}",
                            &file,
                            error
                        );
                        continue;
                    }
                }

                match FileNameHandler::get_file_type_from_name(file.as_path()) {
                    Ok(file_type) => match file_type {
                        ParsedFileType::ManifestFile(manifest_file_num) => {
                            // Keep current manifest as well as any newer manifests (which can
                            // happen if there is an undiscovered race condition)
                            if manifest_file_num
                                < db_fields_guard.version_set.get_manifest_file_number()
                            {
                                log::debug!(
                                    "Marking manifest file {:?} for deletion.",
                                    manifest_file_num
                                );
                                files_to_delete.push(file);
                            }
                        }
                        ParsedFileType::TempFile(temp_file_num) => {
                            if !live_files.contains(&temp_file_num) {
                                log::debug!("Marking temp file {:?} for deletion.", &temp_file_num);
                                files_to_delete.push(file);
                            }
                        }
                        ParsedFileType::CurrentFile => {
                            log::debug!("Keeping `CURRENT` file.");
                        }
                        ParsedFileType::DBLockFile => {
                            log::debug!("Keeping `LOCK` file.");
                        }
                        _ => {
                            log::warn!(
                                "Found an unexpected file in the main database folder. Skipping \
                                it. Path: {:?}",
                                &file
                            );
                        }
                    },
                    Err(parse_err) => {
                        log::warn!(
                            "Encountered an error parsing a file path in the main database folder. \
                            Path: {:?}. Error: {}",
                            &file,
                            parse_err
                        );
                    }
                }
            }
        }

        /*
        Unblock other threads while deleting files. All of the files being deleted have unique
        names that will not collide with newly created files so it is safe to release the lock.
        */
        parking_lot::MutexGuard::<'_, GuardedDbFields>::unlocked_fair(db_fields_guard, move || {
            for file in files_to_delete {
                log::info!("Removing obsolete file: {:?}", &file);
                if let Err(error) = filesystem_provider.remove_file(&file) {
                    log::error!(
                        "There was an error removing the obsolete file {:?}. Error: {}",
                        &file,
                        error
                    );
                }
            }
        });
    }

    /// Return a flattened list of paths to all files under the database root.
    fn get_all_db_files(&self) -> RainDBResult<Vec<PathBuf>> {
        let filesystem = self.options.filesystem_provider();
        let root_files = filesystem.list_dir(&self.file_name_handler.get_db_path())?;
        let wal_files = filesystem.list_dir(&self.file_name_handler.get_wal_dir())?;
        let data_files = filesystem.list_dir(&self.file_name_handler.get_data_dir())?;
        let all_files = [root_files, wal_files, data_files].concat();

        Ok(all_files)
    }

    /**
    Force the compaction of the current memtable.

    # Legacy

    This method is synonomous to LevelDB's `DBImple::TEST_CompactMemTable` method.
    */
    fn force_memtable_compaction(&self) -> RainDBResult<()> {
        // Force a compaction by calling `apply_changes` with no batch
        self.apply_changes(WriteOptions::default(), None)?;

        let mut db_fields_guard = self.guarded_fields.lock();
        while db_fields_guard.maybe_immutable_memtable.is_some()
            && db_fields_guard.maybe_bad_database_state.is_none()
        {
            log::debug!("Waiting for the forced compaction to complete");
            self.background_work_finished_signal
                .wait(&mut db_fields_guard);
        }

        if db_fields_guard.maybe_immutable_memtable.is_some() {
            return Err(db_fields_guard
                .maybe_bad_database_state
                .as_ref()
                .unwrap()
                .clone());
        }

        Ok(())
    }

    /**
    Force the compaction of the specified level for the specified user key range.

    # Panics

    This method will panic if it is given an invalid level. The level provided cannot be the last
    level because there is no next level to compact to.

    # Legacy

    This method is synonomous to LevelDB's `DBImple::TEST_CompactRange` method.
    */
    fn force_level_compaction(&self, level: usize, key_range: &Range<Option<&[u8]>>) {
        assert!(level + 1 < MAX_NUM_LEVELS);

        let manual_compaction = ManualCompactionConfiguration {
            level,
            done: false,
            begin: key_range.start.map(|user_key| {
                InternalKey::new_for_seeking(user_key.to_vec(), MAX_SEQUENCE_NUMBER)
            }),
            end: key_range.end.map(|user_key| {
                InternalKey::new(user_key.to_vec(), 0, Operation::try_from(0).unwrap())
            }),
        };
        let wrapped_manual_compaction = Arc::new(Mutex::new(manual_compaction));
        let mut db_fields_guard = self.guarded_fields.lock();
        let db_state = self.generate_portable_state();

        while !wrapped_manual_compaction.lock().done
            && !self.is_shutting_down.load(Ordering::Acquire)
            && db_fields_guard.maybe_bad_database_state.is_none()
        {
            if db_fields_guard.maybe_manual_compaction.is_none() {
                // A manual compaction is not already being processed
                db_fields_guard.maybe_manual_compaction =
                    Some(Arc::clone(&wrapped_manual_compaction));

                if DB::should_schedule_compaction(&db_state, &mut db_fields_guard) {
                    self.compaction_worker.schedule_task(TaskKind::Compaction);
                }
            } else {
                // A compaction is already in progress or the database is still processing this
                // compaction request and just temporarily unlocked state
                self.background_work_finished_signal
                    .wait(&mut db_fields_guard);
            }
        }

        if db_fields_guard.maybe_manual_compaction.is_some()
            && Arc::ptr_eq(
                &wrapped_manual_compaction,
                db_fields_guard.maybe_manual_compaction.as_ref().unwrap(),
            )
        {
            log::warn!("A manual compaction was canceled early for an unknown reason.");
            db_fields_guard.maybe_manual_compaction.take();
        }
    }

    /// Return a string summarizing the compaction statistics for each level.
    fn summarize_compaction_stats(&self) -> String {
        const MEGABYTE_SIZE_BYTES: f64 = 1.0 * 1024.0 * 1024.0;

        let db_fields_guard = self.guarded_fields.lock();
        let mut summary = "Compactions".to_owned();
        writeln!(
            summary,
            "Level  Files Size(MB) Time(sec) Read(MB) Write(MB)"
        )
        .unwrap();
        writeln!(
            summary,
            "--------------------------------------------------"
        )
        .unwrap();

        for level in 0..MAX_NUM_LEVELS {
            let num_files = db_fields_guard.version_set.num_files_at_level(level);
            let compaction_stats = &db_fields_guard.compaction_stats[level];
            if !compaction_stats.compaction_duration.is_zero() || num_files > 0 {
                writeln!(
                    summary,
                    "{level:3} {num_files:8} {level_size:8.0} {compaction_duration:9.0} \
                    {bytes_read:8.0} {bytes_written:9.0}",
                    level_size = db_fields_guard.version_set.get_current_level_size(level),
                    compaction_duration = compaction_stats.compaction_duration.as_secs_f64(),
                    bytes_read = (compaction_stats.bytes_read as f64) / MEGABYTE_SIZE_BYTES,
                    bytes_written = (compaction_stats.bytes_written as f64) / MEGABYTE_SIZE_BYTES
                )
                .unwrap();
            }
        }

        summary
    }
}

impl Drop for DB {
    fn drop(&mut self) {
        let mut db_fields_guard = self.guarded_fields.lock();

        log::info!(
            "Setting the database to shutdown and waiting for any in progress compaction work to \
            finish."
        );
        self.is_shutting_down.store(true, Ordering::Release);
        while db_fields_guard.background_compaction_scheduled {
            log::info!("Detected pending background work. Waiting for it to finish.");
            self.background_work_finished_signal
                .wait(&mut db_fields_guard);

            log::info!("Checking for more background work.");
        }

        log::info!("No more background work detected. Proceeding with shutdown.");
        drop(db_fields_guard);

        // Drop the database lock
        self.db_lock.take();

        // Clean-up WAL pointer
        unsafe {
            // SAFETY: We do a null check before loading and dropping the memory reference.
            if !self.wal.load(Ordering::SeqCst).is_null() {
                Box::from_raw(self.wal.load(Ordering::SeqCst));
            }
        };

        log::info!("Terminating the compaction worker background thread.");
        if let Some(compaction_worker_join_handle) = Arc::get_mut(&mut self.compaction_worker)
            .unwrap()
            .stop_worker_thread()
        {
            if let Err(thread_panic_val) = compaction_worker_join_handle.join() {
                log::error!(
                    "The compaction worker thread panicked while exiting unwinding the \
                    stack with the panicked value."
                );

                panic::resume_unwind(thread_panic_val);
            }
        }
    }
}

/// Various statistics and summaries that can be requested from the database.
#[derive(Debug, PartialEq, Eq)]
pub enum DatabaseDescriptor {
    /// Get the number of files at the specified level.
    NumFilesAtLevel(usize),

    /**
    Returns a multi-line string that describes statistics about the internal operation of the
    database.
    */
    Stats,

    /**
    Returns a multi-line string that describes the table files that make up the database
    contents.
    */
    SSTables,
}

#[cfg(test)]
mod db_test;

#[cfg(test)]
mod test_utils;
