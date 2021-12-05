use parking_lot::{Condvar, Mutex, MutexGuard};
use std::ops::AddAssign;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{mpsc, Arc};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};
use std::{fmt, io};

use crate::db::GuardedDbFields;
use crate::errors::RainDBResult;
use crate::key::LookupKey;
use crate::memtable::MemTable;
use crate::versioning::file_metadata::FileMetadata;
use crate::versioning::VersionChangeManifest;
use crate::DbOptions;

/// Name of the compaction thread.
const COMPACTION_THREAD_NAME: &str = "raindb-tumtum";

/// Database state used by the worker thread.
struct DatabaseState {
    /// Options for configuring the operation of the database.
    options: DbOptions,

    /**
    Database state that require a lock to be held before being read or written to.

    In the course of database operation, this is usually instantiated in and obtained from [`DB`].

    [`DB`]: crate::db::DB
    */
    guarded_db_fields: Arc<Mutex<GuardedDbFields>>,

    /// Field indicating if the database is shutting down.
    is_shutting_down: Arc<AtomicBool>,

    /**
    Field indicating if there is an immutable memtable.

    An memtable is made immutable when it is undergoing the compaction process.
    */
    has_immutable_memtable: Arc<AtomicBool>,

    /**
    A condition variable used to notify parked threads that background work (e.g. compaction) has
    finished.
    */
    background_work_finished_signal: Arc<Condvar>,
}

/// The kinds of tasks that the compaction worker can schedule.
#[derive(Debug)]
pub enum TaskKind {
    /// Variant for scheduling a compaction job.
    Compaction,

    /// Variant for shutting down the compaction thread.
    Terminate,
}

/**
The compaction worker manages a thread that performs compaction actions.

The worker uses a channel to communicate new tasks to the background thread so that we can keep
reusing the same thread as opposed to continually spawning new threads.
*/
pub(crate) struct CompactionWorker {
    /**
    The join handle of the background compaction thread.

    This is used to try to gracefully shutdown the background compaction thread during database
    shutdown.
    */
    maybe_background_compaction_handle: Option<JoinHandle<()>>,

    /// Sender end of the channel that the worker utilizes to schedule tasks.
    task_sender: mpsc::Sender<TaskKind>,
}

/// Public methods
impl CompactionWorker {
    /**
    Create a new instance of [`CompactionWorker`].

    * `options` - Options for configuring the operation of the database.
    * `guarded_db_fields` - Database state that require a lock to be held before being read or written to.
    * `is_shutting_down` - Indicator for if the database is shutting down.
    * `has_immutable_memtable` - Indicator for if there is an immutable memtable.
    * `background_work_finished_signal` - Signal the completion of background work

    Most of the parameters are put into the [`DatabaseState`] struct and passed to the actual
    compaction thread for convenience. Expanded documentation of these params can be found in said
    struct.
    */
    pub fn new(
        options: DbOptions,
        guarded_db_fields: Arc<Mutex<GuardedDbFields>>,
        is_shutting_down: Arc<AtomicBool>,
        has_immutable_memtable: Arc<AtomicBool>,
        background_work_finished_signal: Arc<Condvar>,
    ) -> CompactionWorkerResult<Self> {
        let db_state = DatabaseState {
            options,
            guarded_db_fields,
            is_shutting_down,
            has_immutable_memtable,
            background_work_finished_signal,
        };

        // Create a channel for sending tasks
        let (task_sender, receiver) = mpsc::channel();

        log::info!("Starting up the background compaction thread.");
        let background_thread_handle = thread::Builder::new()
            .name(COMPACTION_THREAD_NAME.to_string())
            .spawn(move || {
                log::info!("Compaction thread initializing.");
                let database_state = db_state;

                loop {
                    log::info!("Compaction thread waiting for tasks.");
                    let task = receiver.recv().unwrap();

                    match task {
                        TaskKind::Compaction => {
                            log::info!("Compaction thread receieved the compaction command.");
                            CompactionWorker::compaction_task(&database_state);
                        }
                        TaskKind::Terminate => {
                            log::info!(
                                "Compaction thread receieved the termination command. Shutting \
                                down the thread."
                            );
                            break;
                        }
                    }
                }
            })?;

        let thread = background_thread_handle.thread();
        let thread_name = thread.name().map_or("<unnamed>", |name| name);
        log::info!(
            "Compaction thread started with name {thread_name}.",
            thread_name = thread_name
        );

        let worker = Self {
            maybe_background_compaction_handle: Some(background_thread_handle),
            task_sender,
        };

        Ok(worker)
    }

    /// Schedule a task in the background thread.
    pub fn schedule_task(&self, task_kind: TaskKind) {
        self.task_sender.send(task_kind);
    }
}

/// Private methods
impl CompactionWorker {
    /**
    The primary entry point to start compaction.

    It re-checks some compaction pre-conditions and does some clean-up work at the end.
    */
    fn compaction_task(db_state: &DatabaseState) {
        let DatabaseState {
            guarded_db_fields,
            is_shutting_down,
            background_work_finished_signal,
            ..
        } = db_state;
        let db_fields_guard = guarded_db_fields.lock();

        if is_shutting_down.load(Ordering::Acquire) {
            log::info!(
                "Compaction thread discovered that the database was shutting down. Halting \
                compaction work."
            );
        } else if db_fields_guard.maybe_bad_database_state.is_some() {
            log::warn!(
                "Compaction thread discovered that the database was in a bad state. Halting \
                compaction work."
            );
        } else {
            CompactionWorker::compact_memtable(db_state, &mut db_fields_guard);
        }

        db_fields_guard.background_compaction_scheduled = false;

        // The previous compaction may have created too many files in a level, so check and
        // schedule a compaction if needed
        // CompactionWorker::maybe_schedule_compaction - share with db.rs
        background_work_finished_signal.notify_all();
    }

    /// Performs the actual compaction tasks.
    fn coordinate_compaction(
        db_state: &DatabaseState,
        db_fields_guard: &mut MutexGuard<GuardedDbFields>,
    ) {
        if db_fields_guard.maybe_immutable_memtable.is_some() {
            // There is an immutable memtable. Compact that.
            CompactionWorker::compact_memtable(db_state, &mut db_fields_guard);
            return;
        }
    }

    /**
    Performs a compaction routine on the immutable memtable.

    An immutable memtable must exist if this method is called.
    */
    fn compact_memtable(
        db_state: &DatabaseState,
        db_fields_guard: &mut MutexGuard<GuardedDbFields>,
    ) {
        assert!(db_fields_guard.maybe_immutable_memtable.is_some());

        // Save the memtable to a new table file
        let change_manifest = VersionChangeManifest::default();
        // Apply the changes to the current version
        let base_version = db_fields_guard.version_set.get_current_version().unwrap();
        CompactionWorker::write_level0_table(
            db_state,
            &mut db_fields_guard,
            &db_fields_guard.maybe_immutable_memtable.as_ref().unwrap(),
            &mut change_manifest,
        );
    }

    /// Convert the immutable memtable to a table file.
    pub(crate) fn write_level0_table(
        db_state: &DatabaseState,
        db_fields_guard: &mut MutexGuard<GuardedDbFields>,
        memtable: &Box<dyn MemTable>,
        change_manifest: &mut VersionChangeManifest,
    ) -> RainDBResult<()> {
        // Actual work starts here so get a timer for metric gathering purposes
        let compaction_instant = Instant::now();
        let file_number = db_fields_guard.version_set.get_new_file_number();
        let file_metadata = FileMetadata::new(file_number);
        db_fields_guard.tables_in_use.insert(file_number);

        let stats = LevelCompactionStats {
            compaction_duration: compaction_instant.elapsed(),
            ..LevelCompactionStats::default()
        };
        Ok(())
    }
}

/// Type alias for [`Result`]'s with [`CompactionWorkerError`]'s.
pub(crate) type CompactionWorkerResult<T> = Result<T, CompactionWorkerError>;

/// Errors that occur during compaction worker operations.
#[derive(Debug)]
pub enum CompactionWorkerError {
    /// Variant for IO errors encountered during worker operations.
    IO(io::Error),
}

impl std::error::Error for CompactionWorkerError {}

impl fmt::Display for CompactionWorkerError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CompactionWorkerError::IO(base_err) => write!(f, "{}", base_err),
        }
    }
}

impl From<io::Error> for CompactionWorkerError {
    fn from(err: io::Error) -> Self {
        CompactionWorkerError::IO(err)
    }
}

// Carries information for performing a manual compaction and it's completion state.
pub struct ManualCompaction {
    /// The level to compact.
    pub level: u64,
    /// True if the compaction is completed. Otherwise, false.
    pub done: bool,
    /// The key to start compaction from. `None` means the start of the key range.
    pub begin: Option<LookupKey>,
    /// The key to end compaction at. `None` means the end of the key range.
    pub end: Option<LookupKey>,
}

/// Carries compaction metrics for a level.
pub(crate) struct LevelCompactionStats {
    /// The time it took for a compaction to complete.
    compaction_duration: Duration,

    /// The number of bytes read during the compaction.
    bytes_read: usize,

    /// The number of bytes written during the compaction.
    bytes_written: usize,
}

/// Private methods
impl LevelCompactionStats {
    /// Add the statistic values in `other_stats` to the statistics in this object.
    fn add_stats(&mut self, other_stats: &LevelCompactionStats) {
        self.compaction_duration += other_stats.compaction_duration;
        self.bytes_read += other_stats.bytes_read;
        self.bytes_written += other_stats.bytes_written;
    }
}

impl Default for LevelCompactionStats {
    fn default() -> Self {
        Self {
            compaction_duration: Duration::new(0, 0),
            bytes_read: 0,
            bytes_written: 0,
        }
    }
}

impl AddAssign for LevelCompactionStats {
    fn add_assign(&mut self, rhs: Self) {
        self.add_stats(&rhs);
    }
}
