/*!
This module contains abstractions used in compaction operations. Core to this is the
[`CompactionWorker`] worker thread.
*/

use parking_lot::MutexGuard;
use std::ops::AddAssign;
use std::sync::atomic::Ordering;
use std::sync::{mpsc, Arc};
use std::thread::{self, JoinHandle};
use std::time::Duration;
use std::{fmt, io};

use crate::db::{GuardedDbFields, PortableDatabaseState};
use crate::errors::{DBIOError, RainDBError};
use crate::key::InternalKey;
use crate::versioning::errors::WriteError;
use crate::versioning::{VersionChangeManifest, VersionSet};
use crate::DB;

/**
Name of the compaction thread.

Tumtum is the name of a friends dog.
*/
#[cfg(not(feature = "strict"))]
const COMPACTION_THREAD_NAME: &str = "raindb-tumtum";

/// Name of the compaction thread.
#[cfg(feature = "strict")]
const COMPACTION_THREAD_NAME: &str = "raindb-compact";

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
    /// Create a new instance of [`CompactionWorker`].
    pub fn new(db_state: PortableDatabaseState) -> CompactionWorkerResult<Self> {
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
    fn compaction_task(db_state: &'static PortableDatabaseState) {
        let PortableDatabaseState {
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
            CompactionWorker::coordinate_compaction(db_state, &mut db_fields_guard);
        }

        db_fields_guard.background_compaction_scheduled = false;

        // The previous compaction may have created too many files in a level, so check and
        // schedule a compaction if needed
        // TODO: CompactionWorker::maybe_schedule_compaction - share with db.rs
        background_work_finished_signal.notify_all();
    }

    /**
    Performs the actual compaction tasks.

    # Legacy

    This is synonomous to LevelDB's `DBImpl::BackgroundCompaction` method.
    */
    fn coordinate_compaction(
        db_state: &PortableDatabaseState,
        db_fields_guard: &'static mut MutexGuard<GuardedDbFields>,
    ) {
        if db_fields_guard.maybe_immutable_memtable.is_some() {
            // There is an immutable memtable. Compact that.
            CompactionWorker::compact_memtable(db_state, db_fields_guard);
            return;
        }
    }

    /**
    Performs a compaction routine on the immutable memtable.

    # Panics

    An immutable memtable must exist if this method is called.
    */
    fn compact_memtable(
        db_state: &PortableDatabaseState,
        db_fields_guard: &'static mut MutexGuard<GuardedDbFields>,
    ) {
        assert!(db_fields_guard.maybe_immutable_memtable.is_some());

        log::info!("Compacting the immutable memtable to a table file.");
        let change_manifest = VersionChangeManifest::default();
        let base_version = db_fields_guard.version_set.get_current_version();
        let write_table_result = DB::write_level0_table(
            db_state,
            db_fields_guard,
            db_fields_guard.maybe_immutable_memtable.as_ref().unwrap(),
            base_version,
            &mut change_manifest,
        );

        if let Err(write_table_error) = write_table_result {
            DB::set_bad_database_state(
                db_state,
                db_fields_guard,
                CompactionWorkerError::WriteTable(Box::new(write_table_error)).into(),
            );
            return;
        }

        // Do periodic check for shutdown state before proceeding to the next major compaction
        // operation
        if db_state.is_shutting_down.load(Ordering::Acquire) {
            log::error!(
                "Compaction thread discovered that the database was shutting down. Halting \
                compaction work. Recording background error to stop other writes from occurring."
            );

            DB::set_bad_database_state(
                db_state,
                db_fields_guard,
                CompactionWorkerError::UnexpectedState(
                    "Detected database shutdown signal while compacting the memtable.".to_string(),
                )
                .into(),
            );
            return;
        }

        // The memtable was converted to a table file so the associated WAL is also obsolete.
        // Remove references via the change manifest.
        change_manifest.prev_wal_file_number = Some(0);
        // The `curr_wal_file_number` field was optimistically updated prior to compaction
        // (e.g. see `DB::make_room_for_write`)
        change_manifest.wal_file_number = Some(db_fields_guard.curr_wal_file_number);

        log::info!(
            "Compaction thread is applying changes memtable compaction change to the current \
            version."
        );

        let apply_result = VersionSet::log_and_apply(db_fields_guard, change_manifest);
        if let Err(apply_error) = apply_result {
            log::error!(
                "There was an error logging and applying the change manifest. Error: {}",
                apply_error
            );

            DB::set_bad_database_state(
                db_state,
                db_fields_guard,
                CompactionWorkerError::VersionManifestError(apply_error).into(),
            );
            return;
        }
    }
}

/// Type alias for [`Result`]'s with [`CompactionWorkerError`]'s.
pub(crate) type CompactionWorkerResult<T> = Result<T, CompactionWorkerError>;

/// Errors that occur during compaction worker operations.
#[derive(Clone, Debug)]
pub enum CompactionWorkerError {
    /// Variant for IO errors encountered during worker operations.
    IO(DBIOError),

    /// Variant for issues that occur when writing a table file.
    WriteTable(Box<RainDBError>),

    /// Variant for issues that occur when persisting and applying a version change manifest.
    VersionManifestError(WriteError),

    /// Variant for halting compactions due to unexpected state.
    UnexpectedState(String),
}

impl std::error::Error for CompactionWorkerError {}

impl fmt::Display for CompactionWorkerError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CompactionWorkerError::IO(base_err) => write!(f, "{}", base_err),
            CompactionWorkerError::WriteTable(base_err) => write!(f, "{}", base_err),
            CompactionWorkerError::VersionManifestError(base_err) => write!(f, "{}", base_err),
            CompactionWorkerError::UnexpectedState(reason) => write!(f, "{}", reason),
        }
    }
}

impl From<io::Error> for CompactionWorkerError {
    fn from(err: io::Error) -> Self {
        CompactionWorkerError::IO(err.into())
    }
}

impl From<WriteError> for CompactionWorkerError {
    fn from(err: WriteError) -> Self {
        CompactionWorkerError::VersionManifestError(err)
    }
}

// Carries information for performing a manual compaction and it's completion state.
pub struct ManualCompaction {
    /// The level to compact.
    pub level: u64,
    /// True if the compaction is completed. Otherwise, false.
    pub done: bool,
    /// The key to start compaction from. `None` means the start of the key range.
    pub begin: Option<InternalKey>,
    /// The key to end compaction at. `None` means the end of the key range.
    pub end: Option<InternalKey>,
}

/// Carries compaction metrics for a level.
pub(crate) struct LevelCompactionStats {
    /// The time it took for a compaction to complete.
    compaction_duration: Duration,

    /// The number of bytes read during the compaction.
    bytes_read: u64,

    /// The number of bytes written during the compaction.
    bytes_written: u64,
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
