use parking_lot::MutexGuard;
use std::sync::atomic::Ordering;
use std::sync::{mpsc, Arc};
use std::thread::{self, JoinHandle};

use crate::compaction::errors::CompactionWorkerError;
use crate::db::{GuardedDbFields, PortableDatabaseState};
use crate::key::InternalKey;
use crate::versioning::{VersionChangeManifest, VersionSet};
use crate::DB;

use super::errors::CompactionWorkerResult;

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
pub(crate) enum TaskKind {
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
        // TODO: restart the compaction thread on error
        self.task_sender.send(task_kind).unwrap();
    }
}

/// Private methods
impl CompactionWorker {
    /**
    The primary entry point to start compaction.

    It re-checks some compaction pre-conditions and does some clean-up work at the end.
    */
    fn compaction_task(db_state: &PortableDatabaseState) {
        let PortableDatabaseState {
            guarded_db_fields,
            is_shutting_down,
            background_work_finished_signal,
            ..
        } = db_state;
        let mut db_fields_guard = guarded_db_fields.lock();

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
        db_fields_guard: &mut MutexGuard<GuardedDbFields>,
    ) {
        if db_fields_guard.maybe_immutable_memtable.is_some() {
            log::info!(
                "Compaction thread found an immutable memtable to compact. Proceeding with \
                memtable compaction."
            );
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
        db_fields_guard: &mut MutexGuard<GuardedDbFields>,
    ) {
        assert!(db_fields_guard.maybe_immutable_memtable.is_some());

        log::info!("Compacting the immutable memtable to a table file.");
        let mut change_manifest = VersionChangeManifest::default();
        let base_version = db_fields_guard.version_set.get_current_version();
        let immutable_memtable = db_fields_guard.maybe_immutable_memtable.clone().unwrap();
        let write_table_result = DB::write_level0_table(
            db_state,
            db_fields_guard,
            Arc::clone(&immutable_memtable),
            &base_version,
            &mut change_manifest,
        );
        db_fields_guard.version_set.release_version(base_version);

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

        log::info!(
            "Compaction thread committing to new database state. Removing immutable memtable and \
            obsolete files."
        );
        db_fields_guard.maybe_immutable_memtable.take();
        db_state
            .has_immutable_memtable
            .store(false, Ordering::Release);
        DB::remove_obsolete_files(
            db_fields_guard,
            db_state.options.filesystem_provider(),
            Arc::clone(&db_state.file_name_handler).as_ref(),
            Arc::clone(&db_state.table_cache).as_ref(),
        );
    }
}
