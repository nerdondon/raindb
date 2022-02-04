use parking_lot::MutexGuard;
use std::ops::Range;
use std::sync::atomic::Ordering;
use std::sync::{mpsc, Arc};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use crate::compaction::errors::CompactionWorkerError;
use crate::db::{GuardedDbFields, PortableDatabaseState};
use crate::errors::{RainDBError, RainDBResult};
use crate::key::{InternalKey, MAX_SEQUENCE_NUMBER};
use crate::versioning::file_iterators::MergingIterator;
use crate::versioning::{VersionChangeManifest, VersionSet};
use crate::{Operation, RainDbIterator, DB};

use super::errors::CompactionWorkerResult;
use super::manifest::CompactionManifest;
use super::state::CompactionState;

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

        let is_manual_compaction = db_fields_guard.maybe_manual_compaction.is_some();
        let mut manual_compaction_end_key: Option<&InternalKey> = None;
        let mut compaction_manifest: Option<CompactionManifest> = None;
        if is_manual_compaction {
            let compaction_level: usize;
            let compaction_range: Range<Option<InternalKey>>;
            {
                let manual_compaction = db_fields_guard
                    .maybe_manual_compaction
                    .as_ref()
                    .unwrap()
                    .lock();
                log::info!(
                    "Found manual compaction request for level {} and attempting to execute.",
                    manual_compaction.level
                );
                compaction_level = manual_compaction.level;
                compaction_range = manual_compaction.clone_key_range();
            }

            compaction_manifest = db_fields_guard
                .version_set
                .compact_range(compaction_level, compaction_range);
            let mut manual_compaction = db_fields_guard
                .maybe_manual_compaction
                .as_ref()
                .unwrap()
                .lock();
            manual_compaction.done = compaction_manifest.is_none();
            if compaction_manifest.is_some() {
                manual_compaction_end_key = Some(
                    compaction_manifest
                        .as_ref()
                        .unwrap()
                        .get_compaction_level_files()
                        .last()
                        .unwrap()
                        .largest_key(),
                );
            }

            let compaction_start_string: String = match manual_compaction.begin.as_ref() {
                Some(key) => format!("{:?}", Vec::<u8>::from(key)),
                None => "(begin)".to_string(),
            };
            let compaction_end_string: String = match manual_compaction.end.as_ref() {
                Some(key) => format!("{:?}", Vec::<u8>::from(key)),
                None => "(end)".to_string(),
            };
            let manual_end_string: String = if manual_compaction.done {
                "the specified end key".to_string()
            } else {
                format!(
                    "potentially smaller key {:?}",
                    Vec::<u8>::from(manual_compaction_end_key.as_deref().unwrap())
                )
            };
            log::info!(
                "Manual compaction requested for level {compaction_level} from {start_key} to \
                {end_key}. Compaction will end at {manual_end_key}.",
                compaction_level = manual_compaction.level,
                start_key = compaction_start_string,
                end_key = compaction_end_string,
                manual_end_key = manual_end_string
            );
        } else {
            log::info!(
                "Compaction thread proceeding with normal compaction procedure. Determining if a \
                size triggered or seek triggered compaction is required."
            );
            compaction_manifest = db_fields_guard.version_set.pick_compaction();
        }

        // TODO: Actual compaction operations

        if is_manual_compaction {
            /*
            The `.take` is fine since the thread requesting the compaction will still have a
            reference through the `Arc`. Even if the compaction request is not completely fulfilled,
            the requesting thread gets a new range to request another compaction through the shared
            state mutation.

            These feels super abusive of shared state and could really benefit from a message
            passing approach. But--(in the tone of Tony Stark)--that's how LevelDB did it, it's how
            everything else built on LevelDB does it, and it's worked out pretty well so far
            (https://youtu.be/KNAgFhh1ji4?t=26). Maybe not entirely accurate but I'm tired.
            */
            let manual_compaction = db_fields_guard.maybe_manual_compaction.take().unwrap();
            let mut manual_compaction_guard = manual_compaction.lock();
            if !manual_compaction_guard.done {
                // Only part of the range was compacted. Update to the part of the range that has
                // not been compacted yet.
                manual_compaction_guard.begin = Some(manual_compaction_end_key.unwrap().clone());
            }
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

    /**
    Enact the compaction specified by the specified compaction state and manifest.

    The compaction work can be interrupted if an immutable memtable is detected. Compaction will
    prioritize compacting the immutable memtable before proceeding with table file compaction.

    # Panics

    The compaction manifest must have files to compact at the specified compaction level.

    # Legacy

    This is equivalent to LevelDB's `DoCompactionWork` method.
    */
    fn compact_tables(
        db_state: &PortableDatabaseState,
        db_fields_guard: &mut MutexGuard<GuardedDbFields>,
        compaction_manifest: CompactionManifest,
    ) -> RainDBResult<CompactionState> {
        let compaction_instant = Instant::now();
        let mut total_memtable_compaction_time: Duration = Duration::default();

        log::info!(
            "Compacting {num_compaction_level_files} files at level {compaction_level} with \
            {num_parent_files} files at parent level {parent_level}.",
            num_compaction_level_files = compaction_manifest.get_compaction_level_files().len(),
            compaction_level = compaction_manifest.level(),
            num_parent_files = compaction_manifest.get_parent_level_files().len(),
            parent_level = compaction_manifest.level() + 1
        );

        // Assert invariants
        assert!(
            db_fields_guard
                .version_set
                .num_files_at_level(compaction_manifest.level())
                > 0
        );

        let mut compaction_state: CompactionState = if db_fields_guard.snapshots.is_empty() {
            CompactionState::new(
                compaction_manifest,
                db_fields_guard.version_set.get_prev_sequence_number(),
            )
        } else {
            CompactionState::new(
                compaction_manifest,
                db_fields_guard
                    .snapshots
                    .oldest()
                    .read()
                    .element
                    .sequence_number(),
            )
        };

        // Release lock while doing actual compaction work
        let compaction_result = parking_lot::MutexGuard::<'_, GuardedDbFields>::unlocked_fair(
            db_fields_guard,
            || -> RainDBResult<MergingIterator> {
                let mut maybe_current_user_key: Option<Vec<u8>> = None;
                let mut last_sequence_for_key = MAX_SEQUENCE_NUMBER;

                let mut file_iterator = compaction_state
                    .compaction_manifest()
                    .make_merging_iterator(Arc::clone(&db_state.table_cache))?;
                file_iterator.seek_to_first()?;

                while file_iterator.is_valid() && !db_state.is_shutting_down.load(Ordering::Acquire)
                {
                    if db_state.has_immutable_memtable.load(Ordering::Acquire) {
                        // Prioritize compacting an immutable memtable if there is one
                        let memtable_compaction_start = Instant::now();
                        let mut db_mutex_guard = db_state.guarded_db_fields.lock();
                        if db_mutex_guard.maybe_immutable_memtable.is_some() {
                            CompactionWorker::compact_memtable(db_state, &mut db_mutex_guard);

                            // Notify waiting writers if there are any
                            db_state.background_work_finished_signal.notify_all();
                        }

                        total_memtable_compaction_time += memtable_compaction_start.elapsed();
                    }

                    /*
                    If the table file that is currently being built overlaps too much of the
                    grandparent files, start a new file.
                    */
                    if compaction_state.has_table_builder()
                        && compaction_state
                            .compaction_manifest_mut()
                            .should_stop_before_key(file_iterator.current().unwrap().0)
                    {
                        compaction_state.finish_compaction_output_file(
                            Arc::clone(&db_state.table_cache),
                            &mut file_iterator,
                        )?;
                    }

                    // Make a determination on whether or not to keep a key e.g. if it there are
                    // newer records of it and no snapshots require anything older.
                    let mut should_drop_entry = false;
                    let (current_key, current_value) = file_iterator.current().unwrap();
                    if maybe_current_user_key.is_none()
                        || current_key.get_user_key() != maybe_current_user_key.as_ref().unwrap()
                    {
                        // This is the first occurrence of this user key
                        maybe_current_user_key = Some(current_key.get_user_key().to_vec());
                        last_sequence_for_key = MAX_SEQUENCE_NUMBER
                    }

                    #[allow(clippy::if_same_then_else)]
                    if last_sequence_for_key <= compaction_state.get_smallest_snapshot() {
                        // Entry is hidden by a newer entry for the same user key
                        should_drop_entry = true;
                    } else if current_key.get_operation() == Operation::Delete
                        && current_key.get_sequence_number()
                            <= compaction_state.get_smallest_snapshot()
                        && compaction_state
                            .compaction_manifest_mut()
                            .is_base_level_for_key(current_key)
                    {
                        /*
                        This user key:
                        - has no data in older levels
                        - data in younger levels will have larger sequence numbers
                        - data in the levels being compacted have smaller sequence numbers that
                          will be dropped in the next iterations of this loop

                        Therefore, this deletion marker is obsolete and can be dropped.
                        */
                        should_drop_entry = true;
                    }

                    last_sequence_for_key = current_key.get_sequence_number();

                    log::debug!(
                        "Compaction thread processing table entry with--user key: {user_key:?}, \
                        seq: {seq_num}, operation: {op:?}, is dropping: {is_dropping}, is base \
                        level: {is_base_level}, last seen sequence: {last_seq}, smallest \
                        snapshot: {smallest_snapshot}",
                        user_key = current_key.get_user_key(),
                        seq_num = current_key.get_sequence_number(),
                        op = current_key.get_operation(),
                        is_dropping = should_drop_entry,
                        is_base_level = compaction_state
                            .compaction_manifest_mut()
                            .is_base_level_for_key(current_key),
                        last_seq = last_sequence_for_key,
                        smallest_snapshot = compaction_state.get_smallest_snapshot()
                    );

                    if !should_drop_entry {
                        // Open output file if one is not already open
                        if !compaction_state.has_table_builder() {
                            compaction_state.open_compaction_output_file(db_state)?;
                        }

                        if compaction_state.table_builder_mut().get_num_entries() == 0 {
                            compaction_state
                                .current_output_mut()
                                .set_smallest_key(Some(current_key.clone()));
                        }

                        compaction_state
                            .current_output_mut()
                            .set_largest_key(Some(current_key.clone()));
                        compaction_state
                            .table_builder_mut()
                            .add_entry(Rc::new(current_key.clone()), current_value)?;

                        // Close output file if it hits the file size threshold
                        if compaction_state.table_builder_mut().file_size()
                            >= compaction_state
                                .compaction_manifest()
                                .max_output_file_size_bytes()
                        {
                            compaction_state.finish_compaction_output_file(
                                Arc::clone(&db_state.table_cache),
                                &mut file_iterator,
                            )?;
                        }
                    }

                    file_iterator.next();
                }

                Ok(file_iterator)
            },
        );

        Ok(compaction_state)
    }
}
