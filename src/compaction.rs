use parking_lot::Mutex;
use std::sync::atomic::AtomicBool;
use std::sync::{mpsc, Arc};
use std::thread::{self, JoinHandle};
use std::{fmt, io};

use crate::db::GuardedDbFields;
use crate::key::LookupKey;
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
    db_guarded_fields: Arc<Mutex<GuardedDbFields>>,

    /// Field indicating if the database is shutting down.
    is_shutting_down: Arc<AtomicBool>,

    /**
    Field indicating if there is an immutable memtable.

    An memtable is made immutable when it is undergoing the compaction process.
    */
    has_immutable_memtable: Arc<AtomicBool>,
}

/// The kinds of tasks that the compaction worker can schedule.
#[derive(Debug)]
pub enum TaskKind {
    Compaction,
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
    * `db_guarded_fields` - Database state that require a lock to be held before being read or written to.
    * `is_shutting_down` - Field indicating if the database is shutting down.
    * `has_immutable_memtable` - Field indicating if there is an immutable memtable.

    Most of the parameters are put into the [`DatabaseState`] struct and passed to the actual
    compaction thread for convenience. Expanded documentation of these params can be found in said
    struct.
    */
    pub fn new(
        options: DbOptions,
        db_guarded_fields: Arc<Mutex<GuardedDbFields>>,
        is_shutting_down: Arc<AtomicBool>,
        has_immutable_memtable: Arc<AtomicBool>,
    ) -> CompactionWorkerResult<Self> {
        let db_state = DatabaseState {
            options,
            db_guarded_fields,
            is_shutting_down,
            has_immutable_memtable,
        };

        // Create a channel for sending tasks
        let (task_sender, receiver) = mpsc::channel();

        log::info!("Starting up the background compaction thread.");
        let background_thread_handle = thread::Builder::new()
            .name(COMPACTION_THREAD_NAME.to_string())
            .spawn(move || {
                log::info!("Compaction thread initializing.");
                loop {
                    log::info!("Compaction thread waiting for tasks.");
                    let task = receiver.recv().unwrap();

                    match task {
                        TaskKind::Compaction => {}
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
    pub fn schedule_task(&self, task_kind: TaskKind) {}
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
