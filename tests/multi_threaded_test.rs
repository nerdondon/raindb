use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc::TryRecvError;
use std::sync::{mpsc, Arc};
use std::thread::{self, JoinHandle};
use std::time::Duration;
use std::{fs, panic, str};

use raindb::fs::{FileSystem, TmpFileSystem};
use raindb::{DbOptions, RainDBError, ReadOptions, WriteOptions, DB};
use rand::distributions;
use rand::prelude::Distribution;
use regex::Regex;

const BASE_TESTING_DIR_NAME: &str = "testing_files/";
const PREFIX_KEY_RANGE: usize = 1000;
const NUM_TEST_THREADS: usize = 4;

/// The kinds of tasks that can be given to the test worker.
#[derive(Debug, Eq, PartialEq)]
enum TaskKind {
    /// Variant for shutting down the worker thread.
    Terminate,
}

/// A struct holding a thread that runs operations for the test.
struct Worker {
    /// A number uniquely identifying this thread.
    thread_id: usize,

    /// A handle to the actual thread.
    thread_handle: Option<JoinHandle<()>>,

    /// Sender end of the channel that the worker utilizes to schedule tasks.
    task_sender: mpsc::Sender<TaskKind>,
}

impl Worker {
    /// Create a new [`Worker`] instance.
    fn new(thread_id: usize, db: &Arc<DB>, worker_ops_counter: Vec<Arc<AtomicUsize>>) -> Self {
        // Create a channel for sending tasks
        let (task_sender, task_receiver) = mpsc::channel();

        log::info!("Starting test worker thread with id {thread_id}");
        let db = Arc::clone(db);
        let thread_handle = thread::Builder::new()
            .name(thread_id.to_string())
            .spawn(move || {
                let mut rng = rand::thread_rng();
                let uniform_dist= distributions::Uniform::from(0..PREFIX_KEY_RANGE);
                let bernoulli_dist =  distributions::Bernoulli::new(0.5).unwrap();
                let mut counter: usize = 0;
                let value_regex = Regex::new(r"(?P<key>\d+)\.(?P<thread_id>\d+).(?P<counter>\d+)").unwrap();

                loop {
                    worker_ops_counter[thread_id].store(counter, Ordering::Release);

                    match task_receiver.try_recv() {
                        Ok(task_kind) => {
                            if task_kind == TaskKind::Terminate {
                                log::info!(
                                    "Thread {thread_id} received the termination command. \
                                    Shutting down the thread",
                                );
                                break;
                            }
                        }
                        Err(err) => {
                            match err {
                                TryRecvError::Empty => {}
                                _ => {
                                    log::warn!(
                                        "Thread {thread_id} received an error when checking the \
                                        task channel for new tasks. Error: {err}"
                                    );
                                }
                            }
                        }
                    };

                    let key = uniform_dist.sample(&mut rng);
                    let formatted_key = format!("{key:016}");
                    if bernoulli_dist.sample(&mut rng) {
                        log::debug!("Thread {thread_id} putting key {key} into the database");
                        let value = Worker::create_test_value(&formatted_key, thread_id, counter);
                        assert!(db.put(
                            WriteOptions::default(),
                            formatted_key.into(),
                            value.into(),
                        )
                        .is_ok());
                        log::debug!("Thread {thread_id} put key {key} successfully");
                    } else {
                        // Read a value and verify that it contains the expected information
                        log::debug!("Thread {thread_id} reading from database with key {key}");
                        let read_result = db.get(ReadOptions::default(), formatted_key.as_bytes());
                        match read_result {
                            Err(read_err) => {
                                if read_err == RainDBError::KeyNotFound {
                                    // The key was not yet written to the database, keep the test going
                                } else {
                                    panic!("There was an error writing to the database in thread {thread_id}. Error: {read_err}")   ;
                                }
                            }
                            Ok(encoded_value) => {
                                let value = str::from_utf8(&encoded_value).unwrap();
                                let captures = value_regex.captures(value).unwrap();
                                let stored_key = captures["key"].parse::<usize>().unwrap();
                                let stored_thread_id = captures["thread_id"].parse::<usize>().unwrap();
                                let stored_counter = captures["counter"].parse::<usize>().unwrap();
                                log::info!(
                                    "Thread {thread_id} used {key} to get ({stored_key}, \
                                    {stored_thread_id}, {stored_counter})"
                                );

                                assert_eq!(
                                    stored_key,
                                    key,
                                    "Expected the key in the value ({stored_key}) to be the same \
                                    as the key ({key}) that we used to retrieve the value."
                                );
                                assert!(
                                    stored_thread_id < NUM_TEST_THREADS,
                                    "Expected a valid thread id to be stored in the value. Got \
                                    {stored_thread_id}"
                                );

                                // Check that the counter value stored in the database is less than
                                // the current counter value of the thread that stored that value
                                let expected_counter_value =
                                    worker_ops_counter[stored_thread_id].load(Ordering::Acquire);
                                assert!(
                                    stored_counter <= expected_counter_value,
                                    "The stored counter value ({stored_counter}) should be less \
                                    than or equal to the current counter value \
                                    ({expected_counter_value}) of the thread that stored it."
                                );
                            }
                        }
                    }

                    counter += 1;
                }

                log::info!("Terminated thread {thread_id}");
            })
            .unwrap();

        Self {
            thread_id,
            thread_handle: Some(thread_handle),
            task_sender,
        }
    }

    fn terminate_thread(&mut self) -> Option<JoinHandle<()>> {
        if let Some(thread_handle) = self.thread_handle.take() {
            if self.task_sender.send(TaskKind::Terminate).is_err() {
                log::debug!(
                    "Worker thread {} has already been terminated.",
                    self.thread_id
                );
            }

            return Some(thread_handle);
        }

        None
    }

    fn create_test_value(key: &str, thread_id: usize, counter: usize) -> String {
        // The counter is padded a bunch to encourage compactions to occur
        format!("{key}.{thread_id}.{counter:<1000}")
    }
}

/// Managed a pool of worker threads used for running testing operations.
struct ThreadManager {
    /// Pool of workers.
    workers: Vec<Worker>,

    /// Counters keeping track of the number of operations performed by each worker.
    worker_op_counters: Vec<Arc<AtomicUsize>>,
}

impl ThreadManager {
    /// Create a new instance of [`ThreadManager`].
    fn new(db: &Arc<DB>, num_workers: usize) -> Self {
        let mut worker_op_counters = Vec::with_capacity(num_workers);
        for _ in 0..num_workers {
            worker_op_counters.push(Arc::new(AtomicUsize::new(0)));
        }

        let mut workers = Vec::with_capacity(num_workers);
        for idx in 0..num_workers {
            workers.push(Worker::new(idx, db, worker_op_counters.clone()));
        }

        Self {
            workers,
            worker_op_counters,
        }
    }

    /// Stop the worker threads. Returns true if all workers shutdown successfully.
    fn stop_workers(&mut self) -> bool {
        log::info!("Terminating worker threads.");
        let mut all_workers_successful = true;
        for worker in &mut self.workers {
            if let Some(join_handle) = worker.terminate_thread() {
                if let Err(thread_panic_val) = join_handle.join() {
                    log::error!(
                        "Worker thread {} panicked while exiting. Unwinding the stack with the \
                        panicked value. Panic value: {:?}",
                        worker.thread_id,
                        thread_panic_val
                    );

                    all_workers_successful = false;
                }
            }
        }

        log::info!(
            "Worker threads terminated. Worker operation summary: [ {ops_summary} ]",
            ops_summary = self.worker_op_counters_to_string()
        );
        all_workers_successful
    }

    /// Get a string representation of the worker operation counters.
    fn worker_op_counters_to_string(&self) -> String {
        self.worker_op_counters
            .iter()
            .map(|counter| counter.load(Ordering::Acquire).to_string())
            .collect::<Vec<String>>()
            .join(", ")
    }
}

fn setup() {
    let _ = env_logger::builder()
        // Include all events in tests
        .filter_level(log::LevelFilter::max())
        // Ensure events are captured by `cargo test`
        .is_test(true)
        // Ignore errors initializing the logger if tests race to configure it
        .try_init();

    // Ensure that the base testing directory exists
    let base_path = Path::new(BASE_TESTING_DIR_NAME);
    if !base_path.exists() {
        fs::create_dir_all(&base_path).unwrap();
    };
}

#[test]
fn multiple_threads_can_write_to_and_read_from_the_database() {
    setup();

    const TEST_RUN_DURATION: Duration = Duration::from_millis(10 * 1000);

    let tmp_fs_root = PathBuf::from(BASE_TESTING_DIR_NAME);
    let tmp_fs = TmpFileSystem::new(Some(&tmp_fs_root));
    let db_path = tmp_fs.get_root_path().join("multi-threaded");
    let shared_tmp_fs: Arc<dyn FileSystem> = Arc::new(tmp_fs);

    let db = DB::open(DbOptions {
        filesystem_provider: Arc::clone(&shared_tmp_fs),
        create_if_missing: true,
        db_path: db_path.to_str().unwrap().to_owned(),
        ..DbOptions::default()
    })
    .unwrap();
    let wrapped_db = Arc::new(db);
    let mut thread_manager = ThreadManager::new(&wrapped_db, NUM_TEST_THREADS);

    // Let tests run for a period of time
    thread::sleep(TEST_RUN_DURATION);

    // Stop threads
    let were_workers_successful = thread_manager.stop_workers();
    assert!(
        were_workers_successful,
        "A worker panicked for some reason. Check the test logs for the failure reason."
    );
}
