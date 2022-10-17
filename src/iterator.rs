use std::sync::Arc;

use rand::distributions::{Distribution, Uniform};
use rand::rngs::StdRng;
use rand::SeedableRng;

use crate::compaction::{CompactionWorker, TaskKind};
use crate::config::ITERATION_READ_BYTES_PERIOD;
use crate::db::PortableDatabaseState;
use crate::errors::RainDBError;
use crate::key::{InternalKey, RainDbKeyType};
use crate::versioning::file_iterators::MergingIterator;
use crate::{Operation, DB};

/**
A RainDB specific iterator implementation that has more cursor-like behavior.

The RainDB iterator differs from the [`std::iter::DoubleEndedIterator`] in that the RainDB iterator
moves one cursor back and forth on the range of values. The `DoubleEndedIterator` essentially moves
two pointers toward each other and ends iteration onces the two pointers cross.
*/
pub trait RainDbIterator
where
    Self::Key: RainDbKeyType,
{
    /// The type of key that is iterated over.
    type Key;

    /// The type of error returned when there is an error during iteration.
    type Error;

    /// The iterator is only valid if the cursor is currently positioned at a key-value pair.
    fn is_valid(&self) -> bool;

    /**
    Position cursor to the first key that is at or past the target.

    Returns an error if there was an issue seeking the target and sets the iterator to invalid.
    */
    fn seek(&mut self, target: &Self::Key) -> Result<(), Self::Error>;

    /**
    Position cursor to the first element.

    Returns an error if there was an issue seeking the target and sets the iterator to invalid.
    */
    fn seek_to_first(&mut self) -> Result<(), Self::Error>;

    /**
    Position cursor to the last element.

    Returns an error if there was an issue seeking the target and sets the iterator to invalid.
    */
    fn seek_to_last(&mut self) -> Result<(), Self::Error>;

    /**
    Move to the next element.

    Returns a tuple (&Self::Key, &V) at the position moved to. If the cursor was on the last
    element, `None` is returned.
    */
    fn next(&mut self) -> Option<(&Self::Key, &Vec<u8>)>;

    /**
    Move to the previous element.

    Returns a tuple (&Self::Key, &V) at the position moved to. If the cursor was on the first
    element, `None` is returned.
    */
    fn prev(&mut self) -> Option<(&Self::Key, &Vec<u8>)>;

    /**
    Return the key and value at the current cursor position.

    Returns a tuple (&Self::Key, &V) at current position if the iterator is valid. Otherwise,
    returns `None`.
    */
    fn current(&self) -> Option<(&Self::Key, &Vec<u8>)>;
}

/**
An wrapper around an iterator that caches the `current()` and `is_valid()` results.

This maps to the LevelDB's `IteratorWrapper` and according to LevelDB is supposed to help avoid
costs from virtual function calls and also "gives better cache locality". Not sure how much the
latter actually saves.
*/
pub(crate) struct CachingIterator {
    /// The underlying iterator.
    iterator: Box<dyn RainDbIterator<Key = InternalKey, Error = RainDBError>>,

    /// The cached validity value.
    is_valid: bool,

    /// The cached key-value pair.
    cached_entry: Option<(InternalKey, Vec<u8>)>,
}

/// Crate-only methods
impl CachingIterator {
    /// Create a new instance of [`CachingIterator`].
    pub(crate) fn new(
        iterator: Box<dyn RainDbIterator<Key = InternalKey, Error = RainDBError>>,
    ) -> Self {
        let is_valid = iterator.is_valid();
        let current_entry = iterator
            .current()
            .map(|(key, value)| (key.clone(), value.clone()));

        Self {
            iterator,
            is_valid,
            cached_entry: current_entry,
        }
    }
}

/// Private methods
impl CachingIterator {
    /// Update the cached values.
    fn update_cached_values(&mut self) {
        self.is_valid = self.iterator.is_valid();
        if self.is_valid {
            self.cached_entry = self
                .iterator
                .current()
                .map(|(key, value)| (key.clone(), value.clone()));
        }
    }
}

impl RainDbIterator for CachingIterator {
    type Key = InternalKey;

    type Error = RainDBError;

    fn is_valid(&self) -> bool {
        self.is_valid
    }

    fn seek(&mut self, target: &Self::Key) -> Result<(), Self::Error> {
        self.iterator.seek(target)?;
        self.update_cached_values();

        Ok(())
    }

    fn seek_to_first(&mut self) -> Result<(), Self::Error> {
        self.iterator.seek_to_first()?;
        self.update_cached_values();

        Ok(())
    }

    fn seek_to_last(&mut self) -> Result<(), Self::Error> {
        self.iterator.seek_to_last()?;
        self.update_cached_values();

        Ok(())
    }

    fn next(&mut self) -> Option<(&Self::Key, &Vec<u8>)> {
        self.iterator.next();
        self.update_cached_values();

        self.current()
    }

    fn prev(&mut self) -> Option<(&Self::Key, &Vec<u8>)> {
        self.iterator.prev();
        self.update_cached_values();

        self.current()
    }

    fn current(&self) -> Option<(&Self::Key, &Vec<u8>)> {
        self.cached_entry.as_ref().map(|entry| (&entry.0, &entry.1))
    }
}

/// Enum for indicating the direction of iteration.
#[derive(Eq, PartialEq)]
enum DbIterationDirection {
    /// When moving forward, the internal iterator is positioned exactly at the current node.
    Forward,

    /**
    When moving backwards, the internal iterator is positioned before all entries whose user key is
    equal to the current node.
    */
    Backward,
}

/**
An iterator that yields client-facing state of the database.

The iterator will yield key-value pairs while accounting for recency via sequence numbers and for
deletion tombstones.
*/

pub struct DatabaseIterator {
    /// Database state that can be accessed from the iterator.
    db_state: PortableDatabaseState,

    /// The database compaction worker to process compaction requests.
    compaction_worker: Arc<CompactionWorker>,

    /// The direction that the iterator is moving.
    direction: DbIterationDirection,

    /// An internal iterator that yields the raw state of the database.
    inner_iter: MergingIterator,

    /// The most recent sequence number to consider when merging records for
    sequence_snapshot: u64,

    /// If the iterator is in a valid state.
    is_valid: bool,

    /**
    Random number generator for determining when to update database read statistics and,
    potentially, schedule a compaction.
    */
    rng: StdRng,

    /// The random distribution for sampling compaction periods from.
    distribution: Uniform<u64>,

    /// The number of bytes to read before performing read statistic sampling.
    bytes_until_read_sampling: usize,

    /// A cached user key. This value does not necessarily correlate to the `cached_value` field.
    cached_user_key: Option<Vec<u8>>,

    /// A cached value. This value does not necessarily correlate to the `cached_key` field.
    cached_value: Option<Vec<u8>>,
}

/// Crate-only methods
impl DatabaseIterator {
    /// Create a new instance of [`DatabaseIterator`].
    pub(crate) fn new(
        db_state: PortableDatabaseState,
        inner_iter: MergingIterator,
        sequence_snapshot: u64,
        sampling_seed: u64,
        compaction_worker: Arc<CompactionWorker>,
    ) -> Self {
        Self {
            db_state,
            direction: DbIterationDirection::Forward,
            inner_iter,
            sequence_snapshot,
            is_valid: false,
            rng: StdRng::seed_from_u64(sampling_seed),
            distribution: Uniform::from(0..(2 * ITERATION_READ_BYTES_PERIOD)),
            bytes_until_read_sampling: 0,
            cached_user_key: None,
            cached_value: None,
            compaction_worker,
        }
    }
}

/// Private methods
impl DatabaseIterator {
    /**
    Get samples of read statistics for the current key.

    This is to check if a seek compaction would be required during iteration.

    # Legacy

    This is synonomous to LevelDB's `DBIter::ParseKey`.
    */
    fn sample_read_stats_for_current_key(&mut self) {
        if !self.inner_iter.is_valid() {
            return;
        }

        let bytes_read = {
            let (current_key, current_val) = self.inner_iter.current().unwrap();
            current_key.get_approximate_size() + current_val.len()
        };
        while self.bytes_until_read_sampling < bytes_read {
            self.bytes_until_read_sampling += self.random_compaction_period();
            let mut db_fields_guard = self.db_state.guarded_db_fields.lock();
            let current_version = &mut db_fields_guard.version_set.get_current_version();
            let (current_key, _current_val) = self.inner_iter.current().unwrap();
            let needs_seek_compaction = current_version
                .write()
                .element
                .record_read_sample(current_key);
            if needs_seek_compaction
                && DB::should_schedule_compaction(&self.db_state, &mut db_fields_guard)
            {
                self.compaction_worker.schedule_task(TaskKind::Compaction);
            }
        }

        self.bytes_until_read_sampling -= bytes_read;
    }

    /// Picks a random number of bytes that can be read before a compaction is scheduled.
    fn random_compaction_period(&mut self) -> usize {
        self.distribution.sample(&mut self.rng) as usize
    }

    /**
    Move the internal iterator forward until there is a valid user value to yield e.g. keys that
    have not been marked with a tombstone.

    Set `initial_is_skipping` to `true` if the inner iterator is positioned such that it is in a
    run of obsolete records for a user key. Set to `false` if we know we are at the first record
    for a given user key.

    # Panics

    This method will panic if the inner iterator is not in a valid position and if the iterator
    is not iterating forward.
    */
    fn find_next_client_entry(&mut self, initial_is_skipping: bool) {
        assert!(self.inner_iter.is_valid());
        assert!(self.direction == DbIterationDirection::Forward);

        let mut is_skipping: bool = initial_is_skipping;
        loop {
            self.sample_read_stats_for_current_key();
            let (current_key, _) = self.inner_iter.current().unwrap();
            let curr_sequence_num = current_key.get_sequence_number();
            let curr_operation = current_key.get_operation();
            if curr_sequence_num > self.sequence_snapshot {
                // This record is more recent than our snapshot allows, don't process it
            } else {
                match curr_operation {
                    Operation::Delete => {
                        // Skip all upcoming entries for this user key because they are hidden by this
                        // deletion
                        is_skipping = true;

                        let current_user_key =
                            self.inner_iter.current().unwrap().0.get_user_key().to_vec();
                        self.cached_user_key = Some(current_user_key);
                    }
                    Operation::Put => {
                        if is_skipping
                            && self.cached_user_key.is_some()
                            && current_key.get_user_key() <= self.cached_user_key.as_ref().unwrap()
                        {
                            // This entry is hidden by more recent updates to the same user key.
                            // Shadowed values (i.e. older records) may be hit first before a different
                            // user key is found because internal keys are sorted in decreasing order
                            // for sequence numbers.
                        } else {
                            // Found the most up-to-date value for a key
                            self.is_valid = true;
                            self.cached_user_key = None;

                            return;
                        }
                    }
                }
            }

            self.inner_iter.next();
            if !self.inner_iter.is_valid() {
                break;
            }
        }

        self.cached_user_key = None;
        self.is_valid = false;
    }

    /**
    Move the internal iterator backward until there is a valid user value to yield e.g. keys that
    have not been marked with a tombstone.

    # Panics

    This method will panic if the inner iterator is moving in the backward direction.
    */
    fn find_prev_client_entry(&mut self) {
        assert!(self.direction == DbIterationDirection::Backward);

        let mut last_operation_type = Operation::Delete;
        if self.inner_iter.is_valid() {
            loop {
                self.sample_read_stats_for_current_key();
                let (current_key, _) = self.inner_iter.current().unwrap();
                let curr_sequence_num = current_key.get_sequence_number();
                let curr_operation = current_key.get_operation();
                if curr_sequence_num > self.sequence_snapshot {
                    // This record is more recent than our snapshot allows, don't process it
                } else {
                    if last_operation_type != Operation::Delete
                        && current_key.get_user_key() < self.cached_user_key.as_ref().unwrap()
                    {
                        // Encountered a non-deleted value in records for the previous user key
                        break;
                    }

                    last_operation_type = curr_operation;
                    match curr_operation {
                        Operation::Delete => {
                            // Encountered a more recent deletion for the user, so clear the cached
                            // values
                            self.cached_user_key = None;
                            self.cached_value = None;
                        }
                        Operation::Put => {
                            let (current_key, current_val) = self.inner_iter.current().unwrap();
                            self.cached_user_key = Some(current_key.get_user_key().to_vec());
                            self.cached_value = Some(current_val.clone());
                        }
                    }
                }

                self.inner_iter.prev();
                if !self.inner_iter.is_valid() {
                    break;
                }
            }
        }

        if last_operation_type == Operation::Delete {
            self.is_valid = false;
            self.cached_user_key = None;
            self.cached_value = None;
            self.direction = DbIterationDirection::Forward;
        } else {
            self.is_valid = true;
        }
    }
}

impl RainDbIterator for DatabaseIterator {
    type Key = Vec<u8>;

    type Error = RainDBError;

    fn is_valid(&self) -> bool {
        self.is_valid
    }

    fn seek(&mut self, target: &Self::Key) -> Result<(), Self::Error> {
        self.direction = DbIterationDirection::Forward;
        self.cached_value = None;
        self.cached_user_key = Some(target.clone());

        let lookup_key = InternalKey::new_for_seeking(target.clone(), self.sequence_snapshot);
        self.inner_iter.seek(&lookup_key)?;

        if self.inner_iter.is_valid() {
            self.find_next_client_entry(false);

            return Ok(());
        }

        self.is_valid = false;

        Ok(())
    }

    fn seek_to_first(&mut self) -> Result<(), Self::Error> {
        self.direction = DbIterationDirection::Forward;
        self.cached_value = None;
        self.inner_iter.seek_to_first()?;

        if self.inner_iter.is_valid() {
            self.find_next_client_entry(false);

            return Ok(());
        }

        self.is_valid = false;

        Ok(())
    }

    fn seek_to_last(&mut self) -> Result<(), Self::Error> {
        self.direction = DbIterationDirection::Backward;
        self.cached_value = None;
        self.inner_iter.seek_to_last()?;
        self.find_prev_client_entry();

        Ok(())
    }

    fn next(&mut self) -> Option<(&Self::Key, &Vec<u8>)> {
        assert!(self.is_valid);

        if self.direction == DbIterationDirection::Backward {
            self.direction = DbIterationDirection::Forward;

            /*
            The inner iterator is pointing just before the entries for the cached key, so we must
            first seek into the run of records for the cached user key before we can use the
            `find_next_client_entry` to find valid records.
            */
            if !self.inner_iter.is_valid() {
                let _seek_result = self.inner_iter.seek_to_first();
            } else {
                self.inner_iter.next();
            }

            if !self.inner_iter.is_valid() {
                self.is_valid = false;
                self.cached_user_key = None;
                return None;
            }
        } else {
            // Save the current key of the inner iterator so that we skip it and it's older records
            self.cached_user_key =
                Some(self.inner_iter.current().unwrap().0.get_user_key().to_vec());

            self.inner_iter.next();
            if !self.inner_iter.is_valid() {
                self.is_valid = false;
                self.cached_user_key = None;
                return None;
            }
        }

        self.find_next_client_entry(true);

        if !self.is_valid() {
            return None;
        }

        self.current()
    }

    fn prev(&mut self) -> Option<(&Self::Key, &Vec<u8>)> {
        assert!(self.is_valid);

        if self.direction == DbIterationDirection::Forward {
            // The inner iterator is point at the current entry. Scan backwards until the user key
            // changes and use `DatabaseIterator::find_prev_client_entry` to find the most recent
            // record for the user key
            self.cached_user_key =
                Some(self.inner_iter.current().unwrap().0.get_user_key().to_vec());
            loop {
                if let Some((current_key, _)) = self.inner_iter.prev() {
                    if current_key.get_user_key() < self.cached_user_key.as_ref().unwrap() {
                        break;
                    }
                } else {
                    self.is_valid = false;
                    self.cached_user_key = None;
                    self.cached_value = None;
                    return None;
                }
            }

            self.direction = DbIterationDirection::Backward;
        }

        self.find_prev_client_entry();

        if !self.is_valid() {
            return None;
        }

        self.current()
    }

    fn current(&self) -> Option<(&Self::Key, &Vec<u8>)> {
        assert!(self.is_valid);

        match self.direction {
            DbIterationDirection::Forward => {
                let (curr_key, curr_val) = self.inner_iter.current().unwrap();
                return Some((curr_key.get_user_key_as_vec(), curr_val));
            }
            DbIterationDirection::Backward => {
                return Some((
                    self.cached_user_key.as_ref().unwrap(),
                    self.cached_value.as_ref().unwrap(),
                ));
            }
        }
    }
}
