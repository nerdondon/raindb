use crate::errors::RainDBError;
use crate::key::{InternalKey, RainDbKeyType};

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
        self.iterator.next()?;
        self.update_cached_values();

        self.current()
    }

    fn prev(&mut self) -> Option<(&Self::Key, &Vec<u8>)> {
        self.iterator.prev()?;
        self.update_cached_values();

        self.current()
    }

    fn current(&self) -> Option<(&Self::Key, &Vec<u8>)> {
        self.cached_entry.as_ref().map(|entry| (&entry.0, &entry.1))
    }
}
