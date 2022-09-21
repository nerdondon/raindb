/*!
This modules contains iterators and utilities for creating iterators that work over a set of table
file metadata (i.e. [`FileMetadata`]).
*/

use std::sync::Arc;

use crate::errors::{RainDBError, RainDBResult};
use crate::iterator::CachingIterator;
use crate::key::InternalKey;
use crate::table_cache::TableCache;
use crate::tables::table::TwoLevelIterator;
use crate::tables::Table;
use crate::{RainDbIterator, ReadOptions};

use super::file_metadata::FileMetadata;

/**
Iterates over the entries in an ordered list of files.

This iterator is meant to serve as an index iterator for two level iterators where a second level
of iterators receives the information yielded by this iterator to retrieve more concrete values.

# Legacy

This iterator combines LevelDB's `Version::LevelFileNumIterator`, `leveldb::GetFileIterator`, and
`Version::NewConcatenatingIterator`. This is because I don't think it is worth the trouble of
making a generic "two-level iterator" like LevelDB's `leveldb::TwoLevelIterator`. Just thinking
about the potential lifetime and closure issues is mind-numbing.
*/
pub(crate) struct FilesEntryIterator {
    /// The ordered list of files to iterate.
    file_list: Vec<Arc<FileMetadata>>,

    /**
    The current index into the file list that the cursor is at.

    This value needs to stay in sync with the current table iterator. To ensure this, we only set
    this value in `FilesEntryIterator::set_table_iter`.
    */
    current_file_index: usize,

    /// The current table being iterated for entries.
    current_table_iter: Option<TwoLevelIterator>,

    /// The table cache to retrieve table files from.
    table_cache: Arc<TableCache>,

    /// Options to configure behavior when reading from table files.
    read_options: ReadOptions,
}

/// Crate-only methods
impl FilesEntryIterator {
    /// Create a new instance of [`FilesEntryIterator`].
    pub(crate) fn new(
        file_list: Vec<Arc<FileMetadata>>,
        table_cache: Arc<TableCache>,
        read_options: ReadOptions,
    ) -> Self {
        Self {
            file_list,
            current_file_index: 0,
            current_table_iter: None,
            table_cache,
            read_options,
        }
    }
}

/// Private methods
impl FilesEntryIterator {
    /// Set the table iterator to be used for iteration.
    fn set_table_iter(&mut self, maybe_new_index: Option<usize>) -> RainDBResult<()> {
        if maybe_new_index.is_none() || maybe_new_index.unwrap() == self.file_list.len() {
            self.current_file_index = self.file_list.len();
            self.current_table_iter = None;
            return Ok(());
        }

        if let Some(new_index) = maybe_new_index {
            if new_index == self.current_file_index && self.current_table_iter.is_some() {
                // The file we need to iterate is the same as the current one. No update necessary
                return Ok(());
            }

            let table = self
                .table_cache
                .find_table(self.file_list[new_index].file_number())?;
            self.current_table_iter = Some(Table::iter_with(table, self.read_options.clone()));
            self.current_file_index = new_index;
        }
        Ok(())
    }

    /// Move forward through any empty files.
    fn skip_empty_table_files_forward(&mut self) -> RainDBResult<()> {
        while self.current_table_iter.is_none()
            || !self.current_table_iter.as_mut().unwrap().is_valid()
        {
            if self.current_file_index == self.file_list.len() {
                // We've reached the end of the file list so there are no more tables to iterate
                self.current_table_iter = None;
                return Ok(());
            }

            // Move index iterator to check for the next data block handle
            self.set_table_iter(Some(self.current_file_index + 1))?;

            if self.current_table_iter.is_some() {
                self.current_table_iter.as_mut().unwrap().seek_to_first()?;
            }
        }

        Ok(())
    }

    /// Move the file index and table iterator backward until we find a non-empty file.
    fn skip_empty_table_files_backward(&mut self) -> RainDBResult<()> {
        while self.current_table_iter.is_none()
            || !self.current_table_iter.as_mut().unwrap().is_valid()
        {
            if self.current_file_index == 0 {
                // We've reached the start of the file list so there are no more tables to iterate
                self.current_table_iter = None;
                return Ok(());
            }

            // Move index iterator to check for the next data block handle
            self.set_table_iter(Some(self.current_file_index - 1))?;

            if self.current_table_iter.is_some() {
                self.current_table_iter.as_mut().unwrap().seek_to_last()?;
            }
        }

        Ok(())
    }
}

impl RainDbIterator for FilesEntryIterator {
    type Key = InternalKey;

    type Error = RainDBError;

    fn is_valid(&self) -> bool {
        self.current_table_iter.is_some() && self.current_table_iter.as_ref().unwrap().is_valid()
    }

    fn seek(&mut self, target: &Self::Key) -> Result<(), Self::Error> {
        let maybe_new_index =
            super::utils::find_file_with_upper_bound_range(&self.file_list, target);
        self.set_table_iter(maybe_new_index)?;

        if self.current_table_iter.is_some() {
            self.current_table_iter.as_mut().unwrap().seek(target)?;
        }

        self.skip_empty_table_files_forward()?;

        Ok(())
    }

    fn seek_to_first(&mut self) -> Result<(), Self::Error> {
        let new_file_index = 0;
        self.set_table_iter(Some(new_file_index))?;

        if self.current_table_iter.is_some() {
            self.current_table_iter.as_mut().unwrap().seek_to_first()?;
        }

        self.skip_empty_table_files_forward()?;

        Ok(())
    }

    fn seek_to_last(&mut self) -> Result<(), Self::Error> {
        let new_file_index = if self.file_list.is_empty() {
            0
        } else {
            self.file_list.len() - 1
        };
        self.set_table_iter(Some(new_file_index))?;

        if self.current_table_iter.is_some() {
            self.current_table_iter.as_mut().unwrap().seek_to_last()?;
        }

        self.skip_empty_table_files_backward()?;

        Ok(())
    }

    fn next(&mut self) -> Option<(&Self::Key, &Vec<u8>)> {
        if !self.is_valid() {
            return None;
        }

        if self.current_table_iter.as_mut().unwrap().next().is_none() {
            if let Err(error) = self.skip_empty_table_files_forward() {
                log::error!(
                    "There was an error skipping forward. Original error: {}",
                    error
                );
                return None;
            }
        }

        if self.is_valid() {
            return self.current_table_iter.as_mut().unwrap().current();
        }

        None
    }

    fn prev(&mut self) -> Option<(&Self::Key, &Vec<u8>)> {
        if !self.is_valid() {
            return None;
        }

        if self.current_table_iter.as_mut().unwrap().prev().is_none() {
            if let Err(error) = self.skip_empty_table_files_backward() {
                log::error!(
                    "There was an error skipping backward. Original error: {}",
                    error
                );
                return None;
            }
        }

        if self.is_valid() {
            return self.current_table_iter.as_mut().unwrap().current();
        }

        None
    }

    fn current(&self) -> Option<(&Self::Key, &Vec<u8>)> {
        if !self.is_valid() {
            return None;
        }

        self.current_table_iter.as_ref().unwrap().current()
    }
}

/// Enum for indicating the direction of iteration.
enum IterationDirection {
    Forward,
    Backward,
}

/**
An iterator that merges the output of a list of iterators in sorted order.

This iterator does not do any sort of de-duplication.

# Legacy

As in LevelDB, a heap is not used to merge the inputs because the number of iterators that would be
merged by the heap/priority queue is small. The only level where multiple iterators is created is
level 0 which has a maximum of 12 files. This means that at most 12 + 1 iterator from the parent
level will need to merged.
*/
pub(crate) struct MergingIterator {
    /// The underlying iterators.
    iterators: Vec<CachingIterator>,

    /// The current direction of iteration.
    direction: IterationDirection,

    /// The index of the iterator we are currently reading.
    current_iterator_index: Option<usize>,

    /**
    Store errors encountered during iteration. The index of the error maps to the index of the
    iterator that encountered the error. Only one error is stored per iterator.

    LevelDB doesn't throw errors when errors are encountered during iteration. A client needs to
    explicitly check for an error status by calling `Iterator::status()`. This vector emulates
    this behavior.
    */
    errors: Vec<Option<RainDBError>>,

    /// Functions called to perform cleanup tasks when the iterator is dropped.
    cleanup_callbacks: Vec<Box<dyn FnOnce()>>,
}

/// Crate-only methods
impl MergingIterator {
    /// Creata a new instance of [`MergingIterator`].
    pub(crate) fn new(
        iterators: Vec<Box<dyn RainDbIterator<Key = InternalKey, Error = RainDBError>>>,
    ) -> Self {
        let wrapped_iterators: Vec<CachingIterator> = iterators
            .into_iter()
            .map(|iter| CachingIterator::new(iter))
            .collect();
        let errors = vec![None; wrapped_iterators.len()];

        Self {
            iterators: wrapped_iterators,
            direction: IterationDirection::Forward,
            current_iterator_index: None,
            errors,
            cleanup_callbacks: vec![],
        }
    }

    /**
    Get the first error if any. This will take ownership of the error, leaving a `None` in its
    place.
    */
    pub(crate) fn get_error(&mut self) -> Option<RainDBError> {
        for maybe_error in self.errors.iter_mut() {
            if maybe_error.is_some() {
                return maybe_error.take();
            }
        }

        None
    }

    /// Register a closure that is called when the iterator is dropped.
    pub(crate) fn register_cleanup_method(&mut self, cleanup: Box<dyn FnOnce()>) {
        self.cleanup_callbacks.push(cleanup);
    }
}

/// Private methods
impl MergingIterator {
    /// Find the iterator with the currently smallest key and update the merging iterator state.
    fn find_smallest(&mut self) {
        if self.iterators.is_empty() {
            return;
        }

        let mut maybe_smallest_iterator_index: Option<usize> = None;
        for (index, iter) in self.iterators.iter().enumerate() {
            if !iter.is_valid() {
                continue;
            }

            if let Some((key, _)) = iter.current() {
                if maybe_smallest_iterator_index.is_none() {
                    maybe_smallest_iterator_index = Some(index);
                } else if let Some(smallest_iterator_index) = maybe_smallest_iterator_index {
                    let current_smallest_key =
                        self.iterators[smallest_iterator_index].current().unwrap().0;

                    if key < current_smallest_key {
                        maybe_smallest_iterator_index = Some(index);
                    }
                }
            }
        }

        self.current_iterator_index = maybe_smallest_iterator_index;
    }

    /// Find the iterator with the currently largest key and update the merging iterator state.
    fn find_largest(&mut self) {
        if self.iterators.is_empty() {
            return;
        }

        let mut maybe_largest_iterator_index: Option<usize> = None;
        for (index, iter) in self.iterators.iter().rev().enumerate() {
            if !iter.is_valid() {
                continue;
            }

            if let Some((key, _)) = iter.current() {
                if maybe_largest_iterator_index.is_none() {
                    maybe_largest_iterator_index = Some(index);
                } else if let Some(largest_iterator_index) = maybe_largest_iterator_index {
                    let current_largest_key =
                        self.iterators[largest_iterator_index].current().unwrap().0;

                    if key > current_largest_key {
                        maybe_largest_iterator_index = Some(index);
                    }
                }
            }
        }

        self.current_iterator_index = maybe_largest_iterator_index;
    }

    /// Store the error at the specified index.
    fn save_error(&mut self, iterator_index: usize, error: RainDBError) {
        log::error!(
            "An error occurred during a merge iteration. Error: {}",
            &error
        );
        self.errors[iterator_index] = Some(error);
    }

    /// Move the current iterator to the next entry.
    fn advance_current_iterator(&mut self) {
        if let Some(current_iter_index) = self.current_iterator_index {
            let current_iter = &mut self.iterators[current_iter_index];
            current_iter.next();
        }
    }

    /// Move the current iterator to the prev entry.
    fn reverse_current_iterator(&mut self) {
        if let Some(current_iter_index) = self.current_iterator_index {
            let current_iter = &mut self.iterators[current_iter_index];
            current_iter.prev();
        }
    }
}

impl RainDbIterator for MergingIterator {
    type Key = InternalKey;

    type Error = RainDBError;

    fn is_valid(&self) -> bool {
        self.current_iterator_index.is_some()
    }

    fn seek(&mut self, target: &Self::Key) -> Result<(), Self::Error> {
        for index in 0..self.iterators.len() {
            let iter = &mut self.iterators[index];
            let seek_result = iter.seek(target);
            if let Err(error) = seek_result {
                self.save_error(index, error);
            }
        }

        self.find_smallest();
        self.direction = IterationDirection::Forward;

        Ok(())
    }

    fn seek_to_first(&mut self) -> Result<(), Self::Error> {
        for index in 0..self.iterators.len() {
            let iter = &mut self.iterators[index];
            let seek_result = iter.seek_to_first();
            if let Err(error) = seek_result {
                self.save_error(index, error);
            }
        }

        self.find_smallest();
        self.direction = IterationDirection::Forward;

        Ok(())
    }

    fn seek_to_last(&mut self) -> Result<(), Self::Error> {
        for index in 0..self.iterators.len() {
            let iter = &mut self.iterators[index];
            let seek_result = iter.seek_to_last();
            if let Err(error) = seek_result {
                self.save_error(index, error);
            }
        }

        self.find_largest();
        self.direction = IterationDirection::Backward;

        Ok(())
    }

    fn next(&mut self) -> Option<(&Self::Key, &Vec<u8>)> {
        /*
        Ensure that all child iterators are positioned after the current key.
        If we are already moving in the forward direction, this is implicitly true for all
        non-current iterators since the current iterator is the smallest iterator. Otherwise, we
        explicitly position the non-current iterators.
        */
        if let IterationDirection::Backward = self.direction {
            let current_key = self.current().unwrap().0.clone();
            for index in 0..self.iterators.len() {
                /*
                We are forced to do this obtuse error saving because Rust can't handle multiple
                mutable borrows to fields we use disjointly. It is also because of this that we
                use indexes instead of `.iter_mut` to iterator the child iterators.
                */
                let mut maybe_error: Option<RainDBError> = None;
                if let Some(current_index) = self.current_iterator_index {
                    if index == current_index {
                        continue;
                    }
                }

                let iter = &mut self.iterators[index];
                let seek_result = iter.seek(&current_key);
                if let Err(error) = seek_result {
                    maybe_error = Some(error);
                }

                if iter.is_valid() && (*iter.current().unwrap().0) == current_key {
                    iter.next();
                }

                if let Some(error) = maybe_error {
                    self.save_error(index, error);
                }
            }

            self.direction = IterationDirection::Forward;
        }

        self.advance_current_iterator();
        self.find_smallest();

        self.current()
    }

    fn prev(&mut self) -> Option<(&Self::Key, &Vec<u8>)> {
        /*
        Ensure that all child iterators are positioned before the current key.
        If we are already moving in the backward direction, this is implicitly true for all
        non-current iterators since the current iterator is the largest iterator. Otherwise, we
        explicitly position the non-current iterators.
        */
        if let IterationDirection::Forward = self.direction {
            for index in 0..self.iterators.len() {
                let current_key = self.current().unwrap().0.clone();
                let mut maybe_error: Option<RainDBError> = None;
                if let Some(current_index) = self.current_iterator_index {
                    if index == current_index {
                        continue;
                    }
                }

                let iter = &mut self.iterators[index];
                let seek_result = iter.seek(&current_key);
                if let Err(error) = seek_result {
                    maybe_error = Some(error);
                }

                if iter.is_valid() {
                    // The child iterator's first entry is >= the current key. Step back one to be
                    // less than the current key
                    iter.prev();
                } else {
                    // The child iterator has no entries with keys >= the current key. Position at
                    // the last entry.
                    let seek_result = iter.seek(&current_key);
                    if let Err(error) = seek_result {
                        maybe_error = Some(error);
                    }
                }

                if let Some(error) = maybe_error {
                    self.save_error(index, error);
                }
            }

            self.direction = IterationDirection::Backward;
        }

        self.reverse_current_iterator();
        self.find_largest();

        self.current()
    }

    fn current(&self) -> Option<(&Self::Key, &Vec<u8>)> {
        if let Some(current_iter_index) = self.current_iterator_index {
            let current_iter = &self.iterators[current_iter_index];
            return current_iter.current();
        }

        None
    }
}

impl Drop for MergingIterator {
    fn drop(&mut self) {
        // Call cleanup closures
        for callback in self.cleanup_callbacks.drain(..) {
            callback();
        }
    }
}

#[cfg(test)]
mod files_entry_iterator_tests {
    use pretty_assertions::assert_eq;
    use std::rc::Rc;

    use crate::tables::TableBuilder;
    use crate::versioning::version::Version;
    use crate::{DbOptions, Operation};

    use super::*;

    #[test]
    fn files_entry_iterator_with_an_empty_file_list_does_not_become_valid() {
        let options = DbOptions::with_memory_env();
        let table_cache = Arc::new(TableCache::new(options.clone(), 1000));
        let version = create_test_version(options, &table_cache);
        let mut iter = FilesEntryIterator::new(
            version.files[4].clone(),
            Arc::clone(&table_cache),
            ReadOptions::default(),
        );

        assert!(!iter.is_valid());
        assert!(iter.next().is_none());
        assert!(iter.prev().is_none());
        assert!(iter
            .seek(&InternalKey::new(
                "a".as_bytes().to_vec(),
                100,
                Operation::Put
            ))
            .is_ok());
        assert!(iter.current().is_none());
        assert!(iter.seek_to_first().is_ok());
        assert!(iter.current().is_none());
        assert!(iter.seek_to_last().is_ok());
        assert!(iter.current().is_none());
    }

    #[test]
    fn files_entry_iterator_can_seek_to_specific_targets() {
        let options = DbOptions::with_memory_env();
        let table_cache = Arc::new(TableCache::new(options.clone(), 1000));
        let version = create_test_version(options, &table_cache);
        let mut iter = FilesEntryIterator::new(
            version.files[1].clone(),
            Arc::clone(&table_cache),
            ReadOptions::default(),
        );

        assert!(
            !iter.is_valid(),
            "The iterator should not be valid until its first seek"
        );
        assert!(iter
            .seek(&InternalKey::new(
                "h".as_bytes().to_vec(),
                100,
                Operation::Put
            ))
            .is_ok());
        let (actual_key, actual_val) = iter.current().unwrap();
        assert_eq!(
            actual_key,
            &InternalKey::new("h".as_bytes().to_vec(), 86, Operation::Put)
        );
        assert_eq!(actual_val, "h".as_bytes());

        assert!(iter
            .seek(&InternalKey::new(
                "w".as_bytes().to_vec(),
                20,
                Operation::Put
            ))
            .is_ok());
        let (actual_key, actual_val) = iter.current().unwrap();
        assert_eq!(
            actual_key,
            &InternalKey::new("x".as_bytes().to_vec(), 78, Operation::Delete)
        );
        assert_eq!(actual_val, &[]);

        let (actual_key, actual_val) = iter.next().unwrap();
        assert_eq!(
            actual_key,
            &InternalKey::new("y".as_bytes().to_vec(), 79, Operation::Put)
        );
        assert_eq!(actual_val, "y".as_bytes());

        assert!(iter.seek_to_first().is_ok());
        let (actual_key, actual_val) = iter.current().unwrap();
        assert_eq!(
            actual_key,
            &InternalKey::new("g".as_bytes().to_vec(), 85, Operation::Put)
        );
        assert_eq!(actual_val, "g".as_bytes());

        assert!(iter.seek_to_last().is_ok());
        let (actual_key, actual_val) = iter.current().unwrap();
        assert_eq!(
            actual_key,
            &InternalKey::new("y".as_bytes().to_vec(), 79, Operation::Put)
        );
        assert_eq!(actual_val, "y".as_bytes());
    }

    #[test]
    fn files_entry_iterator_can_be_iterated_forward_completely() {
        let options = DbOptions::with_memory_env();
        let table_cache = Arc::new(TableCache::new(options.clone(), 1000));
        let version = create_test_version(options, &table_cache);
        let mut iter = FilesEntryIterator::new(
            version.files[1].clone(),
            Arc::clone(&table_cache),
            ReadOptions::default(),
        );

        assert!(iter.seek_to_first().is_ok());
        let (actual_key, _) = iter.current().unwrap();
        assert_eq!(
            actual_key,
            &InternalKey::new("g".as_bytes().to_vec(), 85, Operation::Put)
        );

        while iter.next().is_some() {
            assert!(
                iter.current().is_some(),
                "Iteration did not yield a value but one was expected."
            );
        }

        assert!(
            iter.next().is_none(),
            "Calling `next` after consuming all the values should not return a value"
        );
        assert!(
            !iter.is_valid(),
            "The block iterator should not be valid after moving past the end of the iterator"
        );
    }

    #[test]
    fn files_entry_iterator_can_be_iterated_backward_completely() {
        let options = DbOptions::with_memory_env();
        let table_cache = Arc::new(TableCache::new(options.clone(), 1000));
        let version = create_test_version(options, &table_cache);
        let mut iter = FilesEntryIterator::new(
            version.files[1].clone(),
            Arc::clone(&table_cache),
            ReadOptions::default(),
        );

        assert!(iter.seek_to_last().is_ok());
        let (actual_key, _) = iter.current().unwrap();
        assert_eq!(
            actual_key,
            &InternalKey::new("y".as_bytes().to_vec(), 79, Operation::Put)
        );

        while iter.prev().is_some() {
            assert!(
                iter.current().is_some(),
                "Iteration did not yield a value but one was expected."
            );
        }

        assert!(
            iter.prev().is_none(),
            "Calling `prev` after consuming all the values should not return a value"
        );
        assert!(
            !iter.is_valid(),
            "The block iterator should not be valid after moving past the end of the iterator"
        );
    }

    /// Creates version used to hold files for testing.
    fn create_test_version(db_options: DbOptions, table_cache: &Arc<TableCache>) -> Version {
        let mut version = Version::new(db_options.clone(), table_cache, 200, 30);

        // Level 1
        let entries = vec![
            (
                ("g".as_bytes().to_vec(), Operation::Put),
                ("g".as_bytes().to_vec()),
            ),
            (
                ("h".as_bytes().to_vec(), Operation::Put),
                ("h".as_bytes().to_vec()),
            ),
            (
                ("i".as_bytes().to_vec(), Operation::Put),
                ("i".as_bytes().to_vec()),
            ),
            (
                ("j".as_bytes().to_vec(), Operation::Put),
                ("j".as_bytes().to_vec()),
            ),
        ];
        let table_file_meta = create_table(db_options.clone(), entries, 85, 59);
        version.files[1].push(Arc::new(table_file_meta));

        let entries = vec![
            (
                ("o".as_bytes().to_vec(), Operation::Put),
                ("o".as_bytes().to_vec()),
            ),
            (
                ("r".as_bytes().to_vec(), Operation::Put),
                ("r".as_bytes().to_vec()),
            ),
            (
                ("s".as_bytes().to_vec(), Operation::Put),
                ("s".as_bytes().to_vec()),
            ),
            (
                ("t".as_bytes().to_vec(), Operation::Put),
                ("t".as_bytes().to_vec()),
            ),
            (
                ("u".as_bytes().to_vec(), Operation::Put),
                ("u".as_bytes().to_vec()),
            ),
        ];
        let table_file_meta = create_table(db_options.clone(), entries, 80, 58);
        version.files[1].push(Arc::new(table_file_meta));

        let entries = vec![
            (
                ("v".as_bytes().to_vec(), Operation::Put),
                ("v".as_bytes().to_vec()),
            ),
            (
                ("w".as_bytes().to_vec(), Operation::Put),
                ("w".as_bytes().to_vec()),
            ),
            (("x".as_bytes().to_vec(), Operation::Delete), vec![]),
            (
                ("y".as_bytes().to_vec(), Operation::Put),
                ("y".as_bytes().to_vec()),
            ),
        ];
        let table_file_meta = create_table(db_options, entries, 76, 57);
        version.files[1].push(Arc::new(table_file_meta));

        version
    }

    /**
    Create a table with the provided entries (key-value pairs) with sequence numbers starting
    from the provided start point.
    */
    fn create_table(
        db_options: DbOptions,
        entries: Vec<((Vec<u8>, Operation), Vec<u8>)>,
        starting_sequence_num: u64,
        file_number: u64,
    ) -> FileMetadata {
        let smallest_key = InternalKey::new(
            entries.first().unwrap().0 .0.clone(),
            starting_sequence_num,
            entries.first().unwrap().0 .1,
        );
        let largest_key = InternalKey::new(
            entries.last().unwrap().0 .0.clone(),
            starting_sequence_num + (entries.len() as u64) - 1,
            entries.last().unwrap().0 .1,
        );

        let mut table_builder = TableBuilder::new(db_options, file_number).unwrap();
        let mut curr_sequence_num = starting_sequence_num;
        for ((user_key, operation), value) in entries {
            table_builder
                .add_entry(
                    Rc::new(InternalKey::new(user_key, curr_sequence_num, operation)),
                    &value,
                )
                .unwrap();
            curr_sequence_num += 1;
        }

        table_builder.finalize().unwrap();

        let mut file_meta = FileMetadata::new(file_number);
        file_meta.set_smallest_key(Some(smallest_key));
        file_meta.set_largest_key(Some(largest_key));
        file_meta.set_file_size(table_builder.file_size());

        file_meta
    }
}

#[cfg(test)]
mod merging_iterator_tests {
    use pretty_assertions::assert_eq;
    use std::rc::Rc;

    use crate::tables::TableBuilder;
    use crate::versioning::version::Version;
    use crate::{DbOptions, Operation};

    use super::*;

    #[test]
    fn with_an_empty_list_of_iterators_does_not_become_valid() {
        let mut iter = MergingIterator::new(vec![]);

        assert!(!iter.is_valid());
        assert!(iter.next().is_none());
        assert!(iter.prev().is_none());
        assert!(iter
            .seek(&InternalKey::new(
                "a".as_bytes().to_vec(),
                100,
                Operation::Put
            ))
            .is_ok());
        assert!(iter.current().is_none());
        assert!(iter.seek_to_first().is_ok());
        assert!(iter.current().is_none());
        assert!(iter.seek_to_last().is_ok());
        assert!(iter.current().is_none());
        assert!(!iter.is_valid());
    }

    #[test]
    fn can_be_iterated_forward_completely() {
        let options = DbOptions::with_memory_env();
        let table_cache = Arc::new(TableCache::new(options.clone(), 1000));
        let version = create_test_version(options, &table_cache);
        let version_iterators = version
            .get_representative_iterators(&ReadOptions::default())
            .unwrap();
        let mut iter = MergingIterator::new(version_iterators);

        assert!(iter.seek_to_first().is_ok());
        let (actual_key, _) = iter.current().unwrap();
        assert_eq!(
            actual_key,
            &InternalKey::new("a".as_bytes().to_vec(), 90, Operation::Put)
        );

        let mut expected_value_counter: usize = 2;
        while iter.next().is_some() {
            let (current_key, current_val) = iter.current().unwrap();
            if current_key.get_operation() == Operation::Put {
                assert_eq!(
                    current_val,
                    expected_value_counter.to_string().as_bytes(),
                    "Expecting value to be {:?} but got {current_val:?}",
                    expected_value_counter.to_string().as_bytes()
                );
            }

            expected_value_counter += 1;
        }

        assert!(
            iter.next().is_none(),
            "Calling `next` after consuming all the values should not return a value"
        );
        assert!(
            !iter.is_valid(),
            "The block iterator should not be valid after moving past the end of the iterator"
        );
    }

    /// Creates version used to hold files for testing.
    fn create_test_version(db_options: DbOptions, table_cache: &Arc<TableCache>) -> Version {
        let mut version = Version::new(db_options.clone(), table_cache, 200, 30);

        // Level 0 allows overlapping files
        let entries = vec![
            (
                ("a".as_bytes().to_vec(), Operation::Put),
                ("1".as_bytes().to_vec()),
            ),
            (
                ("b".as_bytes().to_vec(), Operation::Put),
                ("2".as_bytes().to_vec()),
            ),
            (
                ("c".as_bytes().to_vec(), Operation::Put),
                ("4".as_bytes().to_vec()),
            ),
            (
                ("d".as_bytes().to_vec(), Operation::Put),
                ("6".as_bytes().to_vec()),
            ),
        ];
        let table_file_meta = create_table(db_options.clone(), entries, 90, 60);
        version.files[0].push(Arc::new(table_file_meta));

        let entries = vec![
            (
                ("c".as_bytes().to_vec(), Operation::Put),
                ("3".as_bytes().to_vec()),
            ),
            (
                ("d".as_bytes().to_vec(), Operation::Put),
                ("5".as_bytes().to_vec()),
            ),
            (
                ("e".as_bytes().to_vec(), Operation::Put),
                ("7".as_bytes().to_vec()),
            ),
            (
                ("f".as_bytes().to_vec(), Operation::Put),
                ("9".as_bytes().to_vec()),
            ),
        ];
        let table_file_meta = create_table(db_options.clone(), entries, 100, 61);
        version.files[0].push(Arc::new(table_file_meta));

        let entries = vec![
            (
                ("f".as_bytes().to_vec(), Operation::Put),
                ("8".as_bytes().to_vec()),
            ),
            (
                ("f1".as_bytes().to_vec(), Operation::Put),
                ("10".as_bytes().to_vec()),
            ),
            (
                ("f2".as_bytes().to_vec(), Operation::Put),
                ("11".as_bytes().to_vec()),
            ),
            (
                ("f3".as_bytes().to_vec(), Operation::Put),
                ("12".as_bytes().to_vec()),
            ),
        ];
        let table_file_meta = create_table(db_options.clone(), entries, 105, 62);
        version.files[0].push(Arc::new(table_file_meta));

        // Level 1
        let entries = vec![
            (
                ("g".as_bytes().to_vec(), Operation::Put),
                ("13".as_bytes().to_vec()),
            ),
            (
                ("h".as_bytes().to_vec(), Operation::Put),
                ("14".as_bytes().to_vec()),
            ),
            (
                ("i".as_bytes().to_vec(), Operation::Put),
                ("15".as_bytes().to_vec()),
            ),
            (
                ("j".as_bytes().to_vec(), Operation::Put),
                ("16".as_bytes().to_vec()),
            ),
        ];
        let table_file_meta = create_table(db_options.clone(), entries, 85, 59);
        version.files[1].push(Arc::new(table_file_meta));

        let entries = vec![
            (
                ("o".as_bytes().to_vec(), Operation::Put),
                ("21".as_bytes().to_vec()),
            ),
            (
                ("r".as_bytes().to_vec(), Operation::Put),
                ("24".as_bytes().to_vec()),
            ),
            (
                ("s".as_bytes().to_vec(), Operation::Put),
                ("26".as_bytes().to_vec()),
            ),
            (
                ("t".as_bytes().to_vec(), Operation::Put),
                ("28".as_bytes().to_vec()),
            ),
            (
                ("u".as_bytes().to_vec(), Operation::Put),
                ("29".as_bytes().to_vec()),
            ),
        ];
        let table_file_meta = create_table(db_options.clone(), entries, 80, 58);
        version.files[1].push(Arc::new(table_file_meta));

        let entries = vec![
            (
                ("v".as_bytes().to_vec(), Operation::Put),
                ("30".as_bytes().to_vec()),
            ),
            (
                ("w".as_bytes().to_vec(), Operation::Put),
                ("32".as_bytes().to_vec()),
            ),
            (("x".as_bytes().to_vec(), Operation::Delete), vec![]),
            (
                ("y".as_bytes().to_vec(), Operation::Put),
                ("36".as_bytes().to_vec()),
            ),
        ];
        let table_file_meta = create_table(db_options.clone(), entries, 76, 57);
        version.files[1].push(Arc::new(table_file_meta));

        // Level 2
        let entries = vec![
            (
                ("k".as_bytes().to_vec(), Operation::Put),
                ("17".as_bytes().to_vec()),
            ),
            (
                ("l".as_bytes().to_vec(), Operation::Put),
                ("18".as_bytes().to_vec()),
            ),
            (
                ("m".as_bytes().to_vec(), Operation::Put),
                ("19".as_bytes().to_vec()),
            ),
            (
                ("n".as_bytes().to_vec(), Operation::Put),
                ("20".as_bytes().to_vec()),
            ),
        ];
        let table_file_meta = create_table(db_options.clone(), entries, 65, 55);
        version.files[2].push(Arc::new(table_file_meta));

        let entries = vec![
            (
                ("o".as_bytes().to_vec(), Operation::Put),
                ("22".as_bytes().to_vec()),
            ),
            (
                ("p".as_bytes().to_vec(), Operation::Put),
                ("23".as_bytes().to_vec()),
            ),
            (
                ("r".as_bytes().to_vec(), Operation::Put),
                ("25".as_bytes().to_vec()),
            ),
            (
                ("s".as_bytes().to_vec(), Operation::Put),
                ("27".as_bytes().to_vec()),
            ),
        ];
        let table_file_meta = create_table(db_options.clone(), entries, 60, 54);
        version.files[2].push(Arc::new(table_file_meta));

        let entries = vec![
            (
                ("v".as_bytes().to_vec(), Operation::Put),
                ("31".as_bytes().to_vec()),
            ),
            (
                ("w".as_bytes().to_vec(), Operation::Put),
                ("33".as_bytes().to_vec()),
            ),
            (
                ("x".as_bytes().to_vec(), Operation::Put),
                ("35".as_bytes().to_vec()),
            ),
            (
                ("y".as_bytes().to_vec(), Operation::Put),
                ("37".as_bytes().to_vec()),
            ),
        ];
        let table_file_meta = create_table(db_options.clone(), entries, 55, 53);
        version.files[2].push(Arc::new(table_file_meta));

        // Level 3
        let entries = vec![
            (
                ("z1".as_bytes().to_vec(), Operation::Put),
                ("38".as_bytes().to_vec()),
            ),
            (
                ("z2".as_bytes().to_vec(), Operation::Put),
                ("39".as_bytes().to_vec()),
            ),
        ];
        let table_file_meta = create_table(db_options.clone(), entries, 45, 52);
        version.files[3].push(Arc::new(table_file_meta));

        let entries = vec![
            (
                ("z3".as_bytes().to_vec(), Operation::Put),
                ("40".as_bytes().to_vec()),
            ),
            (
                ("z4".as_bytes().to_vec(), Operation::Put),
                ("41".as_bytes().to_vec()),
            ),
        ];
        let table_file_meta = create_table(db_options.clone(), entries, 47, 51);
        version.files[3].push(Arc::new(table_file_meta));

        let entries = vec![
            (
                ("z5".as_bytes().to_vec(), Operation::Put),
                ("42".as_bytes().to_vec()),
            ),
            (
                ("z6".as_bytes().to_vec(), Operation::Put),
                ("43".as_bytes().to_vec()),
            ),
            (
                ("z7".as_bytes().to_vec(), Operation::Put),
                ("44".as_bytes().to_vec()),
            ),
        ];
        let table_file_meta = create_table(db_options, entries, 49, 50);
        version.files[3].push(Arc::new(table_file_meta));

        version
    }

    /**
    Create a table with the provided entries (key-value pairs) with sequence numbers starting
    from the provided start point.
    */
    fn create_table(
        db_options: DbOptions,
        entries: Vec<((Vec<u8>, Operation), Vec<u8>)>,
        starting_sequence_num: u64,
        file_number: u64,
    ) -> FileMetadata {
        let smallest_key = InternalKey::new(
            entries.first().unwrap().0 .0.clone(),
            starting_sequence_num,
            entries.first().unwrap().0 .1,
        );
        let largest_key = InternalKey::new(
            entries.last().unwrap().0 .0.clone(),
            starting_sequence_num + (entries.len() as u64) - 1,
            entries.last().unwrap().0 .1,
        );

        let mut table_builder = TableBuilder::new(db_options, file_number).unwrap();
        let mut curr_sequence_num = starting_sequence_num;
        for ((user_key, operation), value) in entries {
            table_builder
                .add_entry(
                    Rc::new(InternalKey::new(user_key, curr_sequence_num, operation)),
                    &value,
                )
                .unwrap();
            curr_sequence_num += 1;
        }

        table_builder.finalize().unwrap();

        let mut file_meta = FileMetadata::new(file_number);
        file_meta.set_smallest_key(Some(smallest_key));
        file_meta.set_largest_key(Some(largest_key));
        file_meta.set_file_size(table_builder.file_size());

        file_meta
    }
}
