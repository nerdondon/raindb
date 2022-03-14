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

    /// The current index into the file list that the cursor is at.
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
    /// Check if the index into the file list is pointing at valid location.
    fn is_file_index_valid(&self) -> bool {
        self.current_file_index == self.file_list.len()
    }

    /// Set the table iterator to be used for iteration.
    fn set_table_iter(&mut self, maybe_new_index: Option<usize>) -> RainDBResult<()> {
        if maybe_new_index.is_none() || maybe_new_index.unwrap() == self.file_list.len() {
            self.current_file_index = self.file_list.len();
            self.current_table_iter = None;
            return Ok(());
        }

        if let Some(new_index) = maybe_new_index {
            if new_index == self.current_file_index {
                // The file we need to iterate is the same as the current one. No update necessary
                return Ok(());
            }

            let table = self
                .table_cache
                .find_table(self.file_list[self.current_file_index].file_number())?;
            self.current_table_iter = Some(Table::iter_with(table, self.read_options.clone()));
        }
        Ok(())
    }

    /// Move forward through any empty files.
    fn skip_empty_table_files_forward(&mut self) -> RainDBResult<()> {
        while self.current_table_iter.is_none()
            || !self.current_table_iter.as_mut().unwrap().is_valid()
        {
            if !self.is_file_index_valid() {
                // We've reached the end of the file list so there are no more tables to iterate
                self.current_table_iter = None;
                return Ok(());
            }

            // Move index iterator to check for the next data block handle
            self.current_file_index += 1;
            self.set_table_iter(Some(self.current_file_index))?;

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
            if !self.is_file_index_valid() {
                // We've reached the end of the file list so there are no more tables to iterate
                self.current_table_iter = None;
                return Ok(());
            }

            // Move index iterator to check for the next data block handle
            self.current_file_index -= 1;
            self.set_table_iter(Some(self.current_file_index))?;

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

        self.skip_empty_table_files_backward()?;

        Ok(())
    }

    fn seek_to_first(&mut self) -> Result<(), Self::Error> {
        self.current_file_index = 0;
        self.set_table_iter(Some(self.current_file_index))?;

        if self.current_table_iter.is_some() {
            self.current_table_iter.as_mut().unwrap().seek_to_first()?;
        }

        self.skip_empty_table_files_forward()?;

        Ok(())
    }

    fn seek_to_last(&mut self) -> Result<(), Self::Error> {
        self.current_file_index = if self.file_list.is_empty() {
            0
        } else {
            self.file_list.len() - 1
        };
        self.set_table_iter(Some(self.current_file_index))?;

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

        self.current_table_iter.as_mut().unwrap().current()
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

        self.current_table_iter.as_mut().unwrap().current()
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
An iterator that merges the output of a list of iterators sorted order.

This iterator does not do any sort of de-duplication.

A heap is not used to merge the inputs because
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
        let mut smallest_iterator_index: usize = 0;
        for (index, iter) in self.iterators.iter().enumerate() {
            if !iter.is_valid() {
                continue;
            }

            if let Some((key, _)) = iter.current() {
                if key < self.iterators[smallest_iterator_index].current().unwrap().0 {
                    smallest_iterator_index = index;
                }
            }
        }

        self.current_iterator_index = Some(smallest_iterator_index);
    }

    /// Find the iterator with the currently largest key and update the merging iterator state.
    fn find_largest(&mut self) {
        let mut largest_iterator_index: usize = 0;
        for (index, iter) in self.iterators.iter().rev().enumerate() {
            if !iter.is_valid() {
                continue;
            }

            if let Some((key, _)) = iter.current() {
                if key > self.iterators[largest_iterator_index].current().unwrap().0 {
                    largest_iterator_index = index;
                }
            }
        }

        self.current_iterator_index = Some(largest_iterator_index);
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
            let current_key = self.current().unwrap().0.clone();
            for index in 0..self.iterators.len() {
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
