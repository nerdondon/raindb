/*!
This modules contains iterators and utilities for creating iterators that work over a set of table
file metadata (i.e. [`FileMetadata`]).
*/

use std::sync::Arc;

use crate::errors::RainDBError;
use crate::key::InternalKey;
use crate::table_cache::TableCache;
use crate::tables::Table;
use crate::RainDbIterator;

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

    /// The current file being iterated for entries.
    current_file: Option<Arc<Table>>,

    /// The table cache to retrieve table files from.
    table_cache: Arc<TableCache>,
}

/// Crate-only methods
impl FilesEntryIterator {
    /// Create a new instance of [`FilesEntryIterator`].
    pub(crate) fn new(file_list: Vec<Arc<FileMetadata>>, table_cache: Arc<TableCache>) -> Self {
        Self {
            file_list,
            current_file_index: 0,
            current_file: None,
            table_cache,
        }
    }
}

impl RainDbIterator for FilesEntryIterator {
    type Key = InternalKey;

    type Error = RainDBError;

    fn is_valid(&self) -> bool {
        self.current_file_index < self.file_list.len()
    }

    fn seek(&mut self, target: &Self::Key) -> Result<(), Self::Error> {
        let result = super::utils::find_file_with_upper_bound_range(&self.file_list, target);
        self.current_file_index = match result {
            Some(index) => index,
            None => {
                // Make the iterator invalid if the target was not found
                self.file_list.len()
            }
        };

        Ok(())
    }

    fn seek_to_first(&mut self) -> Result<(), Self::Error> {
        self.current_file_index = 0;

        Ok(())
    }

    fn seek_to_last(&mut self) -> Result<(), Self::Error> {
        self.current_file_index = if self.file_list.is_empty() {
            0
        } else {
            self.file_list.len() - 1
        };

        Ok(())
    }

    fn next(&mut self) -> Option<(&Self::Key, &Vec<u8>)> {
        // The iterator is not valid, don't do anything
        if !self.is_valid() {
            return None;
        }

        self.current_file_index += 1;

        // Return the next item if the iterator is still valid
        if self.is_valid() {
            return self.current();
        }

        None
    }

    fn prev(&mut self) -> Option<(&Self::Key, &Vec<u8>)> {
        // The iterator is not valid, don't do anything
        if !self.is_valid() {
            return None;
        }

        self.current_file_index -= 1;

        // Return the previous item if the iterator is still valid
        if self.is_valid() {
            return self.current();
        }

        None
    }

    fn current(&self) -> Option<(&Self::Key, &Vec<u8>)> {
        if !self.is_valid() {
            return None;
        }

        let current_file = Arc::clone(&self.file_list[self.current_file_index]);
        todo!()
    }
}
