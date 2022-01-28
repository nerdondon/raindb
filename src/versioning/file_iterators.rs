/*!
This modules contains iterators and utilities for creating iterators that work over a set of table
file metadata (i.e. [`FileMetadata`]).
*/

use std::sync::Arc;

use crate::errors::{RainDBError, RainDBResult};
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
