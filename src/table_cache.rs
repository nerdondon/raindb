/*!
This module provides a thread-safe table cache.
*/

use std::fmt;
use std::sync::Arc;

use crate::errors::RainDBResult;
use crate::file_names::FileNameHandler;
use crate::fs::FileSystem;
use crate::key::InternalKey;
use crate::tables::Table;
use crate::utils::cache::{CacheEntry, LRUCache};
use crate::DbOptions;
use crate::{Cache, ReadOptions};

type FileNumber = u64;

/// A thread-safe cache of table readers.
pub(crate) struct TableCache {
    /// Database options to refer to when reading the table file.
    options: DbOptions,

    /// The underlying cache storing the table readers.
    cache: Box<dyn Cache<FileNumber, Table>>,

    /// Utility for getting file names used by the database.
    file_name_handler: FileNameHandler,

    /// A reference to the file system provider in use by the database.
    filesystem_provider: Arc<Box<dyn FileSystem>>,
}

/// Public methods
impl TableCache {
    /// Create a new instance of a [`TableCache`].
    pub fn new(options: DbOptions, capacity: usize) -> Self {
        let cache = Box::new(LRUCache::<FileNumber, Table>::new(capacity));
        let file_name_handler = FileNameHandler::new(options.db_path().to_string());
        let filesystem_provider = options.filesystem_provider();

        Self {
            options,
            cache,
            file_name_handler,
            filesystem_provider,
        }
    }

    /// Get the value for the key stored in the specified table file.
    pub fn get(
        &self,
        read_options: ReadOptions,
        file_number: u64,
        key: &InternalKey,
    ) -> RainDBResult<Option<Vec<u8>>> {
        let table_cache_entry = self.find_table(file_number)?;
        let table = table_cache_entry.get_value();

        Ok(table.get(read_options, key)?)
    }

    /// Remove the cached table reader for the given file number.
    pub fn remove(&self, file_number: u64) {
        self.cache.remove(&file_number);
    }

    /// Get a reference to a cache entry of a table reader.
    pub fn find_table(&self, file_number: u64) -> RainDBResult<Box<dyn CacheEntry<Table>>> {
        // Check the cache for if there is already a reader and return that if there is
        let maybe_cached_table = self.cache.get(&file_number);
        if maybe_cached_table.is_some() {
            return Ok(maybe_cached_table.unwrap());
        }

        // Table file was not found in the cache so read from disk
        let table_file_name = self.file_name_handler.get_table_file_name(file_number);
        let table_file = self.filesystem_provider.open_file(&table_file_name)?;
        let table_reader = Table::open(self.options.clone(), table_file)?;
        let cache_entry = self.cache.insert(file_number, table_reader);

        Ok(cache_entry)
    }
}

impl fmt::Debug for TableCache {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TableCache")
            .field("options", &self.options)
            .finish()
    }
}
