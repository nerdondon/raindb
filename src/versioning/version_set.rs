use std::sync::Arc;

use crate::fs::FileSystem;
use crate::table_cache::TableCache;
use crate::utils::linked_list::{LinkedList, SharedNode};
use crate::DbOptions;

use super::errors::{ReadError, ReadResult};
use super::version::Version;

/// Manages the versions of the database.
#[derive(Debug)]
pub(crate) struct VersionSet {
    /// Database options to refer to when reading the table file.
    options: DbOptions,

    /// A reference to the file system provider in use by the database.
    filesystem_provider: Arc<Box<dyn FileSystem>>,

    /**
    A cache for accessing table files.

    This will be shared with the child versions.
    */
    table_cache: Arc<TableCache>,

    /**
    The most recently used file number.

    This is a counter that is incremented as new files are crated. File numbers can be re-used when
    a write-ahead log and memtable pair are converte to a table file.
    */
    curr_file_number: u64,

    /// The current manifest file number.
    curr_manifest_file_number: u64,

    /**
    The most recenly used sequence number for an operation.

    This is a counter that is incremented as new operations are executed.
    */
    prev_sequence_number: u64,

    /// The ID of the currently used write-ahead log.
    curr_wal_number: u64,

    /**
    The ID of the previous write-ahead log.

    This number is only populated if there is a memtable that is currenly undergoing the compaction
    process.
    */
    prev_wal_number: Option<u64>,

    /// A list of versions in the version set where the current version is at the tail of the list.
    versions: LinkedList<Version>,
}

/// Public methods
impl VersionSet {
    /// Create a new instance of [`VersionSet`].
    pub fn new(options: DbOptions, table_cache: TableCache) -> Self {
        let filesystem_provider = options.filesystem_provider();
        let versions = LinkedList::<Version>::new();

        Self {
            options,
            filesystem_provider,
            table_cache: Arc::new(table_cache),
            curr_file_number: 1,
            // This will be updated by [`VersionSet::recover`]
            curr_manifest_file_number: 0,
            prev_sequence_number: 0,
            curr_wal_number: 0,
            prev_wal_number: None,
            versions,
        }
    }

    /// Return the number of table files at the specified level in the current version.
    pub fn num_files_at_level(&self, level: u64) -> ReadResult<usize> {
        if self.get_current_version().is_none() {
            // This should never happen because a version is core to the operation of RainDB.
            return Err(ReadError::NoVersionsFound);
        }

        let current_version = self.get_current_version().unwrap();
        let num_files = current_version.read().element.num_files_at_level(level);

        Ok(num_files)
    }

    // Returns a new file number.
    pub fn get_new_file_number(&self) -> u64 {
        self.curr_file_number += 1;
        self.curr_file_number
    }

    /**
    Reuse a file number.

    We reuse file numbers in cases like when a we fail to create a file. This helps to avoid
    exhausting the file number space.

    **NOTE** The number being reused must have been obtained via [`VersionSet::get_new_file_number`].
    */
    pub fn reuse_file_number(&self, file_number: u64) {
        // If the provided file number is the same as the current file number in the version set,
        // then we know that the counter was just incremented and we can perform the reverse
        // operation.
        if self.curr_file_number == file_number {
            self.curr_file_number -= 1;
        }
    }

    /**
    Get a reference to the version set's previous WAL number.

    This number is only populated if there is a memtable that is currenly undergoing the compaction
    process.
    */
    pub fn maybe_prev_wal_number(&self) -> Option<u64> {
        self.prev_wal_number
    }

    /**
    Set the version set's previous WAL number.

    This number must only be populated if there is a memtable that is currently undergoing
    compaction.
    */
    pub fn set_prev_wal_number(&mut self, prev_wal_number: Option<u64>) {
        self.prev_wal_number = prev_wal_number;
    }

    /// Returns true if a level on the current version needs compaction.
    pub fn needs_compaction(&self) -> ReadResult<bool> {
        if self.get_current_version().is_none() {
            // This should never happen because a version is core to the operation of RainDB.
            return Err(ReadError::NoVersionsFound);
        }

        let curr_version = &self.get_current_version().unwrap().read().element;
        if curr_version.get_size_compaction_metadata().is_some()
            && curr_version
                .get_size_compaction_metadata()
                .unwrap()
                .compaction_score
                >= 1.0
        {
            // The compaction score is too high. We need to do a compaction.
            return Ok(true);
        }

        if curr_version.get_seek_compaction_metadata().is_some()
            && curr_version
                .get_seek_compaction_metadata()
                .unwrap()
                .file_to_compact
                .is_some()
        {
            // There is a file to compact due to too many seeks.
            return Ok(true);
        }

        Ok(false)
    }

    /// Get the most recently used sequence number.
    pub fn get_prev_sequence_number(&self) -> u64 {
        self.prev_sequence_number
    }

    /// Set the most recently used sequence number.
    pub fn set_prev_sequence_number(&mut self, sequence_number: u64) {
        self.prev_sequence_number = sequence_number;
    }
}

/// Private methods
impl VersionSet {
    // Get a reference to the current version.
    fn get_current_version(&self) -> Option<SharedNode<Version>> {
        self.versions.tail()
    }

    fn finalize(&self, version: Version) {
        todo!("working on it!");
    }
}
