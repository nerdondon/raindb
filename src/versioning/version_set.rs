use std::collections::LinkedList;
use std::sync::Arc;

use crate::fs::FileSystem;
use crate::table_cache::TableCache;
use crate::DbOptions;

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
    The most recently created table file number.

    This is a counter that is incremented as new table files are crated.
    */
    curr_table_file_number: u64,

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
    pub fn new(options: DbOptions, table_cache: TableCache) -> Self {
        let filesystem_provider = options.filesystem_provider();
        let versions = LinkedList::<Version>::new();

        Self {
            options,
            filesystem_provider,
            table_cache: Arc::new(table_cache),
            curr_table_file_number: 1,
            // This will be updated by [`VersionSet::recover`]
            curr_manifest_file_number: 0,
            prev_sequence_number: 0,
            curr_wal_number: 0,
            prev_wal_number: None,
            versions,
        }
    }
}

/// Private methods
impl VersionSet {
    fn finalize(&self, version: Version) {
        todo!("working on it!");
    }
}
