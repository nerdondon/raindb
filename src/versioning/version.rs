use std::cell::RefCell;
use std::rc::Rc;
use std::sync::Arc;

use crate::config::MAX_NUM_LEVELS;
use crate::table_cache::TableCache;

use super::errors::ReadResult;
use super::file_metadata::FileMetadata;
use super::version_set::VersionSet;

/**
Metadata required to charge a file for a multi-level disk seek.

This is analogous to [`Version::GetStats`] in LevelDB. The name used in RainDB attempts to be more
descriptive with respect to what this struct is used for.
*/
#[derive(Debug)]
pub(crate) struct SeekChargeMetadata {
    /// The file that will be charged for multi-level seeks.
    seek_file: Option<Rc<RefCell<FileMetadata>>>,

    /// The level of the file being charged.
    seek_file_level: Option<usize>,
}

/// Public methods
impl SeekChargeMetadata {
    /// Create a new instance of [`SeekChargeMetadata`].
    pub fn new(seek_file: Option<Rc<RefCell<FileMetadata>>>, seek_file_level: usize) -> Self {
        Self {
            seek_file: None,
            seek_file_level: None,
        }
    }
}

pub(crate) struct GetResponse {
    /// The found value.
    value: Vec<u8>,

    /// The seek charge metadata that should be applied.
    charge_metadata: SeekChargeMetadata,
}

/**
Metadata used for scoring the necessity of compacting a version based on the size of files within
the version.
*/
#[derive(Debug)]
pub(crate) struct SizeCompactionMetadata {
    /// The level that should be compacted next.
    compaction_level: usize,

    /**
    A score for determining the necessity of compaction for the level that will be compacted next.

    A score < 1 means that compaction is not strictly necessary.
    */
    compaction_score: f64,
}

/**
Metadata used for scoring the necessity of compacting a version based on the number of disk seeks
being performed during read operations within the version.
*/
#[derive(Debug)]
pub(crate) struct SeekCompactionMetadata {
    /// The next file to compact based on seek stats.
    file_to_compact: Arc<FileMetadata>,

    /// The level of the next file to compact.
    level_of_file_to_compact: usize,
}

/// Version contains information that represents a point in time state of the database.
#[derive(Debug)]
pub(crate) struct Version {
    /// List of files per level.
    files: [Vec<Arc<FileMetadata>>; MAX_NUM_LEVELS],

    /**
    A cache for accessing table files.

    This will be shared with the child versions.
    */
    table_cache: Arc<TableCache>,

    /**
    Metadata used for scoring the necessity of compacting a version based on the number of disk
    seeks being performed during read operations within the version.

    This is updated as read requests are serviced.
    */
    seek_compaction_metadata: Option<SeekCompactionMetadata>,

    /**
    Metadata used for scoring the necessity of compacting a version based on the size of files
    within the version.

    This is filled in when the version is finalized (e.g. via [`VersionSet::finalize`]).

    [`VersionSet::finalize`]: super::version_set::VersionSet::finalize
    */
    size_compaction_metadata: Option<SizeCompactionMetadata>,
}

/// Public methods
impl Version {
    pub fn new(table_cache: &Arc<TableCache>) -> Self {
        let files = [vec![]; MAX_NUM_LEVELS];

        Self {
            table_cache: Arc::clone(table_cache),
            seek_compaction_metadata: None,
            size_compaction_metadata: None,
            files,
        }
    }

    /**
    Look up the value for the given key.

    # Concurrency
    Does not require a lock to be held.

    Returns an `GetResponse` struct containing the found value and seek charge metadata.
    */
    pub fn get(&self) -> ReadResult<GetResponse> {
        todo!("working on it!");
    }

    /**
    Apply the charging metadata to the current state.

    # Concurrency
    An external lock is required to be held before exercising this method.

    Returns true if a new compaction may need to be triggered, false otherwise.
    */
    pub fn update_stats(&self, charging_metadata: SeekChargeMetadata) -> ReadResult<bool> {
        todo!("working on it!");
    }

    /// Return the number of table files at the specified level.
    pub fn num_files_at_level(&self, level: u64) -> usize {
        self.files[level as usize].len()
    }
}

/// Private methods
impl Version {
    /**
    Get table files in this version that overlap the specified key in order from newest to oldest.
    */
    fn get_overlapping_files(&self) -> Vec<Arc<FileMetadata>> {
        todo!("working on it!");
    }
}
