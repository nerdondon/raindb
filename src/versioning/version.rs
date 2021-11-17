use std::cell::RefCell;
use std::rc::Rc;

use super::errors::ReadResult;
use super::file_metadata::FileMetadata;
use super::version_set::VersionSet;

/**
Metadata required to charge a file for a multi-level disk seek.

This is analogous to `Version::GetStats` in LevelDB. The name used in RainDB attempts to be more
descriptive with respect to what this struct is used for.
*/
#[derive(Debug)]
pub(crate) struct SeekChargeMetadata {
    /// The file that will be charged for multi-level seeks.
    seek_file: Option<Rc<RefCell<FileMetadata>>>,

    /// The level of the file being charged.
    seek_file_level: Option<usize>,
}

impl SeekChargeMetadata {
    pub(crate) fn new(
        seek_file: Option<Rc<RefCell<FileMetadata>>>,
        seek_file_level: usize,
    ) -> Self {
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

/// Version contains information that represents a point in time state of the database.
pub(crate) struct Version {
    /// List of files per level.
    files: Vec<Rc<FileMetadata>>,

    /// The next file to compact based on seek stats.
    file_to_compact: Rc<FileMetadata>,

    /// The level of the next file to compact.
    level_of_file_to_compact: usize,

    /// The level that should be compacted next.
    compaction_level: usize,

    /**
    A score for determining the necessity of compaction for the level that will be compacted next.

    A score < 1 means that compaction is not strictly necessary.
    */
    compaction_score: f64,
}

/// Public methods of `Version`
impl Version {
    /**
    Look up the value for the given key.

    # Concurrency
    Does not require a lock to be held.

    Returns an `GetResponse` struct containing the found value and seek charge metadata.
    */
    pub fn get(&self) -> ReadResult<GetResponse> {}

    /**
    Apply the charging metadata to the current state.

    # Concurrency
    An external lock is required to be held before exercising this method.

    Returns true if a new compaction may need to be triggered, false otherwise.
    */
    pub fn update_stats(&self, charging_metadata: SeekChargeMetadata) -> ReadResult<bool> {}
}

/// Private methods of `Version`
impl Version {
    /**
    Get table files in this version that overlap the specified key in order from newest to oldest.
    */
    fn get_overlapping_files(&self) -> Vec<Rc<RefCell<TableCache>>> {}
}
