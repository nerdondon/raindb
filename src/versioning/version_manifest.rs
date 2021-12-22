use std::ops::Range;

use crate::key::InternalKey;

use super::file_metadata::FileMetadata;

/**
A manifest of changes and change information to be applied to a [`crate::versioning::VersionSet`].

# Legacy

This is synonymous to LevelDB's [`leveldb::VersionEdit`].

[`leveldb::VersionEdit`]: https://github.com/google/leveldb/blob/e426c83e88c4babc785098d905c2dcb4f4e884af/db/version_edit.h
*/
pub(crate) struct VersionChangeManifest {
    /// The file number for the write-ahead log for the new memtable.
    pub(crate) wal_file_number: Option<u64>,

    /// The file number for the write-ahead log for the memtable that is currently being compacted.
    pub(crate) prev_wal_file_number: Option<u64>,

    /// The last sequence number that is used in the changes
    pub(crate) last_sequence_number: Option<u64>,

    /**
    The number to use when creating a new file.

    This information snapshotted from the version set for persistence to disk and is primarily used
    for recovery operations.
    */
    pub(crate) next_file_number: Option<u64>,

    /// New files to add to the next version with the level the file it should be added at.
    pub(crate) new_files: Vec<(usize, FileMetadata)>,
}

/// Crate-only methods
impl VersionChangeManifest {
    /**
    Add the specified file with the specified number.

    # Invariants

    These invariants must hold true or there will be undefined behavior.

    This change manifest must not have been applied yet e.g. by calling
    [`super::version_set::VersionSetBuilder::save_to_version`]. The smallest and largest keys
    specified in the metadata are actually the smallest and largest keys in the file.
    */
    pub(crate) fn add_file(
        &mut self,
        level: usize,
        file_number: u64,
        file_size: u64,
        key_range: Range<InternalKey>,
    ) {
        let metadata = FileMetadata::new(file_number);
        metadata.set_file_size(file_size);
        metadata.set_smallest_key(Some(key_range.start));
        metadata.set_largest_key(Some(key_range.end));

        self.new_files.push((level, metadata));
    }
}

/// Create an empty [`VersionChangeManifest`].
impl Default for VersionChangeManifest {
    fn default() -> Self {
        Self {
            wal_file_number: None,
            prev_wal_file_number: None,
            last_sequence_number: None,
            next_file_number: None,
            new_files: vec![],
        }
    }
}
