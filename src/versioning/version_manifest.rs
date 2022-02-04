use integer_encoding::VarIntWriter;
use std::collections::HashSet;
use std::io::Write;
use std::ops::Range;

use crate::key::InternalKey;
use crate::utils::write_io::WriteHelpers;

use super::file_metadata::FileMetadata;

/// Represents a file to be deleted at a specified level.
#[derive(Eq, Hash, PartialEq)]
pub(crate) struct DeletedFile {
    /// The level to delete the file from.
    pub(crate) level: usize,

    /// The file number of the file to delete.
    pub(crate) file_number: u64,
}

/// Crate-only methods
impl DeletedFile {
    /// Create a new instance of [`DeletedFile`].
    pub(crate) fn new(level: usize, file_number: u64) -> Self {
        Self { level, file_number }
    }
}

/**
A manifest of changes and change information to be applied to a [`crate::versioning::VersionSet`].

# Legacy

This is synonymous to LevelDB's [`leveldb::VersionEdit`].

[`leveldb::VersionEdit`]: https://github.com/google/leveldb/blob/e426c83e88c4babc785098d905c2dcb4f4e884af/db/version_edit.h
*/
pub(crate) struct VersionChangeManifest {
    /// The file number for the write-ahead log for the new memtable.
    pub(crate) wal_file_number: Option<u64>,

    /**
    The file number for the write-ahead log backing the memtable that is currently being
    compacted.
    */
    pub(crate) prev_wal_file_number: Option<u64>,

    /**
    The last sequence number that was used in the changes.

    This is snapshotted from the version set when changes are being applied.
    */
    pub(crate) prev_sequence_number: Option<u64>,

    /**
    The most recently used file number.

    This information is snapshotted from the version set for persistence to disk and is primarily
    used for recovery operations.

    # Legacy

    This is analogous to LevelDB's `VersionEdit::next_file_number_` field.
    */
    pub(crate) curr_file_number: Option<u64>,

    /// New files to add to the next version with the level the file it should be added at.
    pub(crate) new_files: Vec<(usize, FileMetadata)>,

    /**
    Deleted files per level that will be removed in the next version.

    Deleted files are marked by a tuple of
    */
    pub(crate) deleted_files: HashSet<DeletedFile>,

    /**
    Keys at which the next compaction should start when compacting the associated level.

    Compaction pointers are stored as a tuple of (level, key).
    */
    pub(crate) compaction_pointers: Vec<(usize, InternalKey)>,
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
        let mut metadata = FileMetadata::new(file_number);
        metadata.set_file_size(file_size);
        metadata.set_smallest_key(Some(key_range.start));
        metadata.set_largest_key(Some(key_range.end));

        self.new_files.push((level, metadata));
    }

    /// Mark the specified file to be deleted in at the specified level.
    pub(crate) fn remove_file(&mut self, level: usize, file_number: u64) {
        self.deleted_files
            .insert(DeletedFile { level, file_number });
    }

    /// Add a compaction pointer to the change manifest.
    pub(crate) fn add_compaction_pointer(&mut self, level: usize, key: InternalKey) {
        self.compaction_pointers.push((level, key));
    }
}

/// Create an empty [`VersionChangeManifest`].
impl Default for VersionChangeManifest {
    fn default() -> Self {
        Self {
            wal_file_number: None,
            prev_wal_file_number: None,
            prev_sequence_number: None,
            curr_file_number: None,
            new_files: vec![],
            deleted_files: HashSet::new(),
            compaction_pointers: vec![],
        }
    }
}

/**
Tags to mark fields in the manifest serialization format.

These tags are written to disk and should not be changed.
*/
#[repr(u32)]
enum ManifestFieldTags {
    Comparator = 1,
    CurrentWALNumber = 2,
    CurrentFileNumber = 3,
    PrevSequenceNumber = 4,
    CompactionPointer = 5,
    DeletedFile = 6,
    NewFile = 7,
    /**
    8 was used for large value references in LevelDB. RainDB keeps it reserved in case we want to
    have binary compatibility with LevelDB in the future.
    */
    LargeValueRef = 8,
    PrevWALNumber = 9,
}

impl From<&VersionChangeManifest> for Vec<u8> {
    fn from(manifest: &VersionChangeManifest) -> Self {
        let mut buf: Vec<u8> = vec![];

        if manifest.wal_file_number.is_some() {
            buf.write_varint(ManifestFieldTags::CurrentWALNumber as u32)
                .unwrap();
            buf.write_varint(manifest.wal_file_number.unwrap()).unwrap();
        }

        if manifest.prev_wal_file_number.is_some() {
            buf.write_varint(ManifestFieldTags::PrevWALNumber as u32)
                .unwrap();
            buf.write_varint(manifest.prev_wal_file_number.unwrap())
                .unwrap();
        }

        if manifest.curr_file_number.is_some() {
            buf.write_varint(ManifestFieldTags::CurrentFileNumber as u32)
                .unwrap();
            buf.write_varint(manifest.curr_file_number.unwrap())
                .unwrap();
        }

        if manifest.prev_sequence_number.is_some() {
            buf.write_varint(ManifestFieldTags::PrevSequenceNumber as u32)
                .unwrap();
            buf.write_varint(manifest.prev_sequence_number.unwrap())
                .unwrap();
        }

        for (level, ptr) in &manifest.compaction_pointers {
            buf.write_varint(ManifestFieldTags::CompactionPointer as u32)
                .unwrap();
            buf.write_varint(*level as u32).unwrap();
            buf.write_length_prefixed_slice(&Vec::<u8>::from(ptr))
                .unwrap();
        }

        for file_descriptor in &manifest.deleted_files {
            let DeletedFile { level, file_number } = file_descriptor;
            buf.write_varint(ManifestFieldTags::DeletedFile as u32)
                .unwrap();
            buf.write_varint(*level as u32).unwrap();
            buf.write_varint(*file_number).unwrap();
        }

        for (level, file) in &manifest.new_files {
            buf.write_varint(ManifestFieldTags::NewFile as u32).unwrap();
            buf.write_varint(*level as u32).unwrap();
            buf.write_all(&Vec::<u8>::from(file)).unwrap();
        }

        buf
    }
}
