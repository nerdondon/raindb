use integer_encoding::{VarIntReader, VarIntWriter};
use std::collections::HashSet;
use std::io::Write;
use std::ops::Range;

use crate::key::InternalKey;
use crate::utils::io::{ReadHelpers, WriteHelpers};

use super::errors::{RecoverError, RecoverResult};
use super::file_metadata::FileMetadata;

/// Represents a file to be deleted at a specified level.
#[derive(Debug, Eq, Hash, PartialEq)]
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
#[derive(Debug, Eq, PartialEq)]
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

    /// New files to add to the next version with the level the file should be added at.
    pub(crate) new_files: Vec<(usize, FileMetadata)>,

    /// Deleted files per level that will be removed in the next version.
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
            .insert(DeletedFile::new(level, file_number));
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
#[derive(Debug)]
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

impl TryFrom<u32> for ManifestFieldTags {
    type Error = RecoverError;

    fn try_from(value: u32) -> RecoverResult<ManifestFieldTags> {
        let tag = match value {
            1 => ManifestFieldTags::Comparator,
            2 => ManifestFieldTags::CurrentWALNumber,
            3 => ManifestFieldTags::CurrentFileNumber,
            4 => ManifestFieldTags::PrevSequenceNumber,
            5 => ManifestFieldTags::CompactionPointer,
            6 => ManifestFieldTags::DeletedFile,
            7 => ManifestFieldTags::NewFile,
            8 => ManifestFieldTags::LargeValueRef,
            9 => ManifestFieldTags::PrevWALNumber,
            _ => {
                return Err(RecoverError::ManifestParse(format!(
                    "Found an unknown tag. The value received was {}",
                    value
                )))
            }
        };

        Ok(tag)
    }
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

impl TryFrom<&[u8]> for VersionChangeManifest {
    type Error = RecoverError;

    fn try_from(value: &[u8]) -> Result<Self, Self::Error> {
        let mut value_reader = value;
        let starting_len = value.len();
        let mut version_manifest = VersionChangeManifest::default();
        // Store the field tag if there were issues parsing the field value
        let mut maybe_err_tag: Option<ManifestFieldTags> = None;
        while maybe_err_tag.is_none() && !value_reader.is_empty() {
            match value_reader.read_varint::<u32>() {
                Ok(tag) => match ManifestFieldTags::try_from(tag)? {
                    ManifestFieldTags::Comparator => {
                        maybe_err_tag = Some(ManifestFieldTags::Comparator);
                    }
                    ManifestFieldTags::CurrentWALNumber => {
                        match value_reader.read_varint::<u64>() {
                            Ok(field_val) => {
                                version_manifest.wal_file_number = Some(field_val);
                            }
                            Err(_) => {
                                maybe_err_tag = Some(ManifestFieldTags::CurrentWALNumber);
                            }
                        }
                    }
                    ManifestFieldTags::CurrentFileNumber => {
                        match value_reader.read_varint::<u64>() {
                            Ok(field_val) => {
                                version_manifest.curr_file_number = Some(field_val);
                            }
                            Err(_) => {
                                maybe_err_tag = Some(ManifestFieldTags::CurrentFileNumber);
                            }
                        }
                    }
                    ManifestFieldTags::PrevSequenceNumber => {
                        match value_reader.read_varint::<u64>() {
                            Ok(field_val) => {
                                version_manifest.prev_sequence_number = Some(field_val);
                            }
                            Err(_) => {
                                maybe_err_tag = Some(ManifestFieldTags::PrevSequenceNumber);
                            }
                        }
                    }
                    ManifestFieldTags::CompactionPointer => {
                        if let Ok(level) = value_reader.read_raindb_level() {
                            if let Ok(encoded_key) = value_reader.read_length_prefixed_slice() {
                                if let Ok(key) = InternalKey::try_from(encoded_key) {
                                    version_manifest.add_compaction_pointer(level, key);

                                    continue;
                                }
                            }
                        }

                        maybe_err_tag = Some(ManifestFieldTags::CompactionPointer);
                    }
                    ManifestFieldTags::DeletedFile => {
                        if let Ok(level) = value_reader.read_raindb_level() {
                            if let Ok(file_number) = value_reader.read_varint::<u64>() {
                                version_manifest.remove_file(level, file_number);

                                continue;
                            }
                        }

                        maybe_err_tag = Some(ManifestFieldTags::DeletedFile);
                    }
                    ManifestFieldTags::NewFile => {
                        if let Ok(level) = value_reader.read_raindb_level() {
                            if let Ok(file) = FileMetadata::deserialize(&mut value_reader) {
                                version_manifest.add_file(
                                    level,
                                    file.file_number(),
                                    file.get_file_size(),
                                    file.clone_key_range(),
                                );

                                continue;
                            }
                        }

                        maybe_err_tag = Some(ManifestFieldTags::NewFile);
                    }
                    ManifestFieldTags::LargeValueRef => {
                        maybe_err_tag = Some(ManifestFieldTags::Comparator);
                    }
                    ManifestFieldTags::PrevWALNumber => match value_reader.read_varint::<u64>() {
                        Ok(field_val) => {
                            version_manifest.prev_wal_file_number = Some(field_val);
                        }
                        Err(_) => {
                            maybe_err_tag = Some(ManifestFieldTags::PrevWALNumber);
                        }
                    },
                },
                Err(_) => {
                    return Err(RecoverError::ManifestParse(format!(
                        "Failed to parse the field tag. Read {bytes_read} bytes.",
                        bytes_read = starting_len - value_reader.len()
                    )));
                }
            }
        }

        if let Some(err_tag) = maybe_err_tag {
            return Err(RecoverError::ManifestParse(format!(
                "Failed to parse the value for the field: {field_tag:?}. Read {bytes_read} bytes.",
                field_tag = err_tag,
                bytes_read = starting_len - value_reader.len()
            )));
        }

        Ok(version_manifest)
    }
}

#[cfg(test)]
mod tests {
    use pretty_assertions::assert_eq;

    use crate::Operation;

    use super::*;

    #[test]
    fn version_change_manifest_can_be_serialized_and_deserialized() {
        let mut change_manifest = VersionChangeManifest {
            curr_file_number: Some(3000),
            prev_sequence_number: Some(4000),
            wal_file_number: Some(2900),
            ..VersionChangeManifest::default()
        };

        for idx in 0..4 {
            change_manifest.add_file(
                3,
                2900 + idx,
                4 * 1024 * 1024,
                InternalKey::new(b"abc".to_vec(), (1 << 50) + idx, Operation::Put)
                    ..InternalKey::new(b"xyz".to_vec(), (1 << 50) + 100 + idx, Operation::Delete),
            );
            change_manifest.remove_file(4, 2880 + idx);
            change_manifest.add_compaction_pointer(
                idx as usize,
                InternalKey::new(b"rst".to_vec(), (1 << 50) + 100 + idx, Operation::Put),
            );
        }

        let serialized = Vec::<u8>::from(&change_manifest);
        let deserialized = VersionChangeManifest::try_from(serialized.as_slice()).unwrap();

        assert_eq!(change_manifest, deserialized);
    }
}
