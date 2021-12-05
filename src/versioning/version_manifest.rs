/**
A manifest of changes and change information to be applied to a [`crate::versioning::VersionSet`].

# Legacy

This is synonymous to LevelDB's [`leveldb::VersionEdit`].

[`leveldb::VersionEdit`]: https://github.com/google/leveldb/blob/e426c83e88c4babc785098d905c2dcb4f4e884af/db/version_edit.h
*/
pub(crate) struct VersionChangeManifest {
    pub(crate) wal_file_number: Option<u64>,
    pub(crate) prev_wal_file_number: Option<u64>,
    pub(crate) last_sequence_number: Option<u64>,
    pub(crate) next_file_number: Option<u64>,
}

/// Create an empty [`VersionChangeManifest`].
impl Default for VersionChangeManifest {
    fn default() -> Self {
        Self {
            wal_file_number: None,
            prev_wal_file_number: None,
            last_sequence_number: None,
            next_file_number: None,
        }
    }
}
