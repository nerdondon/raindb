/*!
This module contains utilities for managing file names used by the database.

Files are rooted at the `db_path` as provided in the [database instantiation options](crate::DbOptions).

Files (and their name formats) used by the database are as follows:

- Database lock file: `./LOCK`
- Write-ahead logs: `./wal/wal-[0-9]+.log`
- Table files: `./data/[0-9]+.rdb`
- Manifest files: `./MANIFEST-[0-9]+.manifest`
*/

use std::path::PathBuf;

/// The name of the database lock file.
pub(crate) const LOCK_FILE: &str = "LOCK";

/// The directory name that write-ahead logs will be stored in.
pub(crate) const WAL_DIR: &str = "wal";

/// Suffix for write-ahead log files.
pub(crate) const WAL_EXT: &str = "log";

/// The directory name that data files will be stored in.
pub(crate) const DATA_DIR: &str = "data";

/// Suffix for table files.
pub(crate) const TABLE_EXT: &str = "rdb";

/// The manifest file extension.
pub(crate) const MANIFEST_FILE_EXT: &str = "manifest";

/// Various utilities for managing file and folder names that RainDB uses.
pub(crate) struct FileNameHandler {
    db_path: String,
}

impl FileNameHandler {
    /// Create a new instance of the [`FileNameHandler`].
    pub fn new(db_path: String) -> Self {
        FileNameHandler { db_path }
    }

    /// Resolve the path to the write-ahead log.
    pub fn get_wal_path(&self, wal_number: u64) -> PathBuf {
        let mut buf = PathBuf::from(&self.db_path);
        buf.push(WAL_DIR);
        buf.set_file_name(format!("wal-{number}", number = wal_number));
        buf.set_extension(WAL_EXT);

        buf
    }

    /// Resolve the path to a specific table file.
    pub fn get_table_file_name(&self, file_number: u64) -> PathBuf {
        let mut buf = PathBuf::from(&self.db_path);
        buf.push(DATA_DIR);
        buf.set_file_name(file_number.to_string());
        buf.set_extension(TABLE_EXT);

        buf
    }

    /// Resolve the path to the manifest file.
    pub fn get_manifest_file_name(&self, manifest_number: u64) -> PathBuf {
        let mut buf = PathBuf::from(&self.db_path);
        buf.set_file_name(format!("MANIFEST-{number}", number = manifest_number));
        buf.set_extension(MANIFEST_FILE_EXT);

        buf
    }
}
