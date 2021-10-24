/*!
This module contains utilities for managing file names used by the database.

Files are rooted at the `db_path` as provided in the [database instantiation options](crate::db::DbOptions).

Files (and their name formats) used by the database are as follows:

- Database lock file: `./LOCK`
- Write-ahead logs: `./wal/wal.log`
- SSTable files: `./data/.sst`
- MANIFEST files: `./MANIFEST`
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

/// Suffix for SSTable files.
pub(crate) const SST_EXT: &str = "sst";

pub(crate) struct FileNameHandler {
    db_path: String,
}

impl FileNameHandler {
    pub fn new(db_path: String) -> Self {
        FileNameHandler { db_path }
    }

    pub fn get_wal_path(&self) -> PathBuf {
        let mut buf = PathBuf::from(&self.db_path);
        buf.push(WAL_DIR);
        buf.set_file_name("wal");
        buf.set_extension(WAL_EXT);

        buf
    }
}
