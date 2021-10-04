/*!
This module contains utilities for managing file names used by the database.

Files are rooted at the `db_path` as provided in the [database instantiation options](crate::db::DbOptions).

Files (and their name formats) used by the database are as follows:

- Write-ahead logs: `./wal/wal.log`
- SSTable files: `./data/.sst`
- MANIFEST files: `./MANIFEST`
*/

use std::path::PathBuf;

/// The directory name that write-ahead logs will be stored in.
pub(crate) const WAL_DIR: String = "wal".to_string();

/// Suffix for write-ahead log files.
pub(crate) const WAL_EXT: String = "log".to_string();

/// The directory name that data files will be stored in.
pub(crate) const DATA_DIR: String = "data".to_string();

/// Suffix for SSTable files.
pub(crate) const SST_EXT: String = "sst".to_string();

pub(crate) struct FileNameHandler {
    db_path: String,
}

impl FileNameHandler {
    pub fn new(db_path: String) -> Self {
        FileNameHandler { db_path }
    }

    pub fn get_wal_path(&self) -> PathBuf {
        let buff = PathBuf::from(&self.db_path);
        buff.push(WAL_DIR);
        buff.set_file_name("wal");
        buff.set_extension(WAL_EXT);

        buff
    }
}
