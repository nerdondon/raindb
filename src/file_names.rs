/*!
This module contains utilities for managing file names used by the database.

Files are rooted at the `db_path` as provided in the [database instantiation options](crate::DbOptions).

Files (and their name formats) used by the database are as follows:

- Database lock file: `./LOCK`
- Write-ahead logs: `./wal/wal-[0-9]+.log`
- Table files: `./data/[0-9]+.rdb`
- Manifest files: `./MANIFEST-[0-9]+.manifest`
- CURRENT manifest pointer file: `CURRENT`
- Temp files: `[0-9]+.dbtemp`
*/

use std::path::{Path, PathBuf};

use crate::errors::{RainDBError, RainDBResult};

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

/// Name of the *CURRENT* file.
pub(crate) const CURRENT_FILE_NAME: &str = "CURRENT";

/// The temp file extension.
pub(crate) const TEMP_FILE_EXT: &str = "dbtemp";

/**
Enum of file types used in RainDB.

If appropriate, variants will hold the file number parsed from the file path.
*/
#[derive(Debug, Eq, PartialEq)]
pub(crate) enum ParsedFileType {
    WriteAheadLog(u64),
    DBLockFile,
    TableFile(u64),
    /// Also known as a descriptor file in LevelDB.
    ManifestFile(u64),
    CurrentFile,
    TempFile(u64),
}

/// Various utilities for managing file and folder names that RainDB uses.
#[derive(Debug)]
pub(crate) struct FileNameHandler {
    db_path: String,
}

/// Crate-only methods
impl FileNameHandler {
    /// Create a new instance of the [`FileNameHandler`].
    pub(crate) fn new(db_path: String) -> Self {
        FileNameHandler { db_path }
    }

    /// Get the path to the database directory as a [`PathBuf`].
    pub(crate) fn get_db_path(&self) -> PathBuf {
        PathBuf::from(&self.db_path)
    }

    /// Resolve the path to the write-ahead log directory.
    pub(crate) fn get_wal_dir(&self) -> PathBuf {
        let mut buf = PathBuf::from(&self.db_path);
        buf.push(WAL_DIR);

        buf
    }

    /// Resolve the path to the write-ahead log.
    pub(crate) fn get_wal_file_path(&self, wal_number: u64) -> PathBuf {
        let mut buf = self.get_wal_dir();
        buf.push(format!("wal-{wal_number}"));
        buf.set_extension(WAL_EXT);

        buf
    }

    /// Resolve the path to the data file storage directory.
    pub(crate) fn get_data_dir(&self) -> PathBuf {
        let mut buf = PathBuf::from(&self.db_path);
        buf.push(DATA_DIR);

        buf
    }

    /// Resolve the path to a specific table file.
    pub(crate) fn get_table_file_path(&self, file_number: u64) -> PathBuf {
        let mut buf = self.get_data_dir();
        buf.push(file_number.to_string());
        buf.set_extension(TABLE_EXT);

        buf
    }

    /**
    Resolve the path to the manifest file.

    # Legacy

    This is synonomous to LevelDB's `leveldb::DescriptorFileName` method.
    */
    pub(crate) fn get_manifest_file_path(&self, manifest_number: u64) -> PathBuf {
        let mut buf = PathBuf::from(&self.db_path);
        buf.push(format!("MANIFEST-{manifest_number}"));
        buf.set_extension(MANIFEST_FILE_EXT);

        buf
    }

    /// Resolve the path to the `CURRENT` file.
    pub(crate) fn get_current_file_path(&self) -> PathBuf {
        let mut buf = PathBuf::from(&self.db_path);
        buf.push(CURRENT_FILE_NAME);

        buf
    }

    /// Resolve the path to a temp file.
    pub(crate) fn get_temp_file_path(&self, file_number: u64) -> PathBuf {
        let mut buf = PathBuf::from(&self.db_path);
        buf.push(file_number.to_string());
        buf.set_extension(TEMP_FILE_EXT);

        buf
    }

    /// Resolve the path to the LOCK file.
    pub(crate) fn get_lock_file_path(&self) -> PathBuf {
        let mut buf = PathBuf::from(&self.db_path);
        buf.push(LOCK_FILE);

        buf
    }

    /// Attempts to determine the RainDB file type and file number (if any) from the provided path.
    pub(crate) fn get_file_type_from_name(file_path: &Path) -> RainDBResult<ParsedFileType> {
        let file_name = if let Some(file_name) = file_path.file_name() {
            file_name
        } else {
            return Err(RainDBError::PathResolution(format!(
                "The provided file path is not a recognized RainDB file type. Provided path: {:?}.",
                file_path
            )));
        };

        if file_name == CURRENT_FILE_NAME {
            return Ok(ParsedFileType::CurrentFile);
        }

        if file_name == LOCK_FILE {
            return Ok(ParsedFileType::DBLockFile);
        }

        if let Some(file_extension) = file_path.extension() {
            let file_stem_err_msg = format!(
                "The provided file stem is not a recognized RainDB file name pattern. Provided \
                path: {:?}.",
                file_path
            );
            let file_stem = match file_path.file_stem() {
                Some(stem_os_str) => match stem_os_str.to_str() {
                    Some(stem) => stem,
                    None => return Err(RainDBError::PathResolution(file_stem_err_msg)),
                },
                None => return Err(RainDBError::PathResolution(file_stem_err_msg)),
            };

            if file_extension == MANIFEST_FILE_EXT {
                let file_number: u64 = FileNameHandler::parse_file_number(file_stem, "MANIFEST-")?;
                return Ok(ParsedFileType::ManifestFile(file_number));
            }

            if file_extension == WAL_EXT {
                let file_number: u64 = FileNameHandler::parse_file_number(file_stem, "wal-")?;
                return Ok(ParsedFileType::WriteAheadLog(file_number));
            }

            if file_extension == TABLE_EXT {
                let file_number: u64 = FileNameHandler::parse_file_number(file_stem, "")?;
                return Ok(ParsedFileType::TableFile(file_number));
            }

            if file_extension == TEMP_FILE_EXT {
                let file_number: u64 = FileNameHandler::parse_file_number(file_stem, "")?;
                return Ok(ParsedFileType::TempFile(file_number));
            }
        }

        Err(RainDBError::PathResolution(format!(
            "The provided file path is not a recognized RainDB file type. Provided path: {:?}.",
            file_path
        )))
    }
}

/// Private methods
impl FileNameHandler {
    /// Attempts to parse a file number from the provided file name.
    fn parse_file_number(file_name: &str, prefix: &str) -> RainDBResult<u64> {
        let file_name_err_msg = format!(
            "The provided file name is not a recognized RainDB file name pattern. Provided \
            path: {:?}.",
            file_name
        );

        match file_name.strip_prefix(prefix) {
            Some(maybe_file_num) => match maybe_file_num.parse::<u64>() {
                Ok(file_num) => Ok(file_num),
                Err(_parse_err) => Err(RainDBError::PathResolution(file_name_err_msg)),
            },
            None => Err(RainDBError::PathResolution(file_name_err_msg)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pretty_assertions::assert_eq;

    #[test]
    fn file_name_handler_gets_wal_paths_correctly() {
        let db_path = "/storm/system".to_string();
        let handler = FileNameHandler::new(db_path);

        let wal_dir = handler.get_wal_dir();
        assert!(wal_dir.ends_with("wal"));

        let wal_path = handler.get_wal_file_path(43);
        assert!(
            wal_path.starts_with(&wal_dir),
            "The generated path should be prefixed with the same generated path for the parent \
            directory."
        );
        assert_eq!(wal_path.file_name().unwrap(), "wal-43.log");
        assert_eq!(wal_path.extension().unwrap(), "log");
    }

    #[test]
    fn file_name_handler_gets_data_paths_correctly() {
        let db_path = "/storm/system".to_string();
        let handler = FileNameHandler::new(db_path);

        let data_dir = handler.get_data_dir();
        assert!(data_dir.ends_with("data"));

        let table_path = handler.get_table_file_path(43);
        assert!(
            table_path.starts_with(&data_dir),
            "The generated path should be prefixed with the same generated path for the parent \
            directory."
        );
        assert_eq!(table_path.file_name().unwrap(), "43.rdb");
        assert_eq!(table_path.extension().unwrap(), "rdb");
    }

    #[test]
    fn file_name_handler_gets_root_paths_correctly() {
        let db_path = "/storm/system".to_string();
        let handler = FileNameHandler::new(db_path);
        let saved_db_path = handler.get_db_path();

        let manifest_path = handler.get_manifest_file_path(43);
        assert!(
            manifest_path.starts_with(&saved_db_path),
            "The generated path should be prefixed with the same generated path for the parent \
            directory."
        );
        assert_eq!(manifest_path.file_name().unwrap(), "MANIFEST-43.manifest");

        let current_path = handler.get_current_file_path();
        assert!(
            current_path.starts_with(&saved_db_path),
            "The generated path should be prefixed with the same generated path for the parent \
            directory."
        );
        assert_eq!(current_path.file_name().unwrap(), "CURRENT");

        let temp_path = handler.get_temp_file_path(43);
        assert!(
            temp_path.starts_with(&saved_db_path),
            "The generated path should be prefixed with the same generated path for the parent \
            directory."
        );
        assert_eq!(temp_path.file_name().unwrap(), "43.dbtemp");

        let lock_path = handler.get_lock_file_path();
        assert!(
            lock_path.starts_with(&saved_db_path),
            "The generated path should be prefixed with the same generated path for the parent \
            directory."
        );
        assert_eq!(lock_path.file_name().unwrap(), "LOCK");
    }

    #[test]
    fn parser_can_correctly_parse_valid_file_paths() {
        let valid_paths = vec![
            ("wal-100.log", ParsedFileType::WriteAheadLog(100)),
            ("wal-0.log", ParsedFileType::WriteAheadLog(0)),
            ("LOCK", ParsedFileType::DBLockFile),
            ("43.rdb", ParsedFileType::TableFile(43)),
            (
                "1238097123981723.rdb",
                ParsedFileType::TableFile(1238097123981723),
            ),
            ("MANIFEST-1337.manifest", ParsedFileType::ManifestFile(1337)),
            ("MANIFEST-55.manifest", ParsedFileType::ManifestFile(55)),
            (
                "18446744073709551615.dbtemp",
                ParsedFileType::TempFile(18446744073709551615),
            ),
        ];

        for (path, expected) in valid_paths {
            let file_type = FileNameHandler::get_file_type_from_name(&PathBuf::from(path)).unwrap();
            assert_eq!(file_type, expected, "{path} should be parsed correctly.");
        }
    }

    #[test]
    fn parser_rejects_invalid_paths() {
        let invalid_paths = vec![
            "",
            "foo",
            "foo.log",
            "wal-foo.log",
            "123-wal-123.log",
            "wal-18446744073709551616.log",
            "wal-184467440737095516150.log",
            ".log",
            "wal-1231x.log",
            "manifest",
            "MANIFEST-.manifest",
            "MANIFEST-3x.manifest",
            "XMANIFEST-3.manifest",
            "LOC",
            "LOCKx",
            "CURR",
            "CURRENTx",
            "100",
            "100.",
            "100.rd",
        ];

        for path in invalid_paths {
            let file_type_result = FileNameHandler::get_file_type_from_name(&PathBuf::from(path));
            assert!(
                file_type_result.is_err(),
                "{path} should cause the parser to raise an exception."
            );
        }
    }
}
