/*!
File system wrappers to enable different implementations of file systems to be used.

The primary purpose of this wrapper to enable testing with temp file based or in-memory file
systems. The main implementation will be backed by `std::fs::File`.

TODO: write wrapper for `std::fs::File` if we end up actually needing an in-memory representation.
*/

use core::fmt::Debug;
use std::fs::{self, File, OpenOptions};
use std::io::Result;
use std::path::Path;

use tempfile::TempDir;

pub trait FileSystem {
    fn get_name(&self) -> String;

    /// Creates a new, empty directory at the provided path.
    fn create_dir(&mut self, path: &Path) -> Result<()>;

    /// Recursively create a directory and all of its parent components if they are missing.
    fn create_dir_all(&mut self, path: &Path) -> Result<()>;

    /// Open a file in read-only mode.
    fn open_file(&self, path: &Path) -> Result<File>;

    /**
    Open in read/write mode.

    This function will create the file if it doesn't exist. Critically, it does not truncate an
    existing file and sets the append mode.
    */
    fn create_file(&self, path: &Path) -> Result<File>;

    /// Remove a file from the filesystem.
    fn remove_file(&self, path: &Path) -> Result<()>;
}

impl Debug for dyn FileSystem {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.get_name())
    }
}

/// Wrapper on top of `std::fs` and `std:fs::File`.
pub(crate) struct OsFileSystem {}

impl OsFileSystem {
    pub fn new() -> Self {
        OsFileSystem {}
    }
}

impl FileSystem for OsFileSystem {
    fn get_name(&self) -> String {
        return "OsFileSystem".to_owned();
    }

    fn create_dir(&mut self, path: &Path) -> Result<()> {
        fs::create_dir(path)
    }

    fn create_dir_all(&mut self, path: &Path) -> Result<()> {
        fs::create_dir_all(path)
    }

    fn open_file(&self, path: &Path) -> Result<File> {
        File::open(path)
    }

    fn create_file(&self, path: &Path) -> Result<File> {
        OpenOptions::new()
            .create(true)
            .read(true)
            .append(true)
            .open(path)
    }

    fn remove_file(&self, path: &Path) -> Result<()> {
        fs::remove_file(path)
    }
}

/**
A file system implmentation built on `tempfiles` crate structures.

Really only directory creation is backed by `tempfiles` to take advantage of the auto-cleanup
mechanism. File creation is not used so that control over file naming is retained.
*/
pub(crate) struct TmpFileSystem {
    temp_dirs: Vec<TempDir>,
}

impl TmpFileSystem {
    pub fn new() -> Self {
        TmpFileSystem { temp_dirs: vec![] }
    }
}

impl FileSystem for TmpFileSystem {
    fn get_name(&self) -> String {
        return "TmpFileSystem".to_owned();
    }

    fn create_dir(&mut self, path: &Path) -> Result<()> {
        self.temp_dirs.push(TempDir::new_in(path)?);

        Ok(())
    }

    fn create_dir_all(&mut self, path: &Path) -> Result<()> {
        unimplemented!("Not supported by tempfile crate.")
    }

    fn open_file(&self, path: &Path) -> Result<File> {
        File::open(path)
    }

    fn create_file(&self, path: &Path) -> Result<File> {
        OpenOptions::new()
            .create(true)
            .read(true)
            .append(true)
            .open(path)
    }

    fn remove_file(&self, path: &Path) -> Result<()> {
        fs::remove_file(path)
    }
}
