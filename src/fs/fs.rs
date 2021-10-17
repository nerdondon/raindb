/*!
File system wrappers to enable different implementations of file systems to be used.

The primary purpose of this wrapper to enable testing with temp file based or in-memory file
systems.
*/

use core::fmt::Debug;
use std::fs::File;
use std::io::{Read, Result, Write};
use std::path::{Path, PathBuf};

/// Supertrait that wraps a source of binary content that is both readable and writeable.
pub trait ReadWriteable: Read + Write {}

impl ReadWriteable for File {}

pub trait FileSystem {
    /// Return the name of file system wrapper being used.
    fn get_name(&self) -> String;

    /// Creates a new, empty directory at the provided path.
    fn create_dir(&mut self, path: &Path) -> Result<()>;

    /// Recursively create a directory and all of its parent components if they are missing.
    fn create_dir_all(&mut self, path: &Path) -> Result<()>;

    // List the contents of the given `path`.
    fn list_dir(&self, path: &Path) -> Result<Vec<PathBuf>>;

    /// Open a file in read-only mode.
    fn open_file(&self, path: &Path) -> Result<Box<dyn Read>>;

    /**
    Rename a file or directory. For files, it will attempt to replace a file if it already exists
    at the destination name.

    This corresponds to the [`std::fs::rename`] function when used for disk-based implementations.
    It has the same caveats for platform-specific behavior.
    */
    fn rename(&self, from: &Path, to: &Path) -> Result<()>;

    /**
    Open a file in read/write mode.

    This function will create the file if it doesn't exist. Critically, it does not truncate an
    existing file and sets the append mode.
    */
    fn create_file(&self, path: &Path) -> Result<Box<dyn ReadWriteable>>;

    /// Remove a file from the filesystem.
    fn remove_file(&self, path: &Path) -> Result<()>;

    /// Get the size of the file or directory at the specified path.
    fn get_file_size(&self, path: &Path) -> Result<u64>;
}

impl Debug for dyn FileSystem {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.get_name())
    }
}
