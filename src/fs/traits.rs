/*!
File system wrappers to enable different implementations of file systems to be used.

The primary purpose of this wrapper to enable testing with temp file based or in-memory file
systems.
*/

use core::fmt::Debug;
use std::io::{Read, Result, Seek, Write};
use std::path::{Path, PathBuf};

/**
Supertrait that wraps a source of binary content that is readonly and can read from arbitrary offets
into the content.
*/
pub trait ReadonlyRandomAccessFile: Read + Seek + Send + Sync {
    /// Read a number of bytes starting from a given offset.
    fn read_from(&self, buf: &mut [u8], offset: usize) -> Result<usize>;

    /// Get the length of the file.
    fn len(&self) -> Result<u64>;

    /// Return true if the file is empty. Otherwise, false.
    fn is_empty(&self) -> Result<bool> {
        Ok(self.len()? == 0)
    }
}

/**
Supertrait that wraps a source of binary content that is readable and writable and can operate on
arbitrary offets into the content.
*/
pub trait RandomAccessFile: ReadonlyRandomAccessFile + Write {
    /// Append the buffer `buf` to the end of this writer.
    fn append(&mut self, buf: &[u8]) -> Result<usize>;
}

/// An interface for common file system operations.
pub trait FileSystem: Send + Sync {
    /// Return the name of file system wrapper being used.
    fn get_name(&self) -> String;

    /// Creates a new, empty directory at the provided path.
    fn create_dir(&self, path: &Path) -> Result<()>;

    /// Recursively create a directory and all of its parent components if they are missing.
    fn create_dir_all(&self, path: &Path) -> Result<()>;

    /// List the contents of the given `path`.
    fn list_dir(&self, path: &Path) -> Result<Vec<PathBuf>>;

    /// Open a file in read-only mode.
    fn open_file(&self, path: &Path) -> Result<Box<dyn ReadonlyRandomAccessFile>>;

    /**
    Rename a file or directory. For files, it will attempt to replace a file if it already exists
    at the destination name.

    This corresponds to the [`std::fs::rename`] function when used for disk-based implementations.
    It has the same caveats for platform-specific behavior.
    */
    fn rename(&self, from: &Path, to: &Path) -> Result<()>;

    /**
    Open a file in read/write mode.

    This function will create the file if it doesn't exist. Setting the `append` parameter to true
    will start appending to an existing file, otherwise an existing file is truncated to length 0.
    */
    fn create_file(&self, path: &Path, append: bool) -> Result<Box<dyn RandomAccessFile>>;

    /// Remove a file from the filesystem.
    fn remove_file(&self, path: &Path) -> Result<()>;

    /**
    Remove a directory.

    This will throw if the directory is not empty. This is a thin abstraction over
    [`std::fs::remove_dir`].
    */
    fn remove_dir(&self, path: &Path) -> Result<()>;

    /**
    Remove a directory and its contents.

    This is meant to provide an abstraction over [`std::fs::remove_dir_all`].
    */
    fn remove_dir_all(&self, path: &Path) -> Result<()>;

    /// Get the size of the file or directory at the specified path.
    fn get_file_size(&self, path: &Path) -> Result<u64>;

    /**
    Check if a path is a directory.

    This is meant to provide an abstraction over the built-in [`std::fs::metadata`] function.
    */
    fn is_dir(&self, path: &Path) -> Result<bool>;

    /**
    Place an exclusive lock on the file at the specified path.

    This lock can only be relied on to be advisory. For POSIX, an `flock()` is used.

    # Legacy

    The use of the `flock` instead of `fcntl` in POSIX environments differs from LevelDB (uses
    `fcntl`). This is primarily because of laziness (can't say I'm not honest) since the `fs2`
    crate from Rust uses `flock`. I have a blog post [here] with some notes about the differences.

    [here]: https://www.nerdondon.com/posts/2021-10-17-flock-vs-fcntl/
    */
    fn lock_file(&self, path: &Path) -> Result<FileLock>;
}

impl Debug for dyn FileSystem {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.get_name())
    }
}

/**
An opaque handle for locked files.

The underlying file will be dropped when the handle is dropped.
*/
pub struct FileLock {
    inner: Box<dyn UnlockableFile>,
}

impl FileLock {
    /// Create a new instance of [`FileLock`].
    pub fn new(file: Box<dyn UnlockableFile>) -> Self {
        Self { inner: file }
    }
}

impl Drop for FileLock {
    fn drop(&mut self) {
        if let Err(unlock_error) = self.inner.unlock() {
            log::error!(
                "There was an error trying to release the database lock during shutdown. Error: \
                {error}",
                error = unlock_error
            );
        }
    }
}

/// A file that can be unlocked.
pub trait UnlockableFile {
    /// Unlock the file.
    fn unlock(&self) -> Result<()>;
}
