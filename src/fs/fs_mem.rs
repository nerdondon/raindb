/*!
This module contains a wrapper for an in-memory file system implementation.
*/

use parking_lot::RwLock;
use std::collections::HashMap;
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use super::fs::{FileSystem, ReadWriteable};

/// File system implementation that is backed by memory.
pub struct InMemoryFileSystem {
    /**
    The files on the file system.

    Currently using the familiar Arc<RwLock<>> construct but maybe look into replacing with an
    actual concurrent hashmap. Low priority since in-mem is mainly for testing.
    */
    pub files: Arc<RwLock<HashMap<PathBuf, InMemoryFile>>>,
}

impl InMemoryFileSystem {
    /// Create a new instance of the in-memory file system.
    pub fn new() -> Self {
        InMemoryFileSystem {
            files: Arc::new(RwLock::new(HashMap::new())),
        }
    }
}

/// Private methods.
impl InMemoryFileSystem {
    /**
    Open the file at the specified `path`.

    This method returns the internal representation of an [in-memory file](self::InMemoryFile) and
    is meant for use as an utility since the trait implementations return super-types.
    */
    fn open_mem_file(&self, path: &Path) -> io::Result<InMemoryFile> {
        let files = self.files.read();
        match files.get_mut(path) {
            Some(file) => {
                return Ok(file.clone());
            }
            None => {
                let error_message = format!(
                    "Could not find the file with path {path}",
                    path = path.to_string_lossy()
                );
                return Err(io::Error::new(io::ErrorKind::NotFound, error_message));
            }
        }
    }
}

impl FileSystem for InMemoryFileSystem {
    fn get_name(&self) -> String {
        return "InMemoryFileSystem".to_string();
    }

    fn create_dir(&mut self, path: &Path) -> io::Result<()> {
        Ok(())
    }

    fn create_dir_all(&mut self, path: &Path) -> io::Result<()> {
        Ok(())
    }

    fn list_dir(&self, path: &Path) -> io::Result<Vec<PathBuf>> {
        let files = self.files.read();
        // Iterate the file system map and get all keys that begin with the specified path
        let mut children: Vec<PathBuf> = files
            .keys()
            .filter(|key| key.starts_with(path))
            .map(|key| key.clone())
            .collect();
        children.sort();

        Ok(children)
    }

    fn open_file(&self, path: &Path) -> io::Result<Box<dyn Read>> {
        Ok(Box::new(self.open_mem_file(path)?))
    }

    fn rename(&self, from: &Path, to: &Path) -> io::Result<()> {
        let files = self.files.write();
        match files.remove(from) {
            Some(file) => {
                files.insert(to.to_path_buf(), file);
            }
            None => {
                let error_message = format!(
                    "Could not find the file with path {path}",
                    path = from.to_string_lossy()
                );
                return Err(io::Error::new(io::ErrorKind::NotFound, error_message));
            }
        }

        Ok(())
    }

    fn create_file(&self, path: &Path) -> io::Result<Box<dyn ReadWriteable>> {
        let files = self.files.read();
        match files.get_mut(path) {
            Some(file) => {
                return Ok(Box::new(file.clone()));
            }
            None => {
                let error_message = format!(
                    "Could not find the file with path {path}",
                    path = path.to_string_lossy()
                );
                return Err(io::Error::new(io::ErrorKind::NotFound, error_message));
            }
        }
    }

    fn remove_file(&self, path: &Path) -> io::Result<()> {
        let files = self.files.write();
        match files.remove(path) {
            Some(file) => {
                return Ok(());
            }
            None => {
                let error_message = format!(
                    "Could not find the file with path {path}",
                    path = path.to_string_lossy()
                );
                return Err(io::Error::new(io::ErrorKind::NotFound, error_message));
            }
        }
    }

    fn get_file_size(&self, path: &Path) -> io::Result<u64> {
        Ok(self.open_mem_file(path)?.contents.read().len() as u64)
    }
}

/// Represents a file in the in-memory file system.
#[derive(Clone)]
struct InMemoryFile {
    /// The contents of the "file".
    contents: Arc<RwLock<Vec<u8>>>,
}

impl InMemoryFile {
    /// Create an instance of [`InMemoryFile`](self::InMemoryFile)
    fn new(contents: Arc<RwLock<Vec<u8>>>) -> Self {
        Self {
            contents: Arc::new(RwLock::new(vec![])),
        }
    }

    /// Get the size of the file in bytes.
    pub fn len(&self) -> u64 {
        let contents = self.contents.clone().read();
        contents.len() as u64
    }
}

impl Read for InMemoryFile {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let buf_length = buf.len();
        if buf_length == 0 {
            return Ok(0);
        }

        let file_contents = self.contents.read();
        buf.copy_from_slice(&file_contents[..buf_length]);

        Ok(buf_length)
    }
}

impl Write for InMemoryFile {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let buf_length = buf.len();
        if buf_length == 0 {
            return Ok(0);
        }

        let file_contents = self.contents.write();
        // `copy_from_slice` only allows copying between slices of the same length
        file_contents.copy_from_slice(&buf[..file_contents.len()]);
        // append the rest of the buffer
        file_contents.extend_from_slice(&buf[file_contents.len()..]);

        Ok(buf_length)
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl ReadWriteable for InMemoryFile {}
