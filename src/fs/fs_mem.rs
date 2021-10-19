/*!
This module contains a wrapper for an in-memory file system implementation.
*/

use parking_lot::RwLock;
use std::collections::HashMap;
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use super::fs::{FileSystem, RandomAccessFile};

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

    fn create_file(&self, path: &Path) -> io::Result<Box<dyn RandomAccessFile>> {
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
    /// The contents of the file.
    contents: Arc<RwLock<Vec<u8>>>,
    /// The current position in the file.
    cursor: u64,
}

impl InMemoryFile {
    /// Create an instance of [`InMemoryFile`](self::InMemoryFile)
    fn new(contents: Arc<RwLock<Vec<u8>>>) -> Self {
        Self {
            contents: Arc::new(RwLock::new(vec![])),
            cursor: 0,
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
        // Append the rest of the buffer
        file_contents.extend_from_slice(&buf[file_contents.len()..]);

        Ok(buf_length)
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl Seek for InMemoryFile {
    /**
    Seek to an offset, in bytes, in a stream.

    To keep things simple, `SeekFrom::End` is not implemented and `SeekFrom::Current` only accepts
    positive integers.
    */
    fn seek(&mut self, pos: io::SeekFrom) -> io::Result<u64> {
        let mut offset: u64 = 0;
        match pos {
            SeekFrom::Start(off) => {
                offset = off;
            }
            SeekFrom::Current(off) => {
                if off < 0 {
                    let error_message = format!(
                        "Only integers >= 0 are accepted. The passed in offset was {}.",
                        off
                    );
                    return Err(io::Error::new(io::ErrorKind::InvalidInput, error_message));
                }

                offset = (off as u64) + self.cursor;
            }
            SeekFrom::End(_) => {
                unimplemented!("Not used as part of any database operations.");
            }
        };

        // Truncate `offset` if it is too long. We only allow seeking to the end of the file.
        offset = if offset > self.len() {
            self.len()
        } else {
            offset
        };

        self.cursor = offset;
        Ok(offset)
    }
}

impl RandomAccessFile for InMemoryFile {
    fn read_from(&self, buf: &mut [u8], offset: usize) -> io::Result<usize> {
        let buf_length = buf.len();
        if buf_length == 0 {
            return Ok(0);
        }

        if offset > buf.len() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "The provided offset goes beyond the end of the file.",
            ));
        }

        let file_contents = self.contents.read();
        let end_of_copy_range = if file_contents.len() > (offset + buf.len()) {
            offset + buf.len()
        } else {
            file_contents.len()
        };

        // `copy_from_slice` requires that slices are of the same length or it panics. this is the
        // reason for the range syntax below.
        (&mut buf[..offset]).copy_from_slice(&file_contents[offset..end_of_copy_range]);

        Ok(buf_length)
    }

    fn append(&mut self, buf: &[u8]) -> io::Result<usize> {
        let content = self.contents.write();
        content.extend_from_slice(&buf);
        Ok(buf.len())
    }
}
