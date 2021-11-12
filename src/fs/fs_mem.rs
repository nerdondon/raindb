/*!
This module contains a wrapper for an in-memory file system implementation.
*/

use parking_lot::RwLock;
use std::collections::HashMap;
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use super::traits::{FileSystem, RandomAccessFile, ReadonlyRandomAccessFile};

/// File system implementation that is backed by memory.
pub struct InMemoryFileSystem {
    /**
    The files on the file system.

    Currently using the familiar Arc<RwLock<>> construct but maybe look into replacing with an
    actual concurrent hashmap. Low priority since in-mem is mainly for testing.
    */
    files: Arc<RwLock<HashMap<PathBuf, LockableInMemoryFile>>>,
}

impl InMemoryFileSystem {
    /// Create a new instance of the in-memory file system.
    pub fn new() -> Self {
        InMemoryFileSystem {
            files: Arc::new(RwLock::new(HashMap::new())),
        }
    }
}

impl Default for InMemoryFileSystem {
    fn default() -> Self {
        Self::new()
    }
}

/// Private methods.
impl InMemoryFileSystem {
    /**
    Open the file at the specified `path`.

    This method returns the internal representation of an [in-memory file](self::InMemoryFile)
    wrapped with a lock and is meant for use as an utility since the trait implementations return
    super-types.
    */
    fn open_mem_file(&self, path: &Path) -> io::Result<LockableInMemoryFile> {
        let files = self.files.read();
        match files.get(path) {
            Some(file) => Ok(file.clone()),
            None => {
                let error_message = format!(
                    "Could not find the file with path {path}",
                    path = path.to_string_lossy()
                );
                Err(io::Error::new(io::ErrorKind::NotFound, error_message))
            }
        }
    }
}

impl FileSystem for InMemoryFileSystem {
    fn get_name(&self) -> String {
        "InMemoryFileSystem".to_string()
    }

    fn create_dir(&mut self, _path: &Path) -> io::Result<()> {
        Ok(())
    }

    fn create_dir_all(&mut self, _path: &Path) -> io::Result<()> {
        Ok(())
    }

    fn list_dir(&self, path: &Path) -> io::Result<Vec<PathBuf>> {
        let files = self.files.read();
        // Iterate the file system map and get all keys that begin with the specified path
        let mut children: Vec<PathBuf> = files
            .keys()
            .filter(|key| key.starts_with(path))
            .cloned()
            .collect();
        children.sort();

        Ok(children)
    }

    fn open_file(&self, path: &Path) -> io::Result<Box<dyn ReadonlyRandomAccessFile>> {
        Ok(Box::new(self.open_mem_file(path)?))
    }

    fn rename(&self, from: &Path, to: &Path) -> io::Result<()> {
        let mut files = self.files.write();
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
        let mut files = self.files.write();
        match files.get_mut(path) {
            Some(file) => Ok(Box::new(file.clone())),
            None => {
                let new_file = LockableInMemoryFile::new();
                files.insert(path.to_path_buf(), new_file);
                let file = files.get(path).unwrap();
                Ok(Box::new(file.clone()))
            }
        }
    }

    fn remove_file(&self, path: &Path) -> io::Result<()> {
        let mut files = self.files.write();
        match files.remove(path) {
            Some(_removed_file) => Ok(()),
            None => {
                let error_message = format!(
                    "Could not find the file with path {path}",
                    path = path.to_string_lossy()
                );
                Err(io::Error::new(io::ErrorKind::NotFound, error_message))
            }
        }
    }

    fn get_file_size(&self, path: &Path) -> io::Result<u64> {
        Ok(self.open_mem_file(path)?.len()?)
    }
}

/// Represents a file in the in-memory file system.
#[derive(Clone)]
struct InMemoryFile {
    /// The contents of the file.
    contents: Vec<u8>,
    /// The current position in the file.
    cursor: u64,
}

impl InMemoryFile {
    /// Create an instance of [`InMemoryFile`](self::InMemoryFile)
    fn new() -> Self {
        Self {
            contents: vec![],
            cursor: 0,
        }
    }

    /// Get the size of the file in bytes.
    pub fn len(&self) -> u64 {
        self.contents.len() as u64
    }
}

struct LockableInMemoryFile(Arc<RwLock<InMemoryFile>>);

impl LockableInMemoryFile {
    /// Create an instance of [`LockableInMemoryFile`](self::LockableInMemoryFile).
    fn new() -> Self {
        LockableInMemoryFile(Arc::new(RwLock::new(InMemoryFile::new())))
    }

    /// Make an `Arc` clone of the file.
    fn clone(&self) -> LockableInMemoryFile {
        LockableInMemoryFile(Arc::clone(&self.0))
    }
}

impl Read for LockableInMemoryFile {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let buf_length = buf.len();
        if buf_length == 0 {
            return Ok(0);
        }

        let file = self.0.read();
        buf.copy_from_slice(&file.contents[..buf_length]);

        Ok(buf_length)
    }
}

impl Write for LockableInMemoryFile {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let buf_length = buf.len();
        if buf_length == 0 {
            return Ok(0);
        }

        let mut file = self.0.write();
        // `copy_from_slice` only allows copying between slices of the same length
        let content_length = file.contents.len();
        file.contents.copy_from_slice(&buf[0..content_length]);
        // Append the rest of the buffer
        file.contents.extend_from_slice(&buf[content_length..]);

        Ok(buf_length)
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl Seek for LockableInMemoryFile {
    /**
    Seek to an offset, in bytes, in a stream.

    To keep things simple, `SeekFrom::End` is not implemented and `SeekFrom::Current` only accepts
    positive integers.
    */
    fn seek(&mut self, pos: io::SeekFrom) -> io::Result<u64> {
        let mut file = self.0.write();

        let mut offset: u64;
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

                offset = (off as u64) + file.cursor;
            }
            SeekFrom::End(_) => {
                unimplemented!("Not used as part of any database operations.");
            }
        };

        // Truncate `offset` if it is too long. We only allow seeking to the end of the file.
        offset = if offset > file.len() {
            file.len()
        } else {
            offset
        };

        file.cursor = offset;
        Ok(offset)
    }
}

impl ReadonlyRandomAccessFile for LockableInMemoryFile {
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

        let file = self.0.read();
        let end_of_copy_range = if file.contents.len() > (offset + buf.len()) {
            offset + buf.len()
        } else {
            file.contents.len()
        };

        // `copy_from_slice` requires that slices are of the same length or it panics. this is the
        // reason for the range syntax below.
        (&mut buf[..offset]).copy_from_slice(&file.contents[offset..end_of_copy_range]);

        Ok(buf_length)
    }

    fn len(&self) -> io::Result<u64> {
        Ok(self.0.read().len() as u64)
    }
}

impl RandomAccessFile for LockableInMemoryFile {
    fn append(&mut self, buf: &[u8]) -> io::Result<usize> {
        let mut file = self.0.write();
        file.contents.extend_from_slice(buf);
        Ok(buf.len())
    }
}
