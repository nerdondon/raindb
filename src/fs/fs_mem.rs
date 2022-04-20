/*!
This module contains a wrapper for an in-memory file system implementation.
*/

use parking_lot::RwLock;
use std::collections::{HashMap, HashSet};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use super::traits::{FileSystem, RandomAccessFile, ReadonlyRandomAccessFile, UnlockableFile};
use super::FileLock;

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
            Some(file) => {
                // Make sure to reset the cursor on a newly opened file.
                // TODO: Ideally, we make sure to set a different cursor for each file handle but
                // this memory environment is only used for tests where multiple handles for a file
                // are not held at the same time.
                file.0.write().cursor = 0;

                Ok(file.clone())
            }
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

    fn create_dir(&self, _path: &Path) -> io::Result<()> {
        Ok(())
    }

    fn create_dir_all(&self, _path: &Path) -> io::Result<()> {
        Ok(())
    }

    fn list_dir(&self, path: &Path) -> io::Result<Vec<PathBuf>> {
        let files = self.files.read();
        // Iterate the file system map and get all keys that begin with the specified path
        let children: Vec<PathBuf> = files
            .keys()
            .filter(|key| key.starts_with(path))
            .cloned()
            .collect();

        let mut deduped_children: HashSet<PathBuf> = HashSet::new();
        for child in children {
            let target_path_is_parent = child.parent().map_or(false, |parent| parent == path);

            if target_path_is_parent {
                deduped_children.insert(child);
                continue;
            }

            if let Some(parent) = child.parent() {
                // If parent is a folder and the parent is a direct child of the target path, try
                // adding the folder
                let parent_of_parent = parent.parent().unwrap();
                if !deduped_children.contains(parent) && parent_of_parent == path {
                    deduped_children.insert(parent.to_owned());
                }
            }
        }

        let mut results: Vec<PathBuf> = deduped_children.into_iter().collect();
        results.sort();

        Ok(results)
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

    fn create_file(&self, path: &Path, append: bool) -> io::Result<Box<dyn RandomAccessFile>> {
        let mut files = self.files.write();
        if let Some(file) = files.get_mut(path) {
            if append {
                let mut file_guard = file.0.write();
                file_guard.cursor = file_guard.len();
                return Ok(Box::new(file.clone()));
            }
        }

        let new_file = LockableInMemoryFile::new();
        files.insert(path.to_path_buf(), new_file);
        let file = files.get(path).unwrap();

        Ok(Box::new(file.clone()))
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

    fn remove_dir(&self, path: &Path) -> io::Result<()> {
        let files = self.files.write();
        if files
            .keys()
            .filter(|key| key.starts_with(path))
            .cloned()
            .next()
            .is_some()
        {
            // There are still files in the "directory" so throw and error like the disk
            // implementation would
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "The directory was not empty.",
            ));
        }

        // The in-memory file system does not have a concrete representation for a directory so
        // deletion is just a no-op
        Ok(())
    }

    fn remove_dir_all(&self, path: &Path) -> io::Result<()> {
        let mut files = self.files.write();
        // Iterate the file system map and get all keys that begin with the specified path
        let children: Vec<PathBuf> = files
            .keys()
            .filter(|key| key.starts_with(path))
            .cloned()
            .collect();

        for child_path in children {
            files.remove(&child_path);
        }

        Ok(())
    }

    fn get_file_size(&self, path: &Path) -> io::Result<u64> {
        self.open_mem_file(path)?.len()
    }

    /**
    Check if a path is a directory.

    The in-memory file system map only keep track of files. The heuristic to check if a path
    is a directory is:

    1. Check if any entry in the map has the provided path as a prefix
    2. Check if any entries with the prefix have the prefix as an ancestor. If there is an exact
       match, the prefix is a file.
    */
    fn is_dir(&self, path: &Path) -> io::Result<bool> {
        let files = self.files.write();
        let children: Vec<PathBuf> = files
            .keys()
            .filter(|key| key.starts_with(path))
            .cloned()
            .collect();

        if children.is_empty() {
            return Ok(false);
        }

        if children.iter().any(|child_path| child_path == path) {
            return Ok(false);
        }

        Ok(true)
    }

    fn lock_file(&self, path: &Path) -> io::Result<super::FileLock> {
        Ok(FileLock::new(Box::new(self.open_mem_file(path)?)))
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
    /// Create an instance of [`InMemoryFile`](self::InMemoryFile).
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

        let mut file = self.0.write();
        let contents_len = file.contents.len();
        let num_bytes_to_read = if (contents_len - (file.cursor as usize)) < buf_length {
            contents_len - (file.cursor as usize)
        } else {
            buf_length
        };
        buf.copy_from_slice(
            &file.contents[(file.cursor as usize)..(file.cursor as usize) + num_bytes_to_read],
        );

        file.cursor += num_bytes_to_read as u64;

        Ok(buf_length)
    }

    fn read_to_end(&mut self, buf: &mut Vec<u8>) -> io::Result<usize> {
        let space_required = self.0.read().contents.len() - (self.0.read().cursor as usize);
        buf.reserve_exact(space_required);
        buf.resize(space_required, 0);
        self.read(buf)
    }

    fn read_to_string(&mut self, buf: &mut String) -> io::Result<usize> {
        unsafe {
            // SAFETY: We will check for valid UTF-8 after the buffer is read into
            match self.read_to_end(buf.as_mut_vec()) {
                Ok(bytes_read) => {
                    if std::str::from_utf8(buf.as_mut_vec()).is_err() {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            "The data read is not valid UTF-8.",
                        ));
                    }

                    Ok(bytes_read)
                }
                Err(io_err) => Err(io_err),
            }
        }
    }
}

impl Write for LockableInMemoryFile {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let buf_length = buf.len();
        if buf_length == 0 {
            return Ok(0);
        }

        let mut file = self.0.write();
        let content_length = file.contents.len();

        if file.cursor < (content_length as u64) {
            // Truncate existing contents if the file cursor is not positioned at the end of the
            // file
            let cursor_usize = file.cursor as usize;
            file.contents.truncate(cursor_usize);
        }

        file.contents.write_all(buf)?;
        file.cursor += buf_length as u64;

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

        let mut offset: u64 = match pos {
            SeekFrom::Start(off) => off,
            SeekFrom::Current(off) => {
                if off < 0 {
                    let error_message = format!(
                        "Only integers >= 0 are accepted. The passed in offset was {}.",
                        off
                    );
                    return Err(io::Error::new(io::ErrorKind::InvalidInput, error_message));
                }

                (off as u64) + file.cursor
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

        let file = self.0.read();

        if offset >= file.contents.len() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "The provided offset goes beyond the end of the file.",
            ));
        }

        let bytes_to_read = if file.contents.len() > (offset + buf.len()) {
            buf.len()
        } else {
            file.contents.len() - offset
        };
        let end_of_copy_range = offset + bytes_to_read;

        // `copy_from_slice` requires that slices are of the same length or it panics. This is the
        // reason for the range syntax below.
        (&mut buf[..bytes_to_read]).copy_from_slice(&file.contents[offset..end_of_copy_range]);

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
        file.cursor = file.contents.len() as u64;

        Ok(buf.len())
    }
}

impl UnlockableFile for LockableInMemoryFile {
    fn unlock(&self) -> io::Result<()> {
        // Treat as a no-op
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use pretty_assertions::assert_eq;

    use super::*;

    #[test]
    fn can_create_and_remove_files() {
        let fs = InMemoryFileSystem::new();
        let dir_path = PathBuf::from("/some/database");
        let file_path = PathBuf::from("/some/database/LOCK");

        assert!(fs.create_file(&file_path, false).is_ok());

        let files = fs.list_dir(&dir_path).unwrap();
        assert_eq!(files.len(), 1);
        assert_eq!(files.first().unwrap(), &file_path);

        assert!(fs.remove_file(&file_path).is_ok());
        assert_eq!(fs.list_dir(&dir_path).unwrap().len(), 0);
    }

    #[test]
    fn can_list_files_under_a_path() {
        let fs = InMemoryFileSystem::new();
        let path1 = PathBuf::from("/some/database/LOCK");
        let path2 = PathBuf::from("/some/database/wal/wal-123.log");
        let path3 = PathBuf::from("/some/database/wal/wal-456.log");
        let path4 = PathBuf::from("/some/database/wal/random/something-else.log");
        let path5 = PathBuf::from("/some/database/data/456.rdb");

        // Create the files
        assert!(fs.create_file(&path1, false).is_ok());
        assert!(fs.create_file(&path2, false).is_ok());
        assert!(fs.create_file(&path3, false).is_ok());
        assert!(fs.create_file(&path4, false).is_ok());
        assert!(fs.create_file(&path5, false).is_ok());

        let files = fs.list_dir(Path::new("/some/database")).unwrap();
        assert_eq!(files.len(), 3);
        let mut files_iter = files.iter();
        assert_eq!(
            files_iter.next().unwrap(),
            &PathBuf::from("/some/database/LOCK")
        );
        assert_eq!(
            files_iter.next().unwrap(),
            &PathBuf::from("/some/database/data")
        );
        assert_eq!(
            files_iter.next().unwrap(),
            &PathBuf::from("/some/database/wal")
        );
    }

    #[test]
    fn can_read_and_write_a_file() {
        let fs = InMemoryFileSystem::new();
        let file_path = PathBuf::from("/some/database/wal-123.log");

        let mut file = fs.create_file(&file_path, false).unwrap();
        assert!(file.write(b"Hello World").is_ok());
        assert!(file.flush().is_ok());
        assert_eq!(fs.list_dir(Path::new("/some/database")).unwrap().len(), 1);
        assert_eq!(fs.get_file_size(&file_path).unwrap(), 11);

        let mut file_contents = String::new();
        let bytes_read = file.read_to_string(&mut file_contents).unwrap();
        assert_eq!(bytes_read, 0);

        file.seek(SeekFrom::Start(0)).unwrap();
        let bytes_read = file.read_to_string(&mut file_contents).unwrap();
        assert_eq!(bytes_read, 11);
        assert_eq!(file_contents, "Hello World");
    }

    #[test]
    fn can_read_a_file_from_an_offset() {
        let fs = InMemoryFileSystem::new();
        let file_path = PathBuf::from("/some/database/wal-123.log");

        let mut file = fs.create_file(&file_path, false).unwrap();
        assert!(file.write(b"Hello World").is_ok());
        file.seek(SeekFrom::Start(0)).unwrap();

        let mut buf: [u8; 5] = [0; 5];
        let bytes_read = file.read_from(&mut buf, 6).unwrap();
        assert_eq!(bytes_read, 5);
        assert_eq!(std::str::from_utf8(&buf).unwrap(), "World");
    }

    #[test]
    fn can_check_if_a_path_is_a_directory() {
        let fs = InMemoryFileSystem::new();
        let path1 = PathBuf::from("/some/database/LOCK");
        let path2 = PathBuf::from("/some/database/wal/wal-123.log");
        let path3 = PathBuf::from("/some/database/wal/wal-456.log");
        let path4 = PathBuf::from("/some/database/wal/random/something-else.log");

        // Create the files
        fs.create_file(&path1, false).unwrap();
        fs.create_file(&path2, false).unwrap();
        fs.create_file(&path3, false).unwrap();
        fs.create_file(&path4, false).unwrap();

        assert!(fs.is_dir(Path::new("/some/")).unwrap());
        assert!(fs.is_dir(Path::new("/some/database")).unwrap());
        assert!(fs.is_dir(Path::new("/some/database/wal")).unwrap());
        assert!(fs.is_dir(Path::new("/some/database/wal/random")).unwrap());

        assert!(!fs.is_dir(Path::new("/some/database/LOCK")).unwrap());
        assert!(!fs
            .is_dir(Path::new("/some/database/wal/wal-123.log"))
            .unwrap());
    }
}
