/*!
This module contains file system wrappers for disk-based file systems.
*/

use parking_lot::Mutex;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use tempfile::TempDir;

use super::traits::{FileSystem, RandomAccessFile, ReadonlyRandomAccessFile};

impl ReadonlyRandomAccessFile for File {
    #[cfg(target_family = "windows")]
    fn read_from(&self, buf: &mut [u8], offset: usize) -> io::Result<usize> {
        use std::os::windows::prelude::FileExt;

        self.seek_read(buf, offset);
    }

    #[cfg(target_family = "unix")]
    fn read_from(&self, buf: &mut [u8], offset: usize) -> io::Result<usize> {
        use std::os::unix::prelude::FileExt;

        self.read_at(buf, offset as u64)
    }

    fn len(&self) -> io::Result<u64> {
        Ok(self.metadata()?.len())
    }
}

impl RandomAccessFile for File {
    fn append(&mut self, buf: &[u8]) -> io::Result<usize> {
        // Seek to the end first
        self.seek(SeekFrom::End(0))?;
        self.write(buf)
    }
}

/// File system implementation that delegates I/O to the operating system.
pub struct OsFileSystem {}

/// Public methods.
impl OsFileSystem {
    /// Create an instance of the [`OsFileSystem`].
    pub fn new() -> Self {
        OsFileSystem {}
    }
}

impl Default for OsFileSystem {
    fn default() -> Self {
        Self::new()
    }
}

/// Private methods.
impl OsFileSystem {
    /// Opens a file on disk in readonly mode.
    fn open_disk_file(&self, path: &Path) -> io::Result<File> {
        File::open(path)
    }
}

impl FileSystem for OsFileSystem {
    fn get_name(&self) -> String {
        "OsFileSystem".to_string()
    }

    fn create_dir(&self, path: &Path) -> io::Result<()> {
        fs::create_dir(path)
    }

    fn create_dir_all(&self, path: &Path) -> io::Result<()> {
        fs::create_dir_all(path)
    }

    fn list_dir(&self, path: &Path) -> io::Result<Vec<PathBuf>> {
        let mut entries = fs::read_dir(path)?
            .map(|maybe_entry| maybe_entry.map(|entry| entry.path()))
            .collect::<Result<Vec<_>, io::Error>>()?;
        entries.sort();
        Ok(entries)
    }

    fn open_file(&self, path: &Path) -> io::Result<Box<dyn ReadonlyRandomAccessFile>> {
        let file = self.open_disk_file(path)?;
        Ok(Box::new(file))
    }

    fn rename(&self, from: &Path, to: &Path) -> io::Result<()> {
        fs::rename(from, to)
    }

    fn create_file(&self, path: &Path) -> io::Result<Box<dyn RandomAccessFile>> {
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .append(true)
            .open(path)?;
        Ok(Box::new(file))
    }

    fn remove_file(&self, path: &Path) -> io::Result<()> {
        fs::remove_file(path)
    }

    fn get_file_size(&self, path: &Path) -> io::Result<u64> {
        Ok(self.open_disk_file(path)?.metadata()?.len())
    }
}

/**
SAFETY:
This is safe because the filesystem is more akin to a bag of utilities. It does not contain any
structures itself.
*/
unsafe impl Send for OsFileSystem {}

/**
SAFETY:
This is safe because the filesystem is more akin to a bag of utilities. It does not contain any
structures itself.
*/
unsafe impl Sync for OsFileSystem {}

/**
A file system implmentation built on `tempfiles` crate structures.

Really only directory creation is backed by `tempfiles` to take advantage of the auto-cleanup
mechanism. File creation is not used so that control over file naming is retained.
*/
pub struct TmpFileSystem {
    temp_dirs: Mutex<Vec<TempDir>>,
}

impl TmpFileSystem {
    pub fn new() -> Self {
        TmpFileSystem {
            temp_dirs: Mutex::new(vec![]),
        }
    }
}

impl Default for TmpFileSystem {
    fn default() -> Self {
        Self::new()
    }
}

/// Private methods.
impl TmpFileSystem {
    /// Opens a file on disk in readonly mode.
    fn open_tmp_file(&self, path: &Path) -> io::Result<File> {
        File::open(path)
    }
}

impl FileSystem for TmpFileSystem {
    fn get_name(&self) -> String {
        "TmpFileSystem".to_string()
    }

    fn create_dir(&self, path: &Path) -> io::Result<()> {
        self.temp_dirs.lock().push(TempDir::new_in(path)?);

        Ok(())
    }

    fn create_dir_all(&self, _path: &Path) -> io::Result<()> {
        unimplemented!("Not supported by tempfile crate.")
    }

    fn list_dir(&self, path: &Path) -> io::Result<Vec<PathBuf>> {
        let mut entries = fs::read_dir(path)?
            .map(|maybe_entry| maybe_entry.map(|entry| entry.path()))
            .collect::<Result<Vec<_>, io::Error>>()?;
        entries.sort();
        Ok(entries)
    }

    fn open_file(&self, path: &Path) -> io::Result<Box<dyn ReadonlyRandomAccessFile>> {
        let file = self.open_tmp_file(path)?;
        Ok(Box::new(file))
    }

    fn rename(&self, from: &Path, to: &Path) -> io::Result<()> {
        fs::rename(from, to)
    }

    fn create_file(&self, path: &Path) -> io::Result<Box<dyn RandomAccessFile>> {
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .append(true)
            .open(path)?;
        Ok(Box::new(file))
    }

    fn remove_file(&self, path: &Path) -> io::Result<()> {
        fs::remove_file(path)
    }

    fn get_file_size(&self, path: &Path) -> io::Result<u64> {
        Ok(self.open_tmp_file(path)?.metadata()?.len())
    }
}

#[cfg(test)]
mod tests {
    use pretty_assertions::assert_eq;

    use super::*;

    const BASE_TESTING_DIR_NAME: &str = "testing_files/";

    #[test]
    fn create_dir_creates_an_empty_directory() {
        let file_system: OsFileSystem = OsFileSystem::new();
        let test_dir = BASE_TESTING_DIR_NAME.to_string() + "create_dir";
        fs::create_dir(BASE_TESTING_DIR_NAME).ok();

        file_system.create_dir(Path::new(&test_dir)).unwrap();
        assert_eq!(file_system.list_dir(Path::new(&test_dir)).unwrap().len(), 0);

        // Clean up
        assert!(fs::remove_dir_all(Path::new(&test_dir)).is_ok());
    }

    #[test]
    fn create_dir_all_creates_paths_recursively() {
        fs::create_dir(BASE_TESTING_DIR_NAME).ok();
        let file_system: OsFileSystem = OsFileSystem::new();
        let test_dir = BASE_TESTING_DIR_NAME.to_string() + "create_dir_all";

        let mut full_path = PathBuf::new();
        full_path.push(&test_dir);
        full_path.push("level1");
        full_path.push("level2");

        let mut level1_path = PathBuf::new();
        level1_path.push(&test_dir);
        level1_path.push("level1");

        file_system.create_dir_all(&full_path).unwrap();
        assert!(file_system.list_dir(Path::new(&test_dir)).unwrap()[0]
            .to_str()
            .unwrap()
            .contains(level1_path.to_str().unwrap()));

        assert!(file_system.list_dir(&level1_path).unwrap()[0]
            .to_str()
            .unwrap()
            .contains(full_path.to_str().unwrap()));

        // Clean up
        assert!(fs::remove_dir_all(level1_path).is_ok());
        assert!(fs::remove_dir_all(Path::new(&BASE_TESTING_DIR_NAME)).is_ok());
    }

    #[test]
    fn create_file_creates_a_file_we_can_write_to() {
        fs::create_dir(BASE_TESTING_DIR_NAME).ok();
        let file_system: OsFileSystem = OsFileSystem::new();
        let test_dir = BASE_TESTING_DIR_NAME.to_string() + "create_file";
        file_system.create_dir(Path::new(&test_dir)).unwrap();
        let mut file_path = PathBuf::new();
        file_path.push(&test_dir);
        file_path.push("testing_file");

        let mut file = file_system.create_file(&file_path).unwrap();
        assert!(file.write(b"Hello World").is_ok());
        assert!(file.flush().is_ok());

        assert_eq!(file_system.list_dir(Path::new(&test_dir)).unwrap().len(), 1);
        assert_eq!(file_system.get_file_size(&file_path).unwrap(), 11);

        // Clean up
        assert!(fs::remove_dir_all(Path::new(&test_dir)).is_ok());
    }

    #[test]
    fn remove_file_removes_a_file() {
        fs::create_dir(BASE_TESTING_DIR_NAME).ok();
        let file_system: OsFileSystem = OsFileSystem::new();
        let test_dir = BASE_TESTING_DIR_NAME.to_string() + "remove_file";
        file_system.create_dir(Path::new(&test_dir)).unwrap();
        let mut file_path = PathBuf::new();
        file_path.push(&test_dir);
        file_path.push("testing_file");

        let mut file = file_system.create_file(&file_path).unwrap();
        assert!(file.write(b"Hello World").is_ok());
        assert!(file.flush().is_ok());
        assert_eq!(
            file_system
                .list_dir(Path::new(&BASE_TESTING_DIR_NAME))
                .unwrap()
                .len(),
            1
        );

        assert!(file_system.remove_file(&file_path).is_ok());
        assert_eq!(file_system.list_dir(Path::new(&test_dir)).unwrap().len(), 0);

        // Clean-up
        assert!(fs::remove_dir_all(Path::new(&test_dir)).is_ok());
    }
}
