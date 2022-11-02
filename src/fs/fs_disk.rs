/*!
This module contains file system wrappers for disk-based file systems.
*/

use fs2::FileExt;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use tempfile::TempDir;

use super::traits::{FileSystem, RandomAccessFile, ReadonlyRandomAccessFile, UnlockableFile};
use super::FileLock;

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

impl UnlockableFile for File {
    fn unlock(&self) -> io::Result<()> {
        fs2::FileExt::unlock(self)
    }
}

/// File system implementation that delegates I/O to the operating system.
pub struct OsFileSystem {}

/// Public methods.
impl OsFileSystem {
    /// Create an instance of [`OsFileSystem`].
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

    fn create_file(&self, path: &Path, append: bool) -> io::Result<Box<dyn RandomAccessFile>> {
        let mut open_options = OpenOptions::new();
        open_options.create(true).write(true).read(true);

        if append {
            open_options.append(true);
        } else {
            open_options.truncate(true);
        }

        let file = open_options.open(path)?;

        Ok(Box::new(file))
    }

    fn remove_file(&self, path: &Path) -> io::Result<()> {
        fs::remove_file(path)
    }

    fn remove_dir(&self, path: &Path) -> io::Result<()> {
        fs::remove_dir(path)
    }

    fn remove_dir_all(&self, path: &Path) -> io::Result<()> {
        fs::remove_dir_all(path)
    }

    fn get_file_size(&self, path: &Path) -> io::Result<u64> {
        Ok(self.open_disk_file(path)?.metadata()?.len())
    }

    fn is_dir(&self, path: &Path) -> io::Result<bool> {
        Ok(fs::metadata(path)?.is_dir())
    }

    fn lock_file(&self, path: &Path) -> io::Result<FileLock> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(path)?;
        file.try_lock_exclusive()?;

        Ok(FileLock::new(Box::new(file)))
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
A file system implmentation built on `tempfiles` crate structures. All provided paths must be
relative to the root directory.

Really only directory creation is backed by `tempfiles` to take advantage of the auto-cleanup
mechanism. File creation is not used so that control over file naming is retained.
*/
pub struct TmpFileSystem {
    root_dir: TempDir,
}

/// Public methods
impl TmpFileSystem {
    /// Create a new instance of [`TmpFileSystem`] where all files are created in the provided root.
    pub fn new(root_path: Option<&Path>) -> Self {
        if let Some(path) = root_path {
            return TmpFileSystem {
                root_dir: TempDir::new_in(path).unwrap(),
            };
        }

        TmpFileSystem {
            root_dir: TempDir::new().unwrap(),
        }
    }

    /**
    Get the root path of this temporary file system.

    All methods will operate relatively to this root path.
    */
    pub fn get_root_path(&self) -> PathBuf {
        self.root_dir.path().to_owned()
    }
}

impl Default for TmpFileSystem {
    fn default() -> Self {
        Self::new(None)
    }
}

/// Private methods.
impl TmpFileSystem {
    /// Opens a file on disk in readonly mode.
    fn open_tmp_file(&self, path: &Path) -> io::Result<File> {
        File::open(self.get_rooted_path(path))
    }

    /**
    Get a path rooted by the root path of this file system. Prefixes that match the root path will
    be stripped.
    */
    fn get_rooted_path(&self, mut path: &Path) -> PathBuf {
        path = if let Some(stripped_path) = path
            .to_str()
            .unwrap()
            .strip_prefix(&(self.get_root_path().to_str().unwrap().to_owned() + "/"))
        {
            Path::new(stripped_path)
        } else {
            path
        };

        self.root_dir.path().join(path)
    }
}

impl FileSystem for TmpFileSystem {
    fn get_name(&self) -> String {
        "TmpFileSystem".to_string()
    }

    fn create_dir(&self, path: &Path) -> io::Result<()> {
        let rooted_path = self.get_rooted_path(path);
        fs::create_dir(rooted_path)
    }

    fn create_dir_all(&self, path: &Path) -> io::Result<()> {
        fs::create_dir_all(self.get_rooted_path(path))
    }

    fn list_dir(&self, path: &Path) -> io::Result<Vec<PathBuf>> {
        let mut entries = fs::read_dir(self.get_rooted_path(path))?
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
        fs::rename(self.get_rooted_path(from), self.get_rooted_path(to))
    }

    fn create_file(&self, path: &Path, append: bool) -> io::Result<Box<dyn RandomAccessFile>> {
        let mut open_options = OpenOptions::new();
        open_options.create(true).write(true).read(true);

        if append {
            open_options.append(true);
        } else {
            open_options.truncate(true);
        }

        let rooted_path = self.get_rooted_path(path);
        let file = open_options.open(rooted_path)?;

        Ok(Box::new(file))
    }

    fn remove_file(&self, path: &Path) -> io::Result<()> {
        fs::remove_file(self.get_rooted_path(path))
    }

    fn remove_dir(&self, path: &Path) -> io::Result<()> {
        fs::remove_dir(self.get_rooted_path(path))
    }

    fn remove_dir_all(&self, path: &Path) -> io::Result<()> {
        fs::remove_dir_all(self.get_rooted_path(path))
    }

    fn get_file_size(&self, path: &Path) -> io::Result<u64> {
        Ok(self.open_tmp_file(path)?.metadata()?.len())
    }

    fn is_dir(&self, path: &Path) -> io::Result<bool> {
        Ok(fs::metadata(self.get_rooted_path(path))?.is_dir())
    }

    fn lock_file(&self, path: &Path) -> io::Result<FileLock> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(self.get_rooted_path(path))?;
        file.try_lock_exclusive()?;

        Ok(FileLock::new(Box::new(file)))
    }
}

#[cfg(test)]
mod os_file_system_tests {
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
    fn create_file_creates_a_file_we_can_write_to_and_read_from() {
        fs::create_dir(BASE_TESTING_DIR_NAME).ok();
        let file_system: OsFileSystem = OsFileSystem::new();
        let test_dir = BASE_TESTING_DIR_NAME.to_string() + "create_file";
        file_system.create_dir(Path::new(&test_dir)).unwrap();
        let mut file_path = PathBuf::new();
        file_path.push(&test_dir);
        file_path.push("testing_file");

        let mut file = file_system.create_file(&file_path, true).unwrap();
        assert!(file.write(b"Hello World").is_ok());
        assert!(file.flush().is_ok());
        assert_eq!(file_system.list_dir(Path::new(&test_dir)).unwrap().len(), 1);
        assert_eq!(file_system.get_file_size(&file_path).unwrap(), 11);

        file.seek(SeekFrom::Start(0)).unwrap();
        let mut file_contents = String::new();
        let bytes_read = file.read_to_string(&mut file_contents).unwrap();
        assert_eq!(bytes_read, 11);
        assert_eq!(file_contents, "Hello World");

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

        let mut file = file_system.create_file(&file_path, true).unwrap();
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

#[cfg(test)]
mod tmp_file_system_tests {
    use pretty_assertions::assert_eq;

    use super::*;

    const BASE_TESTING_DIR: &str = "testing_files/tmp_fs";

    fn setup() {
        // Ensure that the base testing directory exists
        let base_path = Path::new(BASE_TESTING_DIR);
        if !base_path.exists() {
            fs::create_dir_all(&base_path).unwrap();
        };
    }

    #[test]
    fn creates_directories_relative_to_provided_root_and_cleans_up_after_dropping() {
        setup();

        let file_system = TmpFileSystem::new(Some(Path::new(BASE_TESTING_DIR)));
        let root_test_dir = file_system.get_root_path();
        assert!(root_test_dir.exists());

        let created_dir_path = root_test_dir.join("created-dir");
        file_system.create_dir(&created_dir_path).unwrap();
        assert!(created_dir_path.exists());
        assert_eq!(
            file_system
                .list_dir(Path::new(&created_dir_path))
                .unwrap()
                .len(),
            0
        );

        drop(file_system);

        assert!(
            !root_test_dir.exists(),
            "The test directory should be cleaned up"
        );
    }

    #[test]
    fn cleanup_when_there_are_nested_files_and_directories_succeeds() {
        setup();

        let file_system = TmpFileSystem::new(Some(Path::new(BASE_TESTING_DIR)));
        let root_test_dir = file_system.get_root_path();
        assert!(root_test_dir.exists());

        // Create a bunch of directories
        let created_dir_path = file_system.get_rooted_path(Path::new("created-dir"));
        let nested_path_1 = created_dir_path.join("nested1");
        let deep_nested_path_1 = nested_path_1.join("deep-nested1");
        let nested_path_2 = created_dir_path.join("nested2");

        file_system.create_dir(&created_dir_path).unwrap();
        file_system.create_dir(&nested_path_1).unwrap();
        file_system.create_dir(&deep_nested_path_1).unwrap();
        file_system.create_dir(&nested_path_2).unwrap();

        assert!(created_dir_path.exists());
        assert!(nested_path_1.exists());
        assert!(deep_nested_path_1.exists());
        assert!(nested_path_2.exists());

        // Create a bunch of files
        let created_dir_relative_file_path = Path::new("created-dir/created-dir_file1");
        let nested_path_file_1_path = nested_path_1.join("nested1_file1");
        let nested_path_file_2_path = nested_path_1.join("nested1_file2");

        file_system
            .create_file(created_dir_relative_file_path, false)
            .unwrap();
        file_system
            .create_file(&nested_path_file_1_path, true)
            .unwrap();
        file_system
            .create_file(&nested_path_file_2_path, false)
            .unwrap();

        assert!(file_system
            .get_rooted_path(created_dir_relative_file_path)
            .exists());
        assert!(nested_path_file_1_path.exists());
        assert!(nested_path_file_2_path.exists());

        drop(file_system);

        // Check that cleanup happened
        assert!(
            !root_test_dir.exists(),
            "The test directory should be cleaned up"
        );
    }
}
