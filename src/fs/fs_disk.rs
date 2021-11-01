/*!
This module contains file system wrappers for disk-based file systems.
*/

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
}

impl RandomAccessFile for File {
    fn append(&mut self, buf: &[u8]) -> io::Result<usize> {
        // Seek to the end first
        self.seek(SeekFrom::End(0))?;
        Ok(self.write(buf)?)
    }
}

/// File system implementation that delegates I/O to the operating system.
pub struct OsFileSystem {}

/// Public methods.
impl OsFileSystem {
    pub fn new() -> Self {
        OsFileSystem {}
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
        return "OsFileSystem".to_owned();
    }

    fn create_dir(&mut self, path: &Path) -> io::Result<()> {
        fs::create_dir(path)
    }

    fn create_dir_all(&mut self, path: &Path) -> io::Result<()> {
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
A file system implmentation built on `tempfiles` crate structures.

Really only directory creation is backed by `tempfiles` to take advantage of the auto-cleanup
mechanism. File creation is not used so that control over file naming is retained.
*/
pub struct TmpFileSystem {
    temp_dirs: Vec<TempDir>,
}

impl TmpFileSystem {
    pub fn new() -> Self {
        TmpFileSystem { temp_dirs: vec![] }
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
        return "TmpFileSystem".to_owned();
    }

    fn create_dir(&mut self, path: &Path) -> io::Result<()> {
        self.temp_dirs.push(TempDir::new_in(path)?);

        Ok(())
    }

    fn create_dir_all(&mut self, path: &Path) -> io::Result<()> {
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
