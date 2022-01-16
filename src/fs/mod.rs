/*!
This module contains abstractions for file systems.

Specifically, this module provides a trait that file systems must implement in order to be used
within RainDB.

Concrete file system implementations include an in-memory file system and a wrapper around the
Rust [`std::fs`] library which already abstracts POSIX and Windows file system operations.
*/

mod traits;
pub use self::traits::{FileSystem, RandomAccessFile, ReadonlyRandomAccessFile};

mod fs_disk;
pub use self::fs_disk::{OsFileSystem, TmpFileSystem};

mod fs_mem;
pub use self::fs_mem::InMemoryFileSystem;
