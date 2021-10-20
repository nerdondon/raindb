mod fs;
pub use self::fs::{FileSystem, RandomAccessFile, ReadonlyRandomAccessFile};

mod fs_disk;
pub use self::fs_disk::{OsFileSystem, TmpFileSystem};

mod fs_mem;
pub use self::fs_mem::InMemoryFileSystem;
