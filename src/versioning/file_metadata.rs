use std::cmp::Ordering;
use std::io::Read;
use std::ops::{Deref, Range};
use std::sync::Arc;
use std::{io, u8};

use integer_encoding::{VarIntReader, VarIntWriter};
use parking_lot::{MappedRwLockReadGuard, RwLock, RwLockReadGuard};

use crate::config::SEEK_DATA_SIZE_THRESHOLD_KIB;
use crate::key::InternalKey;
use crate::utils::comparator::Comparator;
use crate::utils::io::{ReadHelpers, WriteHelpers};

/// Metadata about a table file.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub(crate) struct FileMetadata {
    /**
    The number of multi-level seeks through this file that are allowed before a compaction is
    triggered.
    */
    allowed_seeks: Option<u64>,

    /// The globally increasing, sequential number for on-disk data files.
    file_number: u64,

    /// The size of a table file in bytes.
    file_size: u64,

    /// The smallest internal key served by the table.
    smallest_key: Option<InternalKey>,

    /// The largest internal key served by the table.
    largest_key: Option<InternalKey>,
}

/// Crate-only methods
impl FileMetadata {
    /// Create an empty instance of [`FileMetadata`].
    pub(crate) fn new(file_number: u64) -> Self {
        Self {
            file_number,
            allowed_seeks: None,
            file_size: 0,
            smallest_key: None,
            largest_key: None,
        }
    }

    /// Set file size.
    pub(crate) fn set_file_size(&mut self, file_size: u64) {
        // Allowed seeks is one seek per a configured number of bytes read in a seek
        let mut allowed_seeks = file_size / SEEK_DATA_SIZE_THRESHOLD_KIB;
        if allowed_seeks < 100 {
            allowed_seeks = 100;
        }

        self.file_size = file_size;
        self.allowed_seeks = Some(allowed_seeks);
    }

    /// Get the file size.
    pub(crate) fn get_file_size(&self) -> u64 {
        self.file_size
    }

    /**
    Get a reference to the smallest key in the file.

    # Panics

    Panics if the `smallest_key` field is `None`.
    */
    pub(crate) fn smallest_key(&self) -> &InternalKey {
        self.smallest_key.as_ref().unwrap()
    }

    /**
    Get a reference to the largest key in the file.

    # Panics

    Panics if the `largest_key` field is `None`.
    */
    pub(crate) fn largest_key(&self) -> &InternalKey {
        self.largest_key.as_ref().unwrap()
    }

    /// Set the file metadata's smallest key.
    pub(crate) fn set_smallest_key(&mut self, smallest_key: Option<InternalKey>) {
        self.smallest_key = smallest_key;
    }

    /// Set the file metadata's largest key.
    pub(crate) fn set_largest_key(&mut self, largest_key: Option<InternalKey>) {
        self.largest_key = largest_key;
    }

    /// Get the file number.
    pub(crate) fn file_number(&self) -> u64 {
        self.file_number
    }

    /// Decrement the number of allowed seeks.
    pub(crate) fn decrement_allowed_seeks(&mut self) {
        self.allowed_seeks = self.allowed_seeks.map(|seeks| seeks - 1);
    }

    /**
    Get a clone of the key range.

    # Panics

    This method will panic if there is no concrete key for the smallest key or for the largest key.
    */
    pub(crate) fn clone_key_range(&self) -> Range<InternalKey> {
        self.smallest_key().clone()..self.largest_key().clone()
    }

    /**
    Get the minimal key range that covers all entries in the provided list of files.

    # Panics

    The provided `files` must not be empty.

    # Legacy

    This synonomous with LevelDB's `VersionSet::GetRange`.
    */
    pub(crate) fn get_key_range_for_files(files: &[SharedFileMetadata]) -> Range<InternalKey> {
        assert!(!files.is_empty());

        let mut smallest: &InternalKey = &files[0].smallest_key();
        let mut largest: &InternalKey = &files[0].largest_key();
        for file in files {
            if &*file.smallest_key() < smallest {
                smallest = &file.smallest_key();
            }

            if &*file.largest_key() < largest {
                largest = &file.largest_key()
            }
        }

        smallest.clone()..largest.clone()
    }

    /**
    Get the minimal key range that covers all entries in the provided a list of list of files.

    # Panics

    Every set of files must contain at least one file.

    # Legacy

    This synonomous with LevelDB's `VersionSet::GetRange2`.
    */
    pub(crate) fn get_key_range_for_multiple_levels(
        multi_level_files: &[&[SharedFileMetadata]],
    ) -> Range<InternalKey> {
        let first_range = FileMetadata::get_key_range_for_files(multi_level_files[0]);
        let mut smallest = first_range.start;
        let mut largest = first_range.end;

        for files in multi_level_files.iter().skip(1) {
            let files_key_range = FileMetadata::get_key_range_for_files(files);

            if files_key_range.start < smallest {
                smallest = files_key_range.start;
            }

            if files_key_range.end < largest {
                largest = files_key_range.end
            }
        }

        smallest..largest
    }

    /// Deserialize the contents of the provided reader to a [`FileMetadata`] instance.
    pub(crate) fn deserialize<R: Read>(reader: &mut R) -> io::Result<FileMetadata> {
        let file_number = reader.read_varint::<u64>()?;
        let mut file = FileMetadata::new(file_number);
        let smallest_key = match InternalKey::try_from(reader.read_length_prefixed_slice()?) {
            Ok(key) => key,
            Err(base_err) => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    base_err.to_string(),
                ))
            }
        };
        let largest_key = match InternalKey::try_from(reader.read_length_prefixed_slice()?) {
            Ok(key) => key,
            Err(base_err) => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    base_err.to_string(),
                ))
            }
        };

        file.set_file_size(reader.read_varint::<u64>()?);
        file.set_smallest_key(Some(smallest_key));
        file.set_largest_key(Some(largest_key));

        Ok(file)
    }
}

/// Comparator that orders [`FileMetadata`] instances by their `smallest_key` field.
pub(crate) struct FileMetadataBySmallestKey {}

impl Comparator<&FileMetadata> for FileMetadataBySmallestKey {
    fn compare(a: &FileMetadata, b: &FileMetadata) -> Ordering {
        let a_smallest_key = a.smallest_key();
        let b_smallest_key = b.smallest_key();
        let order = a_smallest_key.cmp(b_smallest_key);

        match &order {
            Ordering::Greater | Ordering::Less => order,
            Ordering::Equal => {
                // Break ties by file number
                a.file_number().cmp(&b.file_number())
            }
        }
    }
}

impl From<&FileMetadata> for Vec<u8> {
    fn from(file: &FileMetadata) -> Self {
        let mut buf = vec![];

        buf.write_varint(file.file_number()).unwrap();
        buf.write_varint(file.get_file_size()).unwrap();
        buf.write_length_prefixed_slice(&Vec::<u8>::from(file.smallest_key()))
            .unwrap();
        buf.write_length_prefixed_slice(&Vec::<u8>::from(file.largest_key()))
            .unwrap();

        buf
    }
}

impl TryFrom<&[u8]> for FileMetadata {
    type Error = io::Error;

    fn try_from(mut value: &[u8]) -> Result<Self, Self::Error> {
        FileMetadata::deserialize(&mut value)
    }
}

#[derive(Clone, Debug)]
pub(crate) struct SharedFileMetadata(Arc<RwLock<FileMetadata>>);

/// Crate-only methods
impl SharedFileMetadata {
    /// Create a new instance of [`SharedFileMetadata`].
    pub(crate) fn new(file: FileMetadata) -> Self {
        SharedFileMetadata(Arc::new(RwLock::new(file)))
    }

    /// Get the file size.
    pub(crate) fn get_file_size(&self) -> u64 {
        self.0.read().get_file_size()
    }

    /**
    Get a reference to the smallest key in the file.

    # Panics

    Panics if the `smallest_key` field is `None`.
    */
    pub(crate) fn smallest_key(&self) -> MappedRwLockReadGuard<InternalKey> {
        RwLockReadGuard::map(self.0.read(), |file| file.smallest_key())
    }

    /**
    Get a reference to the largest key in the file.

    # Panics

    Panics if the `largest_key` field is `None`.
    */
    pub(crate) fn largest_key(&self) -> MappedRwLockReadGuard<InternalKey> {
        RwLockReadGuard::map(self.0.read(), |file| file.largest_key())
    }

    /// Get the file number.
    pub(crate) fn file_number(&self) -> u64 {
        self.0.read().file_number()
    }

    /**
    Get a clone of the key range.

    # Panics

    This method will panic if there is no concrete key for the smallest key or for the largest key.
    */
    pub(crate) fn clone_key_range(&self) -> Range<InternalKey> {
        self.0.read().clone_key_range()
    }
}

impl Deref for SharedFileMetadata {
    type Target = Arc<RwLock<FileMetadata>>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
