// Copyright (c) 2021 Google LLC
//
// Use of this source code is governed by an MIT-style
// license that can be found in the LICENSE file or at
// https://opensource.org/licenses/MIT.

use std::cmp::Ordering;
use std::hash::{Hash, Hasher};
use std::io::Read;
use std::ops::Range;
use std::sync::Arc;
use std::{io, u8};

use integer_encoding::{VarIntReader, VarIntWriter};
use parking_lot::RwLock;

use crate::config::SEEK_DATA_SIZE_THRESHOLD_KIB;
use crate::key::InternalKey;
use crate::utils::comparator::Comparator;
use crate::utils::io::{ReadHelpers, WriteHelpers};

/// Metadata about an SSTable file.
#[derive(Clone, Debug)]
pub(crate) struct FileMetadata {
    /**
    The number of multi-level seeks through this file that are allowed before a compaction is
    triggered.

    This value is wrapped in an [`Arc`] so that we can clone the `FileMetadata`. This value should
    never be cloned and used separately from this field. The value by itself is meaningless.
    */
    allowed_seeks: Option<Arc<RwLock<i64>>>,

    /// The globally increasing, sequential number for on-disk data files.
    file_number: u64,

    /// The size of the SSTable file in bytes.
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
        let mut allowed_seeks = match i64::try_from(file_size / SEEK_DATA_SIZE_THRESHOLD_KIB) {
            Ok(val) => val,
            Err(_) => i64::MAX,
        };
        if allowed_seeks < 100 {
            allowed_seeks = 100;
        }

        self.file_size = file_size;
        self.allowed_seeks = Some(Arc::new(RwLock::new(allowed_seeks)));
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

    /// Get the current number of allowed seeks for this file.
    pub(crate) fn allowed_seeks(&self) -> i64 {
        *self.allowed_seeks.as_ref().unwrap().read()
    }

    /**
    Decrement the number of allowed seeks by 1.

    # Panics

    This method will panic if the file size and thus the allowed seeks fields have not been fully
    initialized.
    */
    pub(crate) fn decrement_allowed_seeks(&self) {
        let mut allowed_seeks_guard = self.allowed_seeks.as_ref().unwrap().write();
        *allowed_seeks_guard -= 1;
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
    pub(crate) fn get_key_range_for_files(files: &[Arc<FileMetadata>]) -> Range<InternalKey> {
        assert!(!files.is_empty());

        let mut smallest: &InternalKey = files[0].smallest_key();
        let mut largest: &InternalKey = files[0].largest_key();
        for file in files {
            if file.smallest_key() < smallest {
                smallest = file.smallest_key();
            }

            if file.largest_key() < largest {
                largest = file.largest_key()
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
        multi_level_files: &[&[Arc<FileMetadata>]],
    ) -> Range<InternalKey> {
        let first_range = FileMetadata::get_key_range_for_files(multi_level_files[0]);
        let mut smallest = first_range.start;
        let mut largest = first_range.end;

        for files in multi_level_files.iter().skip(1) {
            if files.is_empty() {
                continue;
            }

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
        let file_size = reader.read_varint::<u64>()?;
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

        file.set_file_size(file_size);
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

impl Hash for FileMetadata {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.file_number.hash(state);
        self.file_size.hash(state);
        self.smallest_key.hash(state);
        self.largest_key.hash(state);
    }
}

impl PartialEq for FileMetadata {
    fn eq(&self, other: &Self) -> bool {
        self.file_number == other.file_number
            && self.file_size == other.file_size
            && self.smallest_key == other.smallest_key
            && self.largest_key == other.largest_key
    }
}

impl Eq for FileMetadata {}

#[cfg(test)]
mod tests {
    use pretty_assertions::{assert_eq, assert_ne};

    use crate::Operation;

    use super::*;

    #[test]
    fn file_metadata_maintains_identity_invariants() {
        let mut metadata1 = FileMetadata::new(117);
        metadata1.set_smallest_key(Some(InternalKey::new(b"hello".to_vec(), 1, Operation::Put)));
        metadata1.set_largest_key(Some(InternalKey::new(
            b"world".to_vec(),
            1,
            Operation::Delete,
        )));
        metadata1.set_file_size(30_000);

        let mut metadata2 = FileMetadata::new(117);
        metadata2.set_smallest_key(Some(InternalKey::new(b"hello".to_vec(), 1, Operation::Put)));
        metadata2.set_largest_key(Some(InternalKey::new(
            b"world".to_vec(),
            1,
            Operation::Delete,
        )));
        metadata2.set_file_size(30_000);

        let mut metadata3 = FileMetadata::new(117);
        metadata3.set_smallest_key(Some(InternalKey::new(b"hello".to_vec(), 1, Operation::Put)));
        metadata3.set_largest_key(Some(InternalKey::new(
            b"world".to_vec(),
            3,
            Operation::Delete,
        )));
        metadata3.set_file_size(30_000);

        assert_eq!(metadata1, metadata2);
        assert_ne!(metadata1, metadata3);
    }

    #[test]
    fn file_metadata_can_be_serialized_and_deserialized_correct() {
        let mut metadata = FileMetadata::new(117);
        metadata.set_smallest_key(Some(InternalKey::new(b"hello".to_vec(), 1, Operation::Put)));
        metadata.set_largest_key(Some(InternalKey::new(
            b"world".to_vec(),
            1,
            Operation::Delete,
        )));
        metadata.set_file_size(30_000);

        let serialized: Vec<u8> = (&metadata).into();
        let deserialized = FileMetadata::try_from(serialized.as_slice()).unwrap();

        assert_eq!(metadata, deserialized);
    }

    #[test]
    fn file_metadata_by_smallest_key_comparator_correctly_orders_inputs() {
        let mut metadata1 = FileMetadata::new(117);
        metadata1.set_smallest_key(Some(InternalKey::new(b"apple".to_vec(), 1, Operation::Put)));
        metadata1.set_largest_key(Some(InternalKey::new(
            b"this doesn't matter".to_vec(),
            1,
            Operation::Delete,
        )));
        metadata1.set_file_size(30_000);

        let mut metadata2 = FileMetadata::new(117);
        metadata2.set_smallest_key(Some(InternalKey::new(
            b"banana".to_vec(),
            1,
            Operation::Put,
        )));
        metadata2.set_largest_key(Some(InternalKey::new(
            b"this doesn't matter".to_vec(),
            1,
            Operation::Delete,
        )));
        metadata2.set_file_size(30_000);

        let mut metadata3 = FileMetadata::new(104);
        metadata3.set_smallest_key(Some(InternalKey::new(b"apple".to_vec(), 1, Operation::Put)));
        metadata3.set_largest_key(Some(InternalKey::new(
            b"this doesn't matter".to_vec(),
            1,
            Operation::Delete,
        )));
        metadata3.set_file_size(30_000);

        assert_eq!(
            FileMetadataBySmallestKey::compare(&metadata1, &metadata2),
            Ordering::Less,
            "Should be ordered by smallest key."
        );
        assert_eq!(
            FileMetadataBySmallestKey::compare(&metadata1, &metadata3),
            Ordering::Greater,
            "Ties should be broken by file number"
        );
    }

    #[test]
    fn file_metadata_can_get_a_key_range_covering_the_key_range_in_a_list_of_files() {
        let mut metadata1 = FileMetadata::new(87);
        metadata1.set_smallest_key(Some(InternalKey::new(
            b"banana".to_vec(),
            1,
            Operation::Put,
        )));
        metadata1.set_largest_key(Some(InternalKey::new(b"zebra".to_vec(), 3, Operation::Put)));

        let mut metadata2 = FileMetadata::new(104);
        metadata2.set_smallest_key(Some(InternalKey::new(
            b"coconut".to_vec(),
            1,
            Operation::Put,
        )));
        metadata2.set_largest_key(Some(InternalKey::new(b"zebra".to_vec(), 1, Operation::Put)));

        let mut metadata3 = FileMetadata::new(117);
        metadata3.set_smallest_key(Some(InternalKey::new(
            b"apple".to_vec(),
            999,
            Operation::Put,
        )));
        metadata3.set_largest_key(Some(InternalKey::new(b"zebra".to_vec(), 2, Operation::Put)));

        let files = [metadata1, metadata2, metadata3].map(Arc::new);
        let expected_range = InternalKey::new(b"apple".to_vec(), 999, Operation::Put)
            ..InternalKey::new(b"zebra".to_vec(), 3, Operation::Put);

        assert_eq!(
            expected_range,
            FileMetadata::get_key_range_for_files(&files)
        );
    }
}
