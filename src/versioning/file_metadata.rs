use crate::config::SEEK_DATA_SIZE_THRESHOLD_KIB;
use crate::key::LookupKey;

/// Metadata about an SSTable file.
#[derive(Debug)]
pub(crate) struct FileMetadata {
    /**
    The number of multi-level seeks through this file that are allowed before a compaction is
    triggered.
    */
    allowed_seeks: Option<u64>,

    /// The globally increasing, sequential number for on-disk data files.
    file_number: u64,

    /// The size of the SSTable file in bytes.
    file_size: u64,

    /// The smallest internal key served by the table.
    smallest_key: Option<LookupKey>,

    /// The largest internal key served by the table.
    largest_key: Option<LookupKey>,
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
    pub(crate) fn smallest_key(&self) -> &LookupKey {
        self.smallest_key.as_ref().unwrap()
    }

    /**
    Get a reference to the largest key in the file.

    # Panics

    Panics if the `largest_key` field is `None`.
    */
    pub(crate) fn largest_key(&self) -> &LookupKey {
        self.largest_key.as_ref().unwrap()
    }

    /// Set the file metadata's smallest key.
    pub(crate) fn set_smallest_key(&mut self, smallest_key: Option<LookupKey>) {
        self.smallest_key = smallest_key;
    }

    /// Set the file metadata's largest key.
    pub(crate) fn set_largest_key(&mut self, largest_key: Option<LookupKey>) {
        self.largest_key = largest_key;
    }

    /// Get the file number.
    pub(crate) fn file_number(&self) -> u64 {
        self.file_number
    }
}
