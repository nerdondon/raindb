use crate::config::SEEK_DATA_SIZE_THRESHOLD_KIB;
use crate::key::LookupKey;

/// Metadata about an SSTable file.
#[derive(Debug)]
pub(crate) struct FileMetadata {
    /**
    The number of multi-level seeks through this file that are allowed before a compaction is
    triggered.
    */
    allowed_seeks: usize,

    /// The globally increasing, sequential number for on-disk data files.
    file_number: u64,

    /// The size of the SSTable file in bytes.
    file_size: u64,

    /// The smallest internal key served by the table.
    smallest_key: LookupKey,

    /// The largest internal key served by the table.
    largest_key: LookupKey,
}

/// Public methods
impl FileMetadata {
    /// Create a new instance of [`FileMetadata`].
    pub fn new(
        file_number: u64,
        file_size: u64,
        smallest_key: LookupKey,
        largest_key: LookupKey,
    ) -> Self {
        // Allowed seeks is one seek per a configured number of bytes read in a seek
        let mut allowed_seeks = (file_size / SEEK_DATA_SIZE_THRESHOLD_KIB) as usize;
        if allowed_seeks < 100 {
            allowed_seeks = 100;
        }

        Self {
            allowed_seeks,
            file_number,
            file_size,
            smallest_key,
            largest_key,
        }
    }
}
