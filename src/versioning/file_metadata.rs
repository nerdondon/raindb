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
    pub(crate) file_number: u64,

    /// The size of the SSTable file in bytes.
    file_size: Option<u64>,

    /// The smallest internal key served by the table.
    pub(crate) smallest_key: Option<LookupKey>,

    /// The largest internal key served by the table.
    pub(crate) largest_key: Option<LookupKey>,
}

/// Public methods
impl FileMetadata {
    /// Create an empty instance of [`FileMetadata`].
    pub fn new(file_number: u64) -> Self {
        Self {
            file_number,
            allowed_seeks: None,
            file_size: None,
            smallest_key: None,
            largest_key: None,
        }
    }

    pub fn set_file_size(&mut self, file_size: u64) {
        // Allowed seeks is one seek per a configured number of bytes read in a seek
        let mut allowed_seeks = (file_size / SEEK_DATA_SIZE_THRESHOLD_KIB) as usize;
        if allowed_seeks < 100 {
            allowed_seeks = 100;
        }

        self.file_size = Some(file_size);
        self.allowed_seeks = Some(allowed_seeks);
    }
}
