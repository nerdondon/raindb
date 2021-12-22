use std::ops::Range;
use std::sync::Arc;

use crate::config::{MAX_MEM_COMPACT_LEVEL, MAX_NUM_LEVELS};
use crate::key::{InternalKey, MAX_SEQUENCE_NUMBER};
use crate::table_cache::TableCache;
use crate::DbOptions;

use super::errors::ReadResult;
use super::file_metadata::FileMetadata;
use super::utils::sum_file_sizes;

/**
Metadata required to charge a file for a multi-level disk seek.

This is analogous to [`Version::GetStats`] in LevelDB. The name used in RainDB attempts to be more
descriptive with respect to what this struct is used for.
*/
#[derive(Debug)]
pub(crate) struct SeekChargeMetadata {
    /// The file that will be charged for multi-level seeks.
    seek_file: Option<Arc<FileMetadata>>,

    /// The level of the file being charged.
    seek_file_level: Option<usize>,
}

/// Public methods
impl SeekChargeMetadata {
    /// Create a new instance of [`SeekChargeMetadata`].
    pub fn new(seek_file: Option<Arc<FileMetadata>>, seek_file_level: usize) -> Self {
        Self {
            seek_file: None,
            seek_file_level: None,
        }
    }
}

pub(crate) struct GetResponse {
    /// The found value.
    value: Vec<u8>,

    /// The seek charge metadata that should be applied.
    charge_metadata: SeekChargeMetadata,
}

/**
Metadata used for scoring the necessity of compacting a version based on the size of files within
the version.
*/
#[derive(Debug)]
pub(crate) struct SizeCompactionMetadata {
    /// The level that should be compacted next.
    pub(crate) compaction_level: usize,

    /**
    A score for determining the necessity of compaction for the level that will be compacted next.

    A score < 1 means that compaction is not strictly necessary.
    */
    pub(crate) compaction_score: f64,
}

/**
Metadata used for scoring the necessity of compacting a version based on the number of disk seeks
being performed during read operations within the version.
*/
#[derive(Debug)]
pub(crate) struct SeekCompactionMetadata {
    /// The next file to compact based on seek stats.
    pub(crate) file_to_compact: Option<Arc<FileMetadata>>,

    /// The level of the next file to compact.
    pub(crate) level_of_file_to_compact: usize,
}

/// Version contains information that represents a point in time state of the database.
#[derive(Debug)]
pub(crate) struct Version {
    /// Options configuring database behavior.
    db_options: DbOptions,

    /// List of files per level.
    files: [Vec<Arc<FileMetadata>>; MAX_NUM_LEVELS],

    /**
    A cache for accessing table files.

    This will be shared with the child versions.
    */
    table_cache: Arc<TableCache>,

    /**
    Metadata used for scoring the necessity of compacting a version based on the number of disk
    seeks being performed during read operations within the version.

    This is updated as read requests are serviced.
    */
    seek_compaction_metadata: Option<SeekCompactionMetadata>,

    /**
    Metadata used for scoring the necessity of compacting a version based on the size of files
    within the version.

    This is filled in when the version is finalized (e.g. via [`VersionSet::finalize`]).

    [`VersionSet::finalize`]: super::version_set::VersionSet::finalize
    */
    size_compaction_metadata: Option<SizeCompactionMetadata>,
}

/// Public methods
impl Version {
    pub fn new(db_options: DbOptions, table_cache: &Arc<TableCache>) -> Self {
        let files = [vec![]; MAX_NUM_LEVELS];

        Self {
            db_options,
            table_cache: Arc::clone(table_cache),
            seek_compaction_metadata: None,
            size_compaction_metadata: None,
            files,
        }
    }

    /**
    Look up the value for the given key.

    # Concurrency
    Does not require a lock to be held.

    Returns an `GetResponse` struct containing the found value and seek charge metadata.
    */
    pub fn get(&self) -> ReadResult<GetResponse> {
        todo!("working on it!");
    }

    /**
    Apply the charging metadata to the current state.

    # Concurrency
    An external lock is required to be held before exercising this method.

    Returns true if a new compaction may need to be triggered, false otherwise.
    */
    pub fn update_stats(&self, charging_metadata: SeekChargeMetadata) -> ReadResult<bool> {
        todo!("working on it!");
    }

    /// Return the number of table files at the specified level.
    pub fn num_files_at_level(&self, level: u64) -> usize {
        self.files[level as usize].len()
    }

    /// Get a reference to the version's seek compaction metadata.
    pub fn get_seek_compaction_metadata(&self) -> Option<&SeekCompactionMetadata> {
        self.seek_compaction_metadata.as_ref()
    }

    /// Get a reference to the version's size compaction metadata.
    pub fn get_size_compaction_metadata(&self) -> Option<&SizeCompactionMetadata> {
        self.size_compaction_metadata.as_ref()
    }

    /**
    Return the level at which we should place a new memtable compaction result that covers the
    range from `smallest_user_key` to `largest_user_key`.
    */
    pub fn pick_level_for_memtable_output(
        &self,
        smallest_user_key: &[u8],
        largest_user_key: &[u8],
    ) -> usize {
        let mut level: usize = 0;
        if self.has_overlap_in_level(0, smallest_user_key, largest_user_key) {
            // There is an overlap at level 0 and level 0 is the only level that allows overlaps so
            // we must add the new table file here.
            return level;
        }

        /*
        Try to add the new table file as deeply as possible up to `MAX_MEM_COMPACT_LEVEL`. See
        the constant's documentation for more details.

        Do this as long as:

        1. There is no overlap in the next level (l + 1)
        2. The number of overlapping bytes in the level after the next (l + 2) do not breach a set
           threshold
        */
        let start_key =
            InternalKey::new_for_seeking(smallest_user_key.to_vec(), MAX_SEQUENCE_NUMBER);
        let end_key = InternalKey::new(largest_user_key.to_vec(), 0, crate::key::Operation::Delete);
        while level < MAX_MEM_COMPACT_LEVEL {
            if self.has_overlap_in_level(level + 1, smallest_user_key, largest_user_key) {
                break;
            }

            if level + 2 < MAX_NUM_LEVELS {
                let files_overlapping_range: Vec<&Arc<FileMetadata>> =
                    self.get_overlapping_files(level + 2, &start_key..&end_key);

                let total_overlapping_file_size = sum_file_sizes(&files_overlapping_range);
                if total_overlapping_file_size
                    > Version::max_grandparent_overlap_bytes(&self.db_options)
                {
                    break;
                }
            }

            level += 1;
        }

        level
    }

    /**
    Get table files at the specified level that overlap the specified key range.

    # Panics

    This method can only be called on levels greater than zero and below the maximum number of
    levels [`MAX_NUM_LEVELS`].

    # Legacy

    This is synonomous to LevelDB's `Version::GetOverlappingInputs` method.
    */
    pub fn get_overlapping_files(
        &self,
        level: usize,
        key_range: Range<&InternalKey>,
    ) -> Vec<&Arc<FileMetadata>> {
        assert!(level > 0);
        assert!(level <= MAX_NUM_LEVELS);

        let overlapping_files = vec![];
        let mut start_user_key = key_range.start.get_user_key();
        let mut end_user_key = key_range.end.get_user_key();
        let mut index: usize = 0;
        while index < self.files[level].len() {
            let current_file = &self.files[level][index];
            let file_range_start = current_file.smallest_key().get_user_key();
            let file_range_end = current_file.largest_key().get_user_key();
            if file_range_end < start_user_key {
                // File range is completely before the target range so we can skip this file
                index += 1;
            } else if end_user_key < file_range_start {
                // File range is completely after the target range so we can skip this file
                index += 1;
            } else {
                overlapping_files.push(current_file);

                if level != 0 {
                    continue;
                }

                // Level-0 files may have overlapping key ranges. Check if the newly added file
                // expands our target range. If so, update the search range and restart the search.
                if file_range_start < start_user_key {
                    start_user_key = file_range_start;
                    overlapping_files.clear();
                    index = 0;
                } else if file_range_end > end_user_key {
                    end_user_key = file_range_end;
                    overlapping_files.clear();
                    index = 0;
                }
            }
        }

        overlapping_files
    }
}

/// Private methods
impl Version {
    /**
    Returns true if and only if some file in the specified level overlaps some part of the
    range covered by the provided user keys.

    # Legacy

    This is synonomous to LevelDB's `Version::OverlapInLevel`.
    */
    fn has_overlap_in_level(
        &self,
        level: usize,
        smallest_user_key: &[u8],
        largest_user_key: &[u8],
    ) -> bool {
        Version::some_file_overlaps_range(
            level > 0,
            &self.files[level],
            smallest_user_key,
            largest_user_key,
        )
    }

    /**
    Returns true if and only if some file in the provided `files` overlaps the key range formed by
    the provided user keys.

    If `disjoint_sorted_files` is set to true, `files` must contain files with disjoint
    (i.e. non-overlapping) key ranges in sorted order. For example, the files in any one level,
    where the level is > 0, have non-overlapping key ranges.
    */
    fn some_file_overlaps_range(
        disjoint_sorted_files: bool,
        files: &[Arc<FileMetadata>],
        smallest_user_key: &[u8],
        largest_user_key: &[u8],
    ) -> bool {
        if !disjoint_sorted_files {
            // Need to check all files if there are overlapping key ranges
            for file in files {
                let is_key_after_file = smallest_user_key > file.largest_key().get_user_key();
                let is_key_before_file = largest_user_key < file.smallest_key().get_user_key();

                if is_key_after_file || is_key_before_file {
                    // No overlap
                } else {
                    // There is overlap
                    return true;
                }
            }

            return false;
        }

        // Create the earliest full key from the specified user key, recalling that sequence
        // numbers are sorted in decreasing order.
        let smallest_full_key =
            InternalKey::new_for_seeking(smallest_user_key.to_vec(), MAX_SEQUENCE_NUMBER);
        let maybe_file_index = Version::find_file_with_upper_bound_range(files, &smallest_full_key);

        if maybe_file_index.is_none() {
            // The beginning of the range is after all of the files, so there is no overlap
            return false;
        }

        let file_index = maybe_file_index.unwrap();
        // We know file[file_index].largest > smallest_user_key.
        // If the largest_user_key is also < file[file_index].smallest, then there is no overlap.
        largest_user_key >= files[file_index].smallest_key().get_user_key()
    }

    /**
    Binary search a sorted set of disjoint files for a file whose largest key forms a tight upper
    bound on the target key.

    # Invariants

    The passed in `files` **must** be a sorted set and the files must store key ranges that do
    not overlap with the key ranges in any other file.

    Returns the index of the file whose key range creates an upper bound on the target key (i.e.
    its largest key is greater than or equal the target). Otherwise returns `None`.

    # Legacy

    This is synonomous with LevelDB's `leveldb::FindFile` method.
    */
    fn find_file_with_upper_bound_range(
        files: &[Arc<FileMetadata>],
        target_user_key: &InternalKey,
    ) -> Option<usize> {
        let mut left: usize = 0;
        let mut right: usize = files.len();
        while left < right {
            let mid: usize = (left + right) / 2;
            let file = files[mid];

            if file.largest_key() < target_user_key {
                // The largest key in the file at mid is less than the target, so the set of files
                // at or before mid are not interesting.
                left = mid + 1;
            } else {
                // The largest key in the file at mid is greater than or equal to the target, so
                // the set of files after mid are not interesting.
                right = mid;
            }
        }

        if right == files.len() {
            return None;
        }

        Some(right)
    }

    /**
    Get the maximum number of bytes that a level can overlap with it's grandparent (level + 2).

    This can be used to determine placement of a new table file.
    */
    fn max_grandparent_overlap_bytes(options: &DbOptions) -> u64 {
        options.max_file_size() * 10
    }
}
