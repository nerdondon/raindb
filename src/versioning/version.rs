use std::cmp::Reverse;
use std::fmt::Write;
use std::ops::Range;
use std::sync::Arc;

use crate::config::{L0_COMPACTION_TRIGGER, MAX_MEM_COMPACT_LEVEL, MAX_NUM_LEVELS};
use crate::errors::{RainDBError, RainDBResult};
use crate::key::{InternalKey, MAX_SEQUENCE_NUMBER};
use crate::table_cache::TableCache;
use crate::tables::Table;
use crate::{compaction, tables, DbOptions, RainDbIterator, ReadOptions};

use super::errors::{ReadError, ReadResult};
use super::file_iterators::FilesEntryIterator;
use super::file_metadata::FileMetadata;

/**
Metadata required to charge a file for a multi-level disk seek.

This is analogous to [`Version::GetStats`] in LevelDB. The name used in RainDB attempts to be more
descriptive with respect to what this struct is used for.
*/
#[derive(Clone, Debug)]
pub(crate) struct SeekChargeMetadata {
    /// The file that will be charged for multi-level seeks.
    seek_file: Option<Arc<FileMetadata>>,

    /// The level of the file being charged.
    seek_file_level: Option<usize>,
}

/// Crate-only methods
impl SeekChargeMetadata {
    /// Create a new instance of [`SeekChargeMetadata`].
    pub fn new() -> Self {
        Self {
            seek_file: None,
            seek_file_level: None,
        }
    }

    /// Get a strong reference to the file being charged.
    pub(crate) fn seek_file(&self) -> Option<&Arc<FileMetadata>> {
        self.seek_file.as_ref()
    }

    /// Get the level of the file being charged.
    pub(crate) fn seek_file_level(&self) -> usize {
        self.seek_file_level.unwrap()
    }
}

/// Represents the response from a `Version::get` operation.
pub(crate) struct GetResponse {
    /// The found value, if any.
    pub(crate) value: Option<Vec<u8>>,

    /// The seek charge metadata that should be applied.
    pub(crate) charge_metadata: SeekChargeMetadata,
}

/**
Metadata used for scoring the necessity of compacting a version based on the size of files within
the version.
*/
#[derive(Clone, Debug)]
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
#[derive(Clone, Debug)]
pub(crate) struct SeekCompactionMetadata {
    /// The next file to compact based on seek stats.
    pub(crate) file_to_compact: Option<Arc<FileMetadata>>,

    /// The level of the next file to compact.
    pub(crate) level_of_file_to_compact: usize,
}

impl Default for SeekCompactionMetadata {
    fn default() -> Self {
        Self {
            file_to_compact: None,
            level_of_file_to_compact: 0,
        }
    }
}

/// Version contains information that represents a point in time state of the database.
#[derive(Clone, Debug)]
pub(crate) struct Version {
    /// Options configuring database behavior.
    db_options: DbOptions,

    /// List of files per level.
    pub(crate) files: [Vec<Arc<FileMetadata>>; MAX_NUM_LEVELS],

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
    seek_compaction_metadata: SeekCompactionMetadata,

    /**
    Metadata used for scoring the necessity of compacting a version based on the size of files
    within the version.

    These fields are populated when the version is finalized (e.g. via [`Version::finalize`]).

    [`VersionSet::finalize`]: super::version_set::VersionSet::finalize
    */
    size_compaction_metadata: Option<SizeCompactionMetadata>,

    /**
    The sequence number that this version was created at.

    This information is used primarily for debugging purposes.
    */
    last_sequence_number: u64,

    /**
    The file number of the write-ahead log in use at the time that this version was created.

    This information is used primarily for debugging purposes.
    */
    wal_file_number: u64,
}

/// Crate-only methods
impl Version {
    /// Create a new instance of [`Version`].
    pub(crate) fn new(
        db_options: DbOptions,
        table_cache: &Arc<TableCache>,
        last_sequence_number: u64,
        wal_file_number: u64,
    ) -> Self {
        let files = Default::default();

        Self {
            db_options,
            table_cache: Arc::clone(table_cache),
            seek_compaction_metadata: SeekCompactionMetadata::default(),
            size_compaction_metadata: None,
            files,
            last_sequence_number,
            wal_file_number,
        }
    }

    /// Clones the current version while resetting certain state.
    pub(crate) fn new_from_current(
        &self,
        last_sequence_number: u64,
        wal_file_number: u64,
    ) -> Version {
        let mut new_version = self.clone();

        // Reset compaction metadata
        new_version.set_seek_compaction_metadata(SeekCompactionMetadata::default());
        new_version.set_size_compaction_metadata(None);

        // Set debugging info for new version
        new_version.wal_file_number = wal_file_number;
        new_version.last_sequence_number = last_sequence_number;

        new_version
    }

    /**
    Look up the value for the given key.

    # Concurrency
    Does not require a lock to be held.

    Returns an `GetResponse` struct containing the found value and seek charge metadata.
    */
    pub(crate) fn get(
        &self,
        read_options: &ReadOptions,
        key: &InternalKey,
    ) -> ReadResult<GetResponse> {
        let files_per_level = self.get_overlapping_files(key);

        // Keep some state for charging seeks
        let mut last_file_read: Option<&Arc<FileMetadata>> = None;
        let mut last_level_read: usize = 0;
        let mut seek_charge_metadata = SeekChargeMetadata::new();

        for (level, files) in files_per_level.iter().enumerate() {
            for file in files {
                if seek_charge_metadata.seek_file.is_none() && last_file_read.is_some() {
                    // There was more than one disk seek for this get operation. Charge the first
                    // file read.
                    seek_charge_metadata.seek_file = last_file_read.take().cloned();
                    seek_charge_metadata.seek_file_level = Some(last_level_read);
                }

                last_file_read = Some(file);
                last_level_read = level;

                match self.table_cache.get(read_options, file.file_number(), key) {
                    Ok(maybe_val) => {
                        return Ok(GetResponse {
                            value: maybe_val,
                            charge_metadata: seek_charge_metadata,
                        })
                    }
                    Err(tables::errors::ReadError::KeyNotFound) => continue,
                    Err(base_err) => {
                        return Err(ReadError::TableRead((base_err, Some(seek_charge_metadata))))
                    }
                };
            }
        }

        Ok(GetResponse {
            value: None,
            charge_metadata: seek_charge_metadata,
        })
    }

    /**
    Apply the charging metadata to the current state.

    Returns true if a new compaction may need to be triggered, false otherwise.

    # Concurrency

    An external lock is required to be held before exercising this method.
    */
    pub(crate) fn update_stats(&mut self, charging_metadata: &SeekChargeMetadata) -> bool {
        if let Some(file_to_charge) = charging_metadata.seek_file.as_ref() {
            file_to_charge.decrement_allowed_seeks();

            if file_to_charge.allowed_seeks() <= 0
                && self.seek_compaction_metadata.file_to_compact.is_none()
            {
                self.seek_compaction_metadata.file_to_compact = Some(Arc::clone(file_to_charge));
                self.seek_compaction_metadata.level_of_file_to_compact =
                    charging_metadata.seek_file_level.unwrap();

                return true;
            }
        }

        false
    }

    /// Return the number of table files at the specified level.
    pub(crate) fn num_files_at_level(&self, level: usize) -> usize {
        self.files[level].len()
    }

    /// Get a reference to the version's seek compaction metadata.
    pub(crate) fn get_seek_compaction_metadata(&self) -> &SeekCompactionMetadata {
        &self.seek_compaction_metadata
    }

    /// Get a reference to the version's size compaction metadata.
    pub(crate) fn get_size_compaction_metadata(&self) -> Option<&SizeCompactionMetadata> {
        self.size_compaction_metadata.as_ref()
    }

    /// Set the version's seek compaction metadata.
    pub(crate) fn set_seek_compaction_metadata(
        &mut self,
        seek_compaction_metadata: SeekCompactionMetadata,
    ) {
        self.seek_compaction_metadata = seek_compaction_metadata;
    }

    /// Set the version's size compaction msetadata.
    pub(crate) fn set_size_compaction_metadata(
        &mut self,
        size_compaction_metadata: Option<SizeCompactionMetadata>,
    ) {
        self.size_compaction_metadata = size_compaction_metadata;
    }

    /// Get a reference to the version's last sequence number.
    pub(crate) fn last_sequence_number(&self) -> u64 {
        self.last_sequence_number
    }

    /// Get a reference to the WAL file number that the version was created at.
    pub(crate) fn wal_file_number(&self) -> u64 {
        self.wal_file_number
    }

    /**
    Returns true if and only if some file in the specified level overlaps some part of the
    range covered by the provided user keys.

    # Legacy

    This is synonomous to LevelDB's `Version::OverlapInLevel`.
    */
    pub(crate) fn has_overlap_in_level(
        &self,
        level: usize,
        smallest_user_key: Option<&[u8]>,
        largest_user_key: Option<&[u8]>,
    ) -> bool {
        Version::some_file_overlaps_range(
            level > 0,
            &self.files[level],
            smallest_user_key,
            largest_user_key,
        )
    }

    /**
    Return the level at which we should place a new memtable compaction result that covers the
    range from `smallest_user_key` to `largest_user_key`.
    */
    pub(crate) fn pick_level_for_memtable_output(
        &self,
        smallest_user_key: &[u8],
        largest_user_key: &[u8],
    ) -> usize {
        let mut level: usize = 0;
        if self.has_overlap_in_level(0, Some(smallest_user_key), Some(largest_user_key)) {
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
            if self.has_overlap_in_level(level + 1, Some(smallest_user_key), Some(largest_user_key))
            {
                break;
            }

            if level + 2 < MAX_NUM_LEVELS {
                let files_overlapping_range = self
                    .get_overlapping_compaction_inputs(level + 2, Some(&start_key)..Some(&end_key));

                let total_overlapping_file_size =
                    super::utils::sum_file_sizes(&files_overlapping_range);
                if total_overlapping_file_size
                    > compaction::utils::max_grandparent_overlap_bytes_from_options(
                        &self.db_options,
                    )
                {
                    break;
                }
            }

            level += 1;
        }

        level
    }

    /**
    Get files that overlap the provided key.

    # Legacy

    This is synonomous to LevelDB's `Version::ForEachOverlapping` but inverts the iteration i.e.
    the results are externally iterated by the caller. Much of LevelDB relies on internal iteration,
    but if felt more ergnomic to have a inverted relationship in RainDB.
    */
    pub(crate) fn get_overlapping_files(
        &self,
        target_key: &InternalKey,
    ) -> [Vec<Arc<FileMetadata>>; MAX_NUM_LEVELS] {
        let target_user_key = target_key.get_user_key();
        let mut files: [Vec<Arc<FileMetadata>>; MAX_NUM_LEVELS] = Default::default();

        // Get level zero files with key ranges that might include the specified key in order from
        // newest file to oldest
        for file in self.files[0].iter() {
            if file.smallest_key().get_user_key() <= target_user_key
                && file.largest_key().get_user_key() >= target_user_key
            {
                files[0].push(file.clone());
            }
        }
        files[0].sort_by_key(|f| Reverse(f.file_number()));

        // Levels greater than zero do not have files with overlapping key ranges so we can just
        // binary search for the file containing the target key in its key range
        for level in 1..MAX_NUM_LEVELS {
            let level_files = &self.files[level];
            if level_files.is_empty() {
                continue;
            }

            let maybe_file_index =
                super::utils::find_file_with_upper_bound_range(level_files, target_key);
            if let Some(file_index) = maybe_file_index {
                let file = &level_files[file_index];
                if file.smallest_key().get_user_key() <= target_user_key {
                    files[level].push(file.clone());
                }
            }
        }

        files
    }

    /**
    Get table files at the specified level that overlap the specified key range.

    This differs from just getting files overlapping a key range in that, this method will expand
    the key range if there are overlapping files in level zero.

    # Panics

    This method can only be called on a valid level (i.e. > [`MAX_NUM_LEVELS`]).

    # Legacy

    This is synonomous to LevelDB's `Version::GetOverlappingInputs` method.
    */
    pub(crate) fn get_overlapping_compaction_inputs(
        &self,
        level: usize,
        key_range: Range<Option<&InternalKey>>,
    ) -> Vec<&Arc<FileMetadata>> {
        assert!(level < MAX_NUM_LEVELS);

        let mut overlapping_files = vec![];
        let mut start_user_key = key_range
            .start
            .map(|internal_key| internal_key.get_user_key());
        let mut end_user_key = key_range
            .end
            .map(|internal_key| internal_key.get_user_key());
        let mut index: usize = 0;
        while index < self.files[level].len() {
            let current_file = &self.files[level][index];
            let file_range_start = current_file.smallest_key().get_user_key();
            let file_range_end = current_file.largest_key().get_user_key();
            let is_file_range_before_target =
                key_range.start.is_some() && file_range_end < start_user_key.unwrap();
            let is_file_range_after_target =
                key_range.end.is_some() && end_user_key.unwrap() < file_range_start;

            index += 1;
            if is_file_range_before_target || is_file_range_after_target {
                // The file is completely before or completely after the target range. Don't do
                // anything.
            } else {
                overlapping_files.push(current_file);

                if level != 0 {
                    continue;
                }

                // Level-0 files may have overlapping key ranges. Check if the newly added file
                // expands our target range. If so, update the search range and restart the search.
                if key_range.start.is_some() && file_range_start < start_user_key.unwrap() {
                    start_user_key = Some(file_range_start);
                    overlapping_files.clear();
                    index = 0;
                } else if key_range.end.is_some() && file_range_end > end_user_key.unwrap() {
                    end_user_key = Some(file_range_end);
                    overlapping_files.clear();
                    index = 0;
                }
            }
        }

        overlapping_files
    }

    /**
    The same as `Version::get_overlapping_compaction_inputs` but returns owned references instead
    of a reference to the [`Arc`].
    */
    pub(crate) fn get_overlapping_compaction_inputs_strong(
        &self,
        level: usize,
        key_range: Range<Option<&InternalKey>>,
    ) -> Vec<Arc<FileMetadata>> {
        self.get_overlapping_compaction_inputs(level, key_range)
            .into_iter()
            .map(Arc::clone)
            .collect()
    }

    /**
    Finalize a version by calculating compaction scores.

    Level 0 is treated differently than other levels where it is bounded by number of files rather
    than the total bytes in the level for two reasons:

    1. With larger memtables, level 0 compactions can be read intensive

    1. The files in level 0 are merged on every read, so we want to minimize the number of
       individual files when the file size is small. File sizes can be small if the memtable maximum
       size setting is low, if the compression ratios are high, or if there are lots of rights or
       individual deletions.

    # Legacy

    This is synonomous with LevelDB's `VersionSet::Finalize`.
    */
    pub(crate) fn finalize(&mut self) {
        let mut best_level: usize = 0;
        let mut best_score: f64 = -1.;

        for level in 0..MAX_NUM_LEVELS {
            let new_score: f64;
            if level == 0 {
                new_score = (self.files[level].len() / L0_COMPACTION_TRIGGER) as f64;
            } else {
                let level_file_size = super::utils::sum_file_sizes(&self.files[level]) as f64;
                new_score = level_file_size / Version::max_bytes_for_level(level);
            }

            if new_score > best_score {
                best_score = new_score;
                best_level = level;
            }
        }

        self.set_size_compaction_metadata(Some(SizeCompactionMetadata {
            compaction_level: best_level,
            compaction_score: best_score,
        }));
    }

    /**
    Returns true if the version requires a size triggered compaction.

    # Panics

    This will panic if the `size_compaction_metadata` field is not initialized. In practice, this
    cannot happen because `Version::finalize` is always called on newly created versions and it
    will populate the field.
    */
    pub(crate) fn requires_size_compaction(&self) -> bool {
        let compaction_score = self
            .get_size_compaction_metadata()
            .unwrap()
            .compaction_score;

        compaction_score >= 1.
    }

    /// Returns true if the version requires a seek triggered compaction.
    pub(crate) fn requires_seek_compaction(&self) -> bool {
        self.get_seek_compaction_metadata()
            .file_to_compact
            .is_some()
    }

    /**
    Get a list of iterators that will yield the on-disk contents of a version when concatentated.

    # Legacy

    This is synonomous to LevelDB's `Version::AddIterators`.
    */
    pub(crate) fn get_representative_iterators(
        &self,
        read_options: &ReadOptions,
    ) -> RainDBResult<Vec<Box<dyn RainDbIterator<Key = InternalKey, Error = RainDBError>>>> {
        let initial_capacity = self.files[0].len() + MAX_NUM_LEVELS - 1;
        let mut iterators: Vec<Box<dyn RainDbIterator<Key = InternalKey, Error = RainDBError>>> =
            Vec::with_capacity(initial_capacity);

        // Add iterators for all level 0 files since level 0 can have overlapping key ranges
        for file in self.files[0].iter() {
            let table = self.table_cache.find_table(file.file_number())?;
            let table_iter = Box::new(Table::iter_with(table, read_options.clone()));
            iterators.push(table_iter);
        }

        // For levels greater than 0, use a concatenating iterator to walk through the
        // non-overlapping files in the level.
        for level in 1..MAX_NUM_LEVELS {
            let level_files = &self.files[level];
            if level_files.is_empty() {
                continue;
            }

            let file_list_iter = Box::new(FilesEntryIterator::new(
                level_files.clone(),
                Arc::clone(&self.table_cache),
                read_options.clone(),
            ));
            iterators.push(file_list_iter);
        }

        Ok(iterators)
    }

    /**
    Record a read statistics for the specified key.

    Returns true if a compaction should be scheduled.
    */
    pub(crate) fn record_read_sample(&mut self, key: &InternalKey) -> bool {
        let files_per_level = self.get_overlapping_files(key);

        // Keep some state for charging seeks
        let mut seek_charge_metadata = SeekChargeMetadata::new();
        let mut num_files_with_key: usize = 0;

        for (level, files) in files_per_level.iter().enumerate() {
            for file in files {
                num_files_with_key += 1;

                if num_files_with_key == 1 {
                    // Remember the first file with the key
                    seek_charge_metadata.seek_file = Some(Arc::clone(file));
                    seek_charge_metadata.seek_file_level = Some(level);
                }

                if num_files_with_key > 1 {
                    // Stop iteration after on the second file containing the key
                    break;
                }
            }
        }

        // We proceed to update stats only if there are at least 2 matches because we are trying
        // to flatten the search
        if num_files_with_key >= 2 {
            return self.update_stats(&seek_charge_metadata);
        }

        false
    }

    /**
    Get a full summary of the contents of the version including the files at each level and the key
    range and size of the files.

    # Format

    The format of the string is:

    ```text
    --- Level {level_num} ---
    {file_num_1} (size: {file_size})[{key_range_start}..{key_range_end}]
    {file_num_2} (size: {file_size})[{key_range_start}..{key_range_end}]
    ```
    */
    pub(crate) fn debug_summary(&self) -> String {
        let mut summary = "".to_owned();
        for (level, files) in self.files.iter().enumerate() {
            writeln!(summary, "--- Level {level} ---").unwrap();

            for file in files {
                writeln!(
                    summary,
                    "{file_num} (size: {file_size})[{key_range_start:?}..{key_range_end:?}]",
                    file_num = file.file_number(),
                    file_size = file.get_file_size(),
                    key_range_start = file.smallest_key(),
                    key_range_end = file.largest_key()
                )
                .unwrap();
            }
        }

        summary
    }

    /**
    Get the current size of a level in bytes.

    # Panics

    The method will panic if the specified level is not in the valid range.
    */
    pub(crate) fn get_level_size(&self, level: usize) -> u64 {
        assert!(level < MAX_NUM_LEVELS);

        super::utils::sum_file_sizes(&self.files[level])
    }
}

/// Private methods
impl Version {
    /**
    Returns true if and only if some file in the provided `files` overlaps the key range formed by
    the provided user keys.

    If `disjoint_sorted_files` is set to true, `files` must contain files with disjoint
    (i.e. non-overlapping) key ranges in sorted order. For example, the files in any one level,
    where the level is > 0, have non-overlapping key ranges.

    If `smallest_user_key` is set to `None`, this will indicate a key smaller than all keys in the
    database.

    If `largest_user_key` is set to `None`, this will indicate a key larger than all keys in the
    database.
    */
    fn some_file_overlaps_range(
        disjoint_sorted_files: bool,
        files: &[Arc<FileMetadata>],
        smallest_user_key: Option<&[u8]>,
        largest_user_key: Option<&[u8]>,
    ) -> bool {
        if files.is_empty() {
            // Early return since an empty list of files cannot overlap
            return false;
        }

        if !disjoint_sorted_files {
            // Need to check all files if there are overlapping key ranges
            for file in files {
                let is_key_after_file = smallest_user_key.is_some()
                    && smallest_user_key.unwrap() > file.largest_key().get_user_key();
                let is_key_before_file = largest_user_key.is_some()
                    && largest_user_key.unwrap() < file.smallest_key().get_user_key();

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
        let mut overlapping_file_index: usize = 0;
        if let Some(smallest_key) = smallest_user_key {
            let smallest_full_key =
                InternalKey::new_for_seeking(smallest_key.to_vec(), MAX_SEQUENCE_NUMBER);
            if let Some(file_index) =
                super::utils::find_file_with_upper_bound_range(files, &smallest_full_key)
            {
                overlapping_file_index = file_index;
            } else {
                // The beginning of the range is after all of the files, so there is no overlap
                return false;
            }
        }

        // The start of the target range is in key range of the list of files so an open ended end
        // of the target range means that there is an overlap
        if largest_user_key.is_none() {
            return true;
        }

        // We know file[overlapping_file_index].largest > smallest_user_key.
        // If the largest_user_key is also < file[overlapping_file_index].smallest, then there is
        // no overlap.
        largest_user_key.is_some()
            && largest_user_key.unwrap()
                >= files[overlapping_file_index].smallest_key().get_user_key()
    }

    /**
    Calculate the maximum number of bytes allowed for a level.

    Note that the level 0 result is not really used because the level 0 compaction threshold is
    based on the number of files in the level.
    */
    fn max_bytes_for_level(level: usize) -> f64 {
        // The threshold is calculated as 10x multiples of 1 MiB.
        let starting_multiple_bytes: f64 = 1. * 1024. * 1024.;
        let mut level = level;
        let mut result: f64 = 10. * starting_multiple_bytes;
        while level > 1 {
            result *= 10.;
            level -= 1;
        }

        result
    }
}

#[cfg(test)]
mod some_file_overlaps_range_tests {
    use crate::Operation;

    use super::*;

    #[test]
    fn given_an_empty_list_returns_false() {
        let files: Vec<Arc<FileMetadata>> = vec![];

        assert!(!Version::some_file_overlaps_range(
            false,
            &files,
            Some(b"efg"),
            Some(b"tuv")
        ));
        assert!(!Version::some_file_overlaps_range(
            false,
            &files,
            None,
            Some(b"tuv")
        ));
        assert!(!Version::some_file_overlaps_range(
            false,
            &files,
            Some(b"efg"),
            None
        ));
        assert!(!Version::some_file_overlaps_range(
            false, &files, None, None
        ));
    }

    #[test]
    fn given_a_list_of_one_element_succeeds() {
        let mut files: Vec<Arc<FileMetadata>> = vec![];
        insert_files_with_key_range(&mut files, vec![b"200".to_vec()..b"300".to_vec()]);

        assert!(
            !Version::some_file_overlaps_range(true, &files, Some(b"150"), Some(b"160")),
            "A target range completely before the key space in the list of files should not have \
            overlap."
        );
        assert!(
            !Version::some_file_overlaps_range(true, &files, Some(b"301"), Some(b"400")),
            "A target range completely after the key space in the list of files should not have \
            overlap."
        );

        // Check boundaries
        assert!(Version::some_file_overlaps_range(
            true,
            &files,
            Some(b"150"),
            Some(b"200")
        ));
        assert!(Version::some_file_overlaps_range(
            true,
            &files,
            Some(b"200"),
            Some(b"200")
        ));
        assert!(Version::some_file_overlaps_range(
            true,
            &files,
            Some(b"200"),
            Some(b"300")
        ));
        assert!(Version::some_file_overlaps_range(
            true,
            &files,
            Some(b"250"),
            Some(b"300")
        ));
        assert!(Version::some_file_overlaps_range(
            true,
            &files,
            Some(b"300"),
            Some(b"300")
        ));

        // Check partially overlapping ranges
        assert!(Version::some_file_overlaps_range(
            true,
            &files,
            Some(b"150"),
            Some(b"250")
        ));
        assert!(Version::some_file_overlaps_range(
            true,
            &files,
            Some(b"250"),
            Some(b"400")
        ));

        // Check completely overlapped range
        assert!(Version::some_file_overlaps_range(
            true,
            &files,
            Some(b"220"),
            Some(b"280")
        ));
    }

    #[test]
    fn given_a_disjoint_list_multiple_elements_succeeds() {
        let mut files: Vec<Arc<FileMetadata>> = vec![];
        insert_files_with_key_range(
            &mut files,
            vec![
                b"150".to_vec()..b"200".to_vec(),
                b"200".to_vec()..b"250".to_vec(),
                b"300".to_vec()..b"350".to_vec(),
                b"400".to_vec()..b"450".to_vec(),
            ],
        );

        assert!(
            !Version::some_file_overlaps_range(true, &files, Some(b"120"), Some(b"140")),
            "A target range completely before the key space in the list of files should not have \
            overlap."
        );
        assert!(
            !Version::some_file_overlaps_range(true, &files, Some(b"460"), Some(b"470")),
            "A target range completely after the key space in the list of files should not have \
            overlap."
        );
        assert!(
            !Version::some_file_overlaps_range(true, &files, Some(b"251"), Some(b"299")),
            "Should not detect overlap for a target range completely between key spaces of all \
            files."
        );

        // Check boundaries
        assert!(Version::some_file_overlaps_range(
            true,
            &files,
            Some(b"120"),
            Some(b"150")
        ));
        assert!(Version::some_file_overlaps_range(
            true,
            &files,
            Some(b"200"),
            Some(b"200")
        ));
        assert!(Version::some_file_overlaps_range(
            true,
            &files,
            Some(b"250"),
            Some(b"299")
        ));
        assert!(Version::some_file_overlaps_range(
            true,
            &files,
            Some(b"450"),
            Some(b"460")
        ));

        // Check various overlaps
        assert!(Version::some_file_overlaps_range(
            true,
            &files,
            Some(b"150"),
            Some(b"250")
        ));
        assert!(Version::some_file_overlaps_range(
            true,
            &files,
            Some(b"250"),
            Some(b"400")
        ));
        assert!(Version::some_file_overlaps_range(
            false,
            &files,
            Some(b"220"),
            Some(b"280")
        ));
        assert!(Version::some_file_overlaps_range(
            false,
            &files,
            Some(b"275"),
            Some(b"320")
        ));
    }

    #[test]
    fn given_a_disjoint_list_multiple_elements_and_target_ranges_with_open_ranges_succeeds() {
        let mut files: Vec<Arc<FileMetadata>> = vec![];
        insert_files_with_key_range(
            &mut files,
            vec![
                b"150".to_vec()..b"200".to_vec(),
                b"200".to_vec()..b"250".to_vec(),
                b"300".to_vec()..b"350".to_vec(),
                b"400".to_vec()..b"450".to_vec(),
            ],
        );

        // Check boundaries
        assert!(!Version::some_file_overlaps_range(
            true,
            &files,
            None,
            Some(b"149")
        ));
        assert!(!Version::some_file_overlaps_range(
            true,
            &files,
            Some(b"451"),
            None
        ));

        assert!(Version::some_file_overlaps_range(true, &files, None, None));
        assert!(Version::some_file_overlaps_range(
            true,
            &files,
            Some(b"100"),
            None
        ));
        assert!(Version::some_file_overlaps_range(
            true,
            &files,
            Some(b"230"),
            None
        ));
        assert!(Version::some_file_overlaps_range(
            true,
            &files,
            Some(b"450"),
            None
        ));
    }

    #[test]
    fn given_a_non_disjoint_list_multiple_elements_succeeds() {
        let mut files: Vec<Arc<FileMetadata>> = vec![];
        insert_files_with_key_range(
            &mut files,
            vec![
                b"150".to_vec()..b"600".to_vec(),
                b"400".to_vec()..b"500".to_vec(),
            ],
        );

        // Check boundaries
        assert!(!Version::some_file_overlaps_range(
            false,
            &files,
            Some(b"100"),
            Some(b"149")
        ));
        assert!(!Version::some_file_overlaps_range(
            false,
            &files,
            Some(b"601"),
            Some(b"700")
        ));

        assert!(Version::some_file_overlaps_range(
            false,
            &files,
            Some(b"100"),
            Some(b"150"),
        ));
        assert!(Version::some_file_overlaps_range(
            false,
            &files,
            Some(b"100"),
            Some(b"700"),
        ));
        assert!(Version::some_file_overlaps_range(
            false,
            &files,
            Some(b"400"),
            Some(b"600"),
        ));
        assert!(Version::some_file_overlaps_range(
            false,
            &files,
            Some(b"450"),
            Some(b"500"),
        ));
        assert!(Version::some_file_overlaps_range(
            false,
            &files,
            Some(b"450"),
            Some(b"700"),
        ));
        assert!(Version::some_file_overlaps_range(
            false,
            &files,
            Some(b"600"),
            Some(b"700"),
        ));
    }

    /**
    Add files with the largest key set to [`InternalKey`]'s with the specified user keys to the
    provided metadata vector.
    */
    fn insert_files_with_key_range(
        files: &mut Vec<Arc<FileMetadata>>,
        user_keys: Vec<Range<Vec<u8>>>,
    ) {
        for user_key_range in user_keys.into_iter() {
            let mut file = FileMetadata::new(30);
            file.set_smallest_key(Some(create_testing_key(user_key_range.start)));
            file.set_largest_key(Some(create_testing_key(user_key_range.end)));
            files.push(Arc::new(file));
        }
    }

    /// Create an [`InternalKey`] based on the provided user key for testing.
    fn create_testing_key(user_key: Vec<u8>) -> InternalKey {
        InternalKey::new(user_key, 30, Operation::Put)
    }
}

#[cfg(test)]
mod version_tests {

    use pretty_assertions::assert_eq;
    use std::rc::Rc;

    use crate::tables::TableBuilder;
    use crate::Operation;

    use super::*;

    fn setup() {
        let _ = env_logger::builder()
            // Include all events in tests
            .filter_level(log::LevelFilter::max())
            // Ensure events are captured by `cargo test`
            .is_test(true)
            // Ignore errors initializing the logger if tests race to configure it
            .try_init();
    }

    #[test]
    fn get_overlapping_files() {
        let options = DbOptions::with_memory_env();
        let table_cache = Arc::new(TableCache::new(options.clone(), 1000));
        let mut version = Version::new(options.clone(), &table_cache, 99, 30);
        create_test_files_for_version(options, &mut version);

        // Test overlapping range in level 0
        let overlapping_files = version
            .get_overlapping_files(&InternalKey::new_for_seeking("d".as_bytes().to_vec(), 100));
        let expected_file_numbers = [61, 60];
        let actual_file_numbers = overlapping_files[0].iter().map(|file| file.file_number());
        assert!(expected_file_numbers
            .into_iter()
            .zip(actual_file_numbers)
            .all(|(expected, actual)| {
                assert_eq!(expected, actual);
                expected == actual
            }), "Multiple files can be returned for level 0 and should be returned in descending order.");

        // Test overlapping range in levels greater than 0
        let overlapping_files = version
            .get_overlapping_files(&InternalKey::new_for_seeking("x".as_bytes().to_vec(), 100));
        assert_eq!(overlapping_files[1].len(), 1);
        assert_eq!(overlapping_files[1][0].file_number(), 57);
        assert_eq!(overlapping_files[2].len(), 1);
        assert_eq!(overlapping_files[2][0].file_number(), 53);

        // Test single match in level greater than 0
        let overlapping_files = version
            .get_overlapping_files(&InternalKey::new_for_seeking("k".as_bytes().to_vec(), 100));
        assert_eq!(overlapping_files[2].len(), 1);
        assert_eq!(overlapping_files[2][0].file_number(), 55);

        // Test a target key that is not in the version keyspace
        let overlapping_files = version
            .get_overlapping_files(&InternalKey::new_for_seeking("z".as_bytes().to_vec(), 100));
        assert!(overlapping_files
            .iter()
            .all(|files_overlapping_in_level| files_overlapping_in_level.is_empty()));
    }

    #[test]
    fn get_value_from_version() {
        setup();

        let read_options = ReadOptions::default();
        let options = DbOptions::with_memory_env();
        let table_cache = Arc::new(TableCache::new(options.clone(), 1000));
        let mut version = Version::new(options.clone(), &table_cache, 99, 30);
        create_test_files_for_version(options, &mut version);

        // Test retrieval of a value from level 0
        let actual_response = version
            .get(
                &read_options,
                &InternalKey::new_for_seeking("a".as_bytes().to_vec(), 300),
            )
            .unwrap();
        assert_eq!(actual_response.value, Some("a".as_bytes().to_vec()));
        assert!(actual_response.charge_metadata.seek_file.is_none());

        // Test retrieval at an older level
        let actual_response = version
            .get(
                &read_options,
                &InternalKey::new_for_seeking("m".as_bytes().to_vec(), 300),
            )
            .unwrap();
        assert_eq!(actual_response.value, Some("m".as_bytes().to_vec()));
        assert!(actual_response.charge_metadata.seek_file().is_none());

        // Test retrieval requiring read through multiple levels
        let actual_response = version
            .get(
                &read_options,
                &InternalKey::new_for_seeking("p".as_bytes().to_vec(), 300),
            )
            .unwrap();
        assert_eq!(actual_response.value, Some("p".as_bytes().to_vec()));
        assert_eq!(
            actual_response
                .charge_metadata
                .seek_file()
                .unwrap()
                .file_number(),
            58
        );
        assert_eq!(actual_response.charge_metadata.seek_file_level(), 1);

        // Test retrieval at an older sequence number
        let actual_response = version
            .get(
                &read_options,
                &InternalKey::new_for_seeking("r".as_bytes().to_vec(), 64),
            )
            .unwrap();
        assert_eq!(actual_response.value, Some("r-1".as_bytes().to_vec()));
        assert_eq!(
            actual_response
                .charge_metadata
                .seek_file()
                .unwrap()
                .file_number(),
            58
        );
        assert_eq!(actual_response.charge_metadata.seek_file_level(), 1);

        // Test retrieval of a deleted value
        let actual_response = version
            .get(
                &read_options,
                &InternalKey::new_for_seeking("x".as_bytes().to_vec(), 80),
            )
            .unwrap();
        assert!(actual_response.value.is_none());
        assert!(actual_response.charge_metadata.seek_file().is_none());
    }

    #[test]
    fn update_stats_succeeds() {
        let options = DbOptions::with_memory_env();
        let table_cache = Arc::new(TableCache::new(options.clone(), 1000));
        let mut version = Version::new(options.clone(), &table_cache, 99, 30);
        create_test_files_for_version(options, &mut version);
        let mut seek_charge = SeekChargeMetadata::new();
        seek_charge.seek_file = Some(Arc::clone(&version.files[1][1]));
        seek_charge.seek_file_level = Some(1);

        let needs_compaction = version.update_stats(&seek_charge);
        assert!(
            !needs_compaction,
            "Should not need a compaction when there are still allowed seeks for a file."
        );

        // This is ugly but I don't really want to open up the visibility of the `allowed_seeks`
        // field.
        while seek_charge.seek_file().unwrap().allowed_seeks() > 0 {
            seek_charge.seek_file().unwrap().decrement_allowed_seeks();
        }

        let needs_compaction = version.update_stats(&seek_charge);
        assert!(
            needs_compaction,
            "Should need a compaction when there are no more allowed seeks for a file."
        );
        assert_eq!(
            version.get_seek_compaction_metadata().file_to_compact,
            Some(Arc::clone(&version.files[1][1]))
        );
        assert_eq!(
            version
                .get_seek_compaction_metadata()
                .level_of_file_to_compact,
            1
        );
    }

    #[test]
    fn get_overlapping_compaction_inputs_expands_level_0_search_range() {
        let options = DbOptions::with_memory_env();
        let table_cache = Arc::new(TableCache::new(options.clone(), 1000));
        let mut version = Version::new(options.clone(), &table_cache, 99, 30);
        create_test_files_for_version(options, &mut version);

        // Open start range
        let end_key = InternalKey::new("a".as_bytes().to_vec(), 105, Operation::Put);
        let key_range = None..Some(&end_key);
        let actual_inputs = version.get_overlapping_compaction_inputs(0, key_range);
        assert!(
            [60, 61, 62]
                .into_iter()
                .zip(actual_inputs)
                .all(|(expected, actual)| {
                    assert_eq!(expected, actual.file_number());
                    expected == actual.file_number()
                }),
            "Multiple files can be returned for level 0."
        );

        // Open end range
        let start_key = InternalKey::new("e".as_bytes().to_vec(), 105, Operation::Put);
        let key_range = Some(&start_key)..None;
        let actual_inputs = version.get_overlapping_compaction_inputs(0, key_range);
        assert!(
            [60, 61, 62]
                .into_iter()
                .zip(actual_inputs)
                .all(|(expected, actual)| {
                    assert_eq!(expected, actual.file_number());
                    expected == actual.file_number()
                }),
            "Multiple files can be returned for level 0."
        );

        // Closed range
        let start_key = InternalKey::new("c".as_bytes().to_vec(), 105, Operation::Put);
        let end_key = InternalKey::new("e".as_bytes().to_vec(), 105, Operation::Put);
        let key_range = Some(&start_key)..Some(&end_key);
        let actual_inputs = version.get_overlapping_compaction_inputs(0, key_range);
        assert!(
            [60, 61, 62]
                .into_iter()
                .zip(actual_inputs)
                .all(|(expected, actual)| {
                    assert_eq!(expected, actual.file_number());
                    expected == actual.file_number()
                }),
            "Multiple files can be returned for level 0."
        );
    }

    #[test]
    fn get_overlapping_compaction_inputs_gets_correct_files_for_levels_older_than_zero() {
        let options = DbOptions::with_memory_env();
        let table_cache = Arc::new(TableCache::new(options.clone(), 1000));
        let mut version = Version::new(options.clone(), &table_cache, 99, 30);
        create_test_files_for_version(options, &mut version);

        // Open start range
        let end_key = InternalKey::new("s".as_bytes().to_vec(), 105, Operation::Put);
        let key_range = None..Some(&end_key);
        let actual_inputs = version.get_overlapping_compaction_inputs(1, key_range);
        assert!([59, 58]
            .into_iter()
            .zip(actual_inputs)
            .all(|(expected, actual)| {
                assert_eq!(expected, actual.file_number());
                expected == actual.file_number()
            }), "Multiple files can be returned for level 0 and should be returned in descending order.");

        // Open end range
        let start_key = InternalKey::new("v".as_bytes().to_vec(), 105, Operation::Put);
        let key_range = Some(&start_key)..None;
        let actual_inputs = version.get_overlapping_compaction_inputs(1, key_range);
        assert!([57]
            .into_iter()
            .zip(actual_inputs)
            .all(|(expected, actual)| {
                assert_eq!(expected, actual.file_number());
                expected == actual.file_number()
            }), "Multiple files can be returned for level 0 and should be returned in descending order.");

        // Closed range
        let start_key = InternalKey::new("h".as_bytes().to_vec(), 105, Operation::Put);
        let end_key = InternalKey::new("r".as_bytes().to_vec(), 105, Operation::Put);
        let key_range = Some(&start_key)..Some(&end_key);
        let actual_inputs = version.get_overlapping_compaction_inputs(1, key_range);
        assert!([59, 58]
            .into_iter()
            .zip(actual_inputs)
            .all(|(expected, actual)| {
                assert_eq!(expected, actual.file_number());
                expected == actual.file_number()
            }));
    }

    #[test]
    fn pick_level_for_memtable_output_picks_the_correct_level() {
        let options = DbOptions::with_memory_env();
        let table_cache = Arc::new(TableCache::new(options.clone(), 1000));
        let mut version = Version::new(options.clone(), &table_cache, 99, 30);
        create_test_files_for_version(options, &mut version);

        // Test when overlap with level 0
        let actual_level = version.pick_level_for_memtable_output("b".as_bytes(), "e".as_bytes());
        assert_eq!(actual_level, 0);

        // Test when there is an overlap in the L+1 level
        let actual_level = version.pick_level_for_memtable_output("l".as_bytes(), "n".as_bytes());
        assert_eq!(actual_level, 1);
    }

    /// Creates tables files for various levels and adds the file metadata to the provided version.
    fn create_test_files_for_version(db_options: DbOptions, version: &mut Version) {
        // Create files with file numbers in reverse chronological order since upper levels
        // generally have more recently created files

        // Level-0 allows overlapping files
        let entries = vec![
            (
                ("a".as_bytes().to_vec(), Operation::Put),
                ("a".as_bytes().to_vec()),
            ),
            (
                ("b".as_bytes().to_vec(), Operation::Put),
                ("b".as_bytes().to_vec()),
            ),
            (
                ("c".as_bytes().to_vec(), Operation::Put),
                ("c".as_bytes().to_vec()),
            ),
            (
                ("d".as_bytes().to_vec(), Operation::Put),
                ("d".as_bytes().to_vec()),
            ),
        ];
        let table_file_meta = create_table(db_options.clone(), entries, 90, 60);
        version.files[0].push(Arc::new(table_file_meta));

        let entries = vec![
            (
                ("c".as_bytes().to_vec(), Operation::Put),
                ("c1".as_bytes().to_vec()),
            ),
            (
                ("d".as_bytes().to_vec(), Operation::Put),
                ("d1".as_bytes().to_vec()),
            ),
            (
                ("e".as_bytes().to_vec(), Operation::Put),
                ("e".as_bytes().to_vec()),
            ),
            (
                ("f".as_bytes().to_vec(), Operation::Put),
                ("f".as_bytes().to_vec()),
            ),
        ];
        let table_file_meta = create_table(db_options.clone(), entries, 100, 61);
        version.files[0].push(Arc::new(table_file_meta));

        let entries = vec![
            (
                ("f".as_bytes().to_vec(), Operation::Put),
                ("f-2".as_bytes().to_vec()),
            ),
            (
                ("f1".as_bytes().to_vec(), Operation::Put),
                ("f1".as_bytes().to_vec()),
            ),
            (
                ("f2".as_bytes().to_vec(), Operation::Put),
                ("f2".as_bytes().to_vec()),
            ),
            (
                ("f3".as_bytes().to_vec(), Operation::Put),
                ("f3".as_bytes().to_vec()),
            ),
        ];
        let table_file_meta = create_table(db_options.clone(), entries, 105, 62);
        version.files[0].push(Arc::new(table_file_meta));

        // Level 1
        let entries = vec![
            (
                ("g".as_bytes().to_vec(), Operation::Put),
                ("g".as_bytes().to_vec()),
            ),
            (
                ("h".as_bytes().to_vec(), Operation::Put),
                ("h".as_bytes().to_vec()),
            ),
            (
                ("i".as_bytes().to_vec(), Operation::Put),
                ("i".as_bytes().to_vec()),
            ),
            (
                ("j".as_bytes().to_vec(), Operation::Put),
                ("j".as_bytes().to_vec()),
            ),
        ];
        let table_file_meta = create_table(db_options.clone(), entries, 85, 59);
        version.files[1].push(Arc::new(table_file_meta));

        let entries = vec![
            (
                ("o".as_bytes().to_vec(), Operation::Put),
                ("o".as_bytes().to_vec()),
            ),
            (
                ("r".as_bytes().to_vec(), Operation::Put),
                ("r".as_bytes().to_vec()),
            ),
            (
                ("s".as_bytes().to_vec(), Operation::Put),
                ("s".as_bytes().to_vec()),
            ),
            (
                ("t".as_bytes().to_vec(), Operation::Put),
                ("t".as_bytes().to_vec()),
            ),
            (
                ("u".as_bytes().to_vec(), Operation::Put),
                ("u".as_bytes().to_vec()),
            ),
        ];
        let table_file_meta = create_table(db_options.clone(), entries, 80, 58);
        version.files[1].push(Arc::new(table_file_meta));

        let entries = vec![
            (
                ("v".as_bytes().to_vec(), Operation::Put),
                ("v".as_bytes().to_vec()),
            ),
            (
                ("w".as_bytes().to_vec(), Operation::Put),
                ("w".as_bytes().to_vec()),
            ),
            (("x".as_bytes().to_vec(), Operation::Delete), vec![]),
            (
                ("y".as_bytes().to_vec(), Operation::Put),
                ("y".as_bytes().to_vec()),
            ),
        ];
        let table_file_meta = create_table(db_options.clone(), entries, 76, 57);
        version.files[1].push(Arc::new(table_file_meta));

        // Level 2
        let entries = vec![
            (
                ("k".as_bytes().to_vec(), Operation::Put),
                ("k".as_bytes().to_vec()),
            ),
            (
                ("l".as_bytes().to_vec(), Operation::Put),
                ("l".as_bytes().to_vec()),
            ),
            (
                ("m".as_bytes().to_vec(), Operation::Put),
                ("m".as_bytes().to_vec()),
            ),
            (
                ("n".as_bytes().to_vec(), Operation::Put),
                ("n".as_bytes().to_vec()),
            ),
        ];
        let table_file_meta = create_table(db_options.clone(), entries, 65, 55);
        version.files[2].push(Arc::new(table_file_meta));

        let entries = vec![
            (
                ("o".as_bytes().to_vec(), Operation::Put),
                ("o-1".as_bytes().to_vec()),
            ),
            (
                ("p".as_bytes().to_vec(), Operation::Put),
                ("p".as_bytes().to_vec()),
            ),
            (
                ("r".as_bytes().to_vec(), Operation::Put),
                ("r-1".as_bytes().to_vec()),
            ),
            (
                ("s".as_bytes().to_vec(), Operation::Put),
                ("s-1".as_bytes().to_vec()),
            ),
        ];
        let table_file_meta = create_table(db_options.clone(), entries, 60, 54);
        version.files[2].push(Arc::new(table_file_meta));

        let entries = vec![
            (
                ("v".as_bytes().to_vec(), Operation::Put),
                ("v".as_bytes().to_vec()),
            ),
            (
                ("w".as_bytes().to_vec(), Operation::Put),
                ("w".as_bytes().to_vec()),
            ),
            (
                ("x".as_bytes().to_vec(), Operation::Put),
                ("x".as_bytes().to_vec()),
            ),
            (
                ("y".as_bytes().to_vec(), Operation::Put),
                ("y".as_bytes().to_vec()),
            ),
        ];
        let table_file_meta = create_table(db_options, entries, 55, 53);
        version.files[2].push(Arc::new(table_file_meta));
    }

    /**
    Create a table with the provided entries (key-value pairs) with sequence numbers starting
    from the provided start point.
    */
    fn create_table(
        db_options: DbOptions,
        entries: Vec<((Vec<u8>, Operation), Vec<u8>)>,
        starting_sequence_num: u64,
        file_number: u64,
    ) -> FileMetadata {
        let smallest_key = InternalKey::new(
            entries.first().unwrap().0 .0.clone(),
            starting_sequence_num,
            entries.first().unwrap().0 .1,
        );
        let largest_key = InternalKey::new(
            entries.last().unwrap().0 .0.clone(),
            starting_sequence_num + (entries.len() as u64) - 1,
            entries.last().unwrap().0 .1,
        );

        let mut table_builder = TableBuilder::new(db_options, file_number).unwrap();
        let mut curr_sequence_num = starting_sequence_num;
        for ((user_key, operation), value) in entries {
            table_builder
                .add_entry(
                    Rc::new(InternalKey::new(user_key, curr_sequence_num, operation)),
                    &value,
                )
                .unwrap();
            curr_sequence_num += 1;
        }

        table_builder.finalize().unwrap();

        let mut file_meta = FileMetadata::new(file_number);
        file_meta.set_smallest_key(Some(smallest_key));
        file_meta.set_largest_key(Some(largest_key));
        file_meta.set_file_size(table_builder.file_size());

        file_meta
    }
}
