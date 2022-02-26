use std::cmp::Ordering;
use std::collections::HashSet;
use std::ops::Range;
use std::sync::Arc;

use crate::config::{L0_COMPACTION_TRIGGER, MAX_MEM_COMPACT_LEVEL, MAX_NUM_LEVELS};
use crate::key::{InternalKey, MAX_SEQUENCE_NUMBER};
use crate::table_cache::TableCache;
use crate::utils::comparator::Comparator;
use crate::utils::linked_list::SharedNode;
use crate::{compaction, DbOptions};

use super::errors::ReadResult;
use super::file_metadata::{FileMetadata, FileMetadataBySmallestKey};
use super::version_manifest::DeletedFile;
use super::VersionChangeManifest;

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
    pub fn new() -> Self {
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

    /**
    Look up the value for the given key.

    # Concurrency
    Does not require a lock to be held.

    Returns an `GetResponse` struct containing the found value and seek charge metadata.
    */
    pub(crate) fn get(&self) -> ReadResult<GetResponse> {
        todo!("working on it!");
    }

    /**
    Apply the charging metadata to the current state.

    # Concurrency
    An external lock is required to be held before exercising this method.

    Returns true if a new compaction may need to be triggered, false otherwise.
    */
    pub(crate) fn update_stats(&self, charging_metadata: SeekChargeMetadata) -> ReadResult<bool> {
        todo!("working on it!");
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

    /// Clones the current version while resetting compaction metadata.
    pub(crate) fn new_from_current(&self) -> Version {
        let mut new_version = self.clone();
        new_version.set_seek_compaction_metadata(SeekCompactionMetadata::default());
        new_version.set_size_compaction_metadata(None);

        new_version
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

    /// Get files that overlap the provided key.
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
                || file.largest_key().get_user_key() >= target_user_key
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

    This method can only be called on levels greater than zero and below the maximum number of
    levels [`MAX_NUM_LEVELS`].

    # Legacy

    This is synonomous to LevelDB's `Version::GetOverlappingInputs` method.
    */
    pub(crate) fn get_overlapping_compaction_inputs(
        &self,
        level: usize,
        key_range: Range<Option<&InternalKey>>,
    ) -> Vec<&Arc<FileMetadata>> {
        assert!(level > 0);
        assert!(level <= MAX_NUM_LEVELS);

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

            if is_file_range_before_target || is_file_range_after_target {
                index += 1;
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
    The same as [`Version::get_overlapping_files`] but returns owned references instead of a
    reference to the `Arc`.
    */
    pub(crate) fn get_overlapping_compaction_inputs_strong(
        &self,
        level: usize,
        key_range: Range<Option<&InternalKey>>,
    ) -> Vec<Arc<FileMetadata>> {
        self.get_overlapping_compaction_inputs(level, key_range)
            .into_iter()
            .map(|file| Arc::clone(file))
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
        let maybe_file_index =
            super::utils::find_file_with_upper_bound_range(files, &smallest_full_key);

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

/**
Accumulates changes from multiple change manifests to apply to a base version and generate a
new version.
*/
pub(crate) struct VersionBuilder {
    /// The base version to apply on top of.
    base_version: SharedNode<Version>,

    /**
    The per level, set of files (represented by their file number) that will be deleted from a
    version.

    # Legacy

    This is stored in `VersionSet::Builder::LevelState` in LevelDB.
    */
    deleted_files: [HashSet<u64>; MAX_NUM_LEVELS],

    /**
    The set of files that will be added to the version per level.

    # Legacy

    This is stored in `VersionSet::Builder::LevelState` in LevelDB.
    */
    added_files: [HashSet<Arc<FileMetadata>>; MAX_NUM_LEVELS],

    /// Aggregated per-level keys at which the next compaction at that level should start.
    compaction_pointers: [Option<InternalKey>; MAX_NUM_LEVELS],

    /**
    Indicates if the builder has already been used to apply its stored changes.

    Changes stored in a builder cannot be applied multiple times.
    */
    already_invoked: bool,
}

/// Crate-only methods
impl VersionBuilder {
    /// Create a new instance of [`VersionBuilder`].
    pub(crate) fn new(base_version: SharedNode<Version>) -> Self {
        Self {
            base_version,
            deleted_files: Default::default(),
            added_files: Default::default(),
            compaction_pointers: Default::default(),
            already_invoked: false,
        }
    }

    /**
    Accumulate changes from multiple change manifests.

    This is so that a sequence of changes can be applied atomically to a base version.

    # Legacy

    This is synonomous to LevelDB's `VersionSet::Builder::Apply`.
    */
    pub(crate) fn accumulate_changes(&mut self, change_manifest: &VersionChangeManifest) {
        // Update compaction pointers
        for ptr in &change_manifest.compaction_pointers {
            let (level, key) = ptr;
            self.compaction_pointers[*level] = Some(key.clone());
        }

        // Record deleted files
        for file in &change_manifest.deleted_files {
            let DeletedFile { level, file_number } = file;
            self.deleted_files[*level].insert(*file_number);
        }

        // Record new files
        for (level, file) in &change_manifest.new_files {
            let new_file = Arc::new(file.clone());
            self.deleted_files[*level].remove(&new_file.file_number());
            self.added_files[*level].insert(new_file);
        }
    }

    /**
    Apply the accumulated versions on the base version, returning a new [`Version`].

    This method will also update the specified version set compaction pointers.

    **A builder can not be called again after invoking this method.**

    # Panics

    In non-optimized builds (e.g. the debug profile), this method will panic if there are files
    with overlapping key ranges in levels > 0.

    This method will panic if called and the `already_invoked` flag is set to true.

    # Legacy

    This is synonomous to LevelDB's `VersionSet::Builder::SaveTo`.
    */
    pub(crate) fn apply_changes(
        &mut self,
        vset_compaction_pointers: &mut [Option<InternalKey>; MAX_NUM_LEVELS],
    ) -> Version {
        assert!(
            !self.already_invoked,
            "Cannot call `apply_changes` more than once on a `VersionBuilder`."
        );
        self.already_invoked = true;

        // Apply compaction pointer updates
        for (level, ptr) in self.compaction_pointers.iter_mut().enumerate() {
            if ptr.is_some() {
                vset_compaction_pointers[level] = ptr.take();
            }
        }

        let base = &self.base_version.read().element;
        let mut new_version = base.new_from_current();
        for level in 0..MAX_NUM_LEVELS {
            let mut level_added_files: Vec<Arc<FileMetadata>> = self.added_files[level]
                .iter()
                .map(|file| Arc::clone(file))
                .collect();
            level_added_files.sort_by(|a, b| FileMetadataBySmallestKey::compare(a, b));

            // The files in the base version should be sorted already because all changes are made
            // through this builder, but we do it again just to ensure this is the case.
            // The clone should be cheap since we are just cloning references.
            let mut sorted_base_files = base.files[level].clone();
            sorted_base_files.sort_by(|a, b| FileMetadataBySmallestKey::compare(a, b));

            // Merge added files and remove deleted files similar to merge algorithm in merge sort
            let mut added_files_idx = 0;
            let mut base_files_idx = 0;
            new_version.files[level] =
                Vec::with_capacity(base.num_files_at_level(level) + self.added_files.len());
            while added_files_idx < level_added_files.len()
                && base_files_idx < sorted_base_files.len()
            {
                let added_file = &level_added_files[added_files_idx];
                let base_file = &sorted_base_files[base_files_idx];
                if FileMetadataBySmallestKey::compare(&*base_file, &added_file) == Ordering::Less {
                    self.maybe_add_file(&mut new_version, level, Arc::clone(base_file));
                    base_files_idx += 1;
                } else {
                    self.maybe_add_file(&mut new_version, level, Arc::clone(added_file));
                    added_files_idx += 1;
                }
            }

            while base_files_idx < sorted_base_files.len() {
                let base_file = &sorted_base_files[base_files_idx];
                self.maybe_add_file(&mut new_version, level, Arc::clone(base_file));
                base_files_idx += 1;
            }

            while added_files_idx < level_added_files.len() {
                let added_file = &level_added_files[added_files_idx];
                self.maybe_add_file(&mut new_version, level, Arc::clone(added_file));
                added_files_idx += 1;
            }

            if cfg!(debug_assertions) && level > 0 {
                // When in debug mode, assert that there are no overlapping files in levels > 0.
                for file_idx in 1..new_version.files[level].len() {
                    let prev_key_range_end = new_version.files[level][file_idx - 1].largest_key();
                    let curr_key_range_start = new_version.files[level][file_idx].smallest_key();
                    if prev_key_range_end >= curr_key_range_start {
                        panic!("There was an overlapping key-range in level {} while applying changes for a new version", level);
                    }
                }
            }
        }

        new_version
    }
}

/// Private methods
impl VersionBuilder {
    /**
    Checks some invariants before adding the file to the specified level.

    ## Invariants

    1. The file is not in the deleted file set of the change manifest

    1. The file does not overlap the previous file's key range. If there is an overlap, the method
       will panic.

    # Panics

    This method will panic if the file to be added overlaps the previous file's key range.
    */
    fn maybe_add_file(&self, version: &mut Version, level: usize, file: Arc<FileMetadata>) {
        if self.deleted_files[level].contains(&file.file_number()) {
            // Don't add the file if it is marked for deletion
            return;
        }

        let files = &mut version.files[level];
        if level > 0 && !files.is_empty() {
            let last_file = files.last().unwrap();
            assert!(
                last_file.largest_key() < file.smallest_key(),
                "Attempting to add file number {} to level {} created an overlap with file number {}.",
                file.file_number(),
                level,
                last_file.file_number()
            );
        }

        files.push(file);
    }
}
