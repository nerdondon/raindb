use std::sync::Arc;

use crate::config::MAX_NUM_LEVELS;
use crate::key::InternalKey;
use crate::utils::linked_list::SharedNode;
use crate::versioning::file_metadata::FileMetadata;
use crate::versioning::version::Version;
use crate::versioning::{self, VersionChangeManifest};
use crate::DbOptions;

use super::utils;

/**
Encapsulates information about how a compaction should be performed.

# Legacy

This is synonomous to `leveldb::Compaction` in LevelDB.
*/
pub(crate) struct CompactionManifest {
    /// The level that is currently being compacted.
    level: usize,

    /// A manifest of changes from this compaction.
    change_manifest: VersionChangeManifest,

    /// The maximum size for files created during this compaction.
    max_output_file_size_bytes: u64,

    /// The version to perform a compaction for.
    maybe_input_version: Option<SharedNode<Version>>,

    /**
    The files being compacted.

    Files from `level` are read in for compaction and stored at index 0. Files overlapping the key
    range of the files from `level` are read in from `level` + 1 and stored at index 1. We say that
    files in `level` are compacted to a parent level at `level` + 1.

    # Legacy

    This is the `Compaction::inputs_` field in LevelDB.
    */
    input_files: [Vec<Arc<FileMetadata>>; 2],

    /**
    Track files from grandparent level that overlap the key space of the current file being built.

    The overlapping files in the grandparent level (`level` + 2) are tracked as a limit for when a
    new file should be created for compaction. We do not allow too many overlapping grandparent
    files because that can make future compactions more expensive.
    */
    overlapping_grandparents: Vec<Arc<FileMetadata>>,

    /**
    Indices into the `maybe_input_version.files` recording the index of the file at a level
    that satisfy conditions implemented in the [`CompactionManifest::IsBaseLevelForKey`] method.

    Pointers are only stored for levels oldr than the levels being used in the compaction, i.e.
    for all levels >= `level` + 2.
    */
    level_pointers: [usize; MAX_NUM_LEVELS],

    /// The set of files in the grandparent level that overlap the files involved in the compaction.
    grandparent_files: Vec<Arc<FileMetadata>>,
}

/// Crate-only methods
impl CompactionManifest {
    pub(crate) fn new(options: &DbOptions, level_to_compact: usize) -> Self {
        Self {
            level: level_to_compact,
            change_manifest: VersionChangeManifest::default(),
            // This is determined by `MaxFileSizeForLevel` in LevelDB which is not yet implemented
            // and is meant to allow varying the max file size per level to reduce the number of
            // files generated.
            max_output_file_size_bytes: options.max_file_size(),
            maybe_input_version: None,
            input_files: Default::default(),
            overlapping_grandparents: vec![],
            level_pointers: [0; MAX_NUM_LEVELS],
            grandparent_files: vec![],
        }
    }

    /// Set the input version.
    pub(crate) fn set_input_version(&mut self, version: SharedNode<Version>) {
        self.maybe_input_version = Some(version);
    }

    /// Set files to compact for the manifest's specified `level`.
    pub(crate) fn set_compaction_level_files(&mut self, files_to_compact: Vec<Arc<FileMetadata>>) {
        self.input_files[0] = files_to_compact;
    }

    /// Get a reference to the set of files involved at the compaction level.
    pub(crate) fn get_compaction_level_files(&self) -> &[Arc<FileMetadata>] {
        &self.input_files[0]
    }

    /// Get a mutable reference to the set of files involved at the compaction level.
    pub(crate) fn get_mut_compaction_level_files(&mut self) -> &mut Vec<Arc<FileMetadata>> {
        &mut self.input_files[0]
    }

    /**
    Fill out other compaction input fields based on the currently provided set of inputs.

    # Legacy

    This is synonomous to LeveDB's `VersionSet::SetupOtherInputs`.

    # Return

    This method returns the key that the next compaction for this level should start at.
    */
    pub(crate) fn finalize_compaction_inputs(&mut self) -> InternalKey {
        let input_version = self.maybe_input_version.as_ref().unwrap();
        CompactionManifest::add_boundary_inputs(
            &input_version.write().element.files[self.level],
            &mut self.input_files[0],
        );
        let mut compaction_level_key_range =
            FileMetadata::get_key_range_for_files(&self.input_files[0]);

        let optional_compaction_level_key_range =
            Some(&compaction_level_key_range.start)..Some(&compaction_level_key_range.end);
        let parent_level_files: Vec<Arc<FileMetadata>> = input_version
            .read()
            .element
            .get_overlapping_files_strong(self.level + 1, optional_compaction_level_key_range);
        self.input_files[1].extend(parent_level_files);
        CompactionManifest::add_boundary_inputs(
            &input_version.write().element.files[self.level + 1],
            &mut self.input_files[1],
        );

        // Get the key range for all files involved in the compaction
        let mut all_files_range = FileMetadata::get_key_range_for_multiple_levels(&[
            &self.input_files[0],
            &self.input_files[1],
        ]);

        // See if we can grow the number of input files in the compaction level without adding
        // more files from the parent level
        if !self.input_files[1].is_empty() {
            let mut expanded0_compaction_files =
                input_version.read().element.get_overlapping_files_strong(
                    self.level,
                    Some(&all_files_range.start)..Some(&all_files_range.end),
                );
            CompactionManifest::add_boundary_inputs(
                &input_version.write().element.files[self.level],
                &mut expanded0_compaction_files,
            );

            let inputs0_files_size = versioning::utils::sum_file_sizes(&self.input_files[0]);
            let inputs1_files_size = versioning::utils::sum_file_sizes(&self.input_files[1]);
            let expanded0_files_size =
                versioning::utils::sum_file_sizes(&expanded0_compaction_files);
            let has_expanded_files = expanded0_compaction_files.len() > self.input_files[0].len();
            let is_expanded_files_less_than_size_limit = (inputs1_files_size
                + expanded0_files_size)
                < self.expanded_compaction_byte_size_limit();

            if has_expanded_files && is_expanded_files_less_than_size_limit {
                let new_compaction_range =
                    FileMetadata::get_key_range_for_files(&expanded0_compaction_files);
                let expanded1_files = input_version.read().element.get_overlapping_files_strong(
                    self.level + 1,
                    Some(&new_compaction_range.start)..Some(&new_compaction_range.end),
                );
                CompactionManifest::add_boundary_inputs(
                    &input_version.write().element.files[self.level],
                    &mut expanded0_compaction_files,
                );

                if expanded1_files.len() == self.input_files[1].len() {
                    log::info!(
                        "Expanding compaction inputs for compaction at level {level}. Compaction \
                        level and parent level have {compaction_level_files} + \
                        {parent_level_files} files at {compaction_level_bytes} + \
                        {parent_level_bytes} bytes. This is expanding to {expanded0_files} + \
                        {expanded1_files} files ({expanded0_bytes} + {expanded1_bytes} bytes).",
                        level = self.level,
                        compaction_level_files = self.input_files[0].len(),
                        parent_level_files = self.input_files[1].len(),
                        compaction_level_bytes = inputs0_files_size,
                        parent_level_bytes = inputs1_files_size,
                        expanded0_files = expanded0_compaction_files.len(),
                        expanded1_files = expanded1_files.len(),
                        expanded0_bytes = expanded0_files_size,
                        expanded1_bytes = inputs1_files_size
                    );

                    self.input_files[0] = expanded0_compaction_files;
                    self.input_files[1] = expanded1_files;

                    // Update the key range for the files at the compaction level
                    compaction_level_key_range = new_compaction_range;

                    // Update the key range to encompass all files (i.e. including parent files)
                    all_files_range = FileMetadata::get_key_range_for_multiple_levels(&[
                        &self.input_files[0],
                        &self.input_files[1],
                    ]);
                }
            }
        }

        // Compute the set of grandparent files that overlap this compaction
        if self.level + 2 < MAX_NUM_LEVELS {
            self.grandparent_files = input_version.read().element.get_overlapping_files_strong(
                self.level + 2,
                Some(&all_files_range.start)..Some(&all_files_range.end),
            );
        }

        self.change_manifest
            .add_compaction_pointer(self.level, compaction_level_key_range.start.clone());

        compaction_level_key_range.start
    }
}

/// Private methods
impl CompactionManifest {
    /**
    Finds and adds files in the level that overlap user keys with the files chosen for compaction.

    The purpose of this function is to ensure that user key ranges are not split during compaction.
    A split in the user key range can cause an older record to be found in the younger level
    instead of the current record that was compacted.

    The file with the largest user key **F1(l1, u1)** is picked out of the provided
    `compaction_files` where **l1** and **u1** define the key range of the file. A search is then
    performed for some file **F2(l2, u2)** in `level_files` where `user_key(u1) == user_key(l2)`.
    If such a file **F2** is found we call it a boundary file and add it to `compaction_files` and
    continue to search with using this new upper bound.

    Assume there are two files **F1(l1, u1)** and **F2(l2, u2** where
    `user_key(u1) == user_key(l2)`. If we compact **F1** and not **F2**, then subsequent `get`
    operations will return the record from **F2** in the younger level instead of from **F1** in
    the older level that we compacted to.

    This is similar to the [`Version::get_overlapping_files`] method but is more limited in how it
    expands the set of files being compacted.

    # Parameters

    - `level_files` - The files in the level that is being compacted
    - `compaction_files` - A subset of `level_files` that will actually be compacted
    */
    fn add_boundary_inputs(
        level_files: &[Arc<FileMetadata>],
        compaction_files: &mut Vec<Arc<FileMetadata>>,
    ) {
        let maybe_largest_key = CompactionManifest::find_largest_key(compaction_files);
        if maybe_largest_key.is_none() {
            return;
        }

        let mut largest_key = maybe_largest_key.unwrap();
        loop {
            let maybe_smallest_boundary_file =
                CompactionManifest::find_smallest_boundary_file(level_files, largest_key);
            if maybe_smallest_boundary_file.is_none() {
                break;
            }

            let smallest_boundary_file = maybe_smallest_boundary_file.unwrap();
            compaction_files.push(Arc::clone(&smallest_boundary_file));
            // Use the reference stored that was just pushed to tie the reference to the lifetime
            // of `compaction_files`
            largest_key = compaction_files.last().unwrap().largest_key();
        }
    }

    /// Find the largest key stored in the provided files.
    fn find_largest_key(files: &mut Vec<Arc<FileMetadata>>) -> Option<&InternalKey> {
        if files.is_empty() {
            return None;
        }

        let mut largest_key = files[0].largest_key();
        for file in files.iter() {
            if file.largest_key() > largest_key {
                largest_key = file.largest_key();
            }
        }

        Some(largest_key)
    }

    /**
    Find the file with the smallest key that is still greater than the `target_key` but
    contains the same user key.
    */
    fn find_smallest_boundary_file(
        level_files: &[Arc<FileMetadata>],
        target_key: &InternalKey,
    ) -> Option<Arc<FileMetadata>> {
        let mut smallest_boundary_file: Option<Arc<FileMetadata>> = None;
        for file in level_files {
            if file.smallest_key() > target_key
                && file.smallest_key().get_user_key() == target_key.get_user_key()
            {
                if let Some(current_boundary_key) = smallest_boundary_file
                    .as_ref()
                    .map(|boundary_file| boundary_file.smallest_key())
                {
                    if file.smallest_key() < current_boundary_key {
                        smallest_boundary_file = Some(Arc::clone(file));
                    }
                }
            }
        }

        smallest_boundary_file
    }

    /**
    Maximum number of bytes in all compacted files.

    We avoid expanding the lower level file set of a compaction if it would make the total
    compaction cover more than this many bytes.
    */
    fn expanded_compaction_byte_size_limit(&self) -> u64 {
        self.max_output_file_size_bytes * 25
    }
}
