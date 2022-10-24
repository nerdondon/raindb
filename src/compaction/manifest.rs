use std::sync::Arc;

use crate::config::MAX_NUM_LEVELS;
use crate::errors::{RainDBError, RainDBResult};
use crate::key::InternalKey;
use crate::table_cache::TableCache;
use crate::tables::Table;
use crate::utils::linked_list::SharedNode;
use crate::versioning::file_iterators::{FilesEntryIterator, MergingIterator};
use crate::versioning::file_metadata::FileMetadata;
use crate::versioning::version::Version;
use crate::versioning::{self, VersionChangeManifest, VersionSet};
use crate::{DbOptions, RainDbIterator, ReadOptions};

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
    The current index into the grandparent files during the generation of new table files.

    This is used to track the overlap of the file being generated with grandparent files.
    */
    grandparent_index: usize,

    /// The number of bytes overlapping with grandparent files for the current file being compacted.
    current_overlapping_bytes: u64,

    /**
    Returns true if the key range we are compacting is currently overlapping grandparent files.

    # Legacy

    This field is synonomous to LevelDB's `Compaction::seen_key_`.
    */
    is_overlappping: bool,

    /**
    Indices into the `maybe_input_version.files` recording the index of the file at a level
    where the keys being processed for a compaction are at the base level.

    This is internal state kept during table file generation and is mostly used by the
    [`CompactionManifest::IsBaseLevelForKey`] method.

    Pointers are only stored for levels older than the levels being used in the compaction, i.e.
    for all levels >= `level` + 2.
    */
    base_level_pointers: [usize; MAX_NUM_LEVELS],
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
            grandparent_index: 0,
            is_overlappping: false,
            current_overlapping_bytes: 0,
            base_level_pointers: [0; MAX_NUM_LEVELS],
        }
    }

    /// Get the level this compaction manifest is for.
    pub(crate) fn level(&self) -> usize {
        self.level
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
    Get a reference to the set of files overlapping the compaction key range at the parent level.
    */
    pub(crate) fn get_parent_level_files(&self) -> &[Arc<FileMetadata>] {
        &self.input_files[1]
    }

    /**
    Get a mutable reference to the version change manifest being managed by this compaction
    manifest.
    */
    pub(crate) fn get_change_manifest_mut(&mut self) -> &mut VersionChangeManifest {
        &mut self.change_manifest
    }

    /// Get the maximum compaction output file size.
    pub(crate) fn max_output_file_size_bytes(&self) -> u64 {
        self.max_output_file_size_bytes
    }

    /// Get the amount of data that needs to be read for compaction in bytes.
    pub(crate) fn compaction_input_read_bytes(&self) -> u64 {
        let mut total_bytes = 0;
        for input_level_files in self.input_files.iter() {
            total_bytes += versioning::utils::sum_file_sizes(input_level_files);
        }

        total_bytes
    }

    /**
    Fill out other compaction input fields based on the currently provided set of inputs.

    Returns the key that the next compaction for this level should start at.

    # Legacy

    This is synonomous to LeveDB's `VersionSet::SetupOtherInputs`.
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
            .get_overlapping_compaction_inputs_strong(
                self.level + 1,
                optional_compaction_level_key_range,
            );
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
            let mut expanded0_compaction_files = input_version
                .read()
                .element
                .get_overlapping_compaction_inputs_strong(
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
                let expanded1_files = input_version
                    .read()
                    .element
                    .get_overlapping_compaction_inputs_strong(
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
            self.overlapping_grandparents = input_version
                .read()
                .element
                .get_overlapping_compaction_inputs_strong(
                    self.level + 2,
                    Some(&all_files_range.start)..Some(&all_files_range.end),
                );
        }

        self.change_manifest
            .add_compaction_pointer(self.level, compaction_level_key_range.start.clone());

        compaction_level_key_range.start
    }

    /**
    Check if a compaction can be completed by just moving a single file to the parent level
    without merging or splitting.

    We avoid a move if there is lots of overlapping grandparent data. Otherwise, the move could
    create a parent file that will require a very expensive merge later on.
    */
    pub(crate) fn is_trivial_move(&self) -> bool {
        let num_compaction_level_files = self.get_compaction_level_files().len();
        let num_parent_level_files = self.input_files[1].len();
        let is_grandparents_overlap_under_limit =
            versioning::utils::sum_file_sizes(&self.overlapping_grandparents)
                <= utils::max_grandparent_overlap_bytes(self.max_output_file_size_bytes);

        num_compaction_level_files == 1
            && num_parent_level_files == 0
            && is_grandparents_overlap_under_limit
    }

    /**
    Create an iterator that will yield entries in the files participating in the compaction.

    # Legacy

    This is synonomous with LevelDB's `VersionSet::MakeInputIterator`. Unlike LevelDB, we return
    the error immediately. LevelDB will push `ErrorIterator`'s onto the iterator list and will
    ignore iteration errors until the very end of the compaction routine where the erroroneous
    compaction output is discarded.
    */
    pub(crate) fn make_merging_iterator(
        &self,
        table_cache: Arc<TableCache>,
    ) -> RainDBResult<MergingIterator> {
        let read_opts = ReadOptions {
            fill_cache: false,
            snapshot: None,
        };

        // Level 0 files overlap so each of their iterators is necessary to merge key ranges. Other
        // levels do not overlap so a regular two level iterator can be used to merge the key
        // ranges.
        let capacity = if self.level == 0 {
            self.get_compaction_level_files().len() + 1
        } else {
            2
        };
        let mut iterators: Vec<Box<dyn RainDbIterator<Key = InternalKey, Error = RainDBError>>> =
            Vec::with_capacity(capacity);

        for input_files_index in 0..self.input_files.len() {
            if self.input_files[input_files_index].is_empty() {
                continue;
            }

            if self.level + input_files_index == 0 {
                log::debug!(
                    "Making a merging iterator with level 0 as an input level. Adding iterators \
                    for all files at the level."
                );
                let files = &self.input_files[input_files_index];
                for file in files {
                    let table_result = table_cache.find_table(file.file_number());
                    match table_result {
                        Ok(table) => {
                            let iterator = Box::new(Table::iter_with(table, read_opts.clone()));
                            iterators.push(iterator);
                        }
                        Err(error) => {
                            log::error!(
                                "Failed to open or find the table at file number {file_number}. \
                                Skipping the file but proceeding with compaction. Error: {error}",
                                file_number = file.file_number(),
                                error = error
                            );

                            return Err(error.into());
                        }
                    }
                }
            } else {
                let file_list = self.input_files[input_files_index].clone();
                let file_list_iter = Box::new(FilesEntryIterator::new(
                    file_list,
                    Arc::clone(&table_cache),
                    read_opts.clone(),
                ));
                iterators.push(file_list_iter);
            }
        }

        // We should not have more iterators than files.
        assert!(iterators.len() <= capacity);

        Ok(MergingIterator::new(iterators))
    }

    /**
    Returns true if and only if we should stop building the current table file before processing
    the specified key.

    This can occur if there would be too much overlap with files in the grandparent level.
    */
    pub(crate) fn should_stop_before_key(&mut self, key: &InternalKey) -> bool {
        // Scan to find the earliest grandparent file with a key range that overlaps the provided
        // key.
        while self.grandparent_index < self.overlapping_grandparents.len()
            && key > self.overlapping_grandparents[self.grandparent_index].largest_key()
        {
            if self.is_overlappping {
                self.current_overlapping_bytes +=
                    &self.overlapping_grandparents[self.grandparent_index].get_file_size();
            }

            self.grandparent_index += 1;
        }

        self.is_overlappping = true;

        if self.current_overlapping_bytes
            > utils::max_grandparent_overlap_bytes(self.max_output_file_size_bytes)
        {
            self.current_overlapping_bytes = 0;

            true
        } else {
            false
        }
    }

    /**
    Returns true if the provided user key will not overlap any information stored in levels older
    than the level that the compaction file will be installed to (i.e. `level + 1`).
    */
    pub(crate) fn is_base_level_for_key(&mut self, key: &InternalKey) -> bool {
        let user_key = key.get_user_key();
        let input_version = &self.maybe_input_version.as_ref().unwrap().read().element;
        for level in (self.level() + 2)..MAX_NUM_LEVELS {
            let level_files = &input_version.files[level];
            while self.base_level_pointers[level] < level_files.len() {
                let file = &level_files[self.base_level_pointers[level]];
                if user_key <= file.largest_key().get_user_key() {
                    if user_key >= file.smallest_key().get_user_key() {
                        // The user key falls in this file's range, so `self.level + 1` is not the
                        // base level for this key.
                        return false;
                    }

                    // We are still processing entries that may overlap with information stored in
                    // an older level so don't move the file pointer forward
                    break;
                }
                self.base_level_pointers[level] += 1;
            }
        }

        true
    }

    /// Mark the compaction inputs as deletions in the version change manifest.
    pub(crate) fn add_input_deletions(&mut self) {
        for (index, input_level_files) in self.input_files.iter().enumerate() {
            for file in input_level_files.iter() {
                self.change_manifest
                    .remove_file(self.level() + index, file.file_number());
            }
        }
    }

    /**
    Set up the version change manifest for a trivial move.

    # Panics

    In order to be considered a trivial move, there can only be one file in the compaction level.
    */
    pub(crate) fn set_change_manifest_for_trivial_move(&mut self) {
        let file_to_compact = Arc::clone(self.get_compaction_level_files().first().unwrap());
        self.change_manifest.add_file(
            self.level,
            file_to_compact.file_number(),
            file_to_compact.get_file_size(),
            file_to_compact.clone_key_range(),
        )
    }

    /// Release the input version once a compaction is complete.
    pub(crate) fn release_inputs(&mut self, version_set: &mut VersionSet) {
        if let Some(input_version) = self.maybe_input_version.take() {
            version_set.release_version(input_version);
        }
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
    fn find_largest_key(files: &mut [Arc<FileMetadata>]) -> Option<&InternalKey> {
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
                } else {
                    smallest_boundary_file = Some(Arc::clone(file));
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

#[cfg(test)]
mod tests {
    use std::rc::Rc;

    use parking_lot::RwLock;
    use pretty_assertions::assert_eq;

    use crate::tables::TableBuilder;
    use crate::utils::linked_list::Node;
    use crate::Operation;

    use super::*;

    #[test]
    fn is_trivial_move_given_compaction_inputs_that_can_be_trivially_moved_returns_true() {
        let mut options = DbOptions::with_memory_env();
        options.max_file_size = 20;
        let mut manifest = CompactionManifest::new(&options, 3);
        let mut compaction_file = FileMetadata::new(30);
        compaction_file.set_file_size(20);
        let compaction_files: Vec<Arc<FileMetadata>> =
            vec![compaction_file].into_iter().map(Arc::new).collect();
        manifest.set_compaction_level_files(compaction_files);

        assert!(manifest.is_trivial_move());
    }

    #[test]
    fn is_trivial_move_given_compaction_inputs_with_too_much_overlap_with_grandparents_returns_false(
    ) {
        let mut options = DbOptions::with_memory_env();
        options.max_file_size = 20;
        let mut manifest = CompactionManifest::new(&options, 3);
        let mut compaction_file = FileMetadata::new(30);
        compaction_file.set_file_size(20);
        let compaction_files: Vec<Arc<FileMetadata>> =
            vec![compaction_file].into_iter().map(Arc::new).collect();
        manifest.set_compaction_level_files(compaction_files);

        let mut grandparent_file = FileMetadata::new(10);
        grandparent_file.set_file_size(300);
        manifest.overlapping_grandparents =
            vec![grandparent_file].into_iter().map(Arc::new).collect();

        assert!(!manifest.is_trivial_move());
    }

    #[test]
    fn add_boundary_inputs_does_not_add_extra_files_for_empty_compaction_inputs() {
        let level_files: &[Arc<FileMetadata>] = &[];
        let mut compaction_files: Vec<Arc<FileMetadata>> = vec![];

        CompactionManifest::add_boundary_inputs(level_files, &mut compaction_files);

        assert!(compaction_files.is_empty());
    }

    #[test]
    fn add_boundary_inputs_with_empty_level_files_does_not_add_extra_files_for_compaction_inputs() {
        let level_files: &[Arc<FileMetadata>] = &[];
        let mut compaction_files: Vec<Arc<FileMetadata>> = vec![create_file_metadata(
            100,
            InternalKey::new("a".as_bytes().to_vec(), 2, Operation::Put),
            InternalKey::new("a".as_bytes().to_vec(), 1, Operation::Put),
        )]
        .into_iter()
        .map(Arc::new)
        .collect();

        CompactionManifest::add_boundary_inputs(level_files, &mut compaction_files);

        assert_eq!(compaction_files.len(), 1);
        assert_eq!(compaction_files[0].file_number(), 100);
    }

    #[test]
    fn add_boundary_inputs_with_empty_compaction_files_and_single_level_file_does_not_add_additional_files(
    ) {
        let level_files: Vec<Arc<FileMetadata>> = vec![create_file_metadata(
            100,
            InternalKey::new("a".as_bytes().to_vec(), 2, Operation::Put),
            InternalKey::new("a".as_bytes().to_vec(), 1, Operation::Put),
        )]
        .into_iter()
        .map(Arc::new)
        .collect();
        let mut compaction_files: Vec<Arc<FileMetadata>> = vec![];

        CompactionManifest::add_boundary_inputs(&level_files, &mut compaction_files);

        assert!(compaction_files.is_empty());
    }

    #[test]
    fn add_boundary_inputs_adds_a_boundary_file_if_one_exists() {
        let initial_file = Arc::new(create_file_metadata(
            100,
            InternalKey::new("a".as_bytes().to_vec(), 29, Operation::Put),
            InternalKey::new("a".as_bytes().to_vec(), 20, Operation::Put),
        ));
        let mut level_files: Vec<Arc<FileMetadata>> = vec![
            create_file_metadata(
                101,
                InternalKey::new("a".as_bytes().to_vec(), 1, Operation::Put),
                InternalKey::new("b".as_bytes().to_vec(), 10, Operation::Put),
            ),
            create_file_metadata(
                102,
                InternalKey::new("c".as_bytes().to_vec(), 30, Operation::Put),
                InternalKey::new("g".as_bytes().to_vec(), 50, Operation::Put),
            ),
        ]
        .into_iter()
        .map(Arc::new)
        .collect();
        level_files.push(Arc::clone(&initial_file));

        let mut compaction_files: Vec<Arc<FileMetadata>> = vec![initial_file];

        CompactionManifest::add_boundary_inputs(&level_files, &mut compaction_files);

        assert_eq!(compaction_files.len(), 2);
        assert_eq!(compaction_files[0].file_number(), 100);
        assert_eq!(compaction_files[1].file_number(), 101);
    }

    #[test]
    fn add_boundary_inputs_adds_multiple_boundary_files_if_they_exist() {
        let initial_file = Arc::new(create_file_metadata(
            100,
            InternalKey::new("a".as_bytes().to_vec(), 6, Operation::Put),
            InternalKey::new("a".as_bytes().to_vec(), 5, Operation::Put),
        ));
        let mut level_files: Vec<Arc<FileMetadata>> = vec![
            create_file_metadata(
                101,
                InternalKey::new("a".as_bytes().to_vec(), 2, Operation::Put),
                InternalKey::new("b".as_bytes().to_vec(), 1, Operation::Put),
            ),
            create_file_metadata(
                102,
                InternalKey::new("a".as_bytes().to_vec(), 4, Operation::Put),
                InternalKey::new("a".as_bytes().to_vec(), 3, Operation::Put),
            ),
        ]
        .into_iter()
        .map(Arc::new)
        .collect();
        level_files.push(Arc::clone(&initial_file));

        let mut compaction_files: Vec<Arc<FileMetadata>> = vec![initial_file];

        CompactionManifest::add_boundary_inputs(&level_files, &mut compaction_files);

        assert_eq!(compaction_files.len(), 3);
        assert_eq!(compaction_files[0].file_number(), 100);
        assert_eq!(compaction_files[1].file_number(), 102);
        assert_eq!(compaction_files[2].file_number(), 101);
    }

    #[test]
    fn should_stop_before_key_given_too_much_compaction_overlap_with_grandparent_files_returns_true(
    ) {
        let mut options = DbOptions::with_memory_env();
        let table_cache = Arc::new(TableCache::new(options.clone(), 1000));
        options.max_file_size = 20;

        let mut version = Version::new(options.clone(), &table_cache, 200, 30);
        create_test_files_for_version(options.clone(), &mut version);

        let mut manifest = CompactionManifest::new(&options, 1);
        manifest.overlapping_grandparents = version.files[3].iter().map(Arc::clone).collect();
        manifest.set_input_version(Arc::new(RwLock::new(Node::new(version))));

        assert!(!manifest.is_overlappping);

        // Test no overlap
        let actual = manifest
            .should_stop_before_key(&InternalKey::new_for_seeking("z1".as_bytes().to_vec(), 99));
        assert!(!actual);
        assert!(manifest.is_overlappping);

        // Test not enough overlapping bytes to stop
        let actual = manifest
            .should_stop_before_key(&InternalKey::new_for_seeking("z2".as_bytes().to_vec(), 20));
        assert!(!actual);
        assert_eq!(manifest.grandparent_index, 1);

        // Test too much overlap
        let actual = manifest
            .should_stop_before_key(&InternalKey::new_for_seeking("z6".as_bytes().to_vec(), 20));
        assert!(actual);
        assert_eq!(manifest.grandparent_index, 2);
    }

    /// Create a [`FileMetadata`] object with the specified properties.
    fn create_file_metadata(
        file_number: u64,
        smallest_key: InternalKey,
        largest_key: InternalKey,
    ) -> FileMetadata {
        let mut file = FileMetadata::new(file_number);
        file.set_smallest_key(Some(smallest_key));
        file.set_largest_key(Some(largest_key));

        file
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

        let entries = vec![
            (
                ("z1".as_bytes().to_vec(), Operation::Put),
                ("z1".as_bytes().to_vec()),
            ),
            (
                ("z2".as_bytes().to_vec(), Operation::Put),
                ("z2".as_bytes().to_vec()),
            ),
            (
                ("z3".as_bytes().to_vec(), Operation::Put),
                ("z3".as_bytes().to_vec()),
            ),
            (
                ("z4".as_bytes().to_vec(), Operation::Put),
                ("z4".as_bytes().to_vec()),
            ),
            (
                ("z5".as_bytes().to_vec(), Operation::Put),
                ("z5".as_bytes().to_vec()),
            ),
            (
                ("z6".as_bytes().to_vec(), Operation::Put),
                ("z6".as_bytes().to_vec()),
            ),
        ];
        let table_file_meta = create_table(db_options.clone(), entries, 30, 56);
        version.files[0].push(Arc::new(table_file_meta));

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
        let table_file_meta = create_table(db_options.clone(), entries, 55, 53);
        version.files[2].push(Arc::new(table_file_meta));

        // Level 3
        let entries = vec![
            (
                ("z1".as_bytes().to_vec(), Operation::Put),
                ("z1".as_bytes().to_vec()),
            ),
            (
                ("z2".as_bytes().to_vec(), Operation::Put),
                ("z2".as_bytes().to_vec()),
            ),
        ];
        let table_file_meta = create_table(db_options.clone(), entries, 45, 52);
        version.files[3].push(Arc::new(table_file_meta));

        let entries = vec![
            (
                ("z3".as_bytes().to_vec(), Operation::Put),
                ("z3".as_bytes().to_vec()),
            ),
            (
                ("z4".as_bytes().to_vec(), Operation::Put),
                ("z4".as_bytes().to_vec()),
            ),
        ];
        let table_file_meta = create_table(db_options.clone(), entries, 47, 51);
        version.files[3].push(Arc::new(table_file_meta));

        let entries = vec![
            (
                ("z5".as_bytes().to_vec(), Operation::Put),
                ("z5".as_bytes().to_vec()),
            ),
            (
                ("z6".as_bytes().to_vec(), Operation::Put),
                ("z6".as_bytes().to_vec()),
            ),
            (
                ("z7".as_bytes().to_vec(), Operation::Put),
                ("z7".as_bytes().to_vec()),
            ),
        ];
        let table_file_meta = create_table(db_options, entries, 49, 50);
        version.files[3].push(Arc::new(table_file_meta));
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
