use std::cmp::Ordering;
use std::collections::HashSet;
use std::sync::Arc;

use crate::config::MAX_NUM_LEVELS;
use crate::key::InternalKey;
use crate::utils::comparator::Comparator;
use crate::utils::linked_list::SharedNode;

use super::file_metadata::{FileMetadata, FileMetadataBySmallestKey};
use super::version::Version;
use super::version_manifest::DeletedFile;
use super::VersionChangeManifest;

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

    This method will also update the specified compaction pointers which usually come from the
    version set that the new version is being added to.

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
        wal_file_number: u64,
        prev_sequence_number: u64,
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
        let mut new_version = base.new_from_current(wal_file_number, prev_sequence_number);
        for level in 0..MAX_NUM_LEVELS {
            let mut level_added_files: Vec<Arc<FileMetadata>> =
                self.added_files[level].iter().map(Arc::clone).collect();
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
                if FileMetadataBySmallestKey::compare(base_file, added_file) == Ordering::Less {
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

#[cfg(test)]
mod version_builder_tests {
    use parking_lot::RwLock;
    use pretty_assertions::assert_eq;

    use crate::table_cache::TableCache;
    use crate::utils::linked_list::Node;
    use crate::{DbOptions, Operation};

    use super::*;

    #[test]
    fn accumulates_changes_successfully() {
        let db_options = create_testing_options();
        let table_cache = Arc::new(TableCache::new(db_options.clone(), 10));
        let base_version = Arc::new(RwLock::new(Node::new(Version::new(
            db_options,
            &table_cache,
            1,
            1,
        ))));
        let mut version_builder = VersionBuilder::new(base_version);
        let mut change_manifest = VersionChangeManifest::default();
        for idx in 0..4 {
            change_manifest.add_file(
                3,
                2900 + idx,
                4 * 1024 * 1024,
                InternalKey::new(b"abc".to_vec(), idx, Operation::Put)
                    ..InternalKey::new(b"xyz".to_vec(), 100 + idx, Operation::Delete),
            );
            change_manifest.remove_file(4, 2880 + idx);
            change_manifest.add_compaction_pointer(
                idx as usize,
                InternalKey::new(b"rst".to_vec(), 100 + idx, Operation::Put),
            );
        }

        version_builder.accumulate_changes(&change_manifest);

        for idx in 0..4 {
            assert!(version_builder.compaction_pointers[idx].is_some());
            assert!(version_builder.deleted_files[4].contains(&(2880 + idx as u64)));
        }
        assert_eq!(version_builder.added_files[3].len(), 4);

        assert_eq!(version_builder.added_files[4].len(), 0);
        let mut change_manifest2 = VersionChangeManifest::default();
        // Add a file that was previously marked for deletion
        change_manifest2.add_file(
            4,
            2882,
            4 * 1024 * 1024,
            InternalKey::new(b"abc".to_vec(), 2, Operation::Put)
                ..InternalKey::new(b"xyz".to_vec(), 102, Operation::Delete),
        );
        version_builder.accumulate_changes(&change_manifest2);

        assert!(!version_builder.deleted_files[4].contains(&2882));
        assert_eq!(version_builder.added_files[4].len(), 1);
    }

    #[test]
    fn files_marked_to_be_added_and_compaction_pointers_are_added_to_new_version() {
        let db_options = create_testing_options();
        let table_cache = Arc::new(TableCache::new(db_options.clone(), 10));
        let base_version = Arc::new(RwLock::new(Node::new(Version::new(
            db_options,
            &table_cache,
            1,
            1,
        ))));
        let mut version_builder = VersionBuilder::new(base_version);
        let mut change_manifest = VersionChangeManifest::default();
        for idx in 1..5 {
            change_manifest.add_file(
                3,
                2900 + idx,
                4 * 1024 * 1024,
                InternalKey::new(
                    (idx * 100).to_string().as_bytes().to_vec(),
                    idx + 200,
                    Operation::Put,
                )
                    ..InternalKey::new(
                        (idx * 100 + 99).to_string().as_bytes().to_vec(),
                        idx + 300,
                        Operation::Delete,
                    ),
            );
            change_manifest.remove_file(4, 2880 + idx);
            change_manifest.add_compaction_pointer(
                idx as usize,
                InternalKey::new(b"rst".to_vec(), 100 + idx, Operation::Put),
            );
        }
        version_builder.accumulate_changes(&change_manifest);

        let mut compaction_pointers: [Option<InternalKey>; MAX_NUM_LEVELS] = Default::default();
        let new_version = version_builder.apply_changes(2899, 5000, &mut compaction_pointers);

        for level in 1..5 {
            let ptr = compaction_pointers[level].as_ref();
            assert_eq!(
                *ptr.unwrap(),
                InternalKey::new(b"rst".to_vec(), (100 + level) as u64, Operation::Put)
            );
        }

        assert!(version_builder.already_invoked);
        assert_eq!(new_version.num_files_at_level(3), 4);
    }

    #[test]
    fn files_marked_to_be_deleted_are_removed_in_the_new_version() {
        let db_options = create_testing_options();
        let table_cache = Arc::new(TableCache::new(db_options.clone(), 10));
        let base_version = Arc::new(RwLock::new(Node::new(Version::new(
            db_options,
            &table_cache,
            1,
            1,
        ))));
        for idx in 1..5 {
            let mut file = FileMetadata::new(2900 + idx);
            file.set_smallest_key(Some(InternalKey::new(
                (idx * 100).to_string().as_bytes().to_vec(),
                idx + 200,
                Operation::Put,
            )));
            file.set_largest_key(Some(InternalKey::new(
                (idx * 100 + 99).to_string().as_bytes().to_vec(),
                idx + 300,
                Operation::Delete,
            )));
            base_version.write().element.files[3].push(Arc::new(file));
        }

        let mut version_builder = VersionBuilder::new(base_version);
        let mut change_manifest = VersionChangeManifest::default();
        change_manifest.remove_file(3, 2902);
        version_builder.accumulate_changes(&change_manifest);

        let mut compaction_pointers: [Option<InternalKey>; MAX_NUM_LEVELS] = Default::default();
        let new_version = version_builder.apply_changes(2899, 5000, &mut compaction_pointers);

        assert_eq!(new_version.num_files_at_level(3), 3);
    }

    /// Create default options for testing versions.
    fn create_testing_options() -> DbOptions {
        let mut options = DbOptions::with_memory_env();
        options.max_block_size = 256;

        options
    }
}
