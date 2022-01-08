use std::sync::Arc;

use parking_lot::{Mutex, MutexGuard};

use crate::config::MAX_NUM_LEVELS;
use crate::db::GuardedDbFields;
use crate::file_names::FileNameHandler;
use crate::fs::FileSystem;
use crate::key::InternalKey;
use crate::logs::LogWriter;
use crate::table_cache::TableCache;
use crate::utils::linked_list::{LinkedList, SharedNode};
use crate::versioning::errors::{ManifestWriteErrorKind, WriteError};
use crate::{DbOptions, DB};

use super::errors::WriteResult;
use super::version::{Version, VersionBuilder};
use super::VersionChangeManifest;

/// Manages the versions of the database.
#[derive(Debug)]
pub(crate) struct VersionSet {
    /// Database options to refer to when reading the table file.
    options: DbOptions,

    /// A reference to the file system provider in use by the database.
    filesystem_provider: Arc<Box<dyn FileSystem>>,

    /// Handler for file names used by the database.
    file_name_handler: Arc<FileNameHandler>,

    /**
    A cache for accessing table files.

    This will be shared with the child versions.
    */
    table_cache: Arc<TableCache>,

    /**
    The most recently used file number.

    This is a counter that is incremented as new files are crated. File numbers can be re-used when
    a write-ahead log and memtable pair are converted to a table file.

    # Legacy

    This is analogous to LevelDB's `VersionSet::next_file_number_` field.
    */
    curr_file_number: u64,

    /// The file number for the current manifest.
    manifest_file_number: u64,

    /**
    The most recenly used sequence number for an operation.

    This is a counter that is incremented as new operations are executed.
    */
    prev_sequence_number: u64,

    /// The ID of the currently used write-ahead log.
    curr_wal_number: u64,

    /**
    The ID of the previous write-ahead log.

    This number is only populated if there is a memtable that is currenly undergoing the compaction
    process.
    */
    prev_wal_number: Option<u64>,

    /// A list of versions in the version set where the current version is at the tail of the list.
    versions: LinkedList<Version>,

    /// The most up to date version.
    current_version: Option<SharedNode<Version>>,

    /// Per-level keys at which the next compaction at that level should start.
    compaction_pointers: [Option<InternalKey>; MAX_NUM_LEVELS],

    /**
    The manifest file to persist version mutations to.

    The underlying [`LogWriter`] is wrapped in an [`Arc`]/[`Mutex`] combo to provide access to the
    manifest file.

    # Legacy

    This field corresponds to the `VersionSet::descriptor_log_` field.
    */
    maybe_manifest_file: Option<Arc<Mutex<LogWriter>>>,
}

/// Public methods
impl VersionSet {
    /// Create a new instance of [`VersionSet`].
    pub fn new(options: DbOptions, table_cache: TableCache) -> Self {
        let filesystem_provider = options.filesystem_provider();
        let versions = LinkedList::<Version>::new();
        let file_name_handler = Arc::new(FileNameHandler::new(options.db_path().to_string()));

        Self {
            options,
            filesystem_provider,
            file_name_handler,
            table_cache: Arc::new(table_cache),
            curr_file_number: 1,
            // This will be updated by [`VersionSet::recover`]
            manifest_file_number: 0,
            prev_sequence_number: 0,
            curr_wal_number: 0,
            prev_wal_number: None,
            versions,
            current_version: None,
            compaction_pointers: Default::default(),
            maybe_manifest_file: None,
        }
    }

    /// Return the number of table files at the specified level in the current version.
    pub fn num_files_at_level(&self, level: usize) -> usize {
        // We have a shared reference to `self` so the `current_version` field cannot have been
        // uninstalled or released.
        let current_version = self.current_version.as_ref().unwrap();
        let num_files = current_version.read().element.num_files_at_level(level);

        num_files
    }

    // Returns a new file number.
    pub fn get_new_file_number(&mut self) -> u64 {
        self.curr_file_number += 1;
        self.curr_file_number
    }

    /**
    Reuse a file number.

    We reuse file numbers in cases like when a we fail to create a file. This helps to avoid
    exhausting the file number space.

    **NOTE** The number being reused must have been obtained via [`VersionSet::get_new_file_number`].
    */
    pub fn reuse_file_number(&mut self, file_number: u64) {
        // If the provided file number is the same as the current file number in the version set,
        // then we know that the counter was just incremented and we can perform the reverse
        // operation.
        if self.curr_file_number == file_number {
            self.curr_file_number -= 1;
        }
    }

    /**
    Get a reference to the version set's previous WAL number.

    This number is only populated if there is a memtable that is currenly undergoing the compaction
    process.
    */
    pub fn maybe_prev_wal_number(&self) -> Option<u64> {
        self.prev_wal_number
    }

    /**
    Set the version set's previous WAL number.

    This number must only be populated if there is a memtable that is currently undergoing
    compaction.
    */
    pub fn set_prev_wal_number(&mut self, prev_wal_number: Option<u64>) {
        self.prev_wal_number = prev_wal_number;
    }

    /// Returns true if a level on the current version needs compaction.
    pub fn needs_compaction(&self) -> bool {
        // We have a shared reference to `self` so the `current_version` field cannot have been
        // uninstalled or released.
        let curr_version_handle = self.current_version.as_ref().unwrap();
        let curr_version = &curr_version_handle.read().element;
        if curr_version.get_size_compaction_metadata().is_some()
            && curr_version
                .get_size_compaction_metadata()
                .unwrap()
                .compaction_score
                >= 1.0
        {
            // The compaction score is too high. We need to do a compaction.
            return true;
        }

        if curr_version.get_seek_compaction_metadata().is_some()
            && curr_version
                .get_seek_compaction_metadata()
                .unwrap()
                .file_to_compact
                .is_some()
        {
            // There is a file to compact due to too many seeks.
            return true;
        }

        false
    }

    /// Get the most recently used sequence number.
    pub fn get_prev_sequence_number(&self) -> u64 {
        self.prev_sequence_number
    }

    /// Set the most recently used sequence number.
    pub fn set_prev_sequence_number(&mut self, sequence_number: u64) {
        self.prev_sequence_number = sequence_number;
    }

    /**
    Get a reference to the current version.

    # Panics

    This method will panic if it is called before the version set has been initialized with a
    version. This should not ever happen because versions are populated whenever a database is
    (re-)opened.
    */
    pub fn get_current_version(&self) -> SharedNode<Version> {
        self.current_version.as_ref().unwrap().clone()
    }

    /**
    Persist the changes in the change manifest to the on-disk manifest file and apply the
    changes by creating a new version in the version set.

    # Panics

    Panics if the WAL file number in the change manifest is less than the current WAL file number.
    File numbers can be re-used but otherwise they must be increasing.
    */
    pub fn log_and_apply(
        db_fields_guard: &mut MutexGuard<GuardedDbFields>,
        mut change_manifest: VersionChangeManifest,
    ) -> WriteResult<()> {
        let (new_version, created_new_manifest_file) =
            VersionSet::get_new_version_from_current(db_fields_guard, &mut change_manifest)?;

        let manifest_write_result = VersionSet::persist_changes(
            db_fields_guard,
            &change_manifest,
            created_new_manifest_file,
        );
        let version_set = &mut db_fields_guard.version_set;
        match manifest_write_result {
            Ok(_) => {
                log::info!(
                    "Installing version with WAL file number {:?} and sequence number {:?} as the \
                    current version.",
                    change_manifest.wal_file_number.as_ref(),
                    version_set.prev_sequence_number
                );

                let version_handle = version_set.versions.push(new_version);
                version_set.current_version = Some(version_handle);
                version_set.curr_wal_number = change_manifest.wal_file_number.clone().unwrap();
                version_set.prev_wal_number = change_manifest.prev_wal_file_number.clone();
            }
            Err(error) => {
                log::error!(
                    "Failed to update the manifest file with the version changes. Cleaning up any \
                    side effects. Original error: {}.",
                    &error
                );

                if created_new_manifest_file {
                    let manifest_path = version_set
                        .file_name_handler
                        .get_manifest_file_name(version_set.manifest_file_number);
                    version_set.maybe_manifest_file = None;
                    let remove_file_result =
                        version_set.filesystem_provider.remove_file(&manifest_path);
                    if let Err(remove_file_error) = remove_file_result {
                        log::error!("There was an error cleaning up the newly created manifest file after encountering a different error. Error: {}.", &remove_file_error);
                        return Err(WriteError::ManifestWrite(
                            ManifestWriteErrorKind::ManifestErrorCleanup(remove_file_error.into()),
                        ));
                    }
                }
            }
        }

        Ok(())
    }

    /**
    Releases a version reference and completely removes it from the version set if
    there are no other references to it.

    **This method must be called so that the version is also released from the linked list.**

    # Legacy

    LevelDB keeps the next/prev pointers internal to a version while RainDB uses an external linked
    list that lives within the version set. This makes it difficult to just use the [`Drop`] trait
    to clean up the version from the linked list. We could wrap the linked list in
    [`Arc`]/[`Mutex`] but this would be extra. This wrapper is extra since we want to maintain the
    invariant that access to the version set is exclusive and managed by the top-level RainDB mutex.
    This might be too strong of an invariant but is most like LevelDB so we keep it this way for
    now.

    [`Mutex`]: parking_lot::Mutex
    */
    pub fn release_version(&mut self, version: SharedNode<Version>) {
        if Arc::strong_count(&version) == 2 {
            // Remove if this is the last external reference.
            // 1 external reference + 1 internal (linked list) reference = 2.
            self.versions.remove_node(version);
        } else {
            drop(version);
        }
    }
}

/// Private methods
impl VersionSet {
    /**
    Get a new version by applying the supplied changes to the current version.

    This method will also bootstrap a manifest file if one has not already been created.

    # Panics

    Panics if the WAL file number in the change manifest is less than the current WAL file number.
    File numbers can be re-used but otherwise they must be increasing.

    # Legacy

    This corresponds to the first part of LevelDB's `VersionSet::LogAndApply`. RainDB splits the
    method to facilitate releasing the lock wrapping a version set during expensive disk writing
    operations.

    This method will return a tuple of the newly created [`Version`] and a boolean indicating
    whether or not a new manifest file was created.
    */
    fn get_new_version_from_current(
        db_fields_guard: &mut MutexGuard<GuardedDbFields>,
        change_manifest: &mut VersionChangeManifest,
    ) -> WriteResult<(Version, bool)> {
        let version_set = &mut db_fields_guard.version_set;

        if change_manifest.wal_file_number.is_some() {
            let wal_file_number = *change_manifest.wal_file_number.as_ref().unwrap();
            assert!(wal_file_number >= version_set.curr_wal_number);
            // The new WAL file number must have been one that was handed out by the version set
            // via `VersionSet::get_new_file_number`
            assert!(wal_file_number < version_set.curr_wal_number + 1);
        } else {
            // The WAL file number may not be set when performing a recovery operation
            change_manifest.wal_file_number = Some(version_set.curr_wal_number);
        }

        if change_manifest.prev_wal_file_number.is_none() {
            change_manifest.prev_wal_file_number = version_set.prev_wal_number.clone();
        }

        log::info!(
            "Creating a new version from the change manifest with WAL file number {:?} and \
            sequence number {}.",
            change_manifest.wal_file_number.as_ref(),
            version_set.prev_sequence_number
        );
        change_manifest.curr_file_number = Some(version_set.curr_file_number);
        change_manifest.prev_sequence_number = Some(version_set.prev_sequence_number);
        let current_version = version_set.get_current_version();
        let mut version_builder = VersionBuilder::new(current_version);
        version_builder.accumulate_changes(&change_manifest);
        let mut new_version = version_builder.apply_changes(&mut version_set.compaction_pointers);
        new_version.finalize();

        let created_new_manifest_file: bool = version_set.maybe_manifest_file.is_none();
        if version_set.maybe_manifest_file.is_none() {
            // We do not need to release the lock here because this path is only hit on the first
            // call of this method (e.g. on database opening) so no other work should be
            // waiting.
            let manifest_path = version_set
                .file_name_handler
                .get_manifest_file_name(version_set.manifest_file_number);

            log::info!(
                "Creating a new manifest file at {:?} with a snapshot of the current \
                version set state.",
                &manifest_path
            );
            let mut manifest_file = LogWriter::new(
                version_set.options.filesystem_provider(),
                manifest_path.clone(),
            )?;
            version_set.write_snapshot(&mut manifest_file)?;
            version_set.maybe_manifest_file = Some(Arc::new(Mutex::new(manifest_file)));

            log::info!(
                "Manifest file created at {:?} with a snapshot of the current version set state.",
                &manifest_path
            );
        }

        Ok((new_version, created_new_manifest_file))
    }

    /// Write a snapshot of the version set to the provided log file.
    fn write_snapshot(&mut self, manifest_file: &mut LogWriter) -> WriteResult<()> {
        let mut change_manifest = VersionChangeManifest::default();

        // Save compaction pointers
        for level in 0..MAX_NUM_LEVELS {
            if self.compaction_pointers[level].is_some() {
                change_manifest.add_compaction_pointer(
                    level,
                    self.compaction_pointers[level].clone().unwrap(),
                );
            }
        }

        // Save files
        let current_version = self.get_current_version();
        for level in 0..MAX_NUM_LEVELS {
            let files = &current_version.read().element.files[level];
            for file in files {
                change_manifest.add_file(
                    level,
                    file.file_number(),
                    file.get_file_size(),
                    file.largest_key().clone()..file.smallest_key().clone(),
                )
            }
        }
        self.release_version(current_version);

        let serialized_manifest: Vec<u8> = Vec::from(&change_manifest);
        manifest_file.append(&serialized_manifest)?;

        Ok(())
    }

    fn persist_changes(
        db_fields_guard: &mut MutexGuard<GuardedDbFields>,
        change_manifest: &VersionChangeManifest,
        is_new_manifest_file: bool,
    ) -> WriteResult<()> {
        let prev_sequence_num = db_fields_guard.version_set.prev_sequence_number;
        let filesystem_provider = Arc::clone(&db_fields_guard.version_set.filesystem_provider);
        let file_name_handler = Arc::clone(&db_fields_guard.version_set.file_name_handler);
        let manifest_file_number = db_fields_guard.version_set.manifest_file_number;
        let manifest_file = Arc::clone(
            db_fields_guard
                .version_set
                .maybe_manifest_file
                .as_ref()
                .unwrap(),
        );

        parking_lot::MutexGuard::<'_, GuardedDbFields>::unlocked_fair(
            db_fields_guard,
            || -> WriteResult<()> {
                // Release lock during actual write to the manifest because disk operations are
                // relatively expensive. Let other work progress during these operations.
                log::info!(
                    "Persisting change manifest with WAL file number {:?} and sequence number {} \
                    to the manifest file.",
                    change_manifest.wal_file_number.as_ref(),
                    prev_sequence_num
                );
                let serialized_manifest: Vec<u8> = Vec::from(change_manifest);
                manifest_file.lock().append(&serialized_manifest)?;

                if is_new_manifest_file {
                    log::info!(
                        "Installing manifest file {} as the CURRENT manifest.",
                        manifest_file_number
                    );

                    if let Err(error) = DB::set_current_file(
                        filesystem_provider,
                        file_name_handler.as_ref(),
                        manifest_file_number,
                    ) {
                        return Err(WriteError::ManifestWrite(
                            ManifestWriteErrorKind::SwapCurrentFile(error.into()),
                        ));
                    }
                }

                Ok(())
            },
        )
    }
}
