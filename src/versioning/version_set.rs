use std::sync::Arc;

use parking_lot::MutexGuard;

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
    file_name_handler: FileNameHandler,

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

    # Legacy

    This field corresponds to the `VersionSet::descriptor_log_` field.
    */
    maybe_manifest_file: Option<LogWriter>,
}

/// Public methods
impl VersionSet {
    /// Create a new instance of [`VersionSet`].
    pub fn new(options: DbOptions, table_cache: TableCache) -> Self {
        let filesystem_provider = options.filesystem_provider();
        let versions = LinkedList::<Version>::new();
        let file_name_handler = FileNameHandler::new(options.db_path().to_string());

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
        let current_version = self.get_current_version();
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
        let curr_version = &self.get_current_version().read().element;
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
        &mut self,
        mut change_manifest: VersionChangeManifest,
        db_fields_guard: &mut MutexGuard<GuardedDbFields>,
    ) -> WriteResult<()> {
        if change_manifest.wal_file_number.is_some() {
            let wal_file_number = *change_manifest.wal_file_number.as_ref().unwrap();
            assert!(wal_file_number >= self.curr_wal_number);
            // The new WAL file number must have been one that was handed out by the version set
            // via `VersionSet::get_new_file_number`
            assert!(wal_file_number < self.curr_wal_number + 1);
        } else {
            // The WAL file number may not be set when performing a recovery operation
            change_manifest.wal_file_number = Some(self.curr_wal_number);
        }

        if change_manifest.prev_wal_file_number.is_none() {
            change_manifest.prev_wal_file_number = self.prev_wal_number.clone();
        }

        log::info!(
            "Creating a new version from the change manifest with WAL file number {:?} and \
            sequence number {}.",
            change_manifest.wal_file_number.as_ref(),
            self.prev_sequence_number
        );
        change_manifest.curr_file_number = Some(self.curr_file_number);
        change_manifest.prev_sequence_number = Some(self.prev_sequence_number);
        let current_version = self.get_current_version();
        let mut version_builder = VersionBuilder::new(current_version);
        version_builder.accumulate_changes(&change_manifest);
        let mut new_version = version_builder.apply_changes(&mut self.compaction_pointers);
        new_version.finalize();

        let created_new_manifest_file: bool = self.maybe_manifest_file.is_none();
        if self.maybe_manifest_file.is_none() {
            // We do not need to release the lock here because this path is only hit on the first
            // call of this method (e.g. on database opening) so no other work should be
            // waiting.
            let manifest_path = self
                .file_name_handler
                .get_manifest_file_name(self.manifest_file_number);

            log::info!(
                "Creating a new manifest file at {:?} with a snapshot of the current \
                version set state.",
                &manifest_path
            );
            let mut manifest_file =
                LogWriter::new(self.options.filesystem_provider(), manifest_path.clone())?;
            self.write_snapshot(&mut manifest_file);
            self.maybe_manifest_file = Some(manifest_file);

            log::info!(
                "Manifest file created at {:?} with a snapshot of the current version set state.",
                &manifest_path
            );
        }

        let manifest_write_result = parking_lot::MutexGuard::<'_, GuardedDbFields>::unlocked_fair(
            db_fields_guard,
            || -> WriteResult<()> {
                // Release lock during actual write to the manifest because disk operations are
                // relatively expensive. Let other work progress during these operations.
                log::info!(
                    "Persisting change manifest with WAL file number {:?} and sequence number {} \
                    to the manifest file.",
                    change_manifest.wal_file_number.as_ref(),
                    self.prev_sequence_number
                );
                let serialized_manifest: Vec<u8> = Vec::from(&change_manifest);
                self.maybe_manifest_file
                    .as_mut()
                    .unwrap()
                    .append(&serialized_manifest)?;

                if created_new_manifest_file {
                    log::info!(
                        "Installing manifest file {} as the CURRENT manifest.",
                        self.manifest_file_number
                    );

                    if let Err(error) = DB::set_current_file(
                        Arc::clone(&self.filesystem_provider),
                        &self.file_name_handler,
                        self.manifest_file_number,
                    ) {
                        return Err(WriteError::ManifestWrite(
                            ManifestWriteErrorKind::SwapCurrentFile(error.into()),
                        ));
                    }
                }

                Ok(())
            },
        );

        match manifest_write_result {
            Ok(_) => {
                log::info!(
                    "Installing version with WAL file number {:?} and sequence number {:?} as the \
                    current version.",
                    change_manifest.wal_file_number.as_ref(),
                    self.prev_sequence_number
                );

                self.versions.push(new_version);
                self.curr_wal_number = change_manifest.wal_file_number.clone().unwrap();
                self.prev_wal_number = change_manifest.prev_wal_file_number.clone();
            }
            Err(error) => {
                log::error!(
                    "Failed to update the manifest file with the version changes. Cleaning up any \
                    side effects. Original error: {}.",
                    &error
                );

                if created_new_manifest_file {
                    let manifest_path = self
                        .file_name_handler
                        .get_manifest_file_name(self.manifest_file_number);
                    self.maybe_manifest_file = None;
                    let remove_file_result = self.filesystem_provider.remove_file(&manifest_path);
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
}

/// Private methods
impl VersionSet {
    /// Write a snapshot of the version set to the provided log file.
    fn write_snapshot(&mut self, manifest_file: &mut LogWriter) {
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
        for level in 0..MAX_NUM_LEVELS {
            let files = &self.get_current_version().read().element.files[level];
            for file in files {
                change_manifest.add_file(
                    level,
                    file.file_number(),
                    file.get_file_size(),
                    file.largest_key().clone()..file.smallest_key().clone(),
                )
            }
        }

        let serialized_manifest: Vec<u8> = Vec::from(&change_manifest);
        manifest_file.append(&serialized_manifest);
    }
}
