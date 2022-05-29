use std::collections::HashSet;
use std::io;
use std::ops::Range;
use std::path::Path;
use std::sync::Arc;

use parking_lot::{Mutex, MutexGuard};

use crate::compaction::manifest::CompactionManifest;
use crate::config::MAX_NUM_LEVELS;
use crate::db::GuardedDbFields;
use crate::errors::LogIOError;
use crate::file_names::{FileNameHandler, ParsedFileType};
use crate::fs::FileSystem;
use crate::key::InternalKey;
use crate::logs::{LogReader, LogWriter};
use crate::table_cache::TableCache;
use crate::utils::linked_list::{LinkedList, SharedNode};
use crate::versioning::errors::{
    CurrentFileReadErrorKind, ManifestWriteErrorKind, RecoverError, WriteError,
};
use crate::{DbOptions, DB};

use super::errors::{RecoverResult, WriteResult};
use super::file_metadata::FileMetadata;
use super::version::Version;
use super::version_builder::VersionBuilder;
use super::VersionChangeManifest;

/// Manages the versions of the database.
#[derive(Debug)]
pub(crate) struct VersionSet {
    /// Database options to refer to when reading the table file.
    options: DbOptions,

    /// A reference to the file system provider in use by the database.
    filesystem_provider: Arc<dyn FileSystem>,

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
    current_version: SharedNode<Version>,

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
    pub fn new(options: DbOptions, table_cache: Arc<TableCache>) -> Self {
        let filesystem_provider = options.filesystem_provider();
        let file_name_handler = Arc::new(FileNameHandler::new(options.db_path().to_string()));

        let mut versions = LinkedList::<Version>::new();
        let prev_sequence_number: u64 = 0;
        let curr_wal_number: u64 = 0;
        let base_version = Version::new(
            options.clone(),
            &table_cache,
            prev_sequence_number,
            curr_wal_number,
        );
        let current_version = versions.push(base_version);

        Self {
            options,
            filesystem_provider,
            file_name_handler,
            table_cache,
            curr_file_number: 1,
            // This will be updated by [`VersionSet::recover`]
            manifest_file_number: 0,
            prev_sequence_number,
            curr_wal_number,
            prev_wal_number: None,
            versions,
            current_version,
            compaction_pointers: Default::default(),
            maybe_manifest_file: None,
        }
    }

    /// Return the number of table files at the specified level in the current version.
    pub fn num_files_at_level(&self, level: usize) -> usize {
        // We have a shared reference to `self` so the `current_version` field cannot have been
        // uninstalled or released.
        let num_files = self
            .current_version
            .read()
            .element
            .num_files_at_level(level);

        num_files
    }

    // Returns a new file number.
    pub fn get_new_file_number(&mut self) -> u64 {
        self.curr_file_number += 1;
        self.curr_file_number
    }

    // Mark the specified file number as used`.
    pub fn mark_file_number_used(&mut self, file_number: u64) {
        if self.curr_file_number <= file_number {
            self.curr_file_number = file_number;
        }
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

    /// Get the file number of the current write-ahead log.
    pub fn get_curr_wal_number(&self) -> u64 {
        self.curr_wal_number
    }

    /// Get the file number of the current manifest file.
    pub fn get_manifest_file_number(&self) -> u64 {
        self.manifest_file_number
    }

    /// Get an owned reference to the table cache.
    pub fn get_table_cache(&self) -> Arc<TableCache> {
        Arc::clone(&self.table_cache)
    }

    /// Returns true if a level on the current version needs compaction.
    pub fn needs_compaction(&self) -> bool {
        // We have a shared reference to `self` so the `current_version` field cannot have been
        // uninstalled or released.
        let curr_version = &self.current_version.read().element;
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

        if curr_version
            .get_seek_compaction_metadata()
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
        Arc::clone(&self.current_version)
    }

    /**
    Load version information saved stored in the manifest file on persistent storage.

    Returns true if the existing manifest file was reused.
    */
    pub fn recover(&mut self) -> RecoverResult<bool> {
        let filesystem = self.options.filesystem_provider();
        let current_file_path = self.file_name_handler.get_current_file_path();
        let mut current_file = match filesystem.open_file(&current_file_path) {
            Ok(file) => file,
            Err(err) => {
                return Err(RecoverError::CurrentFileRead(CurrentFileReadErrorKind::IO(
                    err.into(),
                )))
            }
        };
        let mut current_file_contents = String::new();
        if let Err(read_err) = current_file.read_to_string(&mut current_file_contents) {
            return Err(RecoverError::CurrentFileRead(
                CurrentFileReadErrorKind::Parse(read_err.to_string()),
            ));
        }

        if current_file_contents.is_empty() || !current_file_contents.ends_with('\n') {
            let error_msg = format!(
                "The CURRENT file ({file_size} bytes) was either empty or did not have a newline \
                at the end.",
                file_size = current_file_contents.len()
            );
            log::error!("{}", &error_msg);

            return Err(RecoverError::CurrentFileRead(
                CurrentFileReadErrorKind::Parse(error_msg),
            ));
        }

        current_file_contents.truncate(current_file_contents.len() - 1);
        let manifest_file_number = match current_file_contents.parse::<u64>() {
            Ok(file_num) => file_num,
            Err(parse_err) => {
                let err_msg = format!(
                    "There was an error parsing the file number from the CURRENT file. Error: \
                    {err}",
                    err = parse_err
                );
                log::error!("{}", &err_msg);
                return Err(RecoverError::CurrentFileRead(
                    CurrentFileReadErrorKind::Parse(err_msg),
                ));
            }
        };
        let manifest_file_path = self
            .file_name_handler
            .get_manifest_file_path(manifest_file_number);
        let manifest_reader_result =
            LogReader::new(Arc::clone(&filesystem), &manifest_file_path, 0);
        if let Err(LogIOError::IO(manifest_read_err)) = &manifest_reader_result {
            if manifest_read_err.kind() == io::ErrorKind::NotFound {
                let err_msg = "The CURRENT file points at a non-existent manifest file.";
                log::error!("{}", err_msg);
                return Err(RecoverError::CurrentFileRead(
                    CurrentFileReadErrorKind::Parse(err_msg.to_string()),
                ));
            }
        }
        let mut manifest_reader = match manifest_reader_result {
            Ok(reader) => reader,
            Err(error) => return Err(RecoverError::ManifestRead(error)),
        };

        // Aggregate state from manifest file to apply back to the version set
        let mut maybe_curr_file_num: Option<u64> = None;
        let mut maybe_prev_sequence: Option<u64> = None;
        let mut maybe_curr_wal_num: Option<u64> = None;
        let mut maybe_prev_wal_num: Option<u64> = None;
        let mut version_builder = VersionBuilder::new(self.get_current_version());

        let mut manifest_record: Vec<u8> = vec![];
        let mut maybe_manifest_read_error: Option<RecoverError> = None;
        let mut manifest_records_read: usize = 0;
        match manifest_reader.read_record() {
            Ok(record) => manifest_record = record,
            Err(log_err) => maybe_manifest_read_error = Some(RecoverError::ManifestRead(log_err)),
        }

        while maybe_manifest_read_error.is_none() && !manifest_record.is_empty() {
            manifest_records_read += 1;
            match VersionChangeManifest::try_from(manifest_record.as_slice()) {
                Err(err) => {
                    maybe_manifest_read_error = Some(err);
                    break;
                }
                Ok(version_manifest) => {
                    version_builder.accumulate_changes(&version_manifest);

                    // Update aggregate state
                    if version_manifest.wal_file_number.is_some() {
                        maybe_curr_wal_num = version_manifest.wal_file_number;
                    }

                    if version_manifest.prev_wal_file_number.is_some() {
                        maybe_prev_wal_num = version_manifest.prev_wal_file_number
                    }

                    if version_manifest.curr_file_number.is_some() {
                        maybe_curr_file_num = version_manifest.curr_file_number
                    }

                    if version_manifest.prev_sequence_number.is_some() {
                        maybe_prev_sequence = version_manifest.prev_sequence_number
                    }
                }
            }
        }

        if maybe_manifest_read_error.is_none() {
            if maybe_curr_file_num.is_none() {
                maybe_manifest_read_error = Some(RecoverError::ManifestParse(
                    "The current file number was not found in any manifest entries.".to_string(),
                ));
            } else if maybe_curr_wal_num.is_none() {
                maybe_manifest_read_error = Some(RecoverError::ManifestParse(
                    "The current WAL file number was not found in any manifest entries."
                        .to_string(),
                ));
            } else if maybe_prev_sequence.is_none() {
                maybe_manifest_read_error = Some(RecoverError::ManifestParse(
                    "The most recently used sequence number was not found in any manifest entries."
                        .to_string(),
                ));
            }

            if maybe_prev_wal_num.is_none() {
                maybe_prev_wal_num = Some(0);
            }

            self.mark_file_number_used(maybe_prev_wal_num.clone().unwrap());
            self.mark_file_number_used(maybe_curr_wal_num.clone().unwrap_or(0));
        }

        if let Some(read_err) = maybe_manifest_read_error {
            log::error!(
                "There was an error loading database state from disk. Read {num_records_read} \
                manifest records. Error {manifest_read_error}",
                num_records_read = manifest_records_read,
                manifest_read_error = &read_err
            );

            return Err(read_err);
        }

        let mut recovered_version = version_builder.apply_changes(
            maybe_curr_wal_num.unwrap(),
            maybe_prev_sequence.unwrap(),
            &mut self.compaction_pointers,
        );
        recovered_version.finalize();
        self.append_new_version(recovered_version);

        self.curr_file_number = maybe_curr_file_num.unwrap();
        self.manifest_file_number = self.get_new_file_number();
        self.prev_sequence_number = maybe_prev_sequence.unwrap();
        self.curr_wal_number = maybe_curr_wal_num.unwrap();
        self.prev_wal_number = maybe_prev_wal_num;

        // Drop the manifest reader (and therefore the underlying file handle) before attempting to
        // reuse the existing manifest file
        drop(manifest_reader);
        if self.maybe_reuse_manifest(&manifest_file_path) {
            return Ok(true);
        }

        Ok(false)
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
        change_manifest: &mut VersionChangeManifest,
    ) -> WriteResult<()> {
        let (new_version, created_new_manifest_file) =
            VersionSet::get_new_version_from_current(db_fields_guard, change_manifest)?;

        let manifest_write_result = VersionSet::persist_changes(
            db_fields_guard,
            change_manifest,
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

                version_set.append_new_version(new_version);
                version_set.curr_wal_number = change_manifest.wal_file_number.unwrap();
                version_set.prev_wal_number = change_manifest.prev_wal_file_number;
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
                        .get_manifest_file_path(version_set.manifest_file_number);
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
    pub fn release_version(&mut self, version_node: SharedNode<Version>) {
        if Arc::strong_count(&version_node) == 2 {
            // Remove if this is the last external reference.
            // 1 external reference + 1 internal (linked list) reference = 2.
            log::info!(
                "Unlinking version at last sequence number {} with WAL file number {}.",
                &version_node.read().element.last_sequence_number(),
                &version_node.read().element.wal_file_number()
            );

            self.versions.remove_node(version_node);
        } else {
            drop(version_node);
        }
    }

    /// Return a set of file numbers for table files that are still in use in the version set.
    pub fn get_live_files(&self) -> HashSet<u64> {
        let mut live_files: HashSet<u64> = HashSet::new();
        for version in self.versions.iter() {
            for level in 0..MAX_NUM_LEVELS {
                let files = &version.read().element.files[level];
                for file in files.iter() {
                    live_files.insert(file.file_number());
                }
            }
        }

        live_files
    }

    /// Compact the specified key range in the specified level.
    pub fn compact_range(
        &mut self,
        level_to_compact: usize,
        key_range: Range<Option<InternalKey>>,
    ) -> Option<CompactionManifest> {
        let mut compaction_input_files: Vec<Arc<FileMetadata>> = self
            .get_current_version()
            .read()
            .element
            .get_overlapping_compaction_inputs_strong(
                level_to_compact,
                key_range.start.as_ref()..key_range.end.as_ref(),
            );

        if compaction_input_files.is_empty() {
            log::info!(
                "No overlapping files found for level {} in the specified key range so no \
                compaction is necessary.",
                level_to_compact
            );
            return None;
        }

        if level_to_compact > 0 {
            /*
            Avoid compacting too much in one shot in case the range is large.

            This cannot be done for level 0 since level 0 files can overlap and we must not pick
            one file and drop another if the two files overlap.
            */
            let file_size_limit = self.options.max_file_size();
            let mut curr_compaction_size: u64 = 0;
            let mut file_index: usize = 0;
            while !compaction_input_files.is_empty() {
                curr_compaction_size += compaction_input_files[file_index].get_file_size();
                if curr_compaction_size >= file_size_limit {
                    compaction_input_files.truncate(file_index + 1);
                    break;
                }

                file_index += 1;
            }
        }

        let mut compaction_manifest = CompactionManifest::new(&self.options, level_to_compact);
        compaction_manifest.set_input_version(self.get_current_version());
        compaction_manifest.set_compaction_level_files(compaction_input_files);

        /*
        Update the place where the next compaction for this level will start. This is updated
        immediately instead of waiting for the change manifest to be applied so that if the
        compaction fails, a different key range is tried for the next compactino.
        */
        let next_compaction_key = compaction_manifest.finalize_compaction_inputs();
        self.compaction_pointers[level_to_compact] = Some(next_compaction_key);

        Some(compaction_manifest)
    }

    /**
    Pick a level and inputs for a new compaction.

    # Panics

    Invariants that must be maintained:

    1. A size based compaction cannot be triggered by the last level.

    1. A seek based compaction must have a file in the [`SeekCompactionMetadata::file_to_compact`]
       field.
    */
    pub fn pick_compaction(&self) -> Option<CompactionManifest> {
        // Prefer compactions triggered by too much data in a level over the compactions triggered
        // by seeks.
        let current_version_node = self.get_current_version();
        let current_version = &current_version_node.read().element;
        let needs_size_compaction = current_version.requires_size_compaction();
        let needs_seek_compaction = current_version.requires_seek_compaction();
        let mut compaction_manifest: CompactionManifest;
        let level_to_compact: usize;

        if needs_size_compaction {
            level_to_compact = current_version
                .get_size_compaction_metadata()
                .unwrap()
                .compaction_level;
            assert!(level_to_compact + 1 < MAX_NUM_LEVELS);

            compaction_manifest = CompactionManifest::new(&self.options, level_to_compact);

            // Pick the first file that comes after the specified compaction pointer at the level
            for file in current_version.files[level_to_compact].iter() {
                if self.compaction_pointers[level_to_compact].is_none()
                    || file.largest_key()
                        > self.compaction_pointers[level_to_compact].as_ref().unwrap()
                {
                    compaction_manifest
                        .get_mut_compaction_level_files()
                        .push(Arc::clone(file));
                    break;
                }
            }

            if compaction_manifest.get_compaction_level_files().is_empty() {
                // Wrap-around to the beginning of the key space
                compaction_manifest
                    .get_mut_compaction_level_files()
                    .push(Arc::clone(&current_version.files[level_to_compact][0]));
            }
        } else if needs_seek_compaction {
            let seek_compaction_metadata = current_version.get_seek_compaction_metadata();
            level_to_compact = seek_compaction_metadata.level_of_file_to_compact;
            compaction_manifest = CompactionManifest::new(&self.options, level_to_compact);
            compaction_manifest
                .get_mut_compaction_level_files()
                .push(Arc::clone(
                    seek_compaction_metadata.file_to_compact.as_ref().unwrap(),
                ));
        } else {
            return None;
        }

        compaction_manifest.set_input_version(self.get_current_version());

        // Files in level 0 overlap each other, so ensure that all overlapping files are added
        if level_to_compact == 0 {
            let compaction_level_key_range = FileMetadata::get_key_range_for_files(
                compaction_manifest.get_compaction_level_files(),
            );
            let mut new_compaction_files = current_version
                .get_overlapping_compaction_inputs_strong(
                    level_to_compact,
                    Some(&compaction_level_key_range.start)..Some(&compaction_level_key_range.end),
                );

            // We clear the current set of compaction files first. This is ok because the previous
            // call to get overlapping files will include the original file. This is just a simple
            // way to avoid duplicates.
            compaction_manifest.get_mut_compaction_level_files().clear();
            compaction_manifest
                .get_mut_compaction_level_files()
                .append(&mut new_compaction_files);
        }

        compaction_manifest.finalize_compaction_inputs();

        Some(compaction_manifest)
    }

    /// Get a human-readable summary of the files in each of level of the current version.
    pub fn level_summary(&self) -> String {
        let level_summary = self
            .get_current_version()
            .read()
            .element
            .files
            .iter()
            .map(|files| files.len().to_string())
            .collect::<Vec<String>>()
            .join(" ");

        format!("files[ {level_summary} ]", level_summary = level_summary)
    }

    /**
    Get the size of the specified level at the current version in bytes.

    # Panics

    The method will panic if the specified level is not in the valid range.

    # Legacy

    This method is synonomous to LevelDB's `VersionSet::NumLevelBytes`.
    */
    pub fn get_current_level_size(&self, level: usize) -> u64 {
        assert!(level < MAX_NUM_LEVELS);

        self.get_current_version()
            .read()
            .element
            .get_level_size(level)
    }
}

/// Crate-only methods
impl VersionSet {
    /// Add a new version to the version set.
    fn append_new_version(&mut self, new_version: Version) {
        let old_version = self.get_current_version();
        self.current_version = self.versions.push(new_version);

        self.release_version(old_version);
    }
}

/// Private methods
impl VersionSet {
    /**
    If possible, reuse the existing manifest file. Returns true if the manifest was reused.

    # Panics

    This method will panic if the version set is already using a manifest file.
    */
    fn maybe_reuse_manifest(&mut self, manifest_path: &Path) -> bool {
        log::info!(
            "Checking if the manifest at the provided path can be reused. Provided path: {path:?}",
            path = manifest_path
        );
        if let Ok(ParsedFileType::ManifestFile(_manifest_number)) =
            FileNameHandler::get_file_type_from_name(manifest_path)
        {
            if let Ok(file_size) = self
                .options
                .filesystem_provider()
                .get_file_size(manifest_path)
            {
                // Use a new manifest file if the current one is too large
                if file_size >= self.options.max_file_size() {
                    return false;
                }
            } else {
                return false;
            }
        } else {
            return false;
        }

        assert!(self.maybe_manifest_file.is_none());
        log::info!("Attempting to reuse the existing manifest file.");
        match LogWriter::new(self.options.filesystem_provider(), manifest_path, true) {
            Ok(manifest_writer) => {
                self.maybe_manifest_file = Some(Arc::new(Mutex::new(manifest_writer)));

                true
            }
            Err(manifest_err) => {
                log::error!(
                    "Encountered an error attempting to reuse the existing manifest file. \
                    Ignoring and signalling creation of new manifest file. Error: {err}",
                    err = manifest_err
                );
                false
            }
        }
    }

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
            change_manifest.prev_wal_file_number = version_set.prev_wal_number;
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
        version_builder.accumulate_changes(change_manifest);
        let mut new_version = version_builder.apply_changes(
            *change_manifest.wal_file_number.as_ref().unwrap(),
            version_set.prev_sequence_number,
            &mut version_set.compaction_pointers,
        );
        new_version.finalize();

        let created_new_manifest_file: bool = version_set.maybe_manifest_file.is_none();
        if version_set.maybe_manifest_file.is_none() {
            // We do not need to release the lock here because this path is only hit on the first
            // call of this method (e.g. on database opening) so no other work should be
            // waiting.
            let manifest_path = version_set
                .file_name_handler
                .get_manifest_file_path(version_set.manifest_file_number);

            log::info!(
                "Creating a new manifest file at {:?} with a snapshot of the current \
                version set state.",
                &manifest_path
            );
            let mut manifest_file = LogWriter::new(
                version_set.options.filesystem_provider(),
                manifest_path.clone(),
                false,
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

    /// Persist the specified change manifest to the manifest file on disk.
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
