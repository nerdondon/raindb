use std::sync::Arc;

use crate::db::PortableDatabaseState;
use crate::errors::{RainDBError, RainDBResult};
use crate::table_cache::TableCache;
use crate::tables::TableBuilder;
use crate::versioning::file_iterators::MergingIterator;
use crate::versioning::file_metadata::FileMetadata;

use super::manifest::CompactionManifest;

/// Holds the intermediate state of an ongoing compaction and the changes that were made.
pub(crate) struct CompactionState {
    /**
    Metadata for the files generated by the compaction.

    # Legacy

    This is the equivalent of LevelDB's `DBImpl::CompactionState::outputs` field. We re-use
    [`FileMetadata`] for the outputs because the only thing different about LevelDB's
    `DBImpl::CompactionState::Output` struct is the absence of the `allowed_seeks` field.
    */
    output_files: Vec<FileMetadata>,

    /// The compaction manifest configuring the behavior of the compaction.
    compaction_manifest: CompactionManifest,

    /**
    The smallest sequence number that can be dropped due to the compaction.

    Sequence numbers < `smallest_snapshot` are not significant because we will never service a
    database request with a snapshot less than the `smallest_snapshot`. Therefore, if we have seen
    a sequence number S <= `smallest_snapshot`, we can drop all entries for the same user key with
    a sequence number < S.
    */
    smallest_snapshot: u64,

    /// A running count of the size of the files being output by the compaction in bytes.
    pub(crate) total_size_bytes: u64,

    /// The builder for the table file being produced.
    table_builder: Option<TableBuilder>,
}

/// Crate-only methods
impl CompactionState {
    /// Create a new instance of [`CompactionState`].
    pub(crate) fn new(compaction_manifest: CompactionManifest, smallest_snapshot: u64) -> Self {
        Self {
            output_files: vec![],
            compaction_manifest,
            smallest_snapshot,
            total_size_bytes: 0,
            table_builder: None,
        }
    }

    /**
    Get a mutable reference to the output file currently being processed.

    # Panics

    Panics if there are no output files.
    */
    pub(crate) fn current_output_mut(&mut self) -> &mut FileMetadata {
        self.output_files.last_mut().unwrap()
    }

    /// Get a reference to the compaction manifest.
    pub(crate) fn compaction_manifest(&self) -> &CompactionManifest {
        &self.compaction_manifest
    }

    /// Get a mutable reference to the compaction manifest
    pub(crate) fn compaction_manifest_mut(&mut self) -> &mut CompactionManifest {
        &mut self.compaction_manifest
    }

    /// Returns true if there is an existing table builder.
    pub(crate) fn has_table_builder(&self) -> bool {
        self.table_builder.is_some()
    }

    /// Set current the table builder.
    pub(crate) fn set_table_builder(&mut self, builder: TableBuilder) {
        self.table_builder = Some(builder);
    }

    /// Get a mutable reference to the table builder currently being used.
    pub(crate) fn table_builder_mut(&mut self) -> &mut TableBuilder {
        self.table_builder.as_mut().unwrap()
    }

    /// Get the smallest sequence number that can be dropped in a compaction.
    pub(crate) fn get_smallest_snapshot(&self) -> u64 {
        self.smallest_snapshot
    }

    /// Get the current size of the outputs in bytes.
    pub(crate) fn get_output_size(&self) -> u64 {
        self.output_files
            .iter()
            .map(|metadata| metadata.get_file_size())
            .sum()
    }

    /**
    Finalize and check usability of a generated table file.

    # Panics

    There must be a table builder for a table file in the `table_builder` field.
    */
    pub(crate) fn finish_compaction_output_file(
        &mut self,
        table_cache: Arc<TableCache>,
        iterator: &mut MergingIterator,
    ) -> RainDBResult<()> {
        // Assert invariants
        assert!(self.has_table_builder());

        let table_file_number = self.current_output_mut().file_number();
        assert!(table_file_number != 0);

        // Check for iterator errors
        let mut maybe_error: Option<RainDBError> = None;
        let num_table_entries = self.table_builder_mut().get_num_entries();
        if let Some(iter_err) = iterator.get_error() {
            maybe_error = Some(iter_err);
            self.table_builder_mut().abandon();
        } else {
            let finalize_result = self.table_builder_mut().finalize();
            if let Err(finalize_err) = finalize_result {
                maybe_error = Some(RainDBError::TableBuild(finalize_err));
            }
        }

        // Record some table build statistics and clear the builder
        let table_size = self.table_builder_mut().file_size();
        self.current_output_mut().set_file_size(table_size);
        self.total_size_bytes += table_size;
        self.table_builder.take();

        if let Some(err) = maybe_error {
            return Err(err);
        }

        if num_table_entries > 0 {
            // Verify the table file if there were no errors during generation
            let _table_iter = table_cache.find_table(table_file_number)?;

            log::info!(
                "Generated table {table_num} at level {level}. It has {num_entries} entries \
                ({num_bytes} bytes).",
                table_num = table_file_number,
                level = self.compaction_manifest.level(),
                num_entries = num_table_entries,
                num_bytes = table_size
            );
        }

        Ok(())
    }

    /**
    Create a new table builder to aggregate compaction results into a new table file.

    # Panics

    There must not already be a table builder in use.
    */
    pub(crate) fn open_compaction_output_file(
        &mut self,
        db_state: &PortableDatabaseState,
    ) -> RainDBResult<()> {
        assert!(!self.has_table_builder());

        let file_number: u64;
        {
            let mut db_fields_guard = db_state.guarded_db_fields.lock();
            file_number = db_fields_guard.version_set.get_new_file_number();
            db_fields_guard.tables_in_use.insert(file_number);
            let file_metadata = FileMetadata::new(file_number);
            self.output_files.push(file_metadata);
        }

        // Create the actual file
        let table_builder = TableBuilder::new(db_state.options.clone(), file_number)?;
        self.table_builder = Some(table_builder);

        Ok(())
    }
}
