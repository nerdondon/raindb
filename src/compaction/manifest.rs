use std::sync::Arc;

use crate::config::MAX_NUM_LEVELS;
use crate::key::InternalKey;
use crate::utils::linked_list::SharedNode;
use crate::versioning::file_metadata::FileMetadata;
use crate::versioning::version::Version;
use crate::versioning::VersionChangeManifest;
use crate::DbOptions;

/**
Encapsulates information about a compaction.

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

    Only level 0 should have multiple files read in for compaction into a parent level since only
    level 0 is allowed to have files with overlapping key ranges.

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
}
