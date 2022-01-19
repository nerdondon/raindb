//! Shared utilities for compaction related activities.

use crate::DbOptions;

/**
Get the maximum number of bytes that a level can overlap with it's grandparent (level + 2).

This can be used to determine placement of a new table file.
*/
pub(crate) fn max_grandparent_overlap_bytes(target_file_size: u64) -> u64 {
    target_file_size * 10
}

/// Same as [`max_grandparent_overlap_bytes`] but from an options object.
pub(crate) fn max_grandparent_overlap_bytes_from_options(options: &DbOptions) -> u64 {
    options.max_file_size() * 10
}
