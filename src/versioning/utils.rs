//! Contains utilities used for various versioning operations.

use std::sync::Arc;

use crate::key::InternalKey;

use super::file_metadata::FileMetadata;

/// Sums the file sizes for the specified vector of file metadata.
pub(crate) fn sum_file_sizes<F: AsRef<FileMetadata>>(files: &[F]) -> u64 {
    files
        .iter()
        .map(|metadata| metadata.as_ref().get_file_size())
        .sum()
}

/**
Binary search a sorted set of disjoint files for a file whose largest key forms a tight upper
bound on the target key.

# Invariants

The passed in `files` **must** be a sorted set and the files must store key ranges that do
not overlap with the key ranges in any other file.

Returns the index of the file whose key range creates an upper bound on the target key (i.e.
its largest key is greater than or equal the target). Otherwise returns `None`.

# Legacy

This is synonomous with LevelDB's `leveldb::FindFile` method.
*/
pub(crate) fn find_file_with_upper_bound_range(
    files: &[Arc<FileMetadata>],
    target_user_key: &InternalKey,
) -> Option<usize> {
    let mut left: usize = 0;
    let mut right: usize = files.len();
    while left < right {
        let mid: usize = (left + right) / 2;
        let file = &files[mid];

        if file.largest_key() < target_user_key {
            // The largest key in the file at mid is less than the target, so the set of files
            // at or before mid are not interesting.
            left = mid + 1;
        } else {
            // The largest key in the file at mid is greater than or equal to the target, so
            // the set of files after mid are not interesting.
            right = mid;
        }
    }

    if right == files.len() {
        return None;
    }

    Some(right)
}
