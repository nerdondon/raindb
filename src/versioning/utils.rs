//! Contains utilities used for various versioning operations.

use super::file_metadata::FileMetadata;

/// Sums the file sizes for the specified vector of file metadata.
pub(crate) fn sum_file_sizes<F: AsRef<FileMetadata>>(files: &[F]) -> u64 {
    files
        .iter()
        .map(|metadata| metadata.as_ref().get_file_size())
        .sum()
}
