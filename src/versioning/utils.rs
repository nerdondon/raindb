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

Returns the index of the file whose key range creates an upper bound on the target key (i.e.
its largest key is greater than or equal the target). Otherwise returns `None`.

# Invariants

The passed in `files` **must** be a sorted set and the files must store key ranges that do
not overlap with the key ranges in any other file.

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

    if left == files.len() {
        return None;
    }

    Some(left)
}

#[cfg(test)]
mod find_file_with_upper_bound_range_tests {
    use pretty_assertions::assert_eq;

    use crate::Operation;

    use super::*;

    #[test]
    fn returns_nothing_for_an_empty_list() {
        let files: Vec<Arc<FileMetadata>> = vec![];
        let actual = find_file_with_upper_bound_range(
            &files,
            &InternalKey::new(b"bobo".to_vec(), 30, Operation::Put),
        );

        assert_eq!(actual, None);
    }

    #[test]
    fn when_there_is_only_one_file_returns_the_index_of_the_file_if_it_exists() {
        let mut files: Vec<Arc<FileMetadata>> = vec![];
        insert_files_with_largest_keys(&mut files, vec![b"batmann".to_vec()]);

        assert_eq!(
            find_file_with_upper_bound_range(
                &files,
                &InternalKey::new(b"batmann".to_vec(), 30, Operation::Put),
            )
            .unwrap(),
            0
        );
        assert_eq!(
            find_file_with_upper_bound_range(
                &files,
                &InternalKey::new(b"bat".to_vec(), 30, Operation::Put),
            )
            .unwrap(),
            0,
            "The target is not a boundary but is included in a file's key range so return the \
            index of that file."
        );
        assert_eq!(
            find_file_with_upper_bound_range(
                &files,
                &InternalKey::new(b"bb".to_vec(), 30, Operation::Put),
            ),
            None,
            "The target key is larger than every other key so we return None since no file's range \
            bounds the target from above."
        );
    }

    #[test]
    fn when_there_are_multiple_files_returns_the_index_of_the_file_if_it_exists() {
        let mut files: Vec<Arc<FileMetadata>> = vec![];
        insert_files_with_largest_keys(
            &mut files,
            vec![
                b"batmann".to_vec(),
                b"blue".to_vec(),
                b"bobo".to_vec(),
                b"patty".to_vec(),
                b"robin".to_vec(),
            ],
        );

        assert_eq!(
            find_file_with_upper_bound_range(&files, &create_testing_key(b"apple".to_vec()))
                .unwrap(),
            0,
            "The target is not a boundary but is included in a file's key range so return the \
            index of that file."
        );
        assert_eq!(
            find_file_with_upper_bound_range(
                &files,
                &InternalKey::new(b"bat".to_vec(), 30, Operation::Put),
            )
            .unwrap(),
            0,
            "The target is not a boundary but is included in a file's key range so return the \
            index of that file."
        );
        assert_eq!(
            find_file_with_upper_bound_range(
                &files,
                &InternalKey::new(b"batmann".to_vec(), 30, Operation::Put),
            )
            .unwrap(),
            0
        );

        assert_eq!(
            find_file_with_upper_bound_range(&files, &create_testing_key(b"blue".to_vec()))
                .unwrap(),
            1
        );
        assert_eq!(
            find_file_with_upper_bound_range(
                &files,
                &InternalKey::new(b"bb".to_vec(), 30, Operation::Put),
            )
            .unwrap(),
            1,
            "The target is not a boundary but is included in a file's key range so return the \
            index of that file."
        );

        assert_eq!(
            find_file_with_upper_bound_range(&files, &create_testing_key(b"bobo".to_vec()))
                .unwrap(),
            2
        );
        assert_eq!(
            find_file_with_upper_bound_range(&files, &create_testing_key(b"bm".to_vec())).unwrap(),
            2,
            "The target is not a boundary but is included in a file's key range so return the \
            index of that file."
        );

        assert_eq!(
            find_file_with_upper_bound_range(&files, &create_testing_key(b"patty".to_vec()))
                .unwrap(),
            3
        );
        assert_eq!(
            find_file_with_upper_bound_range(&files, &create_testing_key(b"robin".to_vec()))
                .unwrap(),
            4
        );

        assert_eq!(
            find_file_with_upper_bound_range(&files, &create_testing_key(b"s".to_vec())),
            None,
            "The target key is larger than every other key so we return None since no file's range \
            bounds the target from above."
        );
    }

    /**
    Add files with the largest key set to [`InternalKey`]'s with the specified user keys to the
    provided metadata vector.
    */
    fn insert_files_with_largest_keys(files: &mut Vec<Arc<FileMetadata>>, user_keys: Vec<Vec<u8>>) {
        for user_key in user_keys.into_iter() {
            let mut file = FileMetadata::new(30);
            file.set_largest_key(Some(create_testing_key(user_key)));
            files.push(Arc::new(file));
        }
    }

    /// Create an [`InternalKey`] based on the provided user key for testing.
    fn create_testing_key(user_key: Vec<u8>) -> InternalKey {
        InternalKey::new(user_key, 30, Operation::Put)
    }
}

#[cfg(test)]
mod sum_file_sizes_tests {
    use pretty_assertions::assert_eq;

    use super::*;

    #[test]
    fn can_sum_objects_convertible_to_a_file_metadata_reference() {
        let mut files: Vec<Arc<FileMetadata>> = vec![];
        assert_eq!(sum_file_sizes(&files), 0);

        for idx in 0..5 {
            let mut file = FileMetadata::new(idx + 30);
            file.set_file_size(10);
            files.push(Arc::new(file));
        }

        assert_eq!(sum_file_sizes(&files), 50);
    }
}
