use pretty_assertions::assert_eq;

use super::*;

/// Get the number of files at the specified level.
pub(crate) fn num_files_at_level(db: &DB, level: usize) -> usize {
    db.get_descriptor(DatabaseDescriptor::NumFilesAtLevel(level))
        .unwrap()
        .parse::<usize>()
        .unwrap()
}

/// Assert that the iterator's current key and value are the expected key and value.
pub(crate) fn assert_db_iterator_current_key_value(
    iter: &DatabaseIterator,
    expected_key: &[u8],
    expected_value: &[u8],
) {
    let (curr_key, curr_val) = iter.current().unwrap();
    assert_eq!(curr_key, expected_key);
    assert_eq!(curr_val, expected_value);
}
