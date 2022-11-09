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

/// Get the total number of table files that make up the database.
pub(crate) fn total_table_files(db: &DB) -> usize {
    let mut total: usize = 0;
    for level in 0..MAX_NUM_LEVELS {
        total += num_files_at_level(db, level);
    }

    total
}

/**
Do `n` memtable compactions, each of which produces a table file covering the provided key range.
*/
pub(crate) fn make_tables(db: &DB, n: usize, start_user_key: &[u8], end_user_key: &[u8]) {
    for _ in 0..n {
        db.put(
            WriteOptions::default(),
            start_user_key.into(),
            "begin".into(),
        )
        .unwrap();
        db.put(WriteOptions::default(), end_user_key.into(), "end".into())
            .unwrap();
        db.force_memtable_compaction().unwrap();
    }
}

/**
Fill every level of the database with a table file that covers the provided range.

This method is to prevent compactions from pushing produced table files into deeper levels.
*/
pub(crate) fn fill_levels(db: &DB, start_user_key: &[u8], end_user_key: &[u8]) {
    make_tables(db, MAX_NUM_LEVELS, start_user_key, end_user_key);
}

/// Return a vector where each index contains the number of table files at that level.
pub(crate) fn num_files_per_level(db: &DB) -> Vec<usize> {
    let mut num_files_per_level: Vec<usize> = vec![0; MAX_NUM_LEVELS];
    for level in 0..MAX_NUM_LEVELS {
        num_files_per_level[level] = num_files_at_level(db, level);
    }

    num_files_per_level
}

pub(crate) fn get_and_validate_database_contents(db: &DB) -> Vec<(Vec<u8>, Vec<u8>)> {
    let mut iter = db.new_iterator(ReadOptions::default()).unwrap();
    let mut db_contents: Vec<(Vec<u8>, Vec<u8>)> = vec![];

    iter.seek_to_first().unwrap();
    while iter.is_valid() {
        let (curr_key, curr_val) = iter.current().unwrap();
        db_contents.push((curr_key.to_vec(), curr_val.to_vec()));
        iter.next();
    }

    // Check that reverse iteration results are the reverse of forward results
    let mut matched: usize = 0;
    iter.seek_to_last().unwrap();
    while iter.is_valid() {
        assert!(
            matched < db_contents.len(),
            "Reverse iteration is taking more iterations than expected. Expected {}; Got: \
            {matched}",
            db_contents.len()
        );
        let (curr_key, curr_val) = iter.current().unwrap();
        assert!(
            (curr_key.to_vec(), curr_val.to_vec()) == db_contents[db_contents.len() - matched - 1],
            "Expected current reverse iteration k-v to match the k-v retrieved during forward
            iteration."
        );

        matched += 1;
        iter.prev();
    }

    assert_eq!(matched, db_contents.len());

    db_contents
}
