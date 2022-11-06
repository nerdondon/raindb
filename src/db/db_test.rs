use std::fs;
use std::path::Path;
use std::time::Duration;

use integer_encoding::FixedInt;
use pretty_assertions::assert_eq;

use crate::errors::DBIOError;
use crate::fs::{InMemoryFileSystem, TmpFileSystem};

use super::*;

const BASE_TESTING_DIR_NAME: &str = "testing_files/";

fn setup() {
    let _ = env_logger::builder()
        // Include all events in tests
        .filter_level(log::LevelFilter::max())
        // Ensure events are captured by `cargo test`
        .is_test(true)
        // Ignore errors initializing the logger if tests race to configure it
        .try_init();

    // Ensure that the base testing directory exists
    let base_path = Path::new(BASE_TESTING_DIR_NAME);
    if !base_path.exists() {
        fs::create_dir_all(&base_path).unwrap();
    };
}

#[test]
fn opening_a_new_database_with_create_if_missing_true_succeeds() {
    setup();

    let mut options = DbOptions::with_memory_env();
    options.create_if_missing = true;

    let maybe_db = DB::open(options);

    assert_eq!(
        maybe_db.is_ok(),
        true,
        "Expected to open the database but got an error: {}",
        maybe_db.err().unwrap()
    );
}

#[test]
fn opening_non_existent_database_with_create_if_missing_false_fails() {
    setup();

    let options = DbOptions::with_memory_env();

    assert!(DB::open(options).is_err());
}

#[test]
fn can_write_to_and_read_from_the_database() {
    setup();

    let mut options = DbOptions::with_memory_env();
    options.create_if_missing = true;
    let db = DB::open(options).unwrap();

    assert!(db
        .put(
            WriteOptions::default(),
            "batmann".as_bytes().to_vec(),
            "lab".as_bytes().to_vec(),
        )
        .is_ok());

    let actual_read = db
        .get(ReadOptions::default(), "batmann".as_bytes())
        .unwrap();
    assert_eq!(actual_read, "lab".as_bytes());

    assert_eq!(
        db.get(ReadOptions::default(), "Does not exist".as_bytes())
            .err()
            .unwrap(),
        RainDBError::KeyNotFound
    );
}

#[test]
fn can_delete_values_from_the_database() {
    let mut options = DbOptions::with_memory_env();
    options.create_if_missing = true;
    let db = DB::open(options).unwrap();

    assert!(db
        .put(
            WriteOptions::default(),
            "batmann".as_bytes().to_vec(),
            "lab".as_bytes().to_vec(),
        )
        .is_ok());

    let actual_read = db
        .get(ReadOptions::default(), "batmann".as_bytes())
        .unwrap();

    assert_eq!(actual_read, "lab".as_bytes());

    assert!(db
        .put(
            WriteOptions::default(),
            "batmann".as_bytes().to_vec(),
            "lab retriever".as_bytes().to_vec(),
        )
        .is_ok());

    let actual_read = db
        .get(ReadOptions::default(), "batmann".as_bytes())
        .unwrap();
    assert_eq!(actual_read, "lab retriever".as_bytes());

    assert!(db
        .delete(WriteOptions::default(), "batmann".as_bytes().to_vec())
        .is_ok());

    assert_eq!(
        db.get(ReadOptions::default(), "batmann".as_bytes())
            .err()
            .unwrap(),
        RainDBError::KeyNotFound
    );
}

#[test]
fn can_get_values_after_compaction_to_disk() {
    setup();

    let mut options = DbOptions::with_memory_env();
    options.create_if_missing = true;
    let db = DB::open(options).unwrap();
    db.put(
        WriteOptions::default(),
        "tums".as_bytes().to_vec(),
        "pom".as_bytes().to_vec(),
    )
    .unwrap();

    let actual_read = db.get(ReadOptions::default(), "tums".as_bytes()).unwrap();
    assert_eq!(actual_read, "pom".as_bytes());

    let compaction_result = db.force_memtable_compaction();
    assert!(
        compaction_result.is_ok(),
        "Error forcing compaction of the memtable: {}",
        compaction_result.err().unwrap(),
    );

    let actual_read = db.get(ReadOptions::default(), "tums".as_bytes()).unwrap();
    assert_eq!(actual_read, "pom".as_bytes());
}

#[test]
fn get_with_snapshot_returns_correct_value_even_after_compaction() {
    setup();

    let mut options = DbOptions::with_memory_env();
    options.create_if_missing = true;
    let db = DB::open(options).unwrap();
    db.put(
        WriteOptions::default(),
        "some key that i want to store".as_bytes().to_vec(),
        "v1".as_bytes().to_vec(),
    )
    .unwrap();
    let snapshot1 = db.get_snapshot();

    db.put(
        WriteOptions::default(),
        "some key that i want to store".as_bytes().to_vec(),
        "v2".as_bytes().to_vec(),
    )
    .unwrap();

    let actual_read = db
        .get(
            ReadOptions {
                snapshot: Some(snapshot1.clone()),
                ..ReadOptions::default()
            },
            "some key that i want to store".as_bytes(),
        )
        .unwrap();
    assert_eq!(actual_read, "v1".as_bytes());
    let actual_read = db
        .get(
            ReadOptions::default(),
            "some key that i want to store".as_bytes(),
        )
        .unwrap();
    assert_eq!(actual_read, "v2".as_bytes());

    // Make sure that snapshots remain valid even after a compaction
    let compaction_result = db.force_memtable_compaction();
    assert!(
        compaction_result.is_ok(),
        "Error forcing compaction of the memtable: {}",
        compaction_result.err().unwrap(),
    );

    let actual_read = db
        .get(
            ReadOptions {
                snapshot: Some(snapshot1.clone()),
                ..ReadOptions::default()
            },
            "some key that i want to store".as_bytes(),
        )
        .unwrap();
    assert_eq!(actual_read, "v1".as_bytes());
    let actual_read = db
        .get(
            ReadOptions::default(),
            "some key that i want to store".as_bytes(),
        )
        .unwrap();
    assert_eq!(actual_read, "v2".as_bytes());

    db.release_snapshot(snapshot1);
}

#[test]
fn can_get_multiple_snapshots_of_the_same_state() {
    setup();

    let mut options = DbOptions::with_memory_env();
    options.create_if_missing = true;
    let db = DB::open(options).unwrap();
    db.put(
        WriteOptions::default(),
        "some key that i want to store".into(),
        "v1".into(),
    )
    .unwrap();
    let snapshot1 = db.get_snapshot();
    let snapshot2 = db.get_snapshot();
    let snapshot3 = db.get_snapshot();

    db.put(
        WriteOptions::default(),
        "some key that i want to store".into(),
        "v2".into(),
    )
    .unwrap();

    let actual_read = db
        .get(
            ReadOptions::default(),
            "some key that i want to store".as_bytes(),
        )
        .unwrap();
    assert_eq!(actual_read, "v2".as_bytes());
    let actual_read = db
        .get(
            ReadOptions {
                snapshot: Some(snapshot1.clone()),
                ..ReadOptions::default()
            },
            "some key that i want to store".as_bytes(),
        )
        .unwrap();
    assert_eq!(actual_read, "v1".as_bytes());
    let actual_read = db
        .get(
            ReadOptions {
                snapshot: Some(snapshot2.clone()),
                ..ReadOptions::default()
            },
            "some key that i want to store".as_bytes(),
        )
        .unwrap();
    assert_eq!(actual_read, "v1".as_bytes());
    let actual_read = db
        .get(
            ReadOptions {
                snapshot: Some(snapshot3.clone()),
                ..ReadOptions::default()
            },
            "some key that i want to store".as_bytes(),
        )
        .unwrap();
    assert_eq!(actual_read, "v1".as_bytes());

    // Make sure that snapshots remain valid even after a compaction
    db.release_snapshot(snapshot1);
    let compaction_result = db.force_memtable_compaction();
    assert!(
        compaction_result.is_ok(),
        "Error forcing compaction of the memtable: {}",
        compaction_result.err().unwrap(),
    );

    let actual_read = db
        .get(
            ReadOptions::default(),
            "some key that i want to store".as_bytes(),
        )
        .unwrap();
    assert_eq!(actual_read, "v2".as_bytes());
    let actual_read = db
        .get(
            ReadOptions {
                snapshot: Some(snapshot2.clone()),
                ..ReadOptions::default()
            },
            "some key that i want to store".as_bytes(),
        )
        .unwrap();
    assert_eq!(actual_read, "v1".as_bytes());
    db.release_snapshot(snapshot2);

    let actual_read = db
        .get(
            ReadOptions {
                snapshot: Some(snapshot3.clone()),
                ..ReadOptions::default()
            },
            "some key that i want to store".as_bytes(),
        )
        .unwrap();
    assert_eq!(actual_read, "v1".as_bytes());

    db.release_snapshot(snapshot3);
}

#[test]
fn get_with_different_snapshots_returns_correct_value_for_each_captured_state() {
    setup();

    let mut options = DbOptions::with_memory_env();
    options.create_if_missing = true;
    let db = DB::open(options).unwrap();
    db.put(
        WriteOptions::default(),
        "key1".as_bytes().to_vec(),
        "v1".as_bytes().to_vec(),
    )
    .unwrap();
    let snapshot1 = db.get_snapshot();
    db.put(
        WriteOptions::default(),
        "key1".as_bytes().to_vec(),
        "v2".as_bytes().to_vec(),
    )
    .unwrap();
    let snapshot2 = db.get_snapshot();
    db.put(
        WriteOptions::default(),
        "key1".as_bytes().to_vec(),
        "v3".as_bytes().to_vec(),
    )
    .unwrap();
    let snapshot3 = db.get_snapshot();
    db.put(
        WriteOptions::default(),
        "key1".as_bytes().to_vec(),
        "v4".as_bytes().to_vec(),
    )
    .unwrap();

    let actual_read = db
        .get(
            ReadOptions {
                snapshot: Some(snapshot1.clone()),
                ..ReadOptions::default()
            },
            "key1".as_bytes(),
        )
        .unwrap();
    assert_eq!(actual_read, "v1".as_bytes());

    let actual_read = db
        .get(
            ReadOptions {
                snapshot: Some(snapshot2.clone()),
                ..ReadOptions::default()
            },
            "key1".as_bytes(),
        )
        .unwrap();
    assert_eq!(actual_read, "v2".as_bytes());

    let actual_read = db
        .get(
            ReadOptions {
                snapshot: Some(snapshot3.clone()),
                ..ReadOptions::default()
            },
            "key1".as_bytes(),
        )
        .unwrap();
    assert_eq!(actual_read, "v3".as_bytes());

    let actual_read = db.get(ReadOptions::default(), "key1".as_bytes()).unwrap();
    assert_eq!(actual_read, "v4".as_bytes());

    // Check values remain stable while releasing snapshots
    db.release_snapshot(snapshot3);
    let actual_read = db
        .get(
            ReadOptions {
                snapshot: Some(snapshot1.clone()),
                ..ReadOptions::default()
            },
            "key1".as_bytes(),
        )
        .unwrap();
    assert_eq!(actual_read, "v1".as_bytes());

    let actual_read = db
        .get(
            ReadOptions {
                snapshot: Some(snapshot2.clone()),
                ..ReadOptions::default()
            },
            "key1".as_bytes(),
        )
        .unwrap();
    assert_eq!(actual_read, "v2".as_bytes());

    let actual_read = db.get(ReadOptions::default(), "key1".as_bytes()).unwrap();
    assert_eq!(actual_read, "v4".as_bytes());

    db.release_snapshot(snapshot1);
    let actual_read = db
        .get(
            ReadOptions {
                snapshot: Some(snapshot2.clone()),
                ..ReadOptions::default()
            },
            "key1".as_bytes(),
        )
        .unwrap();
    assert_eq!(actual_read, "v2".as_bytes());

    let actual_read = db.get(ReadOptions::default(), "key1".as_bytes()).unwrap();
    assert_eq!(actual_read, "v4".as_bytes());

    db.release_snapshot(snapshot2);
    let actual_read = db.get(ReadOptions::default(), "key1".as_bytes()).unwrap();
    assert_eq!(actual_read, "v4".as_bytes());
}

#[test]
fn can_iterate_with_a_snapshot_of_empty_state() {
    setup();

    let mut options = DbOptions::with_memory_env();
    options.create_if_missing = true;
    let db = DB::open(options).unwrap();

    let empty_snapshot = db.get_snapshot();

    db.put(
        WriteOptions::default(),
        "thisissomerandomkey".into(),
        "v1".into(),
    )
    .unwrap();
    db.put(
        WriteOptions::default(),
        "thisissomerandomkey".into(),
        "v2".into(),
    )
    .unwrap();

    let mut iter = db
        .new_iterator(ReadOptions {
            snapshot: Some(empty_snapshot.clone()),
            ..Default::default()
        })
        .unwrap();
    let seek_result = iter.seek_to_first();
    assert!(
        seek_result.is_ok(),
        "Expected to be able to seek to the first element but got error: {}",
        seek_result.err().unwrap()
    );
    assert!(
        !iter.is_valid(),
        "Expected iterator over empty database to be invalid after seeking"
    );
    drop(iter);

    // Check stability of snapshot through compaction
    db.force_memtable_compaction().unwrap();

    let mut iter = db
        .new_iterator(ReadOptions {
            snapshot: Some(empty_snapshot.clone()),
            ..Default::default()
        })
        .unwrap();
    let seek_result = iter.seek_to_first();
    assert!(
        seek_result.is_ok(),
        "Expected to be able to seek to the first element but got error: {}",
        seek_result.err().unwrap()
    );
    assert!(
        !iter.is_valid(),
        "Expected iterator over empty database to be invalid after seeking"
    );

    db.release_snapshot(empty_snapshot);
}

#[test]
fn get_with_key_with_values_spread_over_multiple_levels_succeeds() {
    setup();

    let mut options = DbOptions::with_memory_env();
    options.create_if_missing = true;
    let db = DB::open(options).unwrap();

    db.put(WriteOptions::default(), "foo".into(), "v1".into())
        .unwrap();

    // Generate a table file by forcing a memtable compaction
    db.compact_range(Some("a".as_bytes())..Some("z".as_bytes()));

    let read_result = db.get(ReadOptions::default(), "foo".as_bytes()).unwrap();
    assert_eq!(
        read_result,
        "v1".as_bytes().to_vec(),
        "Expected to still be able to get the value after compaction"
    );

    db.put(WriteOptions::default(), "foo".into(), "v2".into())
        .unwrap();

    let read_result = db.get(ReadOptions::default(), "foo".as_bytes()).unwrap();
    assert_eq!(
        read_result,
        "v2".as_bytes().to_vec(),
        "Expected to be able to get newest version of the key in the memtable"
    );

    db.force_memtable_compaction().unwrap();

    let read_result = db.get(ReadOptions::default(), "foo".as_bytes()).unwrap();
    assert_eq!(
        read_result,
        "v2".as_bytes().to_vec(),
        "Expected to be able to get newest version of the key in the memtable"
    );
}

#[test]
fn get_with_multiple_table_files_in_a_non_level_zero_level_succeeds() {
    setup();

    let mut options = DbOptions::with_memory_env();
    options.create_if_missing = true;
    let db = DB::open(options).unwrap();

    // Force compactions to generate multiple files in a non-level 0 level
    db.put(WriteOptions::default(), "a".into(), "a".into())
        .unwrap();
    db.compact_range(Some("a".as_bytes())..Some("b".as_bytes()));

    db.put(WriteOptions::default(), "x".into(), "x".into())
        .unwrap();
    db.compact_range(Some("x".as_bytes())..Some("y".as_bytes()));

    db.put(WriteOptions::default(), "f".into(), "f".into())
        .unwrap();
    db.compact_range(Some("f".as_bytes())..Some("g".as_bytes()));

    let read_result = db.get(ReadOptions::default(), "a".as_bytes()).unwrap();
    assert_eq!(&read_result, "a".as_bytes());
    let read_result = db.get(ReadOptions::default(), "f".as_bytes()).unwrap();
    assert_eq!(&read_result, "f".as_bytes());
    let read_result = db.get(ReadOptions::default(), "x".as_bytes()).unwrap();
    assert_eq!(&read_result, "x".as_bytes());
}

#[test]
fn get_when_there_is_an_empty_level_between_relevant_files_seeks_trigger_compaction_at_the_younger_level(
) {
    /*
    Per the `GetEncountersEmptyLevel` test in LevelDB, we will generate the following setup of
    table files in levels:
    1. table file 1 in level 0
    2. empty level 1
    3. table file 2 in level 2
    After setup, perform a series of `get` calls that will trigger a compaction. The compaction
    should be triggered at level 0. If the compaction was triggered at level 1, this would be
    indicative of a bug.
    */

    setup();

    let mut options = DbOptions::with_memory_env();
    options.create_if_missing = true;
    let db = DB::open(options).unwrap();

    // Place table files in levels 0 and 2
    let mut num_compactions: usize = 0;
    while test_utils::num_files_at_level(&db, 0) == 0 || test_utils::num_files_at_level(&db, 2) == 0
    {
        assert!(
            num_compactions <= 100,
            "Could not put files in levels 0 and 2. Level 0 had {} files and level 2 had {} files",
            test_utils::num_files_at_level(&db, 0),
            test_utils::num_files_at_level(&db, 2)
        );

        num_compactions += 1;

        db.put(WriteOptions::default(), "a".into(), "a".into())
            .unwrap();
        db.put(WriteOptions::default(), "z".into(), "z".into())
            .unwrap();

        db.force_memtable_compaction().unwrap();
    }

    // Clear level 1 of any files by forcing a compaction of the level
    db.force_level_compaction(1, &(None..None));
    assert_eq!(test_utils::num_files_at_level(&db, 0), 1);
    assert_eq!(test_utils::num_files_at_level(&db, 1), 0);
    assert_eq!(test_utils::num_files_at_level(&db, 2), 1);

    // Do a bunch of reads to force a compaction of the level 0 file
    for _ in 0..1000 {
        assert_eq!(
            db.get(ReadOptions::default(), "missing".as_bytes())
                .err()
                .unwrap(),
            RainDBError::KeyNotFound
        );
    }

    // Wait for compactions to finish
    thread::sleep(Duration::from_millis(1000));

    assert_eq!(test_utils::num_files_at_level(&db, 0), 0);
}

#[test]
fn db_iter_with_empty_database_remains_invalid() {
    setup();

    let mut options = DbOptions::with_memory_env();
    options.create_if_missing = true;
    let db = DB::open(options).unwrap();
    let mut iter = db.new_iterator(ReadOptions::default()).unwrap();

    assert!(iter.seek_to_first().is_ok());
    assert!(!iter.is_valid());

    assert!(iter.seek_to_last().is_ok());
    assert!(!iter.is_valid());

    assert!(iter.seek(&"target".into()).is_ok());
    assert!(!iter.is_valid());
}

#[test]
fn db_iter_with_a_single_value_can_seek_randomly() {
    setup();

    let mut options = DbOptions::with_memory_env();
    options.create_if_missing = true;
    let db = DB::open(options).unwrap();
    db.put(WriteOptions::default(), "a".into(), "a".into())
        .unwrap();

    let mut iter = db.new_iterator(ReadOptions::default()).unwrap();

    assert!(iter.seek_to_first().is_ok());
    assert!(iter.is_valid());
    test_utils::assert_db_iterator_current_key_value(&iter, "a".as_bytes(), "a".as_bytes());
    assert!(
        iter.next().is_none(),
        "Expected that a `next` call at the single element would not return a value."
    );
    assert!(!iter.is_valid());

    assert!(iter.seek_to_last().is_ok());
    assert!(iter.is_valid());
    test_utils::assert_db_iterator_current_key_value(&iter, "a".as_bytes(), "a".as_bytes());

    assert!(
        iter.prev().is_none(),
        "Expected that a `prev` call at the single element should not return a value"
    );

    assert!(iter.seek(&"".into()).is_ok());
    assert!(iter.is_valid());
    test_utils::assert_db_iterator_current_key_value(&iter, "a".as_bytes(), "a".as_bytes());

    assert!(iter.seek(&"a".into()).is_ok());
    assert!(iter.is_valid());
    test_utils::assert_db_iterator_current_key_value(&iter, "a".as_bytes(), "a".as_bytes());

    assert!(iter.seek(&"b".into()).is_ok());
    assert!(!iter.is_valid());
}

#[test]
fn db_iter_with_multiple_values_can_seek_randomly() {
    setup();

    let mut options = DbOptions::with_memory_env();
    options.create_if_missing = true;
    let db = DB::open(options).unwrap();
    db.put(WriteOptions::default(), "a".into(), "a".into())
        .unwrap();
    db.put(WriteOptions::default(), "b".into(), "b".into())
        .unwrap();
    db.put(WriteOptions::default(), "c".into(), "c".into())
        .unwrap();

    let mut iter = db.new_iterator(ReadOptions::default()).unwrap();

    // Forward operations
    assert!(iter.seek_to_first().is_ok());
    assert!(iter.is_valid());
    test_utils::assert_db_iterator_current_key_value(&iter, "a".as_bytes(), "a".as_bytes());
    assert_eq!(iter.next().unwrap().0, "b".as_bytes());
    assert_eq!(iter.next().unwrap().0, "c".as_bytes());
    assert!(iter.next().is_none());
    assert!(!iter.is_valid());
    assert!(iter.seek_to_first().is_ok());
    assert!(iter.prev().is_none());
    assert!(!iter.is_valid());

    // Backward operations
    assert!(iter.seek_to_last().is_ok());
    assert!(iter.is_valid());
    test_utils::assert_db_iterator_current_key_value(&iter, "c".as_bytes(), "c".as_bytes());
    assert_eq!(iter.prev().unwrap().0, "b".as_bytes());
    assert_eq!(iter.prev().unwrap().0, "a".as_bytes());
    assert!(iter.prev().is_none());
    assert!(iter.seek_to_last().is_ok());
    assert!(iter.next().is_none());

    // Targeted seeks
    assert!(iter.seek(&"".into()).is_ok());
    assert!(iter.is_valid());
    test_utils::assert_db_iterator_current_key_value(&iter, "a".as_bytes(), "a".as_bytes());

    assert!(iter.seek(&"a".into()).is_ok());
    assert!(iter.is_valid());
    test_utils::assert_db_iterator_current_key_value(&iter, "a".as_bytes(), "a".as_bytes());

    assert!(iter.seek(&"az".into()).is_ok());
    assert!(iter.is_valid());
    test_utils::assert_db_iterator_current_key_value(&iter, "b".as_bytes(), "b".as_bytes());

    assert!(iter.seek(&"b".into()).is_ok());
    assert!(iter.is_valid());
    test_utils::assert_db_iterator_current_key_value(&iter, "b".as_bytes(), "b".as_bytes());

    assert!(iter.seek(&"z".into()).is_ok());
    assert!(!iter.is_valid());
}

#[test]
fn db_iter_with_multiple_values_can_switch_iteration_direction() {
    setup();

    let mut options = DbOptions::with_memory_env();
    options.create_if_missing = true;
    let db = DB::open(options).unwrap();
    db.put(WriteOptions::default(), "a".into(), "a".into())
        .unwrap();
    db.put(WriteOptions::default(), "b".into(), "b".into())
        .unwrap();
    db.put(WriteOptions::default(), "c".into(), "c".into())
        .unwrap();

    let mut iter = db.new_iterator(ReadOptions::default()).unwrap();

    // Switch from forward to reverse
    assert!(iter.seek_to_first().is_ok());
    assert!(iter.next().is_some());
    assert!(iter.next().is_some());
    assert!(iter.prev().is_some());
    test_utils::assert_db_iterator_current_key_value(&iter, "b".as_bytes(), "b".as_bytes());

    // Switch from reverse to forward
    assert!(iter.seek_to_last().is_ok());
    assert!(iter.prev().is_some());
    assert!(iter.prev().is_some());
    assert!(iter.next().is_some());
    test_utils::assert_db_iterator_current_key_value(&iter, "b".as_bytes(), "b".as_bytes());
}

#[test]
fn db_iter_with_multiple_values_remains_at_snapshot_even_after_database_updates() {
    setup();

    let mut options = DbOptions::with_memory_env();
    options.create_if_missing = true;
    let db = DB::open(options).unwrap();
    db.put(WriteOptions::default(), "a".into(), "a".into())
        .unwrap();
    db.put(WriteOptions::default(), "b".into(), "b".into())
        .unwrap();
    db.put(WriteOptions::default(), "c".into(), "c".into())
        .unwrap();

    // When not provided, a snapshot of the current database state should be implicitly acquired
    let mut iter = db.new_iterator(ReadOptions::default()).unwrap();

    // Do some random iterator operations
    assert!(iter.seek_to_first().is_ok());
    assert!(iter.next().is_some());
    assert!(iter.prev().is_some());
    assert!(iter.seek(&"b".as_bytes().to_vec()).is_ok());
    assert!(iter.is_valid());
    test_utils::assert_db_iterator_current_key_value(&iter, "b".as_bytes(), "b".as_bytes());

    // Perform various operations to change database state
    db.put(WriteOptions::default(), "a".into(), "a2".into())
        .unwrap();
    db.put(WriteOptions::default(), "aa".into(), "aa".into())
        .unwrap();
    db.put(WriteOptions::default(), "b".into(), "b2".into())
        .unwrap();
    db.put(WriteOptions::default(), "c".into(), "c2".into())
        .unwrap();
    db.delete(WriteOptions::default(), "b".into()).unwrap();

    // Assert iterator stability
    iter.seek_to_first().unwrap();
    test_utils::assert_db_iterator_current_key_value(&iter, "a".as_bytes(), "a".as_bytes());

    assert!(iter.next().is_some());
    test_utils::assert_db_iterator_current_key_value(&iter, "b".as_bytes(), "b".as_bytes());

    assert!(iter.next().is_some());
    test_utils::assert_db_iterator_current_key_value(&iter, "c".as_bytes(), "c".as_bytes());

    assert!(
        iter.next().is_none(),
        "Expected to be at the end of the database given the snapshot at iterator creation."
    );

    iter.seek_to_last().unwrap();
    test_utils::assert_db_iterator_current_key_value(&iter, "c".as_bytes(), "c".as_bytes());

    assert!(iter.prev().is_some());
    test_utils::assert_db_iterator_current_key_value(&iter, "b".as_bytes(), "b".as_bytes());

    assert!(iter.prev().is_some());
    test_utils::assert_db_iterator_current_key_value(&iter, "a".as_bytes(), "a".as_bytes());

    assert!(
        iter.prev().is_none(),
        "Expected to be at the start of the database given the snapshot at iterator creation."
    );
}

#[test]
fn db_iter_with_deleted_values_will_skip_deleted_values() {
    setup();

    let mut options = DbOptions::with_memory_env();
    options.create_if_missing = true;
    let db = DB::open(options).unwrap();
    db.put(WriteOptions::default(), "a".into(), "a".into())
        .unwrap();
    db.put(WriteOptions::default(), "b".into(), "b".into())
        .unwrap();
    db.put(WriteOptions::default(), "c".into(), "c".into())
        .unwrap();
    db.delete(WriteOptions::default(), "b".into()).unwrap();
    assert_eq!(
        db.get(ReadOptions::default(), "b".as_bytes())
            .err()
            .unwrap(),
        RainDBError::KeyNotFound
    );

    let mut iter = db.new_iterator(ReadOptions::default()).unwrap();

    iter.seek(&"c".into()).unwrap();
    test_utils::assert_db_iterator_current_key_value(&iter, "c".as_bytes(), "c".as_bytes());
    assert!(iter.prev().is_some());
    test_utils::assert_db_iterator_current_key_value(&iter, "a".as_bytes(), "a".as_bytes());
}

#[test]
fn db_iter_with_values_spread_across_levels_iterates_as_expected() {
    setup();

    let mut options = DbOptions::with_memory_env();
    options.create_if_missing = true;
    let db = DB::open(options).unwrap();
    db.put(WriteOptions::default(), "a".into(), "a".into())
        .unwrap();
    db.put(WriteOptions::default(), "b".into(), "b".into())
        .unwrap();
    db.put(WriteOptions::default(), "c".into(), "c".into())
        .unwrap();
    db.force_memtable_compaction().unwrap();
    db.delete(WriteOptions::default(), "b".into()).unwrap();
    assert_eq!(
        db.get(ReadOptions::default(), "b".as_bytes())
            .err()
            .unwrap(),
        RainDBError::KeyNotFound
    );

    let mut iter = db.new_iterator(ReadOptions::default()).unwrap();

    iter.seek(&"c".into()).unwrap();
    test_utils::assert_db_iterator_current_key_value(&iter, "c".as_bytes(), "c".as_bytes());
    assert!(iter.prev().is_some());
    test_utils::assert_db_iterator_current_key_value(&iter, "a".as_bytes(), "a".as_bytes());
    iter.seek(&"b".into()).unwrap();
    test_utils::assert_db_iterator_current_key_value(&iter, "c".as_bytes(), "c".as_bytes());
}

#[test]
fn db_iter_after_compactions_remains_at_initial_snapshot() {
    setup();

    let mut options = DbOptions::with_memory_env();
    options.create_if_missing = true;
    let db = DB::open(options).unwrap();
    db.put(WriteOptions::default(), "a".into(), "a".into())
        .unwrap();

    let mut iter = db.new_iterator(ReadOptions::default()).unwrap();

    // Write a bunch of values and force a compaction
    for key in 0..500_usize {
        db.put(
            WriteOptions::default(),
            key.encode_fixed_vec(),
            "v".repeat(1000).into_bytes(),
        )
        .unwrap();
    }
    db.put(WriteOptions::default(), "a".into(), "a2".into())
        .unwrap();
    db.compact_range(None..None);

    assert!(iter.seek_to_first().is_ok());
    assert!(iter.is_valid());
    test_utils::assert_db_iterator_current_key_value(&iter, "a".as_bytes(), "a".as_bytes());
    assert!(iter.next().is_none());
    assert!(!iter.is_valid());
}

#[test]
fn db_with_values_can_be_reopened() {
    setup();

    let mem_fs: Arc<dyn FileSystem> = Arc::new(InMemoryFileSystem::new());

    {
        let options = DbOptions {
            filesystem_provider: Arc::clone(&mem_fs),
            create_if_missing: true,
            ..DbOptions::default()
        };
        let db = DB::open(options).unwrap();
        db.put(WriteOptions::default(), "a".into(), "a".into())
            .unwrap();
        db.put(WriteOptions::default(), "b".into(), "b".into())
            .unwrap();
    }

    {
        let options = DbOptions {
            filesystem_provider: Arc::clone(&mem_fs),
            ..DbOptions::default()
        };
        let db = DB::open(options).unwrap();

        assert_eq!(
            db.get(ReadOptions::default(), "a".as_bytes()).unwrap(),
            "a".as_bytes()
        );
        assert_eq!(
            db.get(ReadOptions::default(), "b".as_bytes()).unwrap(),
            "b".as_bytes()
        );

        assert!(db
            .put(WriteOptions::default(), "a".into(), "a1".into())
            .is_ok());
        assert!(db
            .put(WriteOptions::default(), "c".into(), "c".into())
            .is_ok());
    }

    {
        let options = DbOptions {
            filesystem_provider: Arc::clone(&mem_fs),
            ..DbOptions::default()
        };
        let db = DB::open(options).unwrap();

        assert_eq!(
            db.get(ReadOptions::default(), "a".as_bytes()).unwrap(),
            "a1".as_bytes()
        );

        assert!(db
            .put(WriteOptions::default(), "a".into(), "a2".into())
            .is_ok());
        assert_eq!(
            db.get(ReadOptions::default(), "a".as_bytes()).unwrap(),
            "a2".as_bytes()
        );

        assert_eq!(
            db.get(ReadOptions::default(), "b".as_bytes()).unwrap(),
            "b".as_bytes()
        );
        assert_eq!(
            db.get(ReadOptions::default(), "c".as_bytes()).unwrap(),
            "c".as_bytes()
        );
    }
}

#[test]
fn db_while_undergoing_a_minor_compaction_can_be_reopened() {
    setup();

    const MEMTABLE_MAX_SIZE_BYTES: usize = 1_000_000;

    let mem_fs: Arc<dyn FileSystem> = Arc::new(InMemoryFileSystem::new());
    let test_options = DbOptions {
        filesystem_provider: Arc::clone(&mem_fs),
        create_if_missing: true,
        max_memtable_size: MEMTABLE_MAX_SIZE_BYTES,
        ..DbOptions::default()
    };

    {
        let db = DB::open(test_options.clone()).unwrap();
        // Initial value in memtable
        db.put(WriteOptions::default(), "initial".into(), "initial".into())
            .unwrap();

        // Fill memtable
        db.put(
            WriteOptions::default(),
            "large".into(),
            "l".repeat(MEMTABLE_MAX_SIZE_BYTES).into_bytes(),
        )
        .unwrap();

        // Force compaction with write
        db.put(
            WriteOptions::default(),
            "large2".into(),
            "z".repeat(1000).into_bytes(),
        )
        .unwrap();

        // Put value into new memtable
        db.put(
            WriteOptions::default(),
            "second_memtable".into(),
            "second_memtable".into(),
        )
        .unwrap();
    }

    {
        let db = DB::open(test_options).unwrap();

        assert_eq!(
            db.get(ReadOptions::default(), "initial".as_bytes())
                .unwrap(),
            "initial".as_bytes()
        );
        assert_eq!(
            db.get(ReadOptions::default(), "second_memtable".as_bytes())
                .unwrap(),
            "second_memtable".as_bytes()
        );

        let read_result = db.get(ReadOptions::default(), "large".as_bytes()).unwrap();
        assert!(
            read_result == "l".repeat(MEMTABLE_MAX_SIZE_BYTES).as_bytes(),
            "Expected to find slice of {MEMTABLE_MAX_SIZE_BYTES} l's but got {} l's",
            read_result.len()
        );

        let read_result = db.get(ReadOptions::default(), "large2".as_bytes()).unwrap();
        assert!(
            read_result == "z".repeat(1000).as_bytes(),
            "Expected to find slice of 1000 z's but got {} z's",
            read_result.len()
        );
    }
}

#[test]
fn database_cannot_be_reopened_if_it_is_already_open_elsewhere() {
    setup();

    let tmp_fs_root = PathBuf::from(BASE_TESTING_DIR_NAME);
    let tmp_fs = TmpFileSystem::new(Some(&tmp_fs_root));
    let db_path = tmp_fs.get_root_path().join("destroy_me");
    let shared_tmp_fs: Arc<dyn FileSystem> = Arc::new(tmp_fs);
    let _db = DB::open(DbOptions {
        filesystem_provider: Arc::clone(&shared_tmp_fs),
        create_if_missing: true,
        db_path: db_path.to_str().unwrap().to_owned(),
        ..DbOptions::default()
    })
    .unwrap();

    let maybe_db2 = DB::open(DbOptions {
        filesystem_provider: Arc::clone(&shared_tmp_fs),
        create_if_missing: true,
        db_path: db_path.to_str().unwrap().to_owned(),
        ..DbOptions::default()
    });
    assert_eq!(
        maybe_db2.err().unwrap(),
        RainDBError::IO(DBIOError::new(
            io::ErrorKind::WouldBlock,
            "Resource temporarily unavailable (os error 11)".to_owned()
        )),
        "Expected there to be an error acquiring a lock on the database"
    );
}

#[test]
fn db_when_the_memtable_gets_filled_up_triggers_a_minor_compaction() {
    setup();

    const MEMTABLE_MAX_SIZE_BYTES: usize = 10_000;

    let mem_fs: Arc<dyn FileSystem> = Arc::new(InMemoryFileSystem::new());

    {
        let db = DB::open(DbOptions {
            filesystem_provider: Arc::clone(&mem_fs),
            create_if_missing: true,
            max_memtable_size: MEMTABLE_MAX_SIZE_BYTES,
            ..DbOptions::default()
        })
        .unwrap();
        let starting_num_table_files = test_utils::total_table_files(&db);
        for key in 0..500_usize {
            db.put(
                WriteOptions::default(),
                key.encode_fixed_vec(),
                "v".repeat(1000).into_bytes(),
            )
            .unwrap();
        }

        let ending_num_table_files = test_utils::total_table_files(&db);
        assert!(ending_num_table_files > starting_num_table_files);

        // Ensure that we can still read the values after compaction
        for key in 0..500_usize {
            assert_eq!(
                db.get(ReadOptions::default(), &key.encode_fixed_vec())
                    .unwrap(),
                "v".repeat(1000).into_bytes()
            );
        }
    }

    {
        // Ensure that we can still read the values after reopening the database
        let db = DB::open(DbOptions {
            filesystem_provider: Arc::clone(&mem_fs),
            create_if_missing: true,
            max_memtable_size: MEMTABLE_MAX_SIZE_BYTES,
            ..DbOptions::default()
        })
        .unwrap();

        for key in 0..500_usize {
            assert_eq!(
                db.get(ReadOptions::default(), &key.encode_fixed_vec())
                    .unwrap(),
                "v".repeat(1000).into_bytes()
            );
        }
    }
}

#[test]
fn db_when_sizing_down_the_memtable_size_can_reopen_from_larger_wal_file() {
    setup();

    let mem_fs: Arc<dyn FileSystem> = Arc::new(InMemoryFileSystem::new());

    {
        let db = DB::open(DbOptions {
            filesystem_provider: Arc::clone(&mem_fs),
            create_if_missing: true,
            ..DbOptions::default()
        })
        .unwrap();
        db.put(
            WriteOptions::default(),
            "big1".into(),
            "1".repeat(200_000).into_bytes(),
        )
        .unwrap();
        db.put(
            WriteOptions::default(),
            "big2".into(),
            "2".repeat(200_000).into_bytes(),
        )
        .unwrap();
        db.put(
            WriteOptions::default(),
            "small3".into(),
            "3".repeat(10).into_bytes(),
        )
        .unwrap();
        db.put(
            WriteOptions::default(),
            "small4".into(),
            "4".repeat(10).into_bytes(),
        )
        .unwrap();

        assert_eq!(test_utils::num_files_at_level(&db, 0), 0);
    }

    {
        // Ensure that we can still read the values after reopening the database
        let db = DB::open(DbOptions {
            filesystem_provider: Arc::clone(&mem_fs),
            create_if_missing: true,
            max_memtable_size: 100_000,
            ..DbOptions::default()
        })
        .unwrap();

        assert_eq!(test_utils::num_files_at_level(&db, 0), 3);

        let read_result = db.get(ReadOptions::default(), "big1".as_bytes()).unwrap();
        assert!(
            read_result == "1".repeat(200_000).as_bytes(),
            "Expected to find slice of 200,000 1's but got {} 1's",
            read_result.len()
        );

        let read_result = db.get(ReadOptions::default(), "big2".as_bytes()).unwrap();
        assert!(
            read_result == "2".repeat(200_000).as_bytes(),
            "Expected to find slice of 200,000 2's but got {} 2's",
            read_result.len()
        );

        let read_result = db.get(ReadOptions::default(), "small3".as_bytes()).unwrap();
        assert!(
            read_result == "3".repeat(10).as_bytes(),
            "Expected to find slice of 10 3's but got {} 3's",
            read_result.len()
        );

        let read_result = db.get(ReadOptions::default(), "small4".as_bytes()).unwrap();
        assert!(
            read_result == "4".repeat(10).as_bytes(),
            "Expected to find slice of 10 4's but got {} 4's",
            read_result.len()
        );
    }
}

#[test]
fn manual_compactions_work_as_expected() {
    setup();

    let mut options = DbOptions::with_memory_env();
    options.create_if_missing = true;
    let db = DB::open(options).unwrap();

    test_utils::make_tables(&db, 3, "p".as_bytes(), "q".as_bytes());
    assert_eq!(
        vec![1, 1, 1, 0, 0, 0, 0],
        test_utils::num_files_per_level(&db)
    );

    // Compaction of key range before the full database key range should not do anything
    db.compact_range(Some("".as_bytes())..Some("c".as_bytes()));
    assert_eq!(
        vec![1, 1, 1, 0, 0, 0, 0],
        test_utils::num_files_per_level(&db)
    );

    // Compaction of key range after the full database key range should not do anything
    db.compact_range(Some("r".as_bytes())..Some("z".as_bytes()));
    assert_eq!(
        vec![1, 1, 1, 0, 0, 0, 0],
        test_utils::num_files_per_level(&db)
    );

    // Compaction of key range overlapping the table files will compact them
    db.compact_range(Some("p1".as_bytes())..Some("p9".as_bytes()));
    assert_eq!(
        vec![0, 0, 1, 0, 0, 0, 0],
        test_utils::num_files_per_level(&db)
    );

    // Create another set of files with a different range
    test_utils::make_tables(&db, 3, "c".as_bytes(), "e".as_bytes());
    assert_eq!(
        vec![1, 1, 2, 0, 0, 0, 0],
        test_utils::num_files_per_level(&db)
    );

    // Compact the new range
    db.compact_range(Some("b".as_bytes())..Some("f".as_bytes()));
    assert_eq!(
        vec![0, 0, 2, 0, 0, 0, 0],
        test_utils::num_files_per_level(&db)
    );

    // Compact everything
    test_utils::make_tables(&db, 1, "a".as_bytes(), "z".as_bytes());
    assert_eq!(
        vec![0, 1, 2, 0, 0, 0, 0],
        test_utils::num_files_per_level(&db)
    );

    db.compact_range(None..None);
    assert_eq!(
        vec![0, 0, 1, 0, 0, 0, 0],
        test_utils::num_files_per_level(&db)
    );
}

#[cfg(feature = "large_tests")]
#[test]
fn compactions_do_not_create_files_with_excessive_overlap() {
    // This is the same as LevelDB's `SparseMerge` test to ensure that there isn't excessive overlap
    // of files between levels. Per LevelDB:
    //
    // ```
    // Suppose there is:
    //    small amount of data with prefix A
    //    large amount of data with prefix B
    //    small amount of data with prefix C
    // and that recent updates have made small changes to all three prefixes.
    // Check that we do not do a compaction that merges all of B in one shot.
    // ```
    //
    // If all of the `B` prefixed values are mereged together, there would be a large amout of
    // overlap for files between levels.
    setup();

    let mut options = DbOptions::with_memory_env();
    options.create_if_missing = true;
    let db = DB::open(options).unwrap();

    test_utils::fill_levels(&db, "a".as_bytes(), "z".as_bytes());
    db.put(WriteOptions::default(), "a".into(), "a".into())
        .unwrap();

    // Write approximately 100MB of "B" prefixed values
    for idx in 0..100_000_usize {
        let key = format!("b{idx:010}");
        db.put(WriteOptions::default(), key.into(), "x".repeat(1000).into())
            .unwrap();
    }

    db.put(WriteOptions::default(), "c".into(), "c".into())
        .unwrap();
    db.force_memtable_compaction().unwrap();
    db.force_level_compaction(0, &(None..None));

    // Make sparse updates
    db.put(WriteOptions::default(), "a".into(), "a2".into())
        .unwrap();
    db.put(WriteOptions::default(), "b100".into(), "b2".into())
        .unwrap();
    db.put(WriteOptions::default(), "c".into(), "c2".into())
        .unwrap();
    db.force_memtable_compaction().unwrap();

    // Compactions should not create files that overlap too much data at the next level
    /// Default max overlapping bytes for level 1 is 20 MB
    /// (see `[raindb::compaction:utils::max_grandparent_overlap_bytes_from_options]`)
    const MAX_OVERLAPPING_BYTES: u64 = 20 * 1024 * 1024;
    assert!(db.max_next_level_overlapping_bytes() < MAX_OVERLAPPING_BYTES);

    db.force_level_compaction(0, &(None..None));
    assert!(db.max_next_level_overlapping_bytes() < MAX_OVERLAPPING_BYTES);

    db.force_level_compaction(1, &(None..None));
    assert!(db.max_next_level_overlapping_bytes() < MAX_OVERLAPPING_BYTES);
}
