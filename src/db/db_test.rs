use pretty_assertions::assert_eq;

use super::*;

fn setup() {
    let _ = env_logger::builder()
        // Include all events in tests
        .filter_level(log::LevelFilter::max())
        // Ensure events are captured by `cargo test`
        .is_test(true)
        // Ignore errors initializing the logger if tests race to configure it
        .try_init();
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
fn can_get_expected_values_with_snapshots() {
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
