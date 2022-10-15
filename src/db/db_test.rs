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
