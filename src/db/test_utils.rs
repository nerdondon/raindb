use super::*;

/// Get the number of files at the specified level.
pub(crate) fn num_files_at_level(db: &DB, level: usize) -> usize {
    db.get_descriptor(DatabaseDescriptor::NumFilesAtLevel(level))
        .unwrap()
        .parse::<usize>()
        .unwrap()
}
