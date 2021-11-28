/*!
This module contains the database versioning abstractions.

Like LevelDB, RainDB is represented by a set of versions and this set of version is maintained by
the [`VersionSet`] struct. Explicitly the most up to date version is tracked as the "current"
version and older versions may be kept around to provide a consistent view to live iterators.

Each [`Version`] keeps track of a set of table files per level.

# Concurrency

[`VersionSet`] and [`Version`] require external synchronization e.g. a mutex.

[`VersionSet`]: version_set::VersionSet
[`Version`]: version::Version
*/

mod errors;
mod file_metadata;
mod version;
mod version_set;
