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

pub mod errors;

pub(crate) mod version_set;
pub(crate) use version_set::VersionSet;

pub(crate) mod version_manifest;
pub(crate) use version_manifest::VersionChangeManifest;

pub(crate) mod file_metadata;
pub(crate) mod utils;
pub(crate) mod version;
