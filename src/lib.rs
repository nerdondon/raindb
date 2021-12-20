/*!
RainDB is a key-value store that is based on [LevelDB]. This project aims to be a learning tool for
those looking to take a deep dive on databases. It strives to uphold a high standard of code clarity
and to have extensive documentation on design and intentions of code. With regard to this, we have
configured the project such that `rustdoc` generates output even for private methods.

Please see the [RainDB repo] for more in-depth treatment on the design and operation of the
database.

[LevelDB]: https://github.com/google/leveldb
[RainDB repo]: https://github.com/nerdondon/raindb
*/

pub mod db;
pub use db::DB;

pub mod fs;

mod compaction;
mod config;
mod errors;
mod file_names;
mod key;
mod memtable;
mod table_cache;
mod tables;
mod utils;
mod versioning;
mod write_ahead_log;
mod writers;

mod batch;
pub use batch::{Batch, BatchElement};

pub mod filter_policy;
pub use filter_policy::{BloomFilterPolicy, FilterPolicy};

mod iterator;
pub use iterator::RainDbIterator;

pub mod options;
pub use options::{DbOptions, ReadOptions, WriteOptions};

pub use utils::cache::Cache;
