# RainDB

A persistent key-value store based on an LSM tree implemented in Rust

[![Build Status](https://github.com/nerdondon/raindb/actions/workflows/ci.yaml/badge.svg)](https://github.com/nerdondon/raindb/actions/workflows/ci.yaml)

## Motivation and goals

This project serves more as a toy project and tool for learning about database internals. As such,
the code will be annotated as much as possible. We aim to achieve high readability via well
structured code, eschew "elegant" tricks, and by having redundant documentation. Concepts and
reasoning are expounded upon in the design docs, the generated API docs, and inline with the code.
Whether your best mode of learning is through reading code or through reading docs, information is
often repeated in multiple areas to make following along easier.

RainDB **does not** aim to be high performance nor aim to have binary compatibility with LevelDB.
These things would be nice side-effects though 😀. Despite not aiming for binary compatibility, the
behaviors are largely the same (i.e. a direct port) and the same test cases from LevelDB are used to
check this conformance.

## High Level Design

RainDB's storage engine is an LSM tree and has/will have the traditional components that come from
the fact:

1. Memtable - Initially a skip list but will be swappable in the future with other data structures
1. Table files
1. Write-ahead log

As can be seen from the list above, RainDB's LSM tree is a two-layer system with an in-memory
memtable and table files persistable to a single storage medium. The table file store can be on disk
but an in-memory store is also provided (and usually used for testing purposes).

See the [docs](./docs) folder for more information on architecture and design.

## Usage

```rust
use raindb::{DbOptions, ReadOptions, WriteOptions, DB};

// Use the default options to create a database that uses disk to store table files
let options = DbOptions {
    create_if_missing: true,
    ..DbOptions::default()
};

// Optionally handle any errors that can occur when opening the database
let db = DB::open(options).unwrap();

db.put(
    WriteOptions::default(),
    "some_key".into(),
    "some_value".into(),
).unwrap();

let read_result = db.get(ReadOptions::default(), "some_key".as_bytes()).unwrap();
```

### Using an in-memory store

```rust
use raindb::{DbOptions, DB};
use raindb::fs::{FileSystem, InMemoryFileSystem};

let options = DbOptions {
    create_if_missing: true,
    ..DbOptions::with_memory_env()
};

// OR
let mem_fs: Arc<dyn FileSystem> = Arc::new(InMemoryFileSystem::new());
let options = DbOptions {
    filesystem_provider: Arc::clone(&mem_fs),
    create_if_missing: true,
    ..DbOptions::default()
};

let db = DB::open(options).unwrap();
```

### Other usage examples

The [examples](./examples) folder has some examples of how to embed RainDB in an application. I
usually hate having to look through source code for examples but the
[`db/db_test.rs`](./src/db/db_test.rs) module contains a lot of example usage of the public API.

## Pedigree

The work here builds heavily on the existing work of [LevelDB](https://github.com/google/leveldb). A
big thank you goes to the creators for making so much of their design documentation available
alongside the code itself. Because of the heavy basis in this project, some of the options and
documentation is pulled directly from it.

Inspiration was also drawn from the [Go port of LevelDB](https://github.com/golang/leveldb) and
[dermesser's Rust port](https://github.com/dermesser/leveldb-rs).

A `Legacy` heading will be present in doc comments where tactical decisions were made to differ from
LevelDB. This can include name changes so that parallels can still be drawn between RainDB and
LevelDB.

A lot of the tests are also taken from LevelDB to ensure behavioral fidelity.

### Other art

- [RocksDB](https://github.com/facebook/rocksdb)
- [Pebble (Go)](https://github.com/cockroachdb/pebble)
- [kezhuw/leveldb (Go)](https://github.com/kezhuw/leveldb)

## Future ideas

- Use as part of a distributed store with Raft for leader election
  - Add data sharding

## References

- Aside from the docs folder, more notes and references for various components can also be found on
  [my blog](https://blog.nerdondon.com)
- Database Internals by Alex Petrov
- [Oren Eini - Blog Series - Reviewing LevelDB](https://ayende.com/blog/posts/series/161410/reviewing-leveldb)
- [MrCroxx - Blog Series](https://mrcroxx.github.io/categories/%E6%B7%B1%E5%85%A5%E6%B5%85%E5%87%BAleveldb/)
  - This is in Chinese but Google translate actually does a good job here. Only skimmed a bit of the
    compaction article but it was very well done and seems like a great learning resource for
    understanding LevelDB.

## Disclaimer

This is not an officially supported Google product.
