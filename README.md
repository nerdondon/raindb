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
1. SSTables (sorted string tables)
1. Write-ahead log

See the [docs](./docs) folder for more information on architecture and design.

## Pedigree

The work here builds heavily on the existing work of [LevelDB](https://github.com/google/leveldb). A
big thank you goes to the creators for making so much of their design documentation available along
side the code itself. Because of the heavy basis in these projects, some of the options and
documentation is pulled from these projects.

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
