# RainDB - Architecture and Design

## Goal

This document is geared more towards the specifics of RainDB than a discussion of LSM trees in
general--though there is a large amount of overlap. For more reading and references on LSM trees you
check out [my notes on my blog](https://https://www.nerdondon.com/posts/2021-08-13-lsm-trees/).

The goal of this project is to create a well-annoated codebase for a key-value store backed by an
LSM tree. In that vein, if anything is unclear and could use a comment please create an issue.

We do not aim to create a highly performant and production ready database off the bat. At least
starting out, this is more of a learning tool for those trying to get more familiar with database
concepts.

## Overview

The architecture of RainDB follows pretty closely to that of LevelDB and follows after that of
log-structured merge-tree (LSM tree) based storage systems in general. The system is comprised of
the following high-level components:

1. Memtable - Initially a skip list but will be swappable in the future with other data structures
1. SSTable (sorted string tables) - A durable storage format for values pushed out of memory
1. Write-ahead log (WAL) - A persistent log of operations

LSM trees are append-only structures where different kinds of storage are used for each tier of the
system. Data is written to the memory layer (memtable) and that initial data is pushed down to
slower storage mediums as more writes are received.

The state of the database is represented as a set of versions. This is to keep iterators valid and
to provide snapshotting capabilities. This state is kept in memory so snapshots should be used with
consideration of that and clients will need to ensure that they release a snapshot so that the
versioned state can be removed.

## Data Formats

### Internal key format (a.k.a.) the lookup key

Because the data structures that make up an LSM tree are append-only, duplicate keys can be
introduced when updates occur. In order to ensure that the most up to date version of a key can be
found, a sequence number is associated with each write operation (i.e. puts and deletes). This a
global, monotonically increasing 64-bit unsigned integer. This number is never reset for the
lifetime of the database. Also, due to the store's append-only nature, a flag must be used to denote
a put operation or a deletion. This can also be referred to as a tombstone and is represented by an
8-bit unsigned integer. In sum the internal key looks as below:

```rust
struct LookupKey {
    user_key: Vec<u8>,
    sequence_number: u64,
    operation: Operation,
}
```

Currently, RainDB uses the `bincode` crate to serialize this struct into binary. We opt to use a
fixed length encoding for the sequence number just for legacy reasons. In LevelDB, the sequence
number is a 56-bit uint and the operation is represented by 8-bits. LevelDB encodes these fields
together to add up to a single 64-bit block.

### Write-ahead log (WAL) format

Write's are always done to the memtable. Because the memtable resides in memory, we need a more
durable structure to persist operations in the event of a crash. As is traditional in database
systems, we utilize a write-ahead log to ensure the durability of operations.

The WAL consists of a series of 32KiB blocks. For each block, RainDB has a 3 byte header that
consists of a 2 byte uint for the length of the data in the block and 1 byte to represent the block
record type. Together, a block in the WAL can be represented by this struct:

```rust
struct BlockRecord {
    length: u16,
    block_type: BlockType,
    data: Vec<u8>,
}
```

Block record types denote whether the data contained in the block is split across multiple blocks or
if they contain all of the data for a single user record. Regarding the splitting of the user data
into blocks, here is an excerpt from the
[LevelDB docs](https://github.com/google/leveldb/blob/c5d5174a66f02e66d8e30c21ff4761214d8e4d6d/doc/log_format.md)
since they are pretty clear about the topic:

> A record never starts within the last [2 bytes] of a block (since it won't fit). Any leftover
> bytes here form the trailer, which must consist entirely of zero bytes and must be skipped by
> readers.
>
> Aside: if exactly [three bytes] are left in the current block, and a new non-zero length record is
> added, the writer must emit a `FIRST` record (which contains zero bytes of user data) to fill up
> the trailing [three bytes] of the block and then emit all of the user data in subsequent blocks.

Block types are represented by this enum:

```rust
enum BlockType {
    /// Denotes that the block contains the entirety of a user record.
    Full = 0,
    /// Denotes the first fragment of a user record.
    First,
    /// Denotes the interior fragments of a user record.
    Middle,
    /// Denotes the last fragment of a user record.
    Last,
}
```

A block that is split into multiple fragments is represented by a first block tagged with
`BlockType::First`, any number of blocks tagged `BlockType::Middle`, and a final block tagged
`BlockType::Last`. The following example (also from the LevelDB docs) provides a concrete example of
how a sequence of records are split into blocks.

Consider a sequence of user records:

```plain
A: length 1000
B: length 97270
C: length 8000
```

**A** will be stored as a `Full` record in the first block.

**B** will be split into three fragments: first fragment occupies the rest of the first block,
second fragment occupies the entirety of the second block, and the third fragment occupies a prefix
of the third block. This will leave six bytes free in the third block, which will be left empty as
the trailer.

**C** will be stored as a `Full` record in the fourth block.

NOTE: Any corruption in the WAL is logged but ignored. LevelDB has a setting for "paranoid checks"
that will raise an error when corruption is detected, but RainDB does not currently have this
setting. The erroroneous WAL will be preserved for investigation but the database process will
start.

### Table file format (a.k.a. sorted string table a.k.a. SSTable)

Table files are maps from binary strings to binary strings that durably persist data to disk. The
table format follows exactly from LevelDB.

```text
<beginning_of_file>
[data block 1]
[data block 2]
...
[data block N]
[meta block 1]
...
[meta block K]
[metaindex block]
[index block]
[Footer (fixed size; starts at file_size - sizeof(Footer))]
<end_of_file>
```

The file contains internal pointers called block handles. This is a structure composed of two
values:

1. An offset to a block in the file encoded as a varint64
2. The size of the block also encoded as a varint64.

The maximum size of a varint64 is 10 bytes. This is important to keep in mind in order to have a
fixed sized footer at the end of the table files. To read more about variable length integers you
can refer to the
[protobuf documentation](https://developers.google.com/protocol-buffers/docs/encoding#varints) or
[this helpful blog post](https://carlmastrangelo.com/blog/lets-make-a-varint).

#### Table File Sections

The key-value pairs in the file are stored in sorted order and partitioned into a sequence of data
blocks. These data blocks are prefix-compressed and the entire block is further compressed by a
compression algorithm (e.g. Snappy).

The meta blocks contain auxiliary information (e.g. a Bloom filter) to provide stats and improve
access to the data.

The metaindex block contains an entry for every meta block where the key is the name of the meta
block and the value is a block handle.

The index block contains an entry for each data block where the key is a string >= the last key in
that data block and before the first key of the next data block. The value is a block handle to the
data block.

At the end of the file there is a fixed size footer. Currently, the footer is 48 bytes long. A table
file's footer consists of the following parts:

- A block handle for the metaindex.
- A block handle for the index.
- 40 zeroed bytes - (size of metaindex block handle) - (size of index block handle) to make the
  footer a fixed length
  - 40 comes from 2 \* 10 bytes (the max size of varint64)
- 8-byte magic number
  - This is just a random quirk and could be zeros. In LevelDB this magic number was picked by
    running `echo http://code.google.com/p/leveldb/ | sha1sum` and taking the leading 64 bits.

#### Data block format

Blocks stores entries of key-value pairs and some metadata related to the compression of the keys.
Block entries are serialized in the following format:

```rust
struct BlockEntry {
    shared_bytes: varint32,
    unshared_bytes: varint32,
    value_length: varint32,
    key_delta: Vec<u8>, // This is a buffer the size of `unshared_bytes`
    value: Vec<u8>,

    // Trailer
    restart_points: Vec<u32> // This array is the size of `num_restarts`
    num_restart_points: u32,
}
```

When a key is stored, the prefix shared with the key of a previous key is dropped. This is to help
reduce disk usage of table files. Once every 16 keys, the prefix compression is not applied and the
full key is stored. This point is called a restart point. The 16 key number comes from LevelDB and
I'm not sure why it was chosen yet. The number of restart points that a block has is based on the
size of the file. The restart points are used to do a binary search for a key and a scan is done for
the prefixed sections in order to improve lookup times.

`shared_bytes` will be zero if there are no restart points. `restart_points[i]` contains the offset
within the block of the `i`th restart point.

#### Meta block types

## Operations

### Writes

The write path is as follows:

1. Write to the write-ahead log for durability
1. Write to the memtable
1. If the memtable memory usage hits a defined threshold (defaults to 4 MiB), then a compaction
   operation is triggered

### Read

The read path is as follows:

1. Consult the memtable to see if it contains the value for the provided key.
1. Consult the immutable memtable (this is an old memtable currently undergoing the compaction
   process)
1. Check the MANIFEST file for the key ranges covered by each level of SSTables
1. Check each level to see if it has a value for the key. If it does return.

## Compactions
