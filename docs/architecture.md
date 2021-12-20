# RainDB - Architecture and Design

## Goal

This document is geared more towards the specifics of RainDB than a discussion of LSM trees in
general--though there is a large amount of overlap. For more reading and references on LSM trees you
check out [my notes on my blog](https://https://www.nerdondon.com/posts/2021-08-13-lsm-trees/).

The goal of this project is to create a well-annotated codebase for a key-value store backed by an
LSM tree. In that vein, if anything is unclear and could use a comment please create an issue. A lot
of information from this document is also inlined in code comments to make reading the code easier.
Conversely, some topics in this document have extra treatment in code comments with finer grain
details on intentions.

We do not aim to create a highly performant and production ready database off the bat...at least
starting out. Currently, this is more of a learning tool for those trying to get more familiar with
database concepts.

LevelDB tends to only keep around byte buffers instead of deserializing those buffers into structs.
LevelDB primarily works with this data using pointers to ranges within those buffers and doing
pointer arithmetic to read or manipulate the data. I am uncertain if this is a memory saving measure
(by reducing allocations), but it makes the LevelDB codebase pretty hard to follow. In our effort to
make things more readable, RainDB will take the opposite approach and eagerly deserialize into
intermediate structs. Explicitly, we will take a performance hit if it means the idea behind the
code can be made more obvious. Cases where this is evident are the following:

1. The memtable will store keys as a struct (`LookupKey`) instead of constantly serializing and
   deserializing byte buffers.
1. Reading blocks from tables ([details](#data-block-format))

As relates to LevelDB code structure, see this
[interesting aside](#appendix-a---leveldb-and-memory-management).

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

Due to the heavy basis of RainDB on LevelDB, this document contains some duplicated portions or
rephrased portions of
[LevelDB's own design documents](https://github.com/google/leveldb/tree/master/doc).

## Operations

### Writes

The write path is as follows:

1. Write to the write-ahead log for durability
1. Write to the memtable
1. If the memtable memory usage is at a defined threshold (defaults to 4 MiB), then a compaction
   operation is triggered

Write requests can be submitted by clients as single `Put`'s and `Delete`'s or in batches of a mix
of the two that will be committed atomically.

While RainDB supports multiple threads for writes and reads, write requests will be performed
serially. That is, if there are multiple threads that make a `Put` or a `Delete` request, the
threads will be placed in a queue to be processed in order. In order to improve write throughput,
RainDB will doing an additional level of grouping on top of the write batches mentioned above. As in
LevelDB, we call this extra level of grouping a group commit. The size of a group commit is limited
so as to not introduce too much additional latency.

If a write operation will cause a memtable to become full, the compaction process is kicked off. The
writing thread will check if there are already any ongoing compactions. If there are, the thread
will pause until the ongoing compaction finishes. If there is not an ongoing compaction, the current
writer thread will do the following:

1. Create a new write-ahead log to replace the current one backing the memtable to be compacted
1. Create a new memtable and make the current memtable immutable (i.e. move to another field)
1. Schedule a compaction
1. Pause the writing thread until the the compaction is finished
1. On waking back up, the writer will attempt to create a group commit batch up to a certain size
   limit in terms of bytes that will be written.
1. After creating the batch of operations that will be performed, the write is committed to the WAL
   and then applied to the memtable.

### Reads and sorted tables

Before getting into reads it is important to have some understanding of the tiered organization of
data within RainDB. To this effect we proceed first with an introduction sorted tables and then go
in to exposition on the read path.

#### Sorted tables

This is straight from
[LevelDB](https://github.com/google/leveldb/blob/master/doc/impl.md#sorted-tables) since it's so
well written already.

A [table file] stores a sequence of entries sorted by key. Each entry is either a value for the key,
or a deletion marker for the key. (Deletion markers are kept around to hide obsolete values present
in older sorted tables).

The set of sorted tables are organized into a sequence of levels. The sorted table generated from a
log file is placed in a special young level (also called level-0). When the number of young files
exceeds a certain threshold (currently four), all of the young files are merged together with all of
the overlapping level-1 files to produce a sequence of new level-1 files (we create a new level-1
file for every 2MB of data.)

Files in the young level may contain overlapping keys. However files in other levels have distinct
non-overlapping key ranges. Consider level number L where L >= 1. When the combined size of files in
level-L exceeds (10^L) MB (i.e., 10MB for level-1, 100MB for level-2, ...), one file in level-L, and
all of the overlapping files in level-(L+1) are merged to form a set of new files for level-(L+1).
These merges have the effect of gradually migrating new updates from the young level to the largest
level using only bulk reads and writes (i.e., minimizing expensive seeks).

#### Manifest files

Manifest files list the set of table files that make up each level, the corresponding key ranges,
and other important metadata. A new manifest file is created whenever the database is reopened. The
manifest file is append only. File removals are indicated by tombstones.

More information on the format of sorted table files can be found in the
[data formats section](#table-file-format).

#### Read path

The read path is as follows:

1. Consult the memtable to see if it contains the value for the provided key.
1. If it exists, consult the immutable memtable (this is an old memtable currently undergoing the
   compaction process)
1. Check the manifest file for the key ranges covered by each level of SSTables
1. Check each level to see if it has a value for the key. If it does return.

### Compactions

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

The sequence number will be serialized in a fixed length format and will be 64 bits unlike in
LevelDB. In LevelDB, the sequence number is a 56-bit uint and the operation is represented by
8-bits. LevelDB encodes these fields together to add up to a single 64-bit block.

### Write-ahead log (WAL) format

Write's are always done to the memtable. Because the memtable resides in memory, we need a more
durable structure to persist operations in the event of a crash. As is traditional in database
systems, we utilize a write-ahead log to ensure the durability of operations. When the write-ahead
log reaches a configured size, it is converted to a table file and a new WAL is created. This is
done as part of the compaction process.

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

### Table file format

Within LevelDB and less so in RainDB, table files can also be referred to as sorted string tables or
SSTables. For RainDB, we try to standardize on always referring to these as table files.

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
blocks. These data blocks have their keys prefix-compressed in order save space.

The meta blocks contain auxiliary information (e.g. a Bloom filter) to provide stats and improve
access to the data.

The metaindex block contains an entry for every meta block where the key is the name (a string) of
the meta block and the value is a block handle.

The index block contains an entry for each data block where the key is a internal lookup key and the
value is a block handle to the data block. Keys are sorted such that a key in the index is >= the
last key in that data block and less than the first key of the next data block.

Each of these blocks are optionally compressed. This information as well as a checksum are stored
after each block in what we will call the block descriptor. In LevelDB, this is confusingly called
the block trailer. This is confusing because LevelDB already has a concept of a block trailer
[within the block itself](https://github.com/google/leveldb/blob/c5d5174a66f02e66d8e30c21ff4761214d8e4d6d/table/block_builder.cc#L24-L27)--we
describe block format more below--and LevelDB also
[calls this descriptor the block trailer](https://github.com/google/leveldb/blob/c5d5174a66f02e66d8e30c21ff4761214d8e4d6d/table/format.h#L78-L79).
The block descriptor is serialized as follows:

```rust
struct BlockDescriptor {
    /// 1-byte enum representing the compression type of the block.
    CompressionType compression_type,

    /// 32-bit CRC
    crc: u32,
}
```

At the end of the file there is a fixed size footer. Currently, the footer is 48 bytes long. A table
file's footer consists of the following parts:

- A block handle for the metaindex.
- A block handle for the index.
- A series of zero bytes for padding. The amount of padding calculated in the following way: 40
  zeroed bytes - (size of metaindex block handle) - (size of index block handle)
  - The padding is to ensure that the footer is a fixed length
  - 40 comes from 2 \* 10 bytes (the max size of varint64)
- 8-byte magic number
  - This is just a random quirk and could be zeros. In LevelDB this magic number was picked by
    running `echo http://code.google.com/p/leveldb/ | sha1sum` and taking the leading 64 bits.

#### Data block format

Blocks store a sequence of entries of key-value pairs and some metadata related to the compression
of the keys. The sequence of block entries if followed by a trailer containing some block metadata.

Block entries are serialized in the following format:

```rust
struct BlockEntry {
    key_num_shared_bytes: varint32,
    key_num_unshared_bytes: varint32,
    value_length: varint32,
    key_delta: Vec<u8>, // This is a buffer the size of `key_num_unshared_bytes`
    value: Vec<u8>,
}
```

The block trailer is serialized in the following format:

```rust
struct BlockTrailer {
    restart_point_offsets: Vec<u32> // This array is the size of `num_restart_points`
    num_restart_points: u32,
}
```

`restart_point_offsets[i]` contains the offset within the block of the `i`th restart point.

When a key is stored, the prefix shared with the key of a previous key is dropped. This is to help
reduce disk usage of table files. Once every 16 keys, the prefix compression is not applied and the
full key is stored. This point is called a restart point. The 16 key number comes from LevelDB and
I'm not sure why it was chosen yet. The number of restart points that a block has is based on the
size of the block. `key_num_shared_bytes` will be zero for a restart point.

In LevelDB, the restart points are used to do a binary search for a range of keys sharing a prefix
and a scan over that range is done to fetch data for a specific key. This is done in order to
improve lookup times. LevelDB has a tendency to keep allocations to a minimum and only have
references to byte buffers. As relates to the restart points, this means that block entries are
lazily deserialized (e.g.
[when an iterator is seeking](https://github.com/google/leveldb/blob/c5d5174a66f02e66d8e30c21ff4761214d8e4d6d/table/block.cc#L187-L226))
and that prefix-compressed keys are lazily reconstructed. I do not know if saving allocations a goal
of LevelDB, but the information for deserialized block entries is not stored. If a block is not
fully iterated, I guess this is some work saved. If the block gets fully iterated or is iterated
multiple times, then the deserialization is repetitive.

An alternative is to just eat the cost of fully deserializing the entries of a block on
initialization. LevelDB already does a linear scan to parse entries within a section after the
restart point. The alternative approach just does a full scan upfront. On the whole, the difference
in speed/memory is probably negligible given the small size of a block (to be benchmarked). But
there is another pro to the alternative, which is that fully deserializing the entries to proper
structs makes the code much more readable. Instead of maintaining a bunch of pointer information in
the iterator and doing a bunch of pointer arithmetic to stitch together ranges of a byte buffer, we
can just keep an array of block entry structs and access the fields on the structs as usual. This
alternative aligns exactly with our goal of readability and is the approach that RainDB takes.

Serializing eagerly kind of removes the need to maintain restart points, but RainDB will still keep
the concept of restart points. Primarily, this is just to help tie concepts in RainDB back to
LevelDB and serve as a bridge for learning about both databases. It's also nice to keep around in
case we try to go for full compatibility with LevelDB later on.

#### Meta block types

##### Filter meta block

The filter block represents the `FilterPolicy` used by the database. A filter block is stored in
each table to represent the applied filter policy. The metaindex block contains an entry that maps
from a string `filter.<Name>` to the block handle for the filter block where `<Name>` is the string
returned by the filter policy's `get_name` method.

With regards to filters, data blocks are divided into ranges of a set size and a filter is created
for all of the keys of the blocks that fall in each range:

```text
[i * range_size...(i+1) * range_size - 1]
```

Currently, filters are created for 2 KiB ranges. As an example, suppose that blocks X and Y start in
the range `[0KiB...2KiB - 1 byte]`. Then all of the keys of blocks X and Y will be converted to a
filter by calling the `create_filter` method with those keys. This filter is stored as the first
filter in the filter block.

The filter block has the following format:

```text
[filter 0]
[filter 1]
[filter 2]
...
[filter N-1]

[offset of filter 0]                  : 4 bytes
[offset of filter 1]                  : 4 bytes
[offset of filter 2]                  : 4 bytes
...
[offset of filter N-1]                : 4 bytes

[offset to beginning of offset array] : 4 bytes
log(range_size)                       : 1 byte
```

The offset array at the end of the filter block allows efficient mapping from a data block offset to
the corresponding filter.

The size of the range has a base 2 logarithm applied to it to save on storage. The LevelDB documents
calls what is called range size here: "base". This can be confusing at first glance because the base
of the logarithm is 2 and I'm not sure the word "base" is particularly accurate in describing the
range of blocks covered by a filter.

##### Stats meta block

_RainDB does not have this block type but for completeness we list a description here from LevelDB.
Actually even this is listed as a `TODO` for LevelDB._

This meta block contains a sequence of key-values pairs for various statistics. The key is the name
of the statistic and the value contains the statistic.

## Appendix A - LevelDB and Memory Management

The section below is inspired by insight from
[Oren Eini's - Reviewing LevelDB Part IV](https://ayende.com/blog/161413/reviewing-leveldb-part-iv-on-std-string-buffers-and-memory-management-in-c)
but is paraphrased to clean up phrasing and includes some additions from myself (@nerdondon).

The fundamental data structure used throughout LevelDB is a C++ string. A string is used as a byte
buffer so that allocation and de-allocation of the underlying is automatically handled by virtue of
the C++ implementation. This logic does not have to be re-written and helps LevelDB adhere to
[RAII](https://en.wikipedia.org/wiki/Resource_acquisition_is_initialization) (resource acquisition
is initialization) principles. Additionally, the methods that come out of the box with strings (e.g.
append) are helpful throughout LevelDB's routines.

In order to squeeze out more performance, LevelDB attempts to keep operations zero-copy as much as
possible. One way that LevelDB refrains from copying memory between locations, is by implementing an
abstraction called a
[`Slice`](https://github.com/google/leveldb/blob/f57513a1d6c99636fc5b710150d0b93713af4e43/include/leveldb/slice.h)
on top of C++'s `std::string`. This abstraction returns offsets and lengths that point at an
underlying string, allowing the creation of different views without allocating additional memory.
LevelDB also removes the copy constructor at various points to make copying explicit and
intentional.
