/*!
The write ahead log (WAL) persists writes to disk to enable recovery of in-memory information in
the event of a crash.

The log file contents are series of 32 KiB blocks.

A record never starts within the last six bytes of a block (since it won't fit). Any leftover bytes
here form the trailer, which must consist entirely of zero bytes and must be skipped by readers.

Aside: if exactly seven bytes are left in the current block, and a new non-zero length record is
added, the writer must emit a FIRST record (which contains zero bytes of user data) to fill up the
trailing seven bytes of the block and then emit all of the user data in subsequent blocks.
*/

/**
Block record types denotes whether the data contained in the record is split across multiple
records or if they contain all of the data for a single user record.

Note, the use of record is overloaded here. Be aware of the distinction between a block record
and the actual user record.
*/
pub(crate) enum RecordType {
    /// Denotes that the block contains the entirety of a user record.
    Full,
    /// Denotes the first fragment of a user record.
    First,
    /// Denotes the interior fragments of a user record.
    Middle,
    /// Denotes the last fragment of a user record.
    Last,
}

/**
A record that is stored in a particular block. It is potentially only a fragement of a full user
record.
*/
pub(crate) struct BlockRecord {
    data: Vec<u8>,
    length: usize,
    record_type: RecordType,
}
