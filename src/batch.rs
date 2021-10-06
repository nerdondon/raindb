/*!
This module contains representations and utility functions for batch functions.

The elements of a batch request are described primarily by the type of operation being performed,
a user supplied key, and--optionally--a user suppplied value. The value is optional in the case of a
delete operation. In industry parlance, the delete operation denotes a "tombstone". A sequence
number is also associated with the operation. The sequence number is a global, monotonically
increasing 64-bit unsigned int. It is never reset.

For storage, a batch is converted to binary with `bincode` which has it's own enconding
specification.
*/

use crate::key::Operation;

/** Type of acceptable keys. */
pub(crate) type Key = Vec<u8>;

/** Type of values. */
pub(crate) type Value = Vec<u8>;

/** Element of a batch operation. */
pub(crate) struct BatchElement {
    /// The operation for this batch element.
    operation: Operation,
    /// The key of the record to perform the operation on.
    key: Key,
    ///The value to set on the key for `Put` operations or `None` for delete operations.
    value: Option<Value>,
    /// A sequence number associated with the operation.
    sequence_num: u64,
}

/** A set of operations to perform atomically. */
pub(crate) struct Batch {
    /// The starting sequence number for this batch of operations.
    starting_seq_num: u64,
    /// A list of operations to perform in a batch.
    operations: Vec<BatchElement>,
}

impl Batch {}
