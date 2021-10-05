/*!
This module contains representations and utility functions for batch functions.
*/

/** The operation that is being applied to the data of a batch entry. */
#[repr(u8)]
pub(crate) enum BatchOperation {
    /// This represents a tombstone and there should not be a value set for the operation.
    Delete = 0,
    /// Add a new key-value pair or updates an existing key-value pair.
    Put = 1,
}

/** Type of acceptable keys. */
pub(crate) type Key = Vec<u8>;

/** Type of values. */
pub(crate) type Value = Vec<u8>;

/** Element of a batch operation. */
pub(crate) struct BatchElement {
    /// The operation for this batch element.
    operation: BatchOperation,
    /// The key of the record to perform the operation on.
    key: Key,
    ///The value to set on the key for `Put` operations or `None` for delete operations.
    value: Option<Value>,
}

/** A set of operations to perform atomically. */
pub(crate) type Batch = Vec<BatchElement>;
