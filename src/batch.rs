/*!
This module contains representations and utility functions for batch functions.

The elements of a batch request are described primarily by the type of operation being performed,
a user supplied key, and--optionally--a user suppplied value. The value is optional in the case of a
delete operation. In industry parlance, the serialized delete operation is a "tombstone". A sequence
number is also associated with the operation. The sequence number is a global, monotonically
increasing 64-bit unsigned int. It is never reset.
*/

use integer_encoding::{FixedInt, VarInt};
use std::slice::Iter;

use crate::key::Operation;

/**
Element of a batch operation.

# Serialization

When serialized a [`BatchElement`] will have the following format:

1. The operation as a 1-byte integer with fixed-length encoding
1. The key is represented by its length encoded as a 32-bit integer with variable length and the
   value of the key
1. If the operation is a `Put` operation, the value is encoded in the same way as the key and
   appended
*/
#[derive(Clone, PartialEq)]
pub struct BatchElement {
    /// The operation for this batch element.
    operation: Operation,

    /// The user provided key.
    user_key: Vec<u8>,

    ///The value to set on the key for `Put` operations or `None` for delete operations.
    value: Option<Vec<u8>>,

    /// The approximate size of the batch element when written to the database in bytes.
    size: usize,
}

/// Public methods
impl BatchElement {
    /// Get a reference to the batch element's operation.
    pub fn get_operation(&self) -> Operation {
        self.operation
    }

    /// Get a reference to the batch element's user supplied key.
    pub fn get_key(&self) -> &[u8] {
        &self.user_key
    }

    /// Get a reference to the batch element's value.
    pub fn value(&self) -> Option<&Vec<u8>> {
        self.value.as_ref()
    }
}

/// Crate-only methods
impl BatchElement {
    /// Create a new instance of [`BatchElement`].
    pub(crate) fn new(operation: Operation, user_key: Vec<u8>, value: Option<Vec<u8>>) -> Self {
        // Size = 1 byte for the operation + size of the user key + size of value + 8 byte sequence number
        let value_size = value.as_ref().map_or(0, |val| val.len());
        let size = 1 + user_key.len() + value_size + 8;

        Self {
            operation,
            user_key,
            value,
            size,
        }
    }

    /// Get the size of the batch element.
    pub(crate) fn size(&self) -> usize {
        self.size
    }
}

impl From<&BatchElement> for Vec<u8> {
    /// Serialize a [`BatchElement`] to bytes.
    fn from(batch_element: &BatchElement) -> Vec<u8> {
        let mut buf = Vec::with_capacity(batch_element.size());
        buf.extend(&[batch_element.operation as u8]);

        // Encode the key with a 32-bit varint of the length and then the key data
        buf.extend(u32::encode_var_vec(batch_element.user_key.len() as u32));
        buf.extend(&batch_element.user_key);

        // If this is a put, encode the value with a 32-bit varint of the length and then the
        // value data
        if batch_element.get_operation() == Operation::Put {
            buf.extend(u32::encode_var_vec(
                batch_element.value.as_ref().unwrap().len() as u32,
            ));
            buf.extend(batch_element.value.as_ref().unwrap());
        }

        buf
    }
}

/**
A set of operations to perform atomically.

The updates are applied in the order in which they are added to the batch.

# Examples

```
use raindb::Batch;

let batch = Batch::new();
batch
    .add_put("key".into(), "v1".into())
    .add_delete("key".into())
    .add_put("key".into(), "v2".into());
    .add_put("key".into(), "v3".into());

// The value of "key" will be "v3" when the batch is applied to the database.
```

# Serialization

A batch has the following layout when serialized:
1. The starting sequence number as a 64-bit fixed-length integer
1. The number operations in the batch as a 32-bit fixed-length integer
1. Serialized [`BatchElement`]'s. See the [`BatchElement`] docs their serialization format.
*/
#[derive(PartialEq)]
pub struct Batch {
    /**
    The starting sequence number for this batch of operations.

    This is set internally when a write is actually executed.
    */
    starting_seq_number: Option<u64>,

    /// A list of operations to perform in a batch.
    operations: Vec<BatchElement>,
}

/// Public methods
impl Batch {
    /// Create an empty [`Batch`].
    pub fn new() -> Self {
        Self {
            starting_seq_number: None,
            operations: vec![],
        }
    }

    /**
    Add a `Put` operation to the batch.

    * `key` - the user provided key to associate with the value to be stored
    * `value` - the value to be stored in the database
    */
    pub fn add_put(&mut self, key: Vec<u8>, value: Vec<u8>) -> &mut Self {
        let batch_element = BatchElement::new(Operation::Put, key, Some(value));
        self.operations.push(batch_element);

        self
    }

    /**
    Add a `Delete` operation to the batch.

    * `key` - the user provided key to associate with the value to be stored
    */
    pub fn add_delete(&mut self, key: Vec<u8>) -> &mut Self {
        let batch_element = BatchElement::new(Operation::Delete, key, None);
        self.operations.push(batch_element);

        self
    }

    /// Clear the operations added to the batch.
    pub fn clear(&mut self) -> &mut Self {
        self.operations.clear();

        self
    }

    /// Get an iterator over references to operations added to the batch.
    pub fn iter(&self) -> Iter<BatchElement> {
        self.operations.iter()
    }

    /// Get the number of entries within the batch.
    pub fn len(&self) -> usize {
        self.operations.len()
    }
}

/// Crate-only methods
impl Batch {
    /// Get the approximate size of the database changes that would be caused by this batch.
    pub(crate) fn get_approximate_size(&self) -> usize {
        self.iter().map(|operation| operation.size()).sum()
    }

    /// The the starting sequence number of the batch.
    pub(crate) fn set_starting_seq_number(&mut self, seq_number: u64) {
        self.starting_seq_number = Some(seq_number);
    }

    /// Get the starting sequence number of the batch.
    pub(crate) fn get_starting_seq_number(&self) -> Option<u64> {
        self.starting_seq_number
    }

    /**
    Append the operations from another batch to the operations of the current batch.

    This is usually done to create a group commit and reduce write latency.
    */
    pub(crate) fn append_batch(&mut self, batch_to_append: &Batch) {
        self.operations
            .extend_from_slice(batch_to_append.iter().as_slice());
    }
}

impl From<&Batch> for Vec<u8> {
    /// Serialize a [`Batch`] to bytes.
    fn from(batch: &Batch) -> Vec<u8> {
        let mut buf = Vec::with_capacity(batch.get_approximate_size());
        buf.extend(u64::encode_fixed_vec(
            batch.starting_seq_number.clone().unwrap(),
        ));
        buf.extend(u32::encode_var_vec(batch.operations.len() as u32));

        for element in batch.operations.iter() {
            buf.extend(Vec::<u8>::from(element));
        }

        buf
    }
}
