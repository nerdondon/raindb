/*!
This module contains representations and utility functions for batch functions.

The elements of a batch request are described primarily by the type of operation being performed,
a user supplied key, and--optionally--a user suppplied value. The value is optional in the case of a
delete operation. In industry parlance, the serialized delete operation is a "tombstone". A sequence
number is also associated with the operation. The sequence number is a global, monotonically
increasing 64-bit unsigned int. It is never reset.
*/

use integer_encoding::{FixedInt, FixedIntReader, VarInt, VarIntReader};
use std::io::Read;
use std::slice::Iter;

use crate::errors::{RainDBError, RainDBResult};
use crate::key::Operation;
use crate::utils::io::ReadHelpers;

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
#[derive(Clone, Debug, PartialEq, Eq)]
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
    pub fn get_value(&self) -> Option<&Vec<u8>> {
        self.value.as_ref()
    }
}

/// Crate-only methods
impl BatchElement {
    /// Create a new instance of [`BatchElement`].
    pub(crate) fn new(operation: Operation, user_key: Vec<u8>, value: Option<Vec<u8>>) -> Self {
        // Size =
        //  1 byte for the operation + size of the user key + size of value + 8 byte sequence number
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

    /**
    Deserializes a [`BatchElement`] from the provided byte buffer and returning a tuple of the
    deserialized value and the number of bytes read.
    */
    pub(crate) fn read_element(mut buf: &[u8]) -> RainDBResult<(BatchElement, usize)> {
        let starting_len = buf.len();
        let mut raw_operation: [u8; 1] = [0; 1];
        buf.read_exact(&mut raw_operation)?;
        let operation = Operation::try_from(raw_operation[0])?;
        let user_key = buf.read_length_prefixed_slice()?;

        let value: Option<Vec<u8>> = if operation == Operation::Put {
            Some(buf.read_length_prefixed_slice()?)
        } else {
            None
        };

        let bytes_read = starting_len - buf.len();

        Ok((BatchElement::new(operation, user_key, value), bytes_read))
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

impl TryFrom<&[u8]> for BatchElement {
    type Error = RainDBError;

    fn try_from(buf: &[u8]) -> Result<Self, Self::Error> {
        Ok(BatchElement::read_element(buf)?.0)
    }
}

/**
A set of operations to perform atomically.

The updates are applied in the order in which they are added to the batch.

# Examples

```
use raindb::Batch;

let mut batch = Batch::new();
batch
    .add_put("key".into(), "v1".into())
    .add_delete("key".into())
    .add_put("key".into(), "v2".into())
    .add_put("key".into(), "v3".into());

// The value of "key" will be "v3" when the batch is applied to the database.
```

# Serialization

A batch has the following layout when serialized:
1. The starting sequence number as a 64-bit fixed-length integer
1. The number operations in the batch as a 32-bit fixed-length integer
1. Serialized [`BatchElement`]'s. See the [`BatchElement`] docs their serialization format.
*/
#[derive(PartialEq, Eq)]
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
        self.add_operation(batch_element);

        self
    }

    /**
    Add a `Delete` operation to the batch.

    * `key` - the user provided key to associate with the value to be stored
    */
    pub fn add_delete(&mut self, key: Vec<u8>) -> &mut Self {
        let batch_element = BatchElement::new(Operation::Delete, key, None);
        self.add_operation(batch_element);

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

    /// Returns true if the batch does not have any operations. Otherwise, returns false.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl Default for Batch {
    fn default() -> Self {
        Self::new()
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

    /// Append an operation.
    pub(crate) fn add_operation(&mut self, batch_element: BatchElement) {
        self.operations.push(batch_element);
    }
}

impl From<&Batch> for Vec<u8> {
    /// Serialize a [`Batch`] to bytes.
    fn from(batch: &Batch) -> Vec<u8> {
        let mut buf = Vec::with_capacity(batch.get_approximate_size());
        buf.extend(u64::encode_fixed_vec(batch.starting_seq_number.unwrap()));
        buf.extend(u32::encode_var_vec(batch.operations.len() as u32));

        for element in batch.operations.iter() {
            buf.extend(Vec::<u8>::from(element));
        }

        buf
    }
}

impl TryFrom<&[u8]> for Batch {
    type Error = RainDBError;

    fn try_from(mut buf: &[u8]) -> Result<Self, Self::Error> {
        let mut batch = Batch::new();
        let starting_seq_num: u64 = buf.read_fixedint()?;
        batch.set_starting_seq_number(starting_seq_num);

        let num_operations: u32 = buf.read_varint()?;
        for _ in 0..num_operations {
            let (element, bytes_read) = BatchElement::read_element(buf)?;
            batch.add_operation(element);
            buf = &buf[bytes_read..];
        }

        Ok(batch)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pretty_assertions::assert_eq;

    #[test]
    fn can_add_and_remove_elements_on_a_batch() {
        let mut batch = Batch::new();

        assert!(batch.is_empty());

        batch.add_put(b"batmann".to_vec(), u32::encode_fixed_vec(55));
        batch.add_delete(b"robin".to_vec());
        batch.add_put(b"robin".to_vec(), u32::encode_fixed_vec(3));

        assert_eq!(batch.len(), 3);

        batch.clear();

        assert!(batch.is_empty());
    }

    #[test]
    fn can_append_another_batch() {
        let mut batch1 = Batch::new();
        batch1.set_starting_seq_number(43);
        batch1.add_put(b"batmann".to_vec(), u32::encode_fixed_vec(55));

        let mut batch2 = Batch::new();
        batch2.set_starting_seq_number(117);
        batch2.add_delete(b"batmann".to_vec());

        batch1.append_batch(&batch2);
        assert_eq!(batch1.len(), 2);
        assert_eq!(batch1.get_starting_seq_number().unwrap(), 43);
        let mut batch1_iter = batch1.iter();
        assert_eq!(
            *batch1_iter.next().unwrap(),
            BatchElement::new(
                Operation::Put,
                b"batmann".to_vec(),
                Some(u32::encode_fixed_vec(55))
            )
        );
        assert_eq!(
            *batch1_iter.next().unwrap(),
            BatchElement::new(Operation::Delete, b"batmann".to_vec(), None)
        );
    }

    #[test]
    fn can_be_serialized_and_deserialized() {
        let expected_elements = [
            BatchElement::new(
                Operation::Put,
                b"batmann".to_vec(),
                Some(u32::encode_fixed_vec(55)),
            ),
            BatchElement::new(Operation::Delete, b"batmann".to_vec(), None),
            BatchElement::new(
                Operation::Put,
                b"robin".to_vec(),
                Some(u32::encode_fixed_vec(3)),
            ),
            BatchElement::new(
                Operation::Put,
                b"batmann".to_vec(),
                Some(u32::encode_fixed_vec(11)),
            ),
        ];

        let mut batch = Batch::new();
        batch.set_starting_seq_number(43);
        batch.add_put(b"batmann".to_vec(), u32::encode_fixed_vec(55));
        batch.add_delete(b"batmann".to_vec());
        batch.add_put(b"robin".to_vec(), u32::encode_fixed_vec(3));
        batch.add_put(b"batmann".to_vec(), u32::encode_fixed_vec(11));

        let serialized_batch = Vec::<u8>::from(&batch);
        let deserialized_batch = Batch::try_from(serialized_batch.as_slice()).unwrap();
        assert_eq!(
            batch.get_starting_seq_number(),
            deserialized_batch.get_starting_seq_number()
        );
        assert_eq!(
            batch.get_approximate_size(),
            deserialized_batch.get_approximate_size(),
            "The batch approximate size ({batch_size}) should match the deserialized size \
            ({deserialized_size})",
            batch_size = batch.get_approximate_size(),
            deserialized_size = deserialized_batch.get_approximate_size()
        );

        for (deserialized_element, expected_element) in
            deserialized_batch.iter().zip(expected_elements.iter())
        {
            assert_eq!(deserialized_element, expected_element);
        }
    }

    #[test]
    fn approximate_sizes_scale_appropriately() {
        let mut batch = Batch::new();

        assert_eq!(batch.get_approximate_size(), 0);

        batch.add_put(b"batmann".to_vec(), u32::encode_fixed_vec(55));
        assert!(batch.get_approximate_size() <= 69);

        batch.add_delete(b"batmann".to_vec());
        assert!(batch.get_approximate_size() <= (69 + 65));

        batch.add_put(b"batmann".to_vec(), u32::encode_fixed_vec(11));
        assert!(batch.get_approximate_size() <= (69 + 65 + 69));
    }
}
