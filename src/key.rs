/*!
This module contains implementations of keys used to index values in the RainDB. There are two main
keys that are used:

    1. Lookup keys for looking up values in the database
    1. Metaindex keys that are used for looking up metadata in table files

# More on lookup keys

Entries in the database are represented by an internal key that adds additional metadata e.g. a
sequence number and the operation that was performed.

The sequence number is a global, monotonically increasing 64-bit unsigned int. It is never reset.
Because writes are append-only, there may be multiple records with the same user key and operation.
The sequence number is used to denote which of the stored records is the most recent version.
*/

use integer_encoding::FixedInt;
use std::convert::{TryFrom, TryInto};

use crate::errors::{RainDBError, RainDBResult};
use crate::utils::bytes::BinarySeparable;

/**
Trait that decorates keys in RainDB that are sortable and deserializable.

This applies to the internal lookup key as well as the key used for sorting meta blocks in table
files.
*/
pub trait RainDbKeyType: Ord + TryFrom<Vec<u8>> {
    /// Returns a byte representation of the key.
    fn as_bytes(&self) -> Vec<u8>;
}

/**
This is the actual key used by RainDB. It is the user provided key with additional metadata.

# Serialization

When the key is serialized to write to disk, it has the following layout:
1. The user key
1. The sequence number with a fixed-length encoding
1. The operation as an 8-bit integer with fixed-length encoding

Unlike in LevelDB, we do not pack the sequence number and operation together into a single 64-bit
integer (forming an 8 byte trailer) where the sequence number takes up the first 56 bits and the
operation takes up the last 8 bits.
*/
#[derive(Clone, Debug, Eq, Hash)]
pub struct LookupKey {
    /// The user suplied key.
    user_key: Vec<u8>,
    /// The sequence number of the operation associated with this generated key.
    sequence_number: u64,
    /// The operation being performed with this key.
    operation: Operation,
}

/// Crate-only methods
impl LookupKey {
    /// Construct a new [`LookupKey`].
    pub(crate) fn new(user_key: Vec<u8>, sequence_number: u64, operation: Operation) -> Self {
        LookupKey {
            user_key,
            sequence_number,
            operation,
        }
    }

    /**
    Construct a new [`LookupKey`] for seek operations.

    # Legacy

    Seek operations will utilize the [`Operation::Put`] operation tag because it is the largest
    discriminant in the [`Operation`] enum. LevelDB does this because it embeds the operation tag in
    the last 8 bits of the sequence number. Sequence numbers are sorted in decreasing order and seek
    operations need the most recent value, so the largest operation needs to be used when doing
    seeks. This is not relevant for RainDB because we mostly deal in fully parsed key structures,
    whereas LevelDB usually only works byte buffers with serialized keys.
    */
    pub(crate) fn new_for_seeking(user_key: Vec<u8>, sequence_number: u64) -> Self {
        LookupKey {
            user_key,
            sequence_number,
            operation: Operation::Put,
        }
    }

    /// Return the user key.
    pub(crate) fn get_user_key(&self) -> &Vec<u8> {
        &self.user_key
    }

    /// Get a reference to the lookup key's operation.
    pub(crate) fn get_operation(&self) -> &Operation {
        &self.operation
    }
}

impl Ord for LookupKey {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // Return ordering by the user provided keys if they are not equal
        if self.user_key.as_slice().ne(other.user_key.as_slice()) {
            return self.user_key.as_slice().cmp(other.user_key.as_slice());
        }

        // Check the sequence number if the keys are equal.
        // This orders the sequence numbers in descending order because we want to bias toward the
        // most recent operations.
        other.sequence_number.cmp(&self.sequence_number)
    }
}

impl PartialOrd for LookupKey {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for LookupKey {
    fn eq(&self, other: &Self) -> bool {
        // Operation is checked here but the equality relation should be implied by the check on
        // `sequence_number`. More clearly, a sequence number is assigned per operation so if the
        // sequence numbers of the elements being compared are equal it should mean that the
        // operation tags are equal as well. It would be a sad day if this invariant was not true
        // at runtime :/.
        self.user_key.cmp(&other.user_key).is_eq()
            && self.sequence_number == other.sequence_number
            && self.operation == other.operation
    }
}

impl TryFrom<Vec<u8>> for LookupKey {
    type Error = RainDBError;

    fn try_from(buf: Vec<u8>) -> RainDBResult<LookupKey> {
        let trailer_size: usize = 8 + 1;
        if buf.len() < trailer_size {
            return Err(RainDBError::KeyParsing(format!(
                "The provided buffer was too small. It needs to be at least {} bytes but was {}.",
                trailer_size,
                buf.len(),
            )));
        }

        let trailer_start_index = buf.len() - trailer_size;
        let sequence_number_end_index = buf.len() - 2;
        let user_key = &buf[..trailer_start_index];
        let sequence_number =
            u64::decode_fixed(&buf[trailer_start_index..sequence_number_end_index]);
        let operation: Operation = buf[buf.len() - 1].try_into()?;

        Ok(LookupKey::new(
            user_key.to_vec(),
            sequence_number,
            operation,
        ))
    }
}

impl From<&LookupKey> for Vec<u8> {
    fn from(key: &LookupKey) -> Vec<u8> {
        key.as_bytes()
    }
}

impl RainDbKeyType for LookupKey {
    fn as_bytes(&self) -> Vec<u8> {
        // We know the size of the buffer we need before hand.
        // size = size of user key + 8 bytes for the sequence number + 1 byte for the operation
        let buf: Vec<u8> = Vec::with_capacity(self.get_user_key().len() + 8 + 1);
        buf.extend_from_slice(self.get_user_key());
        buf.extend_from_slice(&self.sequence_number.encode_fixed_vec());
        buf.extend_from_slice(&[*self.get_operation() as u8]);

        buf
    }
}

impl BinarySeparable for &LookupKey {
    fn find_shortest_separator(smaller: &LookupKey, greater: &LookupKey) -> Vec<u8> {
        // Try to shorten the user part of the key first
        let user_separator = BinarySeparable::find_shortest_separator(
            smaller.get_user_key().as_slice(),
            greater.get_user_key().as_slice(),
        );
        if user_separator.len() < smaller.get_user_key().len()
            && smaller.get_user_key() < &user_separator
        {
            /*
            We found a user key separator is shorter than the full user key and is larger
            logically. Turn it into a valid key by choosing the closest possible sequence
            number. This is the maximum value of a `u64` because sequence numbers are sorted
            in reverse order.
            */
            let full_separator = LookupKey::new_for_seeking(user_separator, u64::MAX);
            assert!(smaller < &full_separator);
            assert!(&full_separator < greater);

            return full_separator.as_bytes();
        }

        smaller.as_bytes()
    }

    fn find_shortest_successor(value: Self) -> Vec<u8> {
        // Try to get a successor for the user key first
        let user_key = value.get_user_key();
        let user_successor = BinarySeparable::find_shortest_successor(user_key.as_slice());

        if user_successor.len() < user_key.len() && user_key < &user_successor {
            /*
            We found a successor shorter than the user key and logically larger than the user
            key. Turn it into a valid key by choosing the closest possible sequence number. This is
            the maximum value of a `u64` because sequence numbers are sorted in reverse order.
            */
            let full_successor = LookupKey::new_for_seeking(user_successor, u64::MAX);
            assert!(value < &full_successor);

            return full_successor.as_bytes();
        }

        value.as_bytes()
    }
}

/**
The operation that is being applied to an entry in the database.

Existing enum values should not be changed since they are serialized as part of the table file
format.
*/
#[repr(u8)]
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub(crate) enum Operation {
    /// This represents a tombstone. There should not be a value set for the operation.
    Delete = 0,
    /// Add a new key-value pair or updates an existing key-value pair.
    Put = 1,
}

impl TryFrom<u8> for Operation {
    type Error = RainDBError;

    fn try_from(value: u8) -> RainDBResult<Operation> {
        let operation = match value {
            0 => Operation::Delete,
            1 => Operation::Put,
            _ => {
                return Err(RainDBError::KeyParsing(format!(
                    "There was an problem parsing the operation. The value received was {}",
                    value
                )))
            }
        };

        Ok(operation)
    }
}
