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

use bincode::Options;
use serde::{Deserialize, Serialize};
use std::convert::TryFrom;

/**
Trait that decorates keys in RainDB that are sortable and deserializable.

This applies to the internal lookup key as well as the key used for sorting meta blocks in table
files.
*/
pub trait RainDbKeyType: Ord + TryFrom<Vec<u8>> {}

/** This is the actual key used by RainDB. It is the user provided key with additional metadata. */
#[derive(Debug, Deserialize, Eq, Hash, Serialize)]
pub struct LookupKey {
    /// The user suplied key.
    user_key: Vec<u8>,
    /// The sequence number of the operation associated with this generated key.
    sequence_number: u64,
    /// The operation being performed with this key.
    operation: Operation,
}

impl LookupKey {
    /// Construct a new `LookupKey`.
    pub(crate) fn new(user_key: Vec<u8>, sequence_number: u64, operation: Operation) -> Self {
        LookupKey {
            user_key,
            sequence_number,
            operation,
        }
    }

    /// Return the user key.
    pub(crate) fn get_user_key(&self) -> &Vec<u8> {
        &self.user_key
    }

    /**
    Return a key suitable for use in internal iterators.

    This must be equivalent to the `Vec::<u8>::from`
    */
    pub(crate) fn get_internal_key(&self) -> Vec<u8> {
        Vec::<u8>::from(self)
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
        // operation tags is equal as well. It would be a sad day if this invariant was not true
        // at runtime :/.
        self.user_key.cmp(&other.user_key).is_eq()
            && self.sequence_number == other.sequence_number
            && self.operation == other.operation
    }
}

impl TryFrom<Vec<u8>> for LookupKey {
    type Error = bincode::Error;

    fn try_from(value: Vec<u8>) -> bincode::Result<LookupKey> {
        bincode::DefaultOptions::new()
            .with_fixint_encoding()
            .deserialize(&value)
    }
}

impl From<&LookupKey> for Vec<u8> {
    fn from(value: &LookupKey) -> Vec<u8> {
        bincode::DefaultOptions::new()
            .with_fixint_encoding()
            .serialize(value)
            .unwrap()
    }
}

impl RainDbKeyType for LookupKey {}

/// The operation that is being applied to an entry in the database.
#[repr(u8)]
#[derive(Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
pub(crate) enum Operation {
    /// This represents a tombstone. There should not be a value set for the operation.
    Delete = 0,
    /// Add a new key-value pair or updates an existing key-value pair.
    Put = 1,
}
