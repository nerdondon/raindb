/*!
Entries in the database are represented by an internal key that adds additional metadata e.g. a
sequence number and the operation that was performed.

The sequence number is a global, monotonically increasing 64-bit unsigned int. It is never reset.
Because writes are append-only, there may be multiple records with the same user key and operation.
The sequence number is used to denote which of the stored records is the most recent version.
*/

use bincode::Options;
use serde::{Deserialize, Serialize};
use std::convert::TryFrom;

#[derive(Deserialize, Eq, Serialize)]
pub(crate) struct InternalKey {
    /// The user suplied key.
    user_key: Vec<u8>,
    /// The sequence number of the operation associated with this generated key.
    sequence_number: u64,
    /// The operation being performed with this key.
    operation: Operation,
}

impl InternalKey {
    pub(crate) fn new(user_key: Vec<u8>, sequence_number: u64, operation: Operation) -> Self {
        InternalKey {
            user_key,
            sequence_number,
            operation,
        }
    }
}

impl Ord for InternalKey {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // Return ordering by the user provided keys if they are not equal
        if self.user_key.as_slice().ne(other.user_key.as_slice()) {
            return self.user_key.as_slice().cmp(other.user_key.as_slice());
        }

        // Check the sequence number if the keys are equal
        return self.sequence_number.cmp(&other.sequence_number);
    }
}

impl PartialOrd for InternalKey {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for InternalKey {
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

impl TryFrom<&[u8]> for InternalKey {
    type Error = bincode::Error;

    fn try_from(value: &[u8]) -> bincode::Result<InternalKey> {
        bincode::DefaultOptions::new()
            .with_fixint_encoding()
            .deserialize(value)
    }
}

impl TryFrom<&InternalKey> for Vec<u8> {
    type Error = bincode::Error;

    fn try_from(value: &InternalKey) -> Result<Self, Self::Error> {
        bincode::DefaultOptions::new()
            .with_fixint_encoding()
            .serialize(value)
    }
}

/** The operation that is being applied to an entry in the database. */
#[repr(u8)]
#[derive(Deserialize, Eq, PartialEq, Serialize)]
pub(crate) enum Operation {
    /// This represents a tombstone. There should not be a value set for the operation.
    Delete = 0,
    /// Add a new key-value pair or updates an existing key-value pair.
    Put = 1,
}
