/*!
Entries in the database are represented by an internal key that adds additional metadata e.g. a
sequence number and the operation that was performed.

The sequence number is a global, monotonically increasing 64-bit unsigned int. It is never reset.
Because writes are append-only, there may be multiple records with the same user key and operation.
The sequence number is used to denote which of the stored records is the most recent version.
*/

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

/** The operation that is being applied to an entry in the database. */
#[repr(u8)]
pub(crate) enum Operation {
    /// This represents a tombstone. There should not be a value set for the operation.
    Delete = 0,
    /// Add a new key-value pair or updates an existing key-value pair.
    Put = 1,
}
