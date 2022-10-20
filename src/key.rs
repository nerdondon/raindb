/*!
This module contains implementations of keys used to index values in the RainDB. There are two main
keys that are used:

1. Internal keys for tracking and looking up values in the database
1. Metaindex keys that are used for looking up metadata in table files

# More on internal keys

Entries in the database are tracked by an internal key that adds additional metadata e.g. a
sequence number and the operation that was performed.

The sequence number is a global, monotonically increasing 64-bit unsigned int. It is never reset.
Because writes are append-only, there may be multiple records with the same user key and operation.
The sequence number is used to denote which of the stored records is the most recent version.
*/

use integer_encoding::FixedInt;
use nerdondon_hopscotch::Sizeable;
use std::convert::{TryFrom, TryInto};
use std::fmt::Debug;
use std::hash::{Hash, Hasher};
use std::mem;

use crate::errors::{RainDBError, RainDBResult};
use crate::utils::bytes::BinarySeparable;

/**
The maximum sequence number allowed.

# Legacy

LevelDB packs the sequence number with an operation tag into 64-bits, where the sequence number
takes the first 56 bits and the operation tag fills the last 8 bits.
*/
pub(crate) const MAX_SEQUENCE_NUMBER: u64 = u64::MAX;

/**
Trait that decorates keys in RainDB that are sortable and deserializable.

This applies to the internal key as well as the key used for sorting meta blocks in table
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
#[derive(Clone, Eq)]
pub struct InternalKey {
    /*
    TODO: Make a version of InternalKey that is tied to the lifetime of a slice to reduce the need
    to clone slices for ownership.
    */
    /// The user suplied key.
    user_key: Vec<u8>,
    /// The sequence number of the operation associated with this generated key.
    sequence_number: u64,
    /// The operation being performed with this key.
    operation: Operation,
}

/// Crate-only methods
impl InternalKey {
    /// Construct a new [`InternalKey`].
    pub(crate) fn new(user_key: Vec<u8>, sequence_number: u64, operation: Operation) -> Self {
        InternalKey {
            user_key,
            sequence_number,
            operation,
        }
    }

    /**
    Construct a new [`InternalKey`] for seek operations.

    # Legacy

    This is analogous to LevelDB's `LookupKey`.

    Seek operations will utilize the [`Operation::Put`] operation tag because it is the largest
    discriminant in the [`Operation`] enum. LevelDB does this because it embeds the operation tag in
    the last 8 bits of the sequence number. Sequence numbers are sorted in decreasing order and seek
    operations need the most recent value, so the largest operation needs to be used when doing
    seeks. This is not relevant for RainDB because we mostly deal in fully parsed key structures,
    whereas LevelDB usually only works byte buffers with serialized keys.
    */
    pub(crate) fn new_for_seeking(user_key: Vec<u8>, sequence_number: u64) -> Self {
        InternalKey {
            user_key,
            sequence_number,
            operation: Operation::Put,
        }
    }

    /// Return the user key.
    pub(crate) fn get_user_key(&self) -> &[u8] {
        self.user_key.as_slice()
    }

    /// Return the user key as a reference to the vector.
    pub(crate) fn get_user_key_as_vec(&self) -> &Vec<u8> {
        &self.user_key
    }

    /// Get a reference to operation tag for this key.
    pub(crate) fn get_operation(&self) -> Operation {
        self.operation
    }

    /// Get the sequence number for this key.
    pub(crate) fn get_sequence_number(&self) -> u64 {
        self.sequence_number
    }

    /**
    Get the approximate size of the key when serialized.

    This is determined by the following formula:

    size = size of user key + 8 bytes for the sequence number + 1 byte for the operation
    */
    pub(crate) fn get_approximate_size(&self) -> usize {
        self.user_key.len() + 8 + 1
    }
}

impl RainDbKeyType for InternalKey {
    fn as_bytes(&self) -> Vec<u8> {
        // We know the size of the buffer we need before hand.
        // size = size of user key + 8 bytes for the sequence number + 1 byte for the operation
        let mut buf: Vec<u8> = Vec::with_capacity(self.get_user_key().len() + 8 + 1);
        buf.extend_from_slice(self.get_user_key());
        buf.extend_from_slice(&self.sequence_number.encode_fixed_vec());
        buf.extend_from_slice(&[self.get_operation() as u8]);

        buf
    }
}

impl Debug for InternalKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::result::Result<(), std::fmt::Error> {
        // We display the user key as a lossy UTF-8 string for simplicity of debugging the database
        // itself. Turning it into hex to would help with client debugging so we will reconsider
        // at a later time.
        write!(
            f,
            "{user_key} @ {seq_num} : {operation:?}",
            user_key = String::from_utf8_lossy(self.user_key.as_slice()).escape_debug(),
            seq_num = self.sequence_number,
            operation = self.operation
        )
    }
}

impl Ord for InternalKey {
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
        // operation tags are equal as well. It would be a sad day if this invariant was not true
        // at runtime :/.
        self.user_key.cmp(&other.user_key).is_eq()
            && self.sequence_number == other.sequence_number
            && self.operation == other.operation
    }
}

impl Hash for InternalKey {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.user_key.hash(state);
        self.sequence_number.hash(state);
        self.operation.hash(state);
    }
}

impl TryFrom<Vec<u8>> for InternalKey {
    type Error = RainDBError;

    fn try_from(buf: Vec<u8>) -> RainDBResult<InternalKey> {
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
            u64::decode_fixed(&buf[trailer_start_index..=sequence_number_end_index]);
        let operation: Operation = buf[buf.len() - 1].try_into()?;

        Ok(InternalKey::new(
            user_key.to_vec(),
            sequence_number,
            operation,
        ))
    }
}

impl From<&InternalKey> for Vec<u8> {
    fn from(key: &InternalKey) -> Vec<u8> {
        key.as_bytes()
    }
}

impl BinarySeparable for &InternalKey {
    fn find_shortest_separator(smaller: &InternalKey, greater: &InternalKey) -> Vec<u8> {
        // Try to shorten the user part of the key first
        let user_separator = BinarySeparable::find_shortest_separator(
            smaller.get_user_key(),
            greater.get_user_key(),
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
            let full_separator = InternalKey::new_for_seeking(user_separator, MAX_SEQUENCE_NUMBER);
            assert!(smaller < &full_separator);
            assert!(&full_separator < greater);

            return full_separator.as_bytes();
        }

        smaller.as_bytes()
    }

    fn find_shortest_successor(value: Self) -> Vec<u8> {
        // Try to get a successor for the user key first
        let user_key = value.get_user_key();
        let user_successor = BinarySeparable::find_shortest_successor(user_key);

        if user_successor.len() < user_key.len() && user_key < &user_successor {
            /*
            We found a successor shorter than the user key and logically larger than the user
            key. Turn it into a valid key by choosing the closest possible sequence number. This is
            the maximum value of a `u64` because sequence numbers are sorted in reverse order.
            */
            let full_successor = InternalKey::new_for_seeking(user_successor, MAX_SEQUENCE_NUMBER);
            assert!(value < &full_successor);

            return full_successor.as_bytes();
        }

        value.as_bytes()
    }
}

impl Sizeable for InternalKey {
    fn get_approx_size(&self) -> usize {
        mem::size_of::<u64>()
            + mem::size_of::<Operation>()
            + mem::size_of::<Vec<u8>>()
            + self.user_key.len()
    }
}

/**
The operation that is being applied to an entry in the database.

Existing enum values should not be changed since they are serialized as part of the table file
format.
*/
#[repr(u8)]
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum Operation {
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

impl RainDbKeyType for Vec<u8> {
    fn as_bytes(&self) -> Vec<u8> {
        self.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pretty_assertions::assert_eq;

    #[test]
    fn internal_key_can_be_converted_to_byte_buffer_and_back() {
        let user_key1 = b"batmann".to_vec();
        let sequence_num1: u64 = 1;
        let operation1 = Operation::Put;
        let internal_key1 = InternalKey::new(user_key1, sequence_num1, operation1);
        let encoded1 = internal_key1.as_bytes();
        let decoded1 = InternalKey::try_from(encoded1).unwrap();
        assert_eq!(internal_key1.get_user_key(), decoded1.get_user_key());
        assert_eq!(
            internal_key1.get_sequence_number(),
            decoded1.get_sequence_number()
        );
        assert_eq!(internal_key1.get_operation(), decoded1.get_operation());
        assert_eq!(internal_key1, decoded1);

        let user_key2 = b"batmann".to_vec();
        let sequence_num2: u64 = u64::MAX;
        let operation2 = Operation::Put;
        let internal_key2 = InternalKey::new(user_key2, sequence_num2, operation2);
        let encoded2 = internal_key2.as_bytes();
        let decoded2 = InternalKey::try_from(encoded2).unwrap();
        assert_eq!(internal_key2.get_user_key(), decoded2.get_user_key());
        assert_eq!(
            internal_key2.get_sequence_number(),
            decoded2.get_sequence_number()
        );
        assert_eq!(internal_key2.get_operation(), decoded2.get_operation());
        assert_eq!(internal_key2, decoded2);

        let user_key3 = b"batmannandrobinarethekeyinthisverylongstory".to_vec();
        let sequence_num3: u64 = (1 << 32) + 1;
        let operation3 = Operation::Delete;
        let internal_key3 = InternalKey::new(user_key3, sequence_num3, operation3);
        let encoded3 = internal_key3.as_bytes();
        let decode3 = InternalKey::try_from(encoded3).unwrap();
        assert_eq!(internal_key3.get_user_key(), decode3.get_user_key());
        assert_eq!(
            internal_key3.get_sequence_number(),
            decode3.get_sequence_number()
        );
        assert_eq!(internal_key3.get_operation(), decode3.get_operation());
        assert_eq!(internal_key3, decode3);
    }

    #[test]
    fn internal_key_returns_an_error_if_trying_to_convert_improperly_formatted_byte_buffer() {
        let encoded1: Vec<u8> = vec![];
        let maybe_decoded1 = InternalKey::try_from(encoded1);
        assert!(maybe_decoded1.is_err());
    }

    #[test]
    fn internal_key_has_proper_ordering_invariants() {
        let internal_key1 = InternalKey::new(b"batmann".to_vec(), 1, Operation::Put);
        let internal_key2 = InternalKey::new(b"robin".to_vec(), 3, Operation::Put);
        let internal_key3 = InternalKey::new(b"robin".to_vec(), 2, Operation::Delete);
        let internal_key4 = InternalKey::new(b"tumtum".to_vec(), 1, Operation::Put);
        let internal_key5 = InternalKey::new(b"tumtum".to_vec(), 1, Operation::Put);

        assert!(
            internal_key1 < internal_key4,
            "key1 should be less than because its user key is alphabetically first"
        );
        assert!(
            internal_key1 < internal_key2,
            "key1 should be less than because its user key is alphabetically first"
        );
        assert!(
            internal_key2 < internal_key3,
            "key2 should be less than because its sequence number is larger"
        );
        assert!(
            internal_key3 > internal_key2,
            "key3 should be greater than because its sequence number is smaller"
        );
        assert!(
            internal_key4 == internal_key5,
            "These keys are equal because the constituent parts of the key are equal"
        );
    }

    #[test]
    fn internal_key_sorting_works_as_expected() {
        let keys = [
            InternalKey::new(b"batmann".to_vec(), 1, Operation::Put),
            InternalKey::new(b"robin".to_vec(), 2, Operation::Delete),
            InternalKey::new(b"robin".to_vec(), 3, Operation::Put),
            InternalKey::new(b"tumtum".to_vec(), 1, Operation::Put),
            InternalKey::new(b"tumtum".to_vec(), 5, Operation::Put),
            InternalKey::new(b"tumtum".to_vec(), 1, Operation::Put),
        ];

        let expected = [
            InternalKey::new(b"batmann".to_vec(), 1, Operation::Put),
            InternalKey::new(b"robin".to_vec(), 3, Operation::Put),
            InternalKey::new(b"robin".to_vec(), 2, Operation::Delete),
            InternalKey::new(b"tumtum".to_vec(), 5, Operation::Put),
            InternalKey::new(b"tumtum".to_vec(), 1, Operation::Put),
            InternalKey::new(b"tumtum".to_vec(), 1, Operation::Put),
        ];

        let mut actual = keys;
        actual.sort();

        assert_eq!(expected, actual);
    }

    #[test]
    fn internal_key_can_generate_the_shortest_separator_between_two_keys_with_the_same_user_key() {
        let internal_key1 = InternalKey::new(b"batmann".to_vec(), 3, Operation::Put);
        let internal_key2 = InternalKey::new(b"batmann".to_vec(), 2, Operation::Put);
        let internal_key3 = InternalKey::new(b"batmann".to_vec(), 1, Operation::Put);
        let internal_key4 = InternalKey::new(b"batmann".to_vec(), 1, Operation::Put);
        let internal_key5 = InternalKey::new(b"batmann".to_vec(), 1, Operation::Delete);
        let internal_key6 = InternalKey::new(b"dog".to_vec(), 1, Operation::Delete);

        assert_eq!(
            BinarySeparable::find_shortest_separator(&internal_key2, &internal_key3),
            InternalKey::new(b"batmann".to_vec(), 2, Operation::Put).as_bytes(),
            "The shortest separator should be the first arg becuase the keys are out of order"
        );

        assert_eq!(
            BinarySeparable::find_shortest_separator(&internal_key2, &internal_key1),
            InternalKey::new(b"batmann".to_vec(), 2, Operation::Put).as_bytes(),
            "The shortest separator should be the one with the smaller sequence number"
        );

        assert_eq!(
            BinarySeparable::find_shortest_separator(&internal_key3, &internal_key4),
            InternalKey::new(b"batmann".to_vec(), 1, Operation::Put).as_bytes(),
            "The shortest separator is the key since the two keys are equal"
        );

        assert_eq!(
            BinarySeparable::find_shortest_separator(&internal_key3, &internal_key5),
            InternalKey::new(b"batmann".to_vec(), 1, Operation::Put).as_bytes(),
            "The shortest separator should the one in the first argument since no shorter user \
            key could be found"
        );

        assert_eq!(
            BinarySeparable::find_shortest_separator(&internal_key1, &internal_key6),
            InternalKey::new(b"c".to_vec(), MAX_SEQUENCE_NUMBER, Operation::Put).as_bytes(),
            "The shortest separator should be between the specified keys"
        );
    }

    #[test]
    fn internal_key_can_find_a_shortest_successor() {
        let internal_key1 = InternalKey::new(b"batmann".to_vec(), 3, Operation::Put);

        let actual = BinarySeparable::find_shortest_successor(&internal_key1);

        assert_eq!(
            InternalKey::new_for_seeking(b"c".to_vec(), MAX_SEQUENCE_NUMBER).as_bytes(),
            actual
        );
    }

    #[test]
    fn internal_key_prints_out_a_human_readable_debug_string() {
        let internal_key1 = InternalKey::new(b"batmann".to_vec(), 55, Operation::Put);
        assert_eq!(format!("{:?}", internal_key1), "batmann @ 55 : Put");

        let internal_key2 =
            InternalKey::new(b"A weird \"key\" 'with' \n".to_vec(), 1, Operation::Put);
        assert_eq!(
            format!("{:?}", internal_key2),
            "A weird \\\"key\\\" \\'with\\' \\n @ 1 : Put"
        );
    }
}
