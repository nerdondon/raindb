/*!
This module provides a common interface for filter policies that can be provided at database
startup. A filter policy is an object used to create a small filter from a set of keys. These
filters are stored and consulted during reads to determine whether or not to read information from
disk. This can have significant speed savings from removing unnecessary disk seeks on database get
operations.

RainDB provides a Bloom filter based filter policy that should be adequate for most situations.
*/

use integer_encoding::FixedInt;
use std::fmt::{self, Debug};
use std::sync::Arc;

/// Trait to be implemented by filter generating structures for use with RainDB.
pub trait FilterPolicy: Debug + Send + Sync {
    /**
    The name of the filter policy.

    The name of a filter policy is recorded on disk alongside the filter in table files. This means
    that if the serialization of this filter changes in any way, the name returned by this method
    must be changed. Otherwise, incompatible filters may be passed to methods of this type.
    */
    fn get_name(&self) -> String;

    /**
    Create a filter for the set of keys provided.

    The keys potentially have duplicates.

    Returns a serialized version of the filter created that should be suitable for writing to a
    file.
    */
    fn create_filter(&self, keys: &[Vec<u8>]) -> Vec<u8>;

    /**
    Returns true if the `key` is in the provided filter.

    The provided filter is a byte vector directly from the table file and may need to be
    deserialized before being operated on.

    # Invariants

    1. The method must return true if the key was in the list of keys used to the create the filter.
    1. The method can return true or false if the key was not on the seed list, but it should aim to
       return false with a high probability.
    */
    fn key_may_match(
        &self,
        key: &[u8],
        serialized_filter: &[u8],
    ) -> Result<bool, FilterPolicyError>;
}

/// A Bloom filter based filter policy.
#[derive(Debug)]
pub struct BloomFilterPolicy {
    /**
    A sizing factor for the Bloom filter that will grow the filter capacity at the specified rate
    per key inserted into the filter.

    Specifically, `bits_per_key * n keys = the length of the bloom filter`.

    A good value for bits per key is 10, which yields a filter with ~1% false positive rate.
    */
    bits_per_key: usize,

    /**
    This is the number of hash functions used for insertion and checking.

    The formula to get this is: `k = (m / n) * ln(2)`
    where m is the number of bits in the array and n is the number of elements that will be inserted
    into the Bloom filter.
    */
    num_hash_functions: usize,
}

/// Public methods
impl BloomFilterPolicy {
    /// Create a new instance of [`BloomFilterPolicy`].
    pub fn new(bits_per_key: usize) -> Self {
        // ln(2) is approximately 0.69
        let mut num_hash_functions = (bits_per_key as f64 * 0.69).floor() as usize;

        if num_hash_functions < 1 {
            num_hash_functions = 1
        } else if num_hash_functions > 30 {
            // The number of hashing functions is rounded down to reduce the probing cost a little
            num_hash_functions = 30
        }

        Self {
            bits_per_key,
            num_hash_functions,
        }
    }
}

/// Private methods
impl BloomFilterPolicy {
    /// Generates a 32-bit hash similar to the Murmur hash.
    fn hash(val: &[u8]) -> u32 {
        let seed: u32 = 0xbc9f1d34;
        let multiplier: u32 = 0xc6a4a793;
        let rotation_factor: u32 = 24;
        let val_length = val.len() as u32;
        let mut hash: u32 = seed ^ (val_length * multiplier);

        // Read and process value in groups of 4 bytes
        let mut idx: usize = 0;
        while idx + 4 < (val_length as usize) {
            let word = u32::decode_fixed(&val[idx..idx + 4]);
            hash += word;
            hash *= multiplier;
            hash ^= hash >> 16;

            idx += 4;
        }

        // Process remaining bytes. There are at most 3 remaining since we processed in 4 byte
        // chunks above.
        let left_over = val_length - idx as u32;
        let remaining_buf = &val[idx..];
        if left_over == 3 {
            hash = hash.overflowing_add((remaining_buf[2] as u32) << 16).0;
        }

        if left_over >= 2 {
            hash = hash.overflowing_add((remaining_buf[1] as u32) << 8).0;
        }

        if left_over >= 1 {
            hash = hash.overflowing_add(remaining_buf[0] as u32).0;
            hash *= multiplier;
            hash ^= hash >> rotation_factor;
        }

        hash
    }
}

impl FilterPolicy for BloomFilterPolicy {
    fn get_name(&self) -> String {
        "RainDB.BloomFilter".to_string()
    }

    /**
    Create a filter for the set of keys provided.

    This implementation follows closely after the LevelDB implementation, particularly in it's usage
    of a in-house Murmur like hash and the usage of double hashing. Double hashing was proposed in a
    paper by [Kirsch and Mitzenmacher] where by two hashes are composed to simulate additional hash
    functions.

    However, the results of Kirsh and Mitzenmacher's research may have been accepted too naively.
    According to [research by Peter Dillinger], the K&M result is an asymptotic result and does not
    necessarily represent real-world patterns. It was found in RocksDB's similar Bloom filter that
    the false positive rate could hit a limit and not go below 0.1% false positives.

    The returned byte buffer has the following layout:

    1. 1-byte representing the number of hash functions used (a.k.a. probers)
    1. Bit vector representing the filter

    Returns a serialized version of the filter created that should be suitable for writing to a
    file.

    [Kirsch and Mitzenmacher]: https://www.eecs.harvard.edu/~michaelm/postscripts/tr-02-05.pdf
    [research by Peter Dillinger]: https://github.com/facebook/rocksdb/issues/4120#issuecomment-753771906
    */
    fn create_filter(&self, keys: &[Vec<u8>]) -> Vec<u8> {
        let num_keys = keys.len();

        // Compute Bloom filter size
        let mut filter_size_bits = num_keys * self.bits_per_key;
        if filter_size_bits < 64 {
            filter_size_bits = 64;
        }

        // The additional 7 is to make the division by 8 round up instead of down for integer
        // division.
        let filter_size_bytes = (filter_size_bits + 7) / 8;

        // The initial calculation for the number of bits needed was an exact prescription but we
        // need to deal with byte arrays, so we round up based on the calculation above.
        filter_size_bits = filter_size_bytes * 8;

        let mut hashes: Vec<u8> = vec![0; filter_size_bytes];
        for key in keys {
            let mut hash = BloomFilterPolicy::hash(key);
            // Use double-hashing to generate a sequence of hash values
            // The sequence of hashes is generated by adding a delta component of the initial hash
            // rotated 17 bits.
            let delta: u32 = (hash >> 17) | (hash << 15);
            for _ in 0..self.num_hash_functions {
                let overall_bit_offset: u32 = hash % (filter_size_bits as u32);
                let byte_offset: usize = (overall_bit_offset / 8) as usize;
                let bit_offset_in_byte = overall_bit_offset % 8;
                hashes[byte_offset] |= 1 << bit_offset_in_byte;

                hash = hash.overflowing_add(delta).0;
            }
        }

        [vec![self.num_hash_functions as u8], hashes].concat()
    }

    /**
    Returns true if the `key` is in the the provided filter.

    The provided byte buffer must have the following layout:

    1. 1-byte representing the number of hash functions used (a.k.a. probers)
    1. Bit vector representing the filter

    # Invariants

    1. The method must return true if the key was in the list of keys used to the create the filter.
    1. The method can return true or false if the key was not on the seed list, but it should aim to
       return false with a high probability.
    */
    fn key_may_match(
        &self,
        key: &[u8],
        serialized_filter: &[u8],
    ) -> Result<bool, FilterPolicyError> {
        // The filter is invalid. There should always be one element denoting the number of probers
        // used per key.
        if serialized_filter.len() < 2 {
            return Err(FilterPolicyError::Parse(
                "The provided filter does not have the proper format for deserialization."
                    .to_string(),
            ));
        }

        let (num_hash_functions, bloom_filter) = serialized_filter.split_first().unwrap();
        if *num_hash_functions != (self.num_hash_functions as u8) {
            log::warn!("The provided filter has an unexpected number of hash functions. Found {} but expected {}. This is not a breaking issue but should be considered an anomoly.",
                num_hash_functions,
                self.num_hash_functions
            );
        }

        let mut hash = BloomFilterPolicy::hash(key);
        // Use double-hashing to generate a sequence of hash values
        // The sequence of hashes is generated by adding a delta component of the initial hash
        // rotated 17 bits.
        let delta: u32 = (hash >> 17) | (hash << 15);
        for _ in 0..*num_hash_functions {
            let overall_bit_offset = hash % serialized_filter.len() as u32;
            let byte_offset: usize = (overall_bit_offset / 8) as usize;
            let bit_offset_in_byte = overall_bit_offset % 8;
            if bloom_filter[byte_offset] & (1 << bit_offset_in_byte) == 0 {
                return Ok(false);
            }

            hash = hash.overflowing_add(delta).0;
        }

        Ok(true)
    }
}

/// Filter policy errors.
#[derive(Debug)]
pub enum FilterPolicyError {
    /// Variant for errors deserializing a filter.
    Parse(String),
}

impl std::error::Error for FilterPolicyError {}

impl fmt::Display for FilterPolicyError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            FilterPolicyError::Parse(base_err) => write!(f, "{}", base_err),
        }
    }
}

/// A prefix for filter block keys.
const FILTER_BLOCK_KEY_PREFIX: &str = "filter";

/// Get the serialized name for a filter block in a table file.
pub(crate) fn get_filter_block_name(filter_policy: Arc<dyn FilterPolicy>) -> String {
    FILTER_BLOCK_KEY_PREFIX.to_string() + "." + &filter_policy.get_name()
}
