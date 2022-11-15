// Copyright (c) 2021 Google LLC
//
// Use of this source code is governed by an MIT-style
// license that can be found in the LICENSE file or at
// https://opensource.org/licenses/MIT.

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
    /**
    Generates a 32-bit hash similar to the Murmur hash.

    # Legacy

    We use wrapping operations to emulate C++ behavior from LevelDB.
    */
    fn hash(val: &[u8]) -> u32 {
        let seed: u32 = 0xbc9f1d34;
        let multiplier: u32 = 0xc6a4a793;
        let rotation_factor: u32 = 24;
        let val_length = val.len() as u32;
        let mut hash: u32 = seed ^ (val_length.wrapping_mul(multiplier));

        // Read and process value in groups of 4 bytes
        let mut idx: usize = 0;
        while idx + 4 <= val.len() {
            let word = u32::decode_fixed(&val[idx..idx + 4]);
            hash = hash.wrapping_add(word).wrapping_mul(multiplier);
            hash ^= hash >> 16;

            idx += 4;
        }

        // Process remaining bytes. There are at most 3 remaining since we processed in 4 byte
        // chunks above.
        let left_over = val.len() - idx;
        let remaining_buf = &val[idx..];
        if left_over == 3 {
            hash = hash.wrapping_add((remaining_buf[2] as u32) << 16);
        }

        if left_over >= 2 {
            hash = hash.wrapping_add((remaining_buf[1] as u32) << 8);
        }

        if left_over >= 1 {
            hash = hash
                .wrapping_add(remaining_buf[0] as u32)
                .wrapping_mul(multiplier);
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
            // There can be a high false positive rate when there are too little bits, so we
            // enforce a minimum length.
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

                hash = hash.wrapping_add(delta);
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
        let filter_length_bits = bloom_filter.len() * 8;
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
            let overall_bit_offset = hash % filter_length_bits as u32;
            let byte_offset: usize = (overall_bit_offset / 8) as usize;
            let bit_offset_in_byte = overall_bit_offset % 8;
            if bloom_filter[byte_offset] & (1 << bit_offset_in_byte) == 0 {
                return Ok(false);
            }

            hash = hash.wrapping_add(delta);
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

#[cfg(test)]
mod bloom_filter_tests {
    use super::*;
    use pretty_assertions::assert_eq;

    #[test]
    fn hash_function_produces_expected_hashes() {
        // Test values from LevelDB hash_test.cc to check for compatibility
        let val1 = [0x62];
        let val2 = [0xc3, 0x97];
        let val3 = [0xe2, 0x99, 0xa5];
        let val4 = [0xe1, 0x80, 0xb9, 0x32];

        assert_eq!(BloomFilterPolicy::hash(&[]), 0xbc9f1d34);
        assert_eq!(BloomFilterPolicy::hash(&val1), 0xef1345c4);
        assert_eq!(BloomFilterPolicy::hash(&val2), 0x5b663814);
        assert_eq!(BloomFilterPolicy::hash(&val3), 0x323c078f);
        assert_eq!(BloomFilterPolicy::hash(&val4), 0xed21633a);
    }

    #[test]
    fn with_an_empty_filter_returns_false_for_any_request() {
        let bloom_filter_policy = BloomFilterPolicy::new(10);
        let filter = bloom_filter_policy.create_filter(&[]);

        assert!(!bloom_filter_policy
            .key_may_match(b"hello", filter.as_slice())
            .unwrap());

        assert!(!bloom_filter_policy
            .key_may_match(b"world", filter.as_slice())
            .unwrap());
    }

    #[test]
    fn with_an_small_filter_returns_expected_responses() {
        let bloom_filter_policy = BloomFilterPolicy::new(10);
        let keys = [b"hello".to_vec(), b"world".to_vec()];
        let filter = bloom_filter_policy.create_filter(&keys);

        assert!(bloom_filter_policy
            .key_may_match(b"hello", filter.as_slice())
            .unwrap());

        assert!(bloom_filter_policy
            .key_may_match(b"world", filter.as_slice())
            .unwrap());

        assert!(!bloom_filter_policy
            .key_may_match(b"batmann", filter.as_slice())
            .unwrap());

        assert!(!bloom_filter_policy
            .key_may_match(b"robin", filter.as_slice())
            .unwrap());
    }

    #[test]
    fn bloom_filters_are_created_with_acceptable_false_positive_rates_for_varying_lengths() {
        const BITS_PER_KEY: usize = 10;
        let bloom_filter_policy = BloomFilterPolicy::new(BITS_PER_KEY);

        // The number of filters that significantly exceed the false positive rate
        let mut num_mediocre_filters: usize = 0;
        let mut num_good_filters: usize = 0;

        let mut num_keys: usize = 1;
        while num_keys <= 10_000 {
            // Build up keys we want to build the filter for
            let mut keys: Vec<Vec<u8>> = Vec::with_capacity(num_keys);
            for idx in 0..num_keys {
                keys.push(u32::encode_fixed_vec(idx as u32));
            }

            let filter = bloom_filter_policy.create_filter(&keys);
            // Calculate filter size based on `BITS_PER_KEY`
            let filter_size: usize = ((num_keys * 10) / 8) + 40;
            assert!(filter.len() <= filter_size);

            // All added keys must be found in the filter
            for idx in 0..num_keys {
                let key = u32::encode_fixed_vec(idx as u32);
                assert!(
                    bloom_filter_policy.key_may_match(&key, &filter).unwrap(),
                    "Expected to find {idx} in the filter."
                );
            }

            // Check that false positive rate is within acceptable bounds i.e. at most 2%
            let false_positive_rate = check_false_positive_rate(&bloom_filter_policy, &filter);
            assert!(
                false_positive_rate <= 0.02,
                "The false positive rate exceeded 2%. Num keys {num_keys} had a false positive \
                rate of {false_positive_rate}."
            );
            if false_positive_rate > 0.0125 {
                num_mediocre_filters += 1;
            } else {
                num_good_filters += 1;
            }

            num_keys = next_num_keys(num_keys);
        }

        println!(
            "There are {num_mediocre_filters} mediocre filters and {num_good_filters} good \
            filters."
        );

        assert!(
            num_mediocre_filters <= (num_good_filters / 5),
            "If there are any mediocre filters, there must be at least 5 times as many good \
            filters (i.e. filters that have < 1.25% false positive rate)."
        );
    }

    /// Get the next filter length for the false positive test.
    fn next_num_keys(prev_num_keys: usize) -> usize {
        let mut num_keys = prev_num_keys;
        if num_keys < 10 {
            num_keys += 1
        } else if num_keys < 100 {
            num_keys += 10;
        } else if num_keys < 1000 {
            num_keys += 100;
        } else {
            num_keys += 1000;
        }

        num_keys
    }

    fn check_false_positive_rate(policy: &BloomFilterPolicy, filter: &[u8]) -> f64 {
        const NUM_ITERATIONS: usize = 10_000;
        let mut matches: f64 = 0.0;
        for idx in 0..NUM_ITERATIONS {
            if policy
                .key_may_match(&u32::encode_fixed_vec(idx as u32 + 1_000_000_000), filter)
                .unwrap()
            {
                matches += 1.0;
            }
        }

        matches / (NUM_ITERATIONS as f64)
    }
}
