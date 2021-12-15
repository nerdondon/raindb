use std::cmp;

/// Utility methods that allow finding a binary boundary between two values.
pub(crate) trait BinarySeparable: Ord + Into<Vec<u8>> {
    /**
    Find the shortest binary separator between two values.

    Implementers of this method may simply return `smaller` and not do anything.

    # Invariants

    - Only return a separator if the `smaller` value is less than the `greater` value.
    - Returns `smaller` if the arguments are out of order.
    - The found separator must satisfy the following inequality:
      `smaller` <= `separator` < `greater`
    */
    fn find_shortest_separator(smaller: Self, greater: Self) -> Vec<u8>;

    /**
    Find the shortest binary successor for the provided value.

    Simple implementations may return the value serialized to a byte buffer i.e. no changes are
    made to the input.
    */
    fn find_shortest_successor(value: Self) -> Vec<u8>;
}

impl BinarySeparable for &[u8] {
    fn find_shortest_separator(smaller: Self, greater: Self) -> Vec<u8> {
        let min_prefix_length = cmp::min(smaller.len(), greater.len());
        let diff_idx: usize = 0;
        while diff_idx < min_prefix_length && smaller[diff_idx] == greater[diff_idx] {
            diff_idx += 1;
        }

        if diff_idx >= min_prefix_length {
            // Don't do any shortening if one key is a prefix of the other
            return smaller.clone().to_vec();
        }

        // Check if there is a byte that we can increment to generate a separator from the prefix
        let diff_byte = smaller[diff_idx];
        if diff_byte < u8::MAX && diff_byte + 1 < greater[diff_idx] {
            let mut separator = &smaller[..diff_idx];
            separator[diff_idx] += 1;
            assert!(separator < greater);

            return separator.to_vec();
        }

        return smaller.clone().to_vec();
    }

    fn find_shortest_successor(value: Self) -> Vec<u8> {
        // Find the first byte that can be incremented
        let mut successor = vec![];
        for byte in value.iter() {
            if *byte != u8::MAX {
                successor.push(*byte + 1);
                return successor;
            }

            successor.push(*byte);
        }

        // There was a run of u8::MAX
        successor
    }
}
