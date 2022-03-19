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
        let mut diff_idx: usize = 0;
        while diff_idx < min_prefix_length && smaller[diff_idx] == greater[diff_idx] {
            diff_idx += 1;
        }

        if diff_idx >= min_prefix_length {
            // Don't do any shortening if one key is a prefix of the other
            return smaller.to_vec();
        }

        // Check if there is a byte that we can increment to generate a separator from the prefix
        let diff_byte = smaller[diff_idx];
        if diff_byte < u8::MAX && diff_byte + 1 < greater[diff_idx] {
            let mut separator = smaller[..=diff_idx].to_vec();
            separator[diff_idx] += 1;
            assert!(separator.as_slice() < greater);

            return separator;
        }

        smaller.to_vec()
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

#[cfg(test)]
mod tests {
    use super::*;
    use pretty_assertions::assert_eq;

    #[test]
    fn for_a_byte_buffer_when_one_buffer_is_a_prefix_can_find_the_shortest_separator() {
        let smaller = b"batman";
        let greater = b"batmann";

        let actual =
            BinarySeparable::find_shortest_separator(smaller.as_slice(), greater.as_slice());

        assert_eq!(smaller, actual.as_slice());
    }

    #[test]
    fn for_a_byte_buffer_when_the_buffers_are_correctly_ordered_can_find_the_shortest_separator() {
        let smaller = b"ryu";
        let greater = b"tumtum";

        let actual =
            BinarySeparable::find_shortest_separator(smaller.as_slice(), greater.as_slice());

        assert_eq!(b"s", actual.as_slice());
    }

    #[test]
    fn for_a_byte_buffer_when_args_are_misordered_returns_the_first_arg() {
        let smaller = b"abc";
        let greater = b"def";

        let actual =
            BinarySeparable::find_shortest_separator(greater.as_slice(), smaller.as_slice());

        assert_eq!(greater, actual.as_slice());
    }

    #[test]
    fn for_a_byte_buffer_can_find_a_shortest_successor() {
        let buf = b"abc";

        let actual = BinarySeparable::find_shortest_successor(buf.as_slice());

        assert_eq!(b"b", actual.as_slice());
    }
}
