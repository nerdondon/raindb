use crate::key::RainDbKeyType;

/**
A RainDB specific iterator implementation that has more cursor-like behavior.

The RainDB iterator differs from the [`std::iter::DoubleEndedIterator`] in that the RainDB iterator
moves one cursor back and forth on the range of values. The `DoubleEndedIterator` essentially moves
two pointers toward each other and ends iteration onces the two pointers cross.
*/
pub trait RainDbIterator<K>
where
    K: RainDbKeyType,
{
    type Error;

    /// The iterator is only valid if the cursor is currently positioned at a key-value pair.
    fn is_valid(&self) -> bool;

    /**
    Position cursor to the first key that is at or past the target.

    Returns an error if there was an issue seeking the target and sets the iterator to invalid.
    */
    fn seek(&mut self, target: &K) -> Result<(), Self::Error>;

    /**
    Position cursor to the first element.

    Returns an error if there was an issue seeking the target and sets the iterator to invalid.
    */
    fn seek_to_first(&mut self) -> Result<(), Self::Error>;

    /**
    Position cursor to the last element.

    Returns an error if there was an issue seeking the target and sets the iterator to invalid.
    */
    fn seek_to_last(&mut self) -> Result<(), Self::Error>;

    /**
    Move to the next element.

    Returns a tuple (&K, &V) at the position moved to. If the cursor was on the last element, `None`
    is returned.
    */
    fn next(&mut self) -> Option<(&K, &Vec<u8>)>;

    /**
    Move to the previous element.

    Returns a tuple (&K, &V) at the position moved to. If the cursor was on the last element, `None`
    is returned.
    */
    fn prev(&mut self) -> Option<(&K, &Vec<u8>)>;

    /**
    Return the key and value at the current cursor position.

    Returns a tuple (&K, &V) at current position if the iterator is valid. Otherwise, returns
    `None`.
    */
    fn current(&self) -> Option<(&K, &Vec<u8>)>;
}
