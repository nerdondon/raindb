/**
A RainDB specific iterator implementation that has more cursor-like behavior.

The RainDB iterator differs from the [`std::iter::DoubleEndedIterator`] in that the RainDB iterator
moves one cursor back and forth on the range of values. The `DoubleEndedIterator` essentially moves
two pointers toward each other and ends iteration onces the two pointers cross.
*/
pub trait RainDbIterator {
    /// Position cursor to the first key that is at or past the target.
    fn seek(&self);

    /// Position cursor to the first element.
    fn seek_to_first(&self);

    /// Position cursor to the last element.
    fn seek_to_last(&self);

    /// Move to the next element.
    ///
    /// Returns a tuple (&K, &V) at the position moved to.
    fn next(&self) -> Option<(&Vec<u8>, &Vec<u8>)>;

    /// Move to the previous element.
    ///
    /// Returns a tuple (&K, &V) at the position moved to.
    fn prev(&self) -> Option<(&Vec<u8>, &Vec<u8>)>;

    /// Return the key and value at the current cursor position.
    ///
    /// Returns a tuple (&K, &V) at the position moved to.
    fn current(&self) -> Option<(&Vec<u8>, &Vec<u8>)>;
}
