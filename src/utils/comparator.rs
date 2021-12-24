/*!
Utilities to assist with comparing based on various characteristics. Useful for sorting by
properties different from the natural ordering provided by ordering traits e.g. [`PartialOrd`].
*/

use std::cmp::Ordering;

/// An interface for structs intended to be used as a comparator.
pub trait Comparator<T> {
    /**
    Return an ordering obtained by comparing `a` and `b`.

    Invariants:

    1. Returns [`Ordering::Greater`] if `a` > `b`
    1. Returns [`Ordering::Equal`] if `a` == `b`
    1. Returns [`Ordering::Less`] if `a` < `b`
    */
    fn compare(a: T, b: T) -> Ordering;
}
