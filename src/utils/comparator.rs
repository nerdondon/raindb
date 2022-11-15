// Copyright (c) 2021 Google LLC
//
// Use of this source code is governed by an MIT-style
// license that can be found in the LICENSE file or at
// https://opensource.org/licenses/MIT.

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
