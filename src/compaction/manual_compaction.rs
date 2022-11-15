// Copyright (c) 2022 Google LLC
//
// Use of this source code is governed by an MIT-style
// license that can be found in the LICENSE file or at
// https://opensource.org/licenses/MIT.

use std::ops::Range;

use crate::key::InternalKey;

/// Carries information for performing a manual compaction and it's completion state.
pub struct ManualCompactionConfiguration {
    /// The level to compact.
    pub level: usize,
    /// True if the compaction is completed. Otherwise, false.
    pub done: bool,
    /// The key to start compaction from. `None` means the start of the key range.
    pub begin: Option<InternalKey>,
    /// The key to end compaction at. `None` means the end of the key range.
    pub end: Option<InternalKey>,
}

/// Crate-only methods
impl ManualCompactionConfiguration {
    /// Get a reference to the key range to manually compact.
    pub(crate) fn get_key_range(&self) -> Range<Option<&InternalKey>> {
        self.begin.as_ref()..self.end.as_ref()
    }

    /// Get a clone of the key range that is being compacted.
    pub(crate) fn clone_key_range(&self) -> Range<Option<InternalKey>> {
        self.begin.clone()..self.end.clone()
    }
}
