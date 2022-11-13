//! Snapshots are used to scope a request to the state of a database at a point in time.

use std::sync::Arc;

use crate::utils::linked_list::{LinkedList, SharedNode};

/**
Represents the state of the database at a particular point in time.

A snapshot is immutable and should be entirely opaque to database clients.

# Legacy

LevelDB had both an opaque interface `leveldb::Snapshot` and a concrete internal representation
`leveldb::SnapshotImpl`. LevelDB utilizes inheritance here whereas RainDB keeps a private inner
field to abstract away the internal representation. We go this route in RainDB because trait
objects are painful to deal with:

1. Trait objects are fat pointers (they store a pointer to a vtable that changes with compilation
   unit) so doing pointer identity requires thinning out the pointers first. This looks ugly AND
   seems to have its own hazards that I don't want to deal with.

1. Downcasting with [`std::any::Any`] looks really ugly.
*/
#[derive(Clone, Debug)]
pub struct Snapshot {
    inner: SharedNode<InnerSnapshot>,
}

/// Crate-only methods
impl Snapshot {
    /// Create a new instance of [`Snapshot`].
    pub(crate) fn new(inner: SharedNode<InnerSnapshot>) -> Self {
        Self { inner }
    }

    /// Get a reference to the snapshot's internal representation.
    pub(crate) fn inner(&self) -> SharedNode<InnerSnapshot> {
        Arc::clone(&self.inner)
    }

    /// Get a reference to the snapshot's sequence number.
    pub(crate) fn sequence_number(&self) -> u64 {
        self.inner.read().element.sequence_number()
    }
}

/// The internal representation of a snapshot.
#[derive(Debug)]
pub(crate) struct InnerSnapshot {
    /// The sequence number at which this snapshot was taken.
    sequence_number: u64,
}

/// Crate-only methods
impl InnerSnapshot {
    /// Create a new instance of [`InnerSnapshot`].
    pub(crate) fn new(sequence_number: u64) -> Self {
        Self { sequence_number }
    }

    /// Get a reference to the snapshot's sequence number.
    pub(crate) fn sequence_number(&self) -> u64 {
        self.sequence_number
    }
}

/// A list of snapshots.
pub(crate) struct SnapshotList {
    /// The actual list of snapshots.
    list: LinkedList<InnerSnapshot>,
}

/// Crate-only methods
impl SnapshotList {
    /// Create a new instance of [`SnapshotList`].
    pub(crate) fn new() -> Self {
        Self {
            list: LinkedList::new(),
        }
    }

    /**
    Creates a new snapshot and appends it to the list.

    # Panics

    The specified sequence number cannot be lower than any of the sequence number currently in the
    list. This should be trivially true because sequence numbers only increase during the operation
    of the database.

    # Legacy

    This is synonomous to LevelDB's `SnapshotList::New`.
    */
    pub(crate) fn new_snapshot(&mut self, sequence_number: u64) -> Snapshot {
        assert!(
            self.is_empty() || self.newest().read().element.sequence_number() <= sequence_number
        );

        let snapshot = InnerSnapshot::new(sequence_number);
        Snapshot::new(self.list.push(snapshot))
    }

    /// Remove a snapshot from the list.
    pub(crate) fn delete_snapshot(&mut self, snapshot: Snapshot) {
        self.list.remove_node(snapshot.inner());
    }

    /// Returns true if the list is currently empty.
    pub(crate) fn is_empty(&self) -> bool {
        self.list.is_empty()
    }

    /**
    Returns the oldest snapshot in the snapshot list.

    # Panics

    This method cannot be called on an empty list.
    */
    pub(crate) fn oldest(&self) -> SharedNode<InnerSnapshot> {
        self.list.head().unwrap()
    }

    /**
    Returns the newest snapshot in the snapshot list.

    # Panics

    This method cannot be called on an empty list.
    */
    pub(crate) fn newest(&self) -> SharedNode<InnerSnapshot> {
        self.list.tail().unwrap()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_new_snapshot_can_be_requested() {
        let mut snapshots = SnapshotList::new();
        let snapshot1 = snapshots.new_snapshot(1000);

        assert!(Arc::ptr_eq(&snapshot1.inner(), &snapshots.newest()));

        let snapshot2 = snapshots.new_snapshot(2000);

        assert!(Arc::ptr_eq(&snapshot2.inner(), &snapshots.newest()));
        assert!(Arc::ptr_eq(&snapshot1.inner(), &snapshots.oldest()));
    }

    #[test]
    fn snapshots_can_be_removed() {
        let mut snapshots = SnapshotList::new();
        let snapshot1 = snapshots.new_snapshot(1000);
        let snapshot2 = snapshots.new_snapshot(2000);

        snapshots.delete_snapshot(snapshot1);

        assert!(Arc::ptr_eq(&snapshot2.inner(), &snapshots.oldest()));

        snapshots.delete_snapshot(snapshot2);

        assert!(snapshots.is_empty());
    }
}
