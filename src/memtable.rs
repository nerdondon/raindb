use std::ptr::NonNull;
use std::sync::Arc;

use nerdondon_hopscotch::concurrent_skiplist::{ConcurrentSkipList, SkipNode};

use crate::errors::{RainDBError, RainDBResult};
use crate::key::InternalKey;
use crate::RainDbIterator;

/// The interface that a data structure must implement to be used as a memtable in RainDB.
pub trait MemTable: Send + Sync {
    /// Returns the approximate memory usage of the memtable in bytes.
    fn approximate_memory_usage(&self) -> usize;

    /// Insert a new key-value pair into the memtable.
    fn insert(&self, key: InternalKey, value: Vec<u8>);

    /**
    Get the `value` for the given `key`.

    Returns [`RainDBError::KeyNotFound`] if the `key` does not exist in the memtable. It returns
    `None` if the key was found but was tagged as deleted.
    */
    fn get(&self, key: &InternalKey) -> RainDBResult<Option<&Vec<u8>>>;

    /// Return a [`RainDbIterator`] over the contents of the memtable.
    fn iter(&self) -> Box<dyn RainDbIterator<Key = InternalKey, Error = RainDBError> + '_>;

    /// Returns the number of entries in the memtable.
    fn len(&self) -> usize;

    /// Returns if the memtable is empty.
    fn is_empty(&self) -> bool {
        self.len() < 1
    }
}

/// A memtable that is backed by a skiplist.
pub(crate) struct SkipListMemTable {
    /// The actual skip list backing the memtable.
    store: Arc<ConcurrentSkipList<InternalKey, Vec<u8>>>,
}

/// Public methods
impl SkipListMemTable {
    /// Create a new instance of the [`SkipListMemTable`].
    pub fn new() -> Self {
        Self {
            store: Arc::new(ConcurrentSkipList::new(None)),
        }
    }
}

impl MemTable for SkipListMemTable {
    fn approximate_memory_usage(&self) -> usize {
        self.store.get_approx_mem_usage()
    }

    fn insert(&self, key: InternalKey, value: Vec<u8>) {
        /*
        SAFETY:
        RainDB enforces that there is only a single writer adding to the memtable at a time.
        */
        unsafe { self.store.insert(key, value) }
    }

    fn get(&self, key: &InternalKey) -> RainDBResult<Option<&Vec<u8>>> {
        // The key has a sequence number that serves as an upper bound on the recency of values that
        // should be considered valid to return i.e. keys with a sequence number higher than
        // provided are not valid.
        let mut iter = self.iter();
        iter.seek(key).unwrap();

        if iter.is_valid() {
            // We only need to check the user key since the call to `seek()` above should have
            // skipped sequence numbers more recent than we want
            let (current_key, _current_val) = iter.current().unwrap();
            if current_key.get_user_key() == key.get_user_key() {
                match current_key.get_operation() {
                    crate::Operation::Put => return Ok(self.store.get(current_key)),
                    crate::Operation::Delete => return Ok(None),
                }
            }
        }

        Err(RainDBError::KeyNotFound)
    }

    fn iter(&self) -> Box<dyn RainDbIterator<Key = InternalKey, Error = RainDBError> + '_> {
        Box::new(SkipListMemTableIter {
            store: Arc::clone(&self.store),
            current_node: NonNull::new(&mut self.store.first_node() as *mut _).unwrap(),
        })
    }

    fn len(&self) -> usize {
        self.store.len()
    }
}

/**
Holds iterator state for the skip list memtable.

# Safety

This is a self-referential structure that holds a pointer (`current_node`) back to a node stored in
the memtable. It is safe for us to dereference the pointer even if the iterator or underlying
skiplist gets moved around because the skiplist is heap allocated (through the [`Arc`]).

# Design

We are back here again at the same self-referential struct issue we "fixed" in commit `789eff9`.
I'm trying to work around this by keeping raw pointers and this is incredibly painful. Another
workaround is to keep the key of the current node and reacquire the current node each time it is
needed but this is an annoying performance penalty. This seems like such a common pattern but there
seem to be so little simple answers. Rust ergonomics just break down so completely and I'm left
questioning are my safety arguments correct? Why were the invariants so easy to maintain in C++?
*/
struct SkipListMemTableIter<'a> {
    /// A reference to the skip list backing the memtable.
    store: Arc<ConcurrentSkipList<InternalKey, Vec<u8>>>,

    /// The key-value pair that was found last.
    current_node: NonNull<Option<&'a SkipNode<InternalKey, Vec<u8>>>>,
}

impl<'a> RainDbIterator for SkipListMemTableIter<'a> {
    type Key = InternalKey;
    type Error = RainDBError;

    fn is_valid(&self) -> bool {
        unsafe {
            // SAFETY: Nodes in ConcurrentSkipList are heap allocated so should not move
            self.current_node.as_ref().is_some()
        }
    }

    fn seek(&mut self, target: &Self::Key) -> Result<(), Self::Error> {
        self.current_node = NonNull::new(&mut self.store.find_greater_or_equal_node(target) as *mut _).unwrap();

        Ok(())
    }

    fn seek_to_first(&mut self) -> Result<(), Self::Error> {
        self.current_node = NonNull::new(&mut self.store.first_node() as *mut _).unwrap();

        Ok(())
    }

    fn seek_to_last(&mut self) -> Result<(), Self::Error> {
        self.current_node = NonNull::new(&mut self.store.last_node() as *mut _).unwrap();

        Ok(())
    }

    fn next(&mut self) -> Option<(&Self::Key, &Vec<u8>)> {
        if !self.is_valid() {
            return None;
        }

        self.current_node = self.current_node.and_then(|node| node.next());
        self.current()
    }

    /**
    Move to the previous element.

    Like LevelDB, there are no back links. So we simulate moving to the previous entry by doing a
    search for the last node that falls before the current key.

    Returns a tuple (&Self::Key, &V) at the position moved to. If the cursor was on the first
    element, `None` is returned.
    */
    fn prev(&mut self) -> Option<(&Self::Key, &Vec<u8>)> {
        if !self.is_valid() {
            return None;
        }

        let (curr_key, _) = self.current_node.unwrap().get_entry();
        self.current_node = NonNull::new(&mut self.store.find_less_than_node(curr_key);
        self.current()
    }

    fn current(&self) -> Option<(&Self::Key, &Vec<u8>)> {
        self.current_node.map(|node| node.get_entry())
    }
}
