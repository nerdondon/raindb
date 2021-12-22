use std::sync::Arc;

use nerdondon_hopscotch::concurrent_skiplist::{ConcurrentSkipList, SkipNode};

use crate::errors::RainDBError;
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

    Returns `None` if the `key` does not exist in the memtable.
    */
    fn get(&self, key: &InternalKey) -> Option<&Vec<u8>>;

    /// Return a [`RainDbIterator`] over the contents of the memtable.
    fn iter(&self) -> Box<dyn RainDbIterator<Key = InternalKey, Error = RainDBError> + '_>;
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

    fn get(&self, key: &InternalKey) -> Option<&Vec<u8>> {
        self.store.get(&key)
    }

    fn iter(&self) -> Box<dyn RainDbIterator<Key = InternalKey, Error = RainDBError> + '_> {
        Box::new(SkipListMemTableIter {
            store: Arc::clone(&self.store),
            current_node: self.store.first_node(),
        })
    }
}

/// Holds iterator state for the skip list memtable.
struct SkipListMemTableIter<'a> {
    /// A reference to the skip list backing the memtable.
    store: Arc<ConcurrentSkipList<InternalKey, Vec<u8>>>,

    /// The key-value pair that was found last.
    current_node: Option<&'a SkipNode<InternalKey, Vec<u8>>>,
}

impl<'a, 'i> RainDbIterator<'i> for SkipListMemTableIter<'a>
where
    'i: 'a,
{
    type Key = InternalKey;
    type Error = RainDBError;

    fn is_valid(&self) -> bool {
        self.current_node.is_some()
    }

    fn seek(&'a mut self, target: &Self::Key) -> Result<(), Self::Error> {
        self.current_node = self.store.find_greater_or_equal_node(target);

        Ok(())
    }

    fn seek_to_first(&'a mut self) -> Result<(), Self::Error> {
        self.current_node = self.store.first_node();

        Ok(())
    }

    fn seek_to_last(&'a mut self) -> Result<(), Self::Error> {
        self.current_node = self.store.last_node();

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
    fn prev(&'a mut self) -> Option<(&Self::Key, &Vec<u8>)> {
        if !self.is_valid() {
            return None;
        }

        let (curr_key, _) = self.current_node.unwrap().get_entry();
        self.current_node = self.store.find_less_than_node(curr_key);
        self.current()
    }

    fn current(&self) -> Option<(&Self::Key, &Vec<u8>)> {
        self.current_node.clone().map(|node| node.get_entry())
    }
}
