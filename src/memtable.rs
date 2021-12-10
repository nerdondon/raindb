use nerdondon_hopscotch::skiplist::SkipList;

use crate::errors::RainDBError;
use crate::key::LookupKey;
use crate::RainDbIterator;

/// The interface that a data structure must implement to be used as a memtable in RainDB.
pub trait MemTable: RainDbIterator<Key = LookupKey, Error = RainDBError> + Send + Sync {
    /// Returns the approximate memory usage of the memtable in bytes.
    fn approximate_memory_usage(&self) -> usize;

    /// Insert a new key-value pair into the memtable.
    fn insert(&mut self, key: LookupKey, value: Vec<u8>);

    /**
    Get the `value` for the given `key`.

    Returns `None` if the `key` does not exist in the memtable.
    */
    fn get(&self, key: &LookupKey) -> Option<&Vec<u8>>;
}

/// A memtable that is backed by a skiplist.
pub(crate) struct SkipListMemTable {
    store: SkipList<LookupKey, Vec<u8>>,
}

/// Public methods
impl SkipListMemTable {
    pub fn new() -> Self {
        Self {
            store: SkipList::new(None),
        }
    }
}

impl MemTable for SkipListMemTable {
    fn approximate_memory_usage(&self) -> usize {
        self.store.get_approx_mem_usage()
    }

    fn insert(&mut self, key: LookupKey, value: Vec<u8>) {
        self.store.insert(key, value)
    }

    fn get(&self, key: &LookupKey) -> Option<&Vec<u8>> {
        self.store.get(&key)
    }
}

/// SAFETY: This is safe because the only way to mutate a memtable is behind a mutex.
unsafe impl Send for SkipListMemTable {}

/// SAFETY: This is safe because the only way to mutate a memtable is behind a mutex.
unsafe impl Sync for SkipListMemTable {}

impl RainDbIterator for SkipListMemTable {
    type Key = LookupKey;
    type Error = RainDBError;

    fn is_valid(&self) -> bool {
        todo!()
    }

    fn seek(&mut self, target: &Self::Key) -> Result<(), Self::Error> {
        todo!()
    }

    fn seek_to_first(&mut self) -> Result<(), Self::Error> {
        todo!()
    }

    fn seek_to_last(&mut self) -> Result<(), Self::Error> {
        todo!()
    }

    fn next(&mut self) -> Option<(&Self::Key, &Vec<u8>)> {
        todo!()
    }

    fn prev(&mut self) -> Option<(&Self::Key, &Vec<u8>)> {
        todo!()
    }

    fn current(&self) -> Option<(&Self::Key, &Vec<u8>)> {
        todo!()
    }
}
