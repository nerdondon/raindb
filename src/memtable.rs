use nerdondon_hopscotch::skiplist::SkipList;

use crate::key::LookupKey;

pub trait MemTable {
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

pub(crate) struct SkipListMemTable {
    store: SkipList<Vec<u8>, Vec<u8>>,
}

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
        self.store.insert(Vec::<u8>::from(&key), value)
    }

    fn get(&self, key: &LookupKey) -> Option<&Vec<u8>> {
        self.store.get(&Vec::<u8>::from(key))
    }
}
