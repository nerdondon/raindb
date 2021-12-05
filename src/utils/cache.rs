/*!
This module provides a `Cache` trait for clients to provide their own cache implementations. A cache
implementing a least-recently-used (LRU) eviction policy is provided.
*/

use parking_lot::RwLock;
use std::fmt::Debug;
use std::sync::Arc;
use std::{collections::HashMap, hash::Hash};

use super::linked_list::{LinkedList, SharedNode};

/// Represents an entry in the cache.
pub trait CacheEntry<V> {
    /// Get the value of the entry.
    fn get_value(&self) -> &V;
}

/**
A cache that stores mappings from keys to values.

# Concurrency

Implementations of this trait must be thread-safe.
*/
pub trait Cache<K, V>: Debug + Send + Sync {
    /**
    Insert the key-value pair into the cache.

    If the key already existed in the cache, no update is performed.

    Returns a [`CacheEntry`] for the inserted value.
    */
    fn insert(&mut self, key: K, value: V) -> Box<dyn CacheEntry<V>>;

    /// Get the cached value for the given key
    fn get(&self, key: &K) -> Option<Box<dyn CacheEntry<V>>>;

    /// Remove the cached value for the given key.
    fn remove(&mut self, key: &K);

    /**
    A numeric ID for different clients of the cache.

    A cache may be used by different clients that are sharing the same cache. This ID is used to
    partition the key space between the clients. Typically a client will allocate a new ID at
    startup and prepend the ID to its cache keys.
    */
    fn new_id(&self) -> u64;
}

/// The inner struct holding the data structures of the LRU cache that need to be protected by a
/// lock.
#[derive(Debug)]
struct LRUCacheInner<K, V>
where
    K: Debug,
    V: Debug,
{
    /// The entries of the cache.
    cache_entries: HashMap<K, SharedNode<(K, V)>>,

    /**
    The list of cache entries in order from most recently used entries to least recently used

    Explicitly, the most recently used item is at the head of the list and the least recently
    used item is at the tail of the list.
    */
    lru_list: LinkedList<(K, V)>,

    /// The last used ID.
    last_id_given: u64,
}

/**
A fixed-size cache that has a least-recently-used eviction policy.

# Concurrency

This cache is thread-safe. It utilizes internal locks to synchronize threads.
*/
#[derive(Debug)]
pub struct LRUCache<K, V>
where
    K: Hash + Eq + Debug,
    V: Debug,
{
    /// The maximum number of entries this cache can hold.
    capacity: usize,

    /**
    The inner structures maintaining the cache entries and least recently used list.

    This nested structure contains the state that requires a lock for concurrency guarantees.
    */
    inner: RwLock<LRUCacheInner<K, V>>,
}

/// Public methods
impl<K, V> LRUCache<K, V>
where
    K: Hash + Eq + Debug,
    V: Debug,
{
    /**
    Create a new instance of the [`LRUCache`].

    # Panic

    The provided capacity must be greater than zero or the program will panic.
    */
    pub fn new(capacity: usize) -> Self {
        if capacity < 1 {
            panic!("Capacity must be greater than 0");
        }

        let inner = LRUCacheInner {
            cache_entries: HashMap::with_capacity(capacity),
            lru_list: LinkedList::new(),
            last_id_given: 0,
        };

        Self {
            capacity,
            inner: RwLock::new(inner),
        }
    }

    /// Get the current number of elements in the cache.
    pub fn len(&self) -> usize {
        self.inner.read().cache_entries.len()
    }

    /// Returns `true` if the cache is empty, otherwise `false`.
    pub fn is_empty(&self) -> bool {
        self.inner.read().cache_entries.is_empty()
    }
}

impl<K, V> Cache<K, V> for LRUCache<K, V>
where
    K: Hash + Eq + Debug + Send + Sync + 'static,
    V: Debug + Send + Sync + 'static,
{
    fn insert(&mut self, key: K, value: V) -> Box<dyn CacheEntry<V>> {
        let writable_inner = self.inner.write();
        let maybe_existing_entry = writable_inner.cache_entries.get(&key);
        if maybe_existing_entry.is_none() {
            // This is new entry
            let shared_node = writable_inner.lru_list.push_front((key, value));
            writable_inner.cache_entries.insert(key, shared_node);
        } else {
            // This key is already in the cache. Do not update the value but update the LRU list to
            // indicate an access.
            let existing_node = maybe_existing_entry.unwrap();
            writable_inner
                .lru_list
                .remove_node(Arc::clone(existing_node));
            writable_inner
                .lru_list
                .push_node_front(Arc::clone(existing_node));
        }

        if writable_inner.cache_entries.len() > self.capacity {
            // Evict least recently used
            let (evicted_key, _) = writable_inner.lru_list.pop().unwrap();
            writable_inner.cache_entries.remove(&evicted_key);
        }

        Box::new(Arc::clone(writable_inner.cache_entries.get(&key).unwrap()))
    }

    fn get(&self, key: &K) -> Option<Box<dyn CacheEntry<V>>> {
        let writable_inner = self.inner.write();
        match writable_inner.cache_entries.get(key) {
            None => return None,
            Some(node) => {
                // Update LRU list
                writable_inner.lru_list.remove_node(Arc::clone(node));
                writable_inner.lru_list.push_node_front(Arc::clone(node));

                return Some(Box::new(Arc::clone(node)));
            }
        }
    }

    fn remove(&mut self, key: &K) {
        let writable_inner = self.inner.write();
        let maybe_removed_node = writable_inner.cache_entries.remove(key);
        match maybe_removed_node {
            None => return,
            Some(removed_node) => writable_inner.lru_list.remove_node(removed_node),
        }
    }

    fn new_id(&self) -> u64 {
        let inner = self.inner.write();
        inner.last_id_given += 1;
        return inner.last_id_given;
    }
}

impl<K, V> CacheEntry<V> for SharedNode<(K, V)> {
    fn get_value(&self) -> &V {
        &self.read().element.1
    }
}
