// Copyright (c) 2021 Google LLC
//
// Use of this source code is governed by an MIT-style
// license that can be found in the LICENSE file or at
// https://opensource.org/licenses/MIT.

/*!
This module provides a `Cache` trait for clients to provide their own cache implementations. A cache
implementing a least-recently-used (LRU) eviction policy is provided.
*/

use parking_lot::{MappedRwLockReadGuard, RwLock, RwLockReadGuard};
use std::fmt::Debug;
use std::sync::Arc;
use std::{collections::HashMap, hash::Hash};

use super::linked_list::{LinkedList, SharedNode};

/// Represents an entry in the cache.
pub trait CacheEntry<V> {
    /// Get the value of the entry.
    fn get_value(&self) -> MappedRwLockReadGuard<V>;
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
    fn insert(&self, key: K, value: V) -> Box<dyn CacheEntry<V>>;

    /// Get the cached value for the given key. Returns [`None`] if the key is not in the cache.
    fn get(&self, key: &K) -> Option<Box<dyn CacheEntry<V>>>;

    /// Remove the cached value for the given key.
    fn remove(&self, key: &K);

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
        assert!(capacity > 1, "Capacity must be greater than 0");

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
    K: Clone + Hash + Eq + Debug + Send + Sync + 'static,
    V: Debug + Send + Sync + 'static,
{
    fn insert(&self, key: K, value: V) -> Box<dyn CacheEntry<V>> {
        let mut writable_inner = self.inner.write();
        if let Some(existing_node) = writable_inner.cache_entries.get(&key).map(Arc::clone) {
            // This key is already in the cache, remove it from the cache so that new handles
            // cannot be obtained to the old value.
            writable_inner.lru_list.remove_node(existing_node);
        }

        // Create a new node and add that to the cache.
        // We do this even for existing entries so that clients with a cache handle to the old
        // keep a consistent view.
        let shared_node = writable_inner.lru_list.push_front((key.clone(), value));
        writable_inner
            .cache_entries
            .insert(key.clone(), shared_node);

        if writable_inner.cache_entries.len() > self.capacity {
            // Evict least recently used
            let lru_node = writable_inner.lru_list.pop().unwrap();
            let (evicted_key, _) = &lru_node.read().element;
            writable_inner.cache_entries.remove(evicted_key);
        }

        Box::new(Arc::clone(writable_inner.cache_entries.get(&key).unwrap()))
    }

    fn get(&self, key: &K) -> Option<Box<dyn CacheEntry<V>>> {
        let mut writable_inner = self.inner.write();
        let maybe_node = writable_inner.cache_entries.get(key).map(Arc::clone);

        if let Some(node) = maybe_node {
            // Update LRU list
            writable_inner.lru_list.remove_node(Arc::clone(&node));
            writable_inner.lru_list.push_node_front(Arc::clone(&node));

            return Some(Box::new(node));
        }

        None
    }

    fn remove(&self, key: &K) {
        let mut writable_inner = self.inner.write();
        let maybe_removed_node = writable_inner.cache_entries.remove(key);
        match maybe_removed_node {
            None => {}
            Some(removed_node) => writable_inner.lru_list.remove_node(removed_node),
        }
    }

    fn new_id(&self) -> u64 {
        let mut inner = self.inner.write();
        inner.last_id_given += 1;
        inner.last_id_given
    }
}

impl<K, V> CacheEntry<V> for SharedNode<(K, V)>
where
    K: Debug,
    V: Debug,
{
    fn get_value(&self) -> MappedRwLockReadGuard<V> {
        RwLockReadGuard::map(self.read(), |node| &node.element.1)
    }
}

#[cfg(test)]
mod lru_cache_tests {
    use pretty_assertions::{assert_eq, assert_ne};

    use super::*;

    #[test]
    fn can_insert_and_retrieve_elements_from_the_cache() {
        let cache: LRUCache<u64, u64> = LRUCache::new(10);

        assert!(cache.is_empty());
        assert!(
            cache.get(&20).is_none(),
            "Should not be able to get a value not in the cache"
        );

        cache.insert(0, 10);
        assert_eq!(cache.len(), 1);
        assert_eq!(*cache.get(&0).unwrap().get_value(), 10);
        assert!(cache.get(&20).is_none());

        cache.insert(1, 11);
        assert_eq!(cache.len(), 2);
        assert_eq!(*cache.get(&1).unwrap().get_value(), 11);

        cache.insert(0, 12);
        assert_eq!(cache.len(), 2);
        assert_eq!(*cache.get(&0).unwrap().get_value(), 12);
    }

    #[test]
    fn can_remove_elements_from_the_cache() {
        let cache: LRUCache<u64, u64> = LRUCache::new(10);

        cache.remove(&0);
        assert!(cache.is_empty());

        cache.insert(0, 10);
        cache.insert(1, 11);
        cache.insert(2, 12);
        assert_eq!(*cache.get(&0).unwrap().get_value(), 10);
        assert_eq!(cache.len(), 3);

        cache.remove(&0);
        assert_eq!(cache.len(), 2);
        assert!(cache.get(&0).is_none());
        assert_eq!(*cache.get(&1).unwrap().get_value(), 11);
        assert_eq!(*cache.get(&2).unwrap().get_value(), 12);

        // Removing after the key has already been removed should be a no-op
        cache.remove(&0);
        assert_eq!(cache.len(), 2);
        assert_eq!(*cache.get(&1).unwrap().get_value(), 11);
        assert_eq!(*cache.get(&2).unwrap().get_value(), 12);
    }

    #[test]
    fn cache_handles_stay_valid_even_after_eviction_from_cache() {
        let cache: LRUCache<u64, u64> = LRUCache::new(10);
        for idx in 0..10 {
            cache.insert(idx, idx);
        }

        let handle1 = cache.get(&1).unwrap();
        assert_eq!(*handle1.get_value(), 1);

        cache.insert(1, 11);
        let handle2 = cache.get(&1).unwrap();
        assert_eq!(
            *handle1.get_value(),
            1,
            "The old handle should have a consistent view of the old value"
        );
        assert_eq!(
            *handle2.get_value(),
            11,
            "New lookups should get the most recent value"
        );
        assert_eq!(
            cache.len(),
            10,
            "The old handle should not be in the cache anymore"
        );

        drop(handle1);
        assert_eq!(
            cache.len(),
            10,
            "Dropping the old handle should not affect the cache"
        );

        cache.remove(&1);
        assert!(cache.get(&1).is_none());
        assert_eq!(
            *handle2.get_value(),
            11,
            "Live handles to cache entries continue to be valid even after the entry is removed \
            from the cache"
        );
        assert_eq!(cache.len(), 9);

        drop(handle2);
        assert_eq!(
            cache.len(),
            9,
            "Dropping the remaining handles to a removed cache entry does not affect the cache"
        );
    }

    #[test]
    fn when_filled_the_cache_evicts_the_least_recently_used_entry() {
        let cache: LRUCache<u64, u64> = LRUCache::new(10);
        for idx in 0..10 {
            cache.insert(idx, idx);
        }

        cache.get(&0);
        cache.insert(10, 10);

        assert_eq!(*cache.get(&0).unwrap().get_value(), 0);
        assert!(cache.get(&1).is_none());
    }

    #[test]
    fn generates_unique_shard_ids() {
        let cache: LRUCache<u64, u64> = LRUCache::new(10);

        let id1 = cache.new_id();
        let id2 = cache.new_id();

        assert_ne!(id1, id2);
    }
}
