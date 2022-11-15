// Copyright (c) 2021 Google LLC
//
// Use of this source code is governed by an MIT-style
// license that can be found in the LICENSE file or at
// https://opensource.org/licenses/MIT.

use parking_lot::RwLock;
use std::fmt::Debug;
use std::sync::{Arc, Weak};

type Link<T> = Option<SharedNode<T>>;

type WeakLink<T> = Option<Weak<RwLock<Node<T>>>>;

/// A [`Node`] wrapped in concurrency primitives.
pub type SharedNode<T> = Arc<RwLock<Node<T>>>;

/// A node in the linked list.
#[derive(Debug)]
pub struct Node<T>
where
    T: Debug,
{
    /// The element that the node represents.
    pub element: T,

    /**
    A link to the next node.

    This should not be changed except through the linked list itself.
    */
    next: Link<T>,

    /**
    A link to the previous node.

    This should not be changed except through the linked list itself.
    */
    prev: WeakLink<T>,
}

/// Public methods
impl<T> Node<T>
where
    T: Debug,
{
    /**
    Create a new [`Node`] that with empty next and previous links.

    **NOTE**: This is accessible to the crate only for testing purposes.
    */
    pub(crate) fn new(element: T) -> Self {
        Self {
            element,
            next: None,
            prev: None,
        }
    }
}

/**
A doubly-linked linked list that exposes it's structural nodes.

The structural nodes are exposed to enable things like O(1) insertion and removal.

# Concurrency

This linked list is not safe for use without external synchronization. A [`RwLock`] is used on the
nodes purely for interior mutability purposes. This could be made obvious with
[`std::cell::UnsafeCell`] but [`RwLock`] was used so that the code didn't get even more verbose.
*/
#[derive(Debug)]
pub struct LinkedList<T>
where
    T: Debug,
{
    /// The head of the list.
    head: Link<T>,

    /// The tail of the list.
    tail: Link<T>,

    /// The length of the list.
    length: usize,
}

/// Public methods
impl<T> LinkedList<T>
where
    T: Debug,
{
    /// Create a new instance of [`LinkedList`]
    pub fn new() -> Self {
        Self {
            head: None,
            tail: None,
            length: 0,
        }
    }

    /// Remove an element from the tail of the list.
    pub fn pop(&mut self) -> Option<SharedNode<T>> {
        self.tail.take().map(|old_tail_node| {
            self.tail = match old_tail_node.write().prev.take() {
                None => None,
                Some(prev_node) => Weak::upgrade(&prev_node),
            };

            match self.tail.as_mut() {
                None => {
                    // There is no new tail node so the list must be empty
                    self.head = None;
                }
                Some(new_tail_node) => {
                    // The new tail's next should now be `None` since it pointed at the old,
                    // removed tail
                    new_tail_node.write().next = None;
                }
            }

            self.length -= 1;

            old_tail_node
        })
    }

    /// Remove an element from the front of the list.
    pub fn pop_front(&mut self) -> Option<SharedNode<T>> {
        self.head.take().map(|old_head_node| {
            self.head = old_head_node.write().next.clone();

            match self.head.as_ref() {
                None => {
                    // There is no new head node so the list must be empty
                    self.tail = None;
                }
                Some(new_head_node) => {
                    // The new head's previous should now be `None` since it pointed at the old,
                    // removed head
                    new_head_node.write().prev = None;
                }
            }

            self.length -= 1;

            old_head_node
        })
    }

    /// Push an element onto the back of the list.
    pub fn push(&mut self, element: T) -> SharedNode<T> {
        let new_node = Arc::new(RwLock::new(Node::new(element)));
        self.push_node(Arc::clone(&new_node));

        new_node
    }

    /// Push a node onto the back of the list.
    pub fn push_node(&mut self, node: SharedNode<T>) {
        node.write().prev = self.tail.as_ref().map(Arc::downgrade);
        node.write().next = None;

        match self.tail.as_ref() {
            Some(tail_node) => {
                // Fix existing links
                tail_node.write().next = Some(Arc::clone(&node))
            }
            None => self.head = Some(Arc::clone(&node)),
        }

        self.tail = Some(node);
        self.length += 1;
    }

    /// Push an element onto the front of the list.
    pub fn push_front(&mut self, element: T) -> SharedNode<T> {
        let new_node = Arc::new(RwLock::new(Node::new(element)));
        self.push_node_front(Arc::clone(&new_node));

        new_node
    }

    /// Push a node onto the front of the list.
    pub fn push_node_front(&mut self, node: SharedNode<T>) {
        node.write().prev = None;
        node.write().next = self.head.clone();

        match self.head.as_ref() {
            Some(head_node) => {
                // Fix existing links
                head_node.write().prev = Some(Arc::downgrade(&node));
            }
            None => self.tail = Some(Arc::clone(&node)),
        }

        self.head = Some(node);
        self.length += 1;
    }

    /// Remove the given node from the linked list.
    pub fn remove_node(&mut self, target_node: SharedNode<T>) {
        let mutable_target = target_node.write();
        let maybe_previous_node = match mutable_target.prev.as_ref() {
            None => None,
            Some(weak_prev) => Weak::upgrade(weak_prev),
        };

        // Fix the links of the previous and next nodes so that they point at each other instead of
        // the node we are removing
        match maybe_previous_node.clone() {
            Some(previous_node) => {
                previous_node.write().next = mutable_target.next.clone();
            }
            None => {
                // Only head nodes have no previous link
                self.head = mutable_target.next.clone();
            }
        }

        match mutable_target.next.as_ref() {
            Some(next_node) => {
                next_node.write().prev = mutable_target.prev.clone();
            }
            None => {
                // Only tail nodes have no next link
                self.tail = maybe_previous_node;
            }
        }

        self.length -= 1;
    }

    /// Get the length of the list.
    pub fn len(&self) -> usize {
        self.length
    }

    /// Returns true if the list is empty, otherwise false.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Return an iterator over the nodes of the linked list.
    pub fn iter(&self) -> NodeIter<T> {
        NodeIter {
            next: self.head.as_ref().cloned(),
        }
    }

    /// Get a reference to the first node of the linked list.
    pub fn head(&self) -> Link<T> {
        self.head.as_ref().cloned()
    }

    /// Get a reference to the last node of the linked list.
    pub fn tail(&self) -> Link<T> {
        self.tail.as_ref().cloned()
    }
}

impl<T> Drop for LinkedList<T>
where
    T: Debug,
{
    fn drop(&mut self) {
        while self.pop_front().is_some() {}
    }
}

/**
An iterator adapter to keep state for iterating the linked list.

Created by calling [`LinkedList::iter`].
*/
pub struct NodeIter<T>
where
    T: Debug,
{
    /// The next value of the iterator.
    next: Option<SharedNode<T>>,
}

impl<T> Iterator for NodeIter<T>
where
    T: Debug,
{
    type Item = SharedNode<T>;

    fn next(&mut self) -> Option<Self::Item> {
        self.next.take().map(|current_node| {
            self.next = current_node.read().next.as_ref().map(Arc::clone);

            current_node
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pretty_assertions::assert_eq;

    #[test]
    fn single_threaded_empty_list_returns_zero_length() {
        let list = LinkedList::<u64>::new();
        assert_eq!(list.len(), 0);
    }

    #[test]
    fn single_threaded_can_push_elements() {
        let mut list = LinkedList::<u64>::new();

        let mut pushed = list.push(1);
        assert_eq!(pushed.read().element, 1);
        assert_eq!(list.len(), 1);

        pushed = list.push(2);
        assert_eq!(pushed.read().element, 2);
        assert_eq!(list.len(), 2);

        pushed = list.push(3);
        assert_eq!(pushed.read().element, 3);
        assert_eq!(list.len(), 3);
    }

    #[test]
    fn single_threaded_can_pop_elements() {
        let mut list = LinkedList::<u64>::new();
        list.push(1);
        list.push(2);
        list.push(3);
        assert_eq!(list.len(), 3);

        assert_eq!(list.pop().unwrap().read().element, 3);
        assert_eq!(list.len(), 2);

        assert_eq!(list.pop().unwrap().read().element, 2);
        assert_eq!(list.len(), 1);

        assert_eq!(list.pop().unwrap().read().element, 1);
        assert_eq!(list.len(), 0);

        assert!(list.pop().is_none());
    }

    #[test]
    fn single_threaded_can_push_elements_to_the_front() {
        let mut list = LinkedList::<u64>::new();

        list.push_front(1);
        assert_eq!(list.len(), 1);
        list.push_front(2);
        assert_eq!(list.len(), 2);
        list.push_front(3);
        assert_eq!(list.len(), 3);
    }

    #[test]
    fn single_threaded_can_pop_elements_from_the_front() {
        let mut list = LinkedList::<u64>::new();
        list.push(1);
        list.push(2);
        list.push(3);
        assert_eq!(list.len(), 3);

        assert_eq!(list.pop_front().unwrap().read().element, 1);
        assert_eq!(list.len(), 2);

        assert_eq!(list.pop_front().unwrap().read().element, 2);
        assert_eq!(list.len(), 1);

        assert_eq!(list.pop_front().unwrap().read().element, 3);
        assert_eq!(list.len(), 0);

        assert!(list.pop_front().is_none());
    }

    #[test]
    fn single_threaded_list_can_unlink_head() {
        let mut list = LinkedList::<u64>::new();
        let pushed = list.push(1);
        list.push(2);
        list.push(3);

        list.remove_node(pushed);

        assert_eq!(list.len(), 2);
        assert_eq!(list.pop_front().unwrap().read().element, 2);
    }

    #[test]
    fn single_threaded_list_can_unlink_tail() {
        let mut list = LinkedList::<u64>::new();
        list.push(1);
        list.push(2);
        let pushed = list.push(3);

        list.remove_node(pushed);

        assert_eq!(list.len(), 2);
        assert_eq!(list.pop().unwrap().read().element, 2);
    }

    #[test]
    fn single_threaded_list_can_unlink_node_from_middle() {
        let mut list = LinkedList::<u64>::new();
        list.push(1);
        let pushed = list.push(2);
        list.push(3);

        list.remove_node(pushed);

        assert_eq!(list.len(), 2);
        assert_eq!(list.pop_front().unwrap().read().element, 1);
    }
}
