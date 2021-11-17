use parking_lot::RwLock;
use std::sync::{Arc, Weak};

type Link<T> = Option<SharedNode<T>>;

type WeakLink<T> = Option<Weak<RwLock<Node<T>>>>;

/// A [`Node`] wrapped in concurrency primitives.
pub type SharedNode<T> = Arc<RwLock<Node<T>>>;

/// A node in the linked list.
pub struct Node<T> {
    /// The element that the node represents.
    pub element: T,

    /// A link to the next node.
    pub next: Link<T>,

    /// A link to the previous node.
    pub prev: WeakLink<T>,
}

impl<T> Node<T> {
    /// Create a new [`Node`] that with empty next and previous links.
    pub fn new(element: T) -> Self {
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
*/
pub struct LinkedList<T> {
    head: Link<T>,
    tail: Link<T>,
    length: usize,
}

impl<T> LinkedList<T> {
    /// Create a new instance of [`LinkedList`]
    pub fn new() -> Self {
        Self {
            head: None,
            tail: None,
            length: 0,
        }
    }

    /// Remove an element from the tail of the list.
    pub fn pop(&mut self) -> Option<T> {
        self.tail.take().map(|tail_node| {
            let old_tail_node = tail_node.write();
            self.tail = match old_tail_node.prev {
                None => None,
                Some(prev_node) => Weak::upgrade(&prev_node),
            };

            match self.tail {
                None => {
                    // There is no new tail node so the list must be empty
                    self.head = None;
                }
                Some(new_tail_node) => {
                    // The new head's previous should now be `None` since it pointed at the old,
                    // removed head
                    new_tail_node.write().prev = None;
                }
            }

            self.length -= 1;

            old_tail_node.element
        })
    }

    /// Remove an element from the front of the list.
    pub fn pop_front(&mut self) -> Option<T> {
        self.head.take().map(|head_node| {
            let old_head_node = head_node.write();
            self.head = old_head_node.next;

            match self.head {
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

            old_head_node.element
        })
    }

    /// Push an element onto the back of the list.
    pub fn push(&mut self, element: T) -> SharedNode<T> {
        let mut new_node = Arc::new(RwLock::new(Node::new(element)));
        self.push_node(Arc::clone(&new_node));

        new_node
    }

    /// Push a node onto the back of the list.
    pub fn push_node(&mut self, node: SharedNode<T>) {
        node.write().prev = self.tail.map(|tail| Arc::downgrade(&tail));
        node.write().next = None;
        let maybe_tail_node = self.tail.map(|tail_node| tail_node.write());

        match maybe_tail_node {
            Some(tail_node) => {
                // Fix existing links
                tail_node.next = Some(node)
            }
            None => self.head = Some(node),
        }

        self.tail = Some(node);
        self.length += 1;
    }

    /// Push an element onto the front of the list.
    pub fn push_front(&mut self, element: T) -> SharedNode<T> {
        let mut new_node = Arc::new(RwLock::new(Node::new(element)));
        self.push_node_front(Arc::clone(&new_node));

        new_node
    }

    /// Push a node onto the front of the list.
    pub fn push_node_front(&mut self, node: SharedNode<T>) {
        node.write().prev = None;
        node.write().next = self.head;
        let maybe_head_node = self.head.map(|head_node| head_node.write());

        match maybe_head_node {
            Some(head_node) => {
                // Fix existing links
                head_node.prev = Some(Arc::downgrade(&node))
            }
            None => self.head = Some(node),
        }

        self.head = Some(node);
        self.length += 1;
    }

    /// Remove the given node from the linked list.
    pub fn remove_node(&mut self, mutable_node: SharedNode<T>) {
        let mutable_node = mutable_node.write();
        let maybe_next_node = mutable_node.next.map(|node| node.write());
        let maybe_previous_node = match mutable_node.prev {
            None => None,
            Some(weak_prev) => Weak::upgrade(&weak_prev).map(|prev_node| prev_node.write()),
        };

        // Fix the links of the previous and next nodes so that they point at each other instead of
        // the node we are removing
        if maybe_previous_node.is_some() {
            let previous_node = maybe_previous_node.unwrap();
            previous_node.next = mutable_node.next;
        } else {
            // Only head nodes have no previous link
            self.head = mutable_node.next;
        }

        if maybe_next_node.is_some() {
            let next_node = maybe_next_node.unwrap();
            next_node.prev = mutable_node.prev;
        } else {
            // Only tail nodes have no next link
            self.tail = match mutable_node.prev {
                Some(prev_node) => Weak::upgrade(&prev_node),
                None => None,
            };
        }

        self.length -= 1;
    }

    /// Get the length of the list.
    pub fn len(&self) -> usize {
        self.length
    }

    /// Returns true if the list is empty, otherwise false.
    pub fn is_empty(&self) -> bool {
        self.length <= 0
    }
}

impl<T> Drop for LinkedList<T> {
    fn drop(&mut self) {
        while self.pop_front().is_some() {}
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
        let list = LinkedList::<u64>::new();

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
        let list = LinkedList::<u64>::new();
        list.push(1);
        list.push(2);
        list.push(3);
        assert_eq!(list.len(), 3);

        assert_eq!(list.pop(), Some(3));
        assert_eq!(list.len(), 2);

        assert_eq!(list.pop(), Some(2));
        assert_eq!(list.len(), 1);

        assert_eq!(list.pop(), Some(1));
        assert_eq!(list.len(), 0);

        assert_eq!(list.pop(), None);
    }

    #[test]
    fn single_threaded_can_push_elements_to_the_front() {
        let list = LinkedList::<u64>::new();

        list.push_front(1);
        assert_eq!(list.len(), 1);
        list.push_front(2);
        assert_eq!(list.len(), 2);
        list.push_front(3);
        assert_eq!(list.len(), 3);
    }

    #[test]
    fn single_threaded_can_pop_elements_from_the_front() {
        let list = LinkedList::<u64>::new();
        list.push(1);
        list.push(2);
        list.push(3);
        assert_eq!(list.len(), 3);

        assert_eq!(list.pop_front(), Some(1));
        assert_eq!(list.len(), 2);

        assert_eq!(list.pop_front(), Some(2));
        assert_eq!(list.len(), 1);

        assert_eq!(list.pop_front(), Some(3));
        assert_eq!(list.len(), 0);

        assert_eq!(list.pop_front(), None);
    }

    #[test]
    fn single_threaded_list_can_unlink_head() {
        let list = LinkedList::<u64>::new();
        let mut pushed = list.push(1);
        list.push(2);
        list.push(3);

        list.remove_node(pushed);

        assert_eq!(list.len(), 2);
        assert_eq!(list.pop_front(), Some(2));
    }

    #[test]
    fn single_threaded_list_can_unlink_tail() {
        let list = LinkedList::<u64>::new();
        list.push(1);
        list.push(2);
        let mut pushed = list.push(3);

        list.remove_node(pushed);

        assert_eq!(list.len(), 2);
        assert_eq!(list.pop(), Some(2));
    }

    #[test]
    fn single_threaded_list_can_unlink_node_from_middle() {
        let list = LinkedList::<u64>::new();
        list.push(1);
        let mut pushed = list.push(2);
        list.push(3);

        list.remove_node(pushed);

        assert_eq!(list.len(), 2);
        assert_eq!(list.pop_front(), None);
    }
}
