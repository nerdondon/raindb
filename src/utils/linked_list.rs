use std::ptr::NonNull;

pub type Link<T> = Option<NonNull<Node<T>>>;

/// A node in the linked list.
pub struct Node<T> {
    /// The element that the node represents.
    pub element: T,

    /// A link to the next node.
    pub next: Link<T>,

    /// A link to the previous node.
    pub prev: Link<T>,

    /**
    Set to true if the node is not part of a [`LinkedList`].

    This value is important for deallocating a node. The node's memory will only be deallocated
    if `is_orphan` is set to `true`.
    */
    is_orphan: bool,
}

impl<T> Node<T> {
    /// Create a new [`Node`] that with empty next and previous links.
    pub fn new(element: T) -> Self {
        Self {
            element,
            next: None,
            prev: None,
            is_orphan: false,
        }
    }

    /// Set the node as orphaned.
    pub fn set_orphaned(&mut self) {
        self.is_orphan = true;
    }
}

impl<T> Drop for Node<T> {
    fn drop(&mut self) {
        // Do not drop the node if it is still part of a list.
        if !self.is_orphan {
            return;
        }

        // Get a raw pointer to this node's memory allocation
        let node_ptr = NonNull::new(self as *mut Node<T>).unwrap();

        /*
        Re-box the allocation the pointer represents so that it can get dropped. Insertions
        leak the boxed node when it is created.
        */
        let current_node = unsafe {
            /*
            SAFETY:
            The node was leaked out of a [`Box`] when it was created. This means that the allocation
            will essentially have a static lifetime (duration of the program). Nothing else in
            [`LinkedList`] will re-box and deallocate the memory so re-boxing here and dropping is
            safe.
            */
            Box::from_raw(node_ptr.as_mut())
        };
    }
}

/**
A doubly-linked linked list that exposes it's structural nodes.

The structural nodes are exposed to enable things like O(1) insertion and removal.

# Safety

This linked list exposes it's structural nodes for performance reasons, however clients should be
careful about keeping references to nodes. There are no guarantees if the node reference is still
valid. Specifically, the links of the node may be invalid. Therefore, methods returning [`Node`]'s
are marked unsafe.
*/
pub struct LinkedList<T> {
    head: Link<T>,
    tail: Link<T>,
    length: usize,
}

impl<T> LinkedList<T> {
    /// Create a new instance of [`LinkedList`]
    pub fn new(head: Link<T>, tail: Link<T>) -> Self {
        Self {
            head: None,
            tail: None,
            length: 0,
        }
    }

    /**
    Remove an element from the front of the list.

    # Safety

    There are no guarantees if the node reference is still valid. Therefore, methods returning
    [`Node`]'s are marked unsafe.
    */
    pub unsafe fn pop_front(&mut self) -> Option<T> {
        self.head.take().map(|head_ptr| {
            /*
            SAFETY:
            Only dereferencing pointers to nodes is done and nodes that exist in the list are
            intrinsically valid.
            */
            unsafe {
                let old_head_node = head_ptr.as_mut();
                self.head = old_head_node.next;

                if self.head.is_none() {
                    self.tail = None;
                } else {
                    // The new head's previous should now be `None` since it pointed at the old,
                    // removed head
                    let new_head_node = self.head.as_mut().unwrap().as_mut();
                    new_head_node.prev = None;
                }

                self.length -= 1;
                old_head_node.element
            }
        })
    }

    /// Remove an element from the tail of the list.
    pub unsafe fn pop(&mut self) -> Option<T> {
        if self.tail.is_none() {
            return None;
        }

        self.tail.take().map(|tail_ptr| {});

        Ok(())
    }

    /// Push an element onto the back of the list.
    pub fn push(&mut self, element: T) -> &mut Node<T> {
        let mut new_node = Box::new(Node::new(element));
        new_node.prev = self.tail;

        /*
        `Box::leak` is called so that the node does not get deallocated at the end of the function.
        The `LinkedList::remove_node` method will ensure to reform the box from the pointer so that
        node is de-allocated on removal.
        */
        let node_ptr = NonNull::new(Box::leak(new_node)).unwrap();

        if self.tail.is_some() {
            // SAFETY: Nodes that exist in the list are intrinsically valid.
            unsafe {
                let tail_node = self.tail.as_mut().unwrap().as_mut();
                tail_node.next = Some(node_ptr);
            }
        } else {
            self.head = Some(node_ptr);
        }

        self.tail = Some(node_ptr);
        self.length += 1;

        return &mut new_node;
    }

    /// Push an element onto the front of the list.
    pub fn push_front(&mut self, elem: T) {
        self.length += 1;
    }

    /// Remove the given node from the linked list.
    pub fn remove_node(&mut self, node: &mut Node<T>) {
        if node.is_orphan {
            // Do nothing for orphaned nodes because their links are not guaranteed to be valid.
            return;
        }

        /*
        SAFETY: Links of nodes still in the linked list are guaranteed to still be valid since
        `remove_node` is the only place we set a node to orphaned besides for when the entire list
        is being dropped.
        */
        unsafe {
            // Fix the links of the previous and next nodes so that they point at each other instead of
            // the node we are removing
            if node.prev.is_some() {
                let previous_node = node.prev.as_mut().unwrap().as_mut();
                previous_node.next = node.next;
            }

            if node.next.is_some() {
                let next_node = node.next.as_mut().unwrap().as_mut();
                next_node.prev = node.prev;
            }
        }

        node.set_orphaned();
    }
}

impl<T> Drop for LinkedList<T> {
    fn drop(&mut self) {
        let mut current_link = self.head.take();
        while let Some(mut node_ptr) = current_link {
            let current_node = unsafe {
                // SAFTEY: Nodes in the list are guaranteed to be valid.
                node_ptr.as_mut()
            };
            current_node.set_orphaned();
            current_link = current_node.next.take();
        }
    }
}
