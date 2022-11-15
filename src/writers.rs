// Copyright (c) 2021 Google LLC
//
// Use of this source code is governed by an MIT-style
// license that can be found in the LICENSE file or at
// https://opensource.org/licenses/MIT.

/*!
This module provides abstractions that represent threads performing write operations, specifically
the [`Writer`] struct.
*/

use std::ptr;

use parking_lot::{Condvar, Mutex, MutexGuard};

use crate::db::GuardedDbFields;
use crate::errors::RainDBResult;
use crate::Batch;

/**
Mutable fields within a [`Writer`].

These will be wrapped by a mutex to provide interior mutability without need to keep a lock
around the entire parent [`Writer`] object.
*/
struct WriterInner {
    /// Whether the requested operation was completed regardless of whether if failed or succeeded.
    pub operation_completed: bool,

    /**
    The result of the operation.

    This field must be populated if `operation_completed` was set to `true`. This field is mostly
    used to indicate status to a writer if its operation was part of a group commit.
    */
    pub operation_result: Option<RainDBResult<()>>,
}

/**
A thread requesting a write operation.

When multiple threads request a write operation, RainDB will queue up the threads so that the
writes occur serially. Threads waiting in the queue are parked and then signalled to wake up when it
is their turn to perform the requested operation.
*/
pub(crate) struct Writer {
    /**
    The batch of operations this writer is requesting to be performed.

    This is `None` if a client is forcing a compaction check.
    */
    maybe_batch: Option<Batch>,

    /// Whether the write operations should be synchronously flushed to disk.
    synchronous_write: bool,

    /**
    Fields in a writer that need to be mutable.

    The mutex is for interior mutability where only an immutable reference is needed to make
    changes and a lock doesn't have to be placed around the entire writer.
    */
    inner: Mutex<WriterInner>,

    /**
    A condition variable to signal the thread to park or wake-up to perform it's requested
    operation.
    */
    thread_signaller: Condvar,
}

impl PartialEq for Writer {
    fn eq(&self, other: &Self) -> bool {
        self.maybe_batch == other.maybe_batch
            && self.synchronous_write == other.synchronous_write
            && ptr::eq(&self.inner, &other.inner)
    }
}

/// Public methods
impl Writer {
    /// Create a new instance of [`Writer`].
    pub fn new(maybe_batch: Option<Batch>, synchronous_write: bool) -> Self {
        let inner = WriterInner {
            operation_completed: false,
            operation_result: None,
        };

        Self {
            maybe_batch,
            synchronous_write,
            inner: Mutex::new(inner),
            thread_signaller: Condvar::new(),
        }
    }

    /// Whether the writer should perform synchronous writes.
    pub fn is_synchronous_write(&self) -> bool {
        self.synchronous_write
    }

    /// Get a reference to the operations this writer needs to perform.
    pub fn maybe_batch(&self) -> Option<&Batch> {
        self.maybe_batch.as_ref()
    }

    /// Parks the thread while it waits for its turn to perform its operation.
    pub fn wait_for_turn(&self, database_mutex_guard: &mut MutexGuard<GuardedDbFields>) {
        self.thread_signaller.wait(database_mutex_guard)
    }

    /**
    Notify that writer that it is potentially it's turn to perform its operation.

    If the operation was already completed as part of a group commit, the thread will return.
    */
    pub fn notify_writer(&self) -> bool {
        self.thread_signaller.notify_one()
    }

    /**
    Return true if the operation is complete. Otherwise, false.

    This will attempt to get a lock on the inner fields.
    */
    pub fn is_operation_complete(&self) -> bool {
        self.inner.lock().operation_completed
    }

    /**
    Set whether or not the operation is complete.

    This will attempt to get a lock on the inner fields.
    */
    pub fn set_operation_completed(&self, is_complete: bool) -> bool {
        let mut mutex_guard = self.inner.lock();
        mutex_guard.operation_completed = is_complete;

        mutex_guard.operation_completed
    }

    /**
    Get a copy of the result of the write operation.

    This will attempt to get a lock on the inner fields.
    */
    pub fn get_operation_result(&self) -> Option<RainDBResult<()>> {
        self.inner.lock().operation_result.clone()
    }

    /**
    Set the result of the write operation.

    This will attempt to get a lock on the inner fields.
    */
    pub fn set_operation_result(&self, operation_result: RainDBResult<()>) {
        let mut mutex_guard = self.inner.lock();
        mutex_guard.operation_result = Some(operation_result);
    }
}
