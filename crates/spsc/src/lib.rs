//! Lock-free SPSC ring buffer.
//!
//! ## Memory ordering model
//!
//! Producer owns slots from `head` forward (up to `head + capacity`).
//! Consumer owns slots from `tail` forward.
//!
//! - Producer: `Acquire` load of tail to check space; `Release` store of head after writing slot.
//! - Consumer: `Acquire` load of head to observe published slot; `Release` store of tail after reading.
//!
//! The `Release`/`Acquire` pair on `head` is the happens-before edge that makes the slot
//! contents visible to the consumer. The symmetric pair on `tail` makes the freed slot
//! visible to the producer.
//!
//! ## Cache line isolation
//!
//! `Head` and `Tail` are each padded to 64 bytes (`#[repr(align(64))]`) so they
//! occupy separate L1 cache lines. Without this, the producer and consumer bounce the
//! same cache line between cores on every operation — measurable as elevated
//! `OFFCORE_RESPONSE` / cache-to-cache transfer events in PMU telemetry.
//!
//! ## False-sharing status
//!
//! Alignment eliminates the structural opportunity for false sharing between head and tail.
//! Causal attribution requires raw PMU hardware counter comparison (aligned vs. unaligned
//! builds) under controlled thread affinity. See `benches/throughput.rs`.

use std::cell::{Cell, UnsafeCell};
use std::marker::PhantomData;
use std::mem::MaybeUninit;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

/// One slot in the ring, isolated to its own 64-byte cache line.
#[repr(C, align(64))]
pub struct RingSlot<T> {
    value: UnsafeCell<MaybeUninit<T>>,
}

impl<T> RingSlot<T> {
    const fn new() -> Self {
        Self {
            value: UnsafeCell::new(MaybeUninit::uninit()),
        }
    }
}

// SAFETY: RingSlot is only accessed under the SPSC ownership protocol.
unsafe impl<T: Send> Send for RingSlot<T> {}
unsafe impl<T: Send> Sync for RingSlot<T> {}

/// Producer-side atomic index, padded to one cache line.
#[repr(align(64))]
struct Head(AtomicUsize);

/// Consumer-side atomic index, padded to one cache line.
#[repr(align(64))]
struct Tail(AtomicUsize);

struct Inner<T> {
    head: Head,
    tail: Tail,
    slots: Box<[RingSlot<T>]>,
    mask: usize,
}

// SAFETY: The SPSC protocol enforces single-writer ownership for each slot.
unsafe impl<T: Send> Send for Inner<T> {}
unsafe impl<T: Send> Sync for Inner<T> {}

impl<T> Drop for Inner<T> {
    fn drop(&mut self) {
        // The last Arc is gone, so neither endpoint can access slots anymore.
        let mut tail = *self.tail.0.get_mut();
        let head = *self.head.0.get_mut();
        while tail != head {
            let slot = &mut self.slots[tail & self.mask];
            // SAFETY: precisely the slots in [tail, head) are initialized; received
            // values already advanced tail and must not be dropped again.
            unsafe { slot.value.get_mut().assume_init_drop() };
            tail = tail.wrapping_add(1);
        }
    }
}

/// Producer handle. Not `Clone` — only one producer is permitted.
///
/// A handle can move between threads, but cannot be shared between them.
///
/// ```compile_fail
/// fn assert_sync<T: Sync>() {}
/// assert_sync::<spsc::Producer<u64>>();
/// ```
pub struct Producer<T> {
    inner: Arc<Inner<T>>,
    // Cell is Send but not Sync: shared references must not create multiple writers.
    _not_sync: PhantomData<Cell<()>>,
}

/// Consumer handle. Not `Clone` — only one consumer is permitted.
///
/// ```compile_fail
/// fn assert_sync<T: Sync>() {}
/// assert_sync::<spsc::Consumer<u64>>();
/// ```
pub struct Consumer<T> {
    inner: Arc<Inner<T>>,
    // Shared references must not create multiple readers of an initialized slot.
    _not_sync: PhantomData<Cell<()>>,
}

/// Create a bounded SPSC queue with capacity rounded up to the next power of two.
///
/// # Panics
/// Panics if `capacity` is zero or greater than `usize::MAX / 2`.
pub fn channel<T>(capacity: usize) -> (Producer<T>, Consumer<T>) {
    assert!(capacity > 0, "capacity must be > 0");
    let cap = capacity.next_power_of_two();
    let mut slots = Vec::with_capacity(cap);
    for _ in 0..cap {
        slots.push(RingSlot::new());
    }
    let inner = Arc::new(Inner {
        head: Head(AtomicUsize::new(0)),
        tail: Tail(AtomicUsize::new(0)),
        slots: slots.into_boxed_slice(),
        mask: cap - 1,
    });
    (
        Producer {
            inner: Arc::clone(&inner),
            _not_sync: PhantomData,
        },
        Consumer {
            inner,
            _not_sync: PhantomData,
        },
    )
}

impl<T> Producer<T> {
    /// Attempt to enqueue `value`. Returns `Err(value)` if the queue is full.
    #[inline]
    pub fn try_send(&self, value: T) -> Result<(), T> {
        let inner = &*self.inner;
        let head = inner.head.0.load(Ordering::Relaxed);
        let tail = inner.tail.0.load(Ordering::Acquire);

        if head.wrapping_sub(tail) >= inner.slots.len() {
            return Err(value);
        }

        let slot = &inner.slots[head & inner.mask];
        // SAFETY: producer exclusively owns this slot (head has not been published yet).
        unsafe { (*slot.value.get()).write(value) };

        // Release: makes the slot contents visible to the consumer.
        inner.head.0.store(head.wrapping_add(1), Ordering::Release);
        Ok(())
    }
}

impl<T> Consumer<T> {
    /// Attempt to dequeue a value. Returns `None` if the queue is empty.
    #[inline]
    pub fn try_recv(&self) -> Option<T> {
        let inner = &*self.inner;
        // Acquire: synchronises with the producer's Release store on head.
        let head = inner.head.0.load(Ordering::Acquire);
        let tail = inner.tail.0.load(Ordering::Relaxed);

        if head == tail {
            return None;
        }

        let slot = &inner.slots[tail & inner.mask];
        // SAFETY: consumer exclusively owns this slot (tail < head).
        let value = unsafe { (*slot.value.get()).assume_init_read() };

        // Release: makes the freed slot visible to the producer.
        inner.tail.0.store(tail.wrapping_add(1), Ordering::Release);
        Some(value)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn queued_values_are_dropped_once_when_last_handle_is_dropped() {
        struct Tracked(Arc<AtomicUsize>);
        impl Drop for Tracked {
            fn drop(&mut self) {
                self.0.fetch_add(1, Ordering::Relaxed);
            }
        }
        let dropped = Arc::new(AtomicUsize::new(0));
        let (tx, rx) = channel(2);
        assert!(tx.try_send(Tracked(dropped.clone())).is_ok());
        assert!(tx.try_send(Tracked(dropped.clone())).is_ok());
        drop(rx.try_recv());
        assert_eq!(dropped.load(Ordering::Relaxed), 1);
        assert!(tx.try_send(Tracked(dropped.clone())).is_ok());
        drop(tx);
        assert_eq!(dropped.load(Ordering::Relaxed), 1);
        drop(rx);
        assert_eq!(dropped.load(Ordering::Relaxed), 3);
    }

    #[test]
    fn indices_wrap_without_losing_values() {
        let (tx, rx) = channel(2);
        tx.inner.head.0.store(usize::MAX, Ordering::Relaxed);
        tx.inner.tail.0.store(usize::MAX, Ordering::Relaxed);
        tx.try_send(1).unwrap();
        tx.try_send(2).unwrap();
        assert_eq!(tx.try_send(3), Err(3));
        assert_eq!(rx.try_recv(), Some(1));
        assert_eq!(rx.try_recv(), Some(2));
        assert_eq!(rx.try_recv(), None);
        tx.try_send(3).unwrap();
        assert_eq!(rx.try_recv(), Some(3));
    }

    #[test]
    fn test_ring_slot_cache_line_size() {
        assert_eq!(std::mem::size_of::<RingSlot<u64>>(), 64);
        assert_eq!(std::mem::align_of::<RingSlot<u64>>(), 64);
    }

    #[test]
    fn test_head_tail_cache_line_isolation() {
        assert_eq!(std::mem::align_of::<Head>(), 64);
        assert_eq!(std::mem::align_of::<Tail>(), 64);
    }

    #[test]
    fn test_single_threaded_roundtrip() {
        let (tx, rx) = channel::<u64>(8);
        assert!(rx.try_recv().is_none());
        tx.try_send(42).unwrap();
        assert_eq!(rx.try_recv(), Some(42));
        assert!(rx.try_recv().is_none());
    }

    #[test]
    fn test_full_queue_returns_err() {
        let (tx, _rx) = channel::<u64>(4);
        for i in 0..4 {
            tx.try_send(i).unwrap();
        }
        assert!(tx.try_send(99).is_err());
    }

    #[test]
    fn test_capacity_rounded_to_power_of_two() {
        let (tx, _rx) = channel::<u64>(5);
        // capacity rounds to 8
        for i in 0..8 {
            tx.try_send(i).unwrap();
        }
        assert!(tx.try_send(99).is_err());
    }

    #[test]
    fn test_multithreaded_throughput() {
        use std::thread;
        const N: u64 = 1_000_000;
        let (tx, rx) = channel::<u64>(1024);
        let producer = thread::spawn(move || {
            let mut sent = 0u64;
            while sent < N {
                if tx.try_send(sent).is_ok() {
                    sent += 1;
                }
            }
        });
        let consumer = thread::spawn(move || {
            let mut received = 0u64;
            while received < N {
                if let Some(v) = rx.try_recv() {
                    assert_eq!(v, received);
                    received += 1;
                }
            }
        });
        producer.join().unwrap();
        consumer.join().unwrap();
    }
}
