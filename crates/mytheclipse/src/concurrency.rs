//! A synchronous concurrency limiter (feature `traffic`).
//!
//! [`ConcurrencyLimiter`] bounds how many threads may hold a permit
//! simultaneously, providing a way to cap concurrent access to an expensive
//! resource (e.g. at most 10 tasks may hold a database connection or compress
//! an image at once) without requiring a runtime. It is built on std's
//! `Mutex` + `Condvar` and therefore works in plain, non-async threads.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::sync::{Condvar, Mutex};

struct Inner {
    state: Mutex<usize>,
    available: Condvar,
    max: usize,
    /// Threads parked in `available.wait`. Lets a release skip the condvar
    /// entirely in the common uncontended case: signalling a condvar with no
    /// waiters still takes its internal lock, which costs more than the rest of
    /// the acquire/release pair put together.
    ///
    /// Updated under `state`, so a releaser holding `state` cannot observe zero
    /// waiters while a thread is between "decided to wait" and "parked".
    waiters: AtomicUsize,
}

/// A thread-safe cap on concurrent in-flight sections.
#[derive(Clone)]
pub struct ConcurrencyLimiter {
    inner: Arc<Inner>,
}

/// An RAII guard holding a concurrency permit.
///
/// Released (the permit returning to the limiter) when this guard is dropped.
#[must_use = "dropping the permit releases the slot; if the caller wants to hold it, keep it alive"]
pub struct ConcurrencyPermit {
    inner: Arc<Inner>,
}

impl ConcurrencyLimiter {
    /// Builds a limiter allowing at most `max_concurrent` held permits.
    ///
    /// # Panics
    ///
    /// Panics if `max_concurrent` is zero.
    pub fn new(max_concurrent: usize) -> Self {
        assert!(max_concurrent > 0, "concurrency limit must be > 0");
        Self {
            inner: Arc::new(Inner {
                state: Mutex::new(max_concurrent),
                available: Condvar::new(),
                max: max_concurrent,
                waiters: AtomicUsize::new(0),
            }),
        }
    }

    /// Blocks the calling thread until a permit is available, then returns it.
    pub fn acquire(&self) -> ConcurrencyPermit {
        let mut available = self.inner.state.lock().unwrap();
        while *available == 0 {
            // Counted while still holding the lock, so a concurrent release
            // either sees this waiter or this thread sees the released slot.
            self.inner.waiters.fetch_add(1, Ordering::SeqCst);
            available = self
                .inner
                .available
                .wait(available)
                .expect("concurrency limiter condvar poisoned");
            self.inner.waiters.fetch_sub(1, Ordering::SeqCst);
        }
        *available -= 1;
        ConcurrencyPermit {
            inner: Arc::clone(&self.inner),
        }
    }

    /// Attempts to acquire a permit without blocking.
    ///
    /// Returns `None` if the limiter is currently at its maximum.
    pub fn try_acquire(&self) -> Option<ConcurrencyPermit> {
        let mut available = self.inner.state.lock().unwrap();
        if *available == 0 {
            return None;
        }
        *available -= 1;
        Some(ConcurrencyPermit {
            inner: Arc::clone(&self.inner),
        })
    }

    /// How many permits are currently held.
    pub fn in_use(&self) -> usize {
        let available = *self.inner.state.lock().unwrap();
        self.inner.max - available
    }

    /// The maximum number of concurrently held permits.
    pub fn max(&self) -> usize {
        self.inner.max
    }
}

impl Drop for ConcurrencyPermit {
    fn drop(&mut self) {
        let mut available = self.inner.state.lock().unwrap();
        *available += 1;
        if self.inner.waiters.load(Ordering::SeqCst) > 0 {
            self.inner.available.notify_one();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    #[test]
    fn acquires_and_releases() {
        let limiter = ConcurrencyLimiter::new(1);
        {
            let _permit = limiter.try_acquire().expect("should acquire");
            assert_eq!(limiter.in_use(), 1);
        }
        assert_eq!(limiter.in_use(), 0);
    }

    #[test]
    fn try_acquire_fails_at_cap() {
        let limiter = ConcurrencyLimiter::new(2);
        let _a = limiter.try_acquire().expect("first");
        let _b = limiter.try_acquire().expect("second");
        assert!(limiter.try_acquire().is_none());
        drop(_a);
        assert!(limiter.try_acquire().is_some());
    }

    #[test]
    fn acquire_blocks_until_slot_frees() {
        let limiter = ConcurrencyLimiter::new(1);
        let permit = limiter.try_acquire().unwrap();

        let in_flight = Arc::new(AtomicUsize::new(0));
        let max_observed = Arc::new(AtomicUsize::new(0));
        let limiter2 = limiter.clone();
        let in_flight2 = Arc::clone(&in_flight);
        let max2 = Arc::clone(&max_observed);

        let thread = std::thread::spawn(move || {
            let _permit = limiter2.acquire();
            in_flight2.fetch_add(1, Ordering::SeqCst);
            max2.fetch_max(in_flight2.load(Ordering::SeqCst), Ordering::SeqCst);
        });

        std::thread::sleep(Duration::from_millis(40));
        assert_eq!(in_flight.load(Ordering::SeqCst), 0);
        // Explicitly drop permit so waiting thread unblocks
        drop(permit);
        thread.join().unwrap();
        assert_eq!(max_observed.load(Ordering::SeqCst), 1);
        assert_eq!(in_flight.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn limits_concurrent_sections() {
        let limiter = ConcurrencyLimiter::new(2);
        let in_flight = Arc::new(AtomicUsize::new(0));
        let max_observed = Arc::new(AtomicUsize::new(0));
        let mut threads = Vec::new();
        for _ in 0..8 {
            let limiter = limiter.clone();
            let in_flight = Arc::clone(&in_flight);
            let max_observed = Arc::clone(&max_observed);
            threads.push(std::thread::spawn(move || {
                let _permit = limiter.acquire();
                in_flight.fetch_add(1, Ordering::SeqCst);
                max_observed.fetch_max(in_flight.load(Ordering::SeqCst), Ordering::SeqCst);
                std::thread::sleep(Duration::from_millis(10));
                in_flight.fetch_sub(1, Ordering::SeqCst);
            }));
        }
        for t in threads {
            t.join().unwrap();
        }
        assert!(max_observed.load(Ordering::SeqCst) <= 2);
    }

    #[test]
    #[should_panic]
    fn zero_limit_panics() {
        let _ = ConcurrencyLimiter::new(0);
    }
}
