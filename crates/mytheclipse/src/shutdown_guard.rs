//! RAII shutdown guard (feature `lifecycle`).
//!
//! [`ShutdownGuard`] gives background workers a *scope-based* way to announce
//! they have finished, without manually de-registering from a
//! [`crate::ShutdownManager`] — the guard fires its completion callback when
//! dropped (RAII), so even a `panic!` or an early `return` cannot leak a
//! dangling "task still running" registration.
//!
//! This complements [`crate::ShutdownManager::register`] (which tracks
//! `JoinHandle`s): a guard is useful when the work isn't behind a joinable
//! handle, or when you want to guarantee cleanup even on unwind.

use std::sync::{Arc, Mutex};

type GuardFn = Box<dyn FnOnce() + Send>;

/// An RAII guard that invokes a notification callback exactly once when
/// dropped.
///
/// The callback is wrapped in a `Mutex<Option<_>>` so it can be taken out and
/// run exactly once — even on a `panic!`-unwound drop — guaranteeing at-most-
/// once semantics (no double-shutdown race).
///
/// ```
/// use std::sync::atomic::{AtomicUsize, Ordering};
/// use std::sync::Arc;
/// use mytheclipse::shutdown_guard::ShutdownGuard;
///
/// let done = Arc::new(AtomicUsize::new(0));
/// let d = Arc::clone(&done);
/// {
///     let _guard = ShutdownGuard::new(move || { d.fetch_add(1, Ordering::SeqCst); });
///     // do work...
/// } // guard dropped here -> callback fires exactly once
/// assert_eq!(done.load(Ordering::SeqCst), 1);
///
/// // Or fire early with `finish()` (disarms the drop):
/// let d2 = Arc::clone(&done);
/// ShutdownGuard::new(move || { d2.fetch_add(1, Ordering::SeqCst); }).finish();
/// assert_eq!(done.load(Ordering::SeqCst), 2);
/// ```
// Type alias for the stored once-only callback, so the nested Arc<Mutex<…>> field type
// stays within clippy's `type_complexity` threshold.
pub struct ShutdownGuard {
    inner: Arc<Mutex<Option<GuardFn>>>,
}

impl ShutdownGuard {
    /// Creates a guard that will run `on_drop` (once, on drop) to mark this
    /// unit of work as complete.
    pub fn new(on_drop: impl FnOnce() + Send + 'static) -> Self {
        Self {
            inner: Arc::new(Mutex::new(Some(Box::new(on_drop)))),
        }
    }

    /// Marks the work as complete immediately, invoking the callback once,
    /// and disarms the guard so a later drop is a no-op.
    pub fn finish(self) {
        self.fire();
        std::mem::forget(self);
    }

    fn fire(&self) {
        let cb = self.inner.lock().unwrap().take();
        if let Some(cb) = cb {
            cb();
        }
    }
}

impl Drop for ShutdownGuard {
    fn drop(&mut self) {
        self.fire();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[test]
    fn fires_on_drop() {
        let calls = Arc::new(AtomicUsize::new(0));
        let c = Arc::clone(&calls);
        {
            let _guard = ShutdownGuard::new(move || {
                c.fetch_add(1, Ordering::SeqCst);
            });
        }
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn finish_fires_once_and_disarms() {
        let calls = Arc::new(AtomicUsize::new(0));
        let c = Arc::clone(&calls);
        let guard = ShutdownGuard::new(move || {
            c.fetch_add(1, Ordering::SeqCst);
        });
        guard.finish();
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn fires_exactly_once_despite_panic_path() {
        let calls = Arc::new(AtomicUsize::new(0));
        let c = Arc::clone(&calls);
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _guard = ShutdownGuard::new(move || {
                c.fetch_add(1, Ordering::SeqCst);
            });
            panic!("boom");
        }));
        assert!(result.is_err());
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }
}
