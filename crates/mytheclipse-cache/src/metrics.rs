//! Cache instrumentation metrics (hit/miss/eviction counters).

use std::sync::atomic::{AtomicU64, Ordering};

/// Tracks cache hit, miss, eviction, and error counts.
#[derive(Default)]
pub struct CacheMetrics {
    hits: AtomicU64,
    misses: AtomicU64,
    evictions: AtomicU64,
    errors: AtomicU64,
}

impl CacheMetrics {
    /// Creates a counter set with every counter at zero.
    pub fn new() -> Self {
        Self::default()
    }

    /// Records one cache hit.
    pub fn hit(&self) {
        self.hits.fetch_add(1, Ordering::Relaxed);
    }

    /// Records one cache miss.
    pub fn miss(&self) {
        self.misses.fetch_add(1, Ordering::Relaxed);
    }

    /// Records one eviction.
    pub fn eviction(&self) {
        self.evictions.fetch_add(1, Ordering::Relaxed);
    }

    /// Records one backend error.
    pub fn error(&self) {
        self.errors.fetch_add(1, Ordering::Relaxed);
    }

    /// Returns the counts as they stand right now.
    pub fn snapshot(&self) -> CacheSnapshot {
        CacheSnapshot {
            hits: self.hits.load(Ordering::Relaxed),
            misses: self.misses.load(Ordering::Relaxed),
            evictions: self.evictions.load(Ordering::Relaxed),
            errors: self.errors.load(Ordering::Relaxed),
        }
    }
}

/// A point-in-time read of cache metrics.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CacheSnapshot {
    /// Lookups that found a live entry.
    pub hits: u64,
    /// Lookups that found nothing.
    pub misses: u64,
    /// Entries dropped to make room.
    pub evictions: u64,
    /// Backend failures, as opposed to misses.
    pub errors: u64,
}

impl CacheSnapshot {
    /// Hits as a fraction of all lookups, or `0.0` when there were none.
    pub fn hit_rate(&self) -> f64 {
        let total = self.hits + self.misses;
        if total == 0 {
            0.0
        } else {
            self.hits as f64 / total as f64
        }
    }
}
