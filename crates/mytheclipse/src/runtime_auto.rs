//! Auto thread/core allocation (feature `lifecycle`).
//!
//! Provides [`RuntimeConfig`] — a small builder that infers sensible thread
//! counts from the host CPU topology (via [`std::thread::available_parallelism`])
//! so callers don't have to hand-tune `worker_threads` / `max_blocking_threads`.
//!
//! The same helpers let you size a thread pool builder of your choice without
//! pulling in an extra CPU-counting crate.
//!
//! ## Rationale
//!
//! Most async/runtime configs default to *one* worker thread per core, which
//! is usually fine — but compute-heavy or blocking-heavy workloads want a
//! separate, explicit breakdown. [`RuntimeConfig`] centralises that logic so
//! you configure it once and reuse the counts everywhere.

use std::num::NonZeroUsize;

/// Auto-computed runtime thread counts derived from the host CPU topology.
///
/// Each field is a concrete `usize` (never zero) so it can be fed directly
/// into a tokio or std-thread builder with no further `max(1)` guards.
///
/// ```
/// use mytheclipse::runtime_auto::RuntimeConfig;
///
/// let cfg = RuntimeConfig::auto();      // from the host CPU
/// let sized = RuntimeConfig::from_cores(4); // or explicit
///
/// // Feed straight into a tokio builder:
/// let rt = tokio::runtime::Builder::new_multi_thread()
///     .worker_threads(cfg.worker_threads)
///     .max_blocking_threads(cfg.max_blocking_threads)
///     .build()
///     .unwrap();
/// let _ = sized.compute_threads;
/// rt.shutdown_timeout(std::time::Duration::from_millis(1));
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RuntimeConfig {
    /// Worker threads for the main async runtime (default: one per core).
    pub worker_threads: usize,
    /// Extra threads reserved for blocking (tokio `max_blocking_threads`).
    pub max_blocking_threads: usize,
    /// Worker threads the compute pool is sized for.
    pub compute_threads: usize,
    /// Suggested background/IO pool size.
    pub io_threads: usize,
}

impl RuntimeConfig {
    /// Sizes every pool to the host's available parallelism (one thread per
    /// logical core), with a small reserved budget for blocking.
    pub fn auto() -> Self {
        let cores = available_parallelism();
        Self {
            worker_threads: cores,
            max_blocking_threads: cores.saturating_add(cores / 2).max(4),
            compute_threads: cores,
            io_threads: cores.saturating_div(2).clamp(1, 8),
        }
    }

    /// Sizes pools heuristically from `cores` (useful in tests or when you
    /// want to override the host topology).
    pub fn from_cores(cores: usize) -> Self {
        let cores = cores.max(1);
        Self {
            worker_threads: cores,
            max_blocking_threads: cores.saturating_add(cores / 2).max(4),
            compute_threads: cores,
            io_threads: cores.saturating_div(2).clamp(1, 8),
        }
    }

    /// A compact runtime: minimal worker + compute threads for constrained
    /// environments (embedded, small containers).
    pub fn compact() -> Self {
        let cores = available_parallelism();
        Self {
            worker_threads: cores.max(2),
            max_blocking_threads: 2,
            compute_threads: 1,
            io_threads: 1,
        }
    }
}

/// Number of the host's logical CPU cores, falling back to 1 on error.
pub fn available_parallelism() -> usize {
    std::thread::available_parallelism()
        .map(NonZeroUsize::get)
        .unwrap_or(1)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn auto_is_nonzero() {
        let c = RuntimeConfig::auto();
        assert!(c.worker_threads >= 1);
        assert!(c.max_blocking_threads >= 4);
        assert!(c.compute_threads >= 1);
        assert!(c.io_threads >= 1);
        assert!(c.io_threads <= 8);
    }

    #[test]
    fn from_cores_clamps() {
        let c = RuntimeConfig::from_cores(4);
        assert_eq!(c.worker_threads, 4);
        assert_eq!(c.max_blocking_threads, 6);
        let one = RuntimeConfig::from_cores(0);
        assert_eq!(one.worker_threads, 1);
    }

    #[test]
    fn compact_is_minimal() {
        let c = RuntimeConfig::compact();
        assert_eq!(c.compute_threads, 1);
        assert_eq!(c.io_threads, 1);
    }
}
