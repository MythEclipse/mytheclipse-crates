//! Global engine context providing resource-aware execution primitives.
//!
//! The context is initialized lazily on first access via [`context`], or
//! explicitly via [`init`]. Resource sizing is derived from the number of
//! logical CPU cores reported by [`std::thread::available_parallelism`].

use std::sync::OnceLock;

static CONTEXT: OnceLock<EngineContext> = OnceLock::new();

/// The global, lazily-initialized engine context.
///
/// Holds the computed thread and concurrency counts for each mytheclipse
/// subsystem, along with the resource pools those counts were used to
/// build. The context lives for the lifetime of the process once
/// initialized: it is stored in a `'static` [`OnceLock`] and is never
/// dropped.
pub struct EngineContext {
    /// Logical CPU core count used for sizing async I/O scheduling.
    pub io_threads: usize,
    /// Number of worker threads allocated to the compute [`rayon::ThreadPool`].
    pub compute_threads: usize,
    /// Maximum number of background tasks permitted to run concurrently.
    pub bg_concurrency: usize,
    #[cfg(feature = "compute")]
    pub(crate) compute_pool: rayon::ThreadPool,
    #[cfg(feature = "bg")]
    pub(crate) bg_semaphore: tokio::sync::Semaphore,
}

impl EngineContext {
    /// Builds a new [`EngineContext`] sized from the current machine's
    /// logical core count.
    ///
    /// # Panics
    ///
    /// Panics if the compute thread pool cannot be constructed. This only
    /// happens under an unrecoverable environment failure, such as the
    /// operating system refusing to spawn any new thread.
    fn build() -> Self {
        // `available_parallelism` is the std answer to "how many threads should
        // I start" (it respects cgroup/affinity limits); it only fails on
        // platforms that cannot report a count, where 1 is the safe assumption.
        let logical_cores = std::thread::available_parallelism()
            .map(std::num::NonZeroUsize::get)
            .unwrap_or(1);
        let io_threads = logical_cores;
        let compute_threads = logical_cores.saturating_sub(1).max(1);
        let bg_concurrency = (logical_cores / 2).max(2);

        #[cfg(feature = "compute")]
        let compute_pool = rayon::ThreadPoolBuilder::new()
            .num_threads(compute_threads)
            .thread_name(|index| format!("mytheclipse-compute-{index}"))
            .build()
            .expect("mytheclipse: failed to build rayon compute thread pool");

        #[cfg(feature = "bg")]
        let bg_semaphore = tokio::sync::Semaphore::new(bg_concurrency);

        Self {
            io_threads,
            compute_threads,
            bg_concurrency,
            #[cfg(feature = "compute")]
            compute_pool,
            #[cfg(feature = "bg")]
            bg_semaphore,
        }
    }
}

/// Returns the global [`EngineContext`], initializing it on first access.
///
/// Safe to call from any thread at any time; initialization happens
/// exactly once regardless of how many callers race to trigger it.
pub fn context() -> &'static EngineContext {
    CONTEXT.get_or_init(EngineContext::build)
}

/// Bootstraps the global [`EngineContext`].
///
/// Behaviorally identical to [`context`]; provided as the explicit,
/// discoverable entry point applications call at startup to force
/// initialization eagerly, for example so resource sizing can be logged
/// before any workload runs. Calling it more than once, or never calling
/// it at all before using [`context`], is equally correct.
pub fn init() -> &'static EngineContext {
    CONTEXT.get_or_init(EngineContext::build)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn context_numbers_are_sane() {
        let ctx = context();
        assert!(ctx.io_threads >= 1);
        assert!(ctx.compute_threads >= 1);
        assert!(ctx.bg_concurrency >= 2);
    }
}
