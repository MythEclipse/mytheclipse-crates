//! Panic-isolated heavy compute.
//!
//! The parallel primitives here fan work out over a process-wide pool of
//! worker threads sized from the host CPU (see [`worker_count`]), so the crate
//! needs no work-stealing runtime of its own.
//!
//! The pool is the whole point: creating a thread costs tens of microseconds,
//! so a primitive that spawns its own workers per call would spend more time
//! starting threads than doing the work it was handed. Workers live for the
//! life of the process and are started on first use.
//!
//! Calls made *from* a pool worker run their chunks inline instead of
//! submitting them, so nesting a parallel map inside another one can never
//! block on a pool whose every worker is already waiting.

use std::cell::Cell;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::mpsc::{channel, Sender};
use std::sync::{Arc, Mutex, OnceLock};

use crate::error::MytheclipseError;

/// A unit of work handed to a pool worker.
type Job = Box<dyn FnOnce() + Send + 'static>;

/// The shared worker pool. Only the submitting half lives here; the workers own
/// the receiving half and never exit while the process is alive.
struct Pool {
    jobs: Sender<Job>,
}

static POOL: OnceLock<Pool> = OnceLock::new();

thread_local! {
    /// Set on every pool worker, so a nested call can tell that blocking on the
    /// pool would deadlock.
    static ON_WORKER: Cell<bool> = const { Cell::new(false) };
}

impl Pool {
    /// The process-wide pool, started on first use.
    fn global() -> &'static Pool {
        POOL.get_or_init(|| {
            let (jobs, queue) = channel::<Job>();
            let queue = Arc::new(Mutex::new(queue));

            for index in 0..worker_count() {
                let queue = Arc::clone(&queue);
                std::thread::Builder::new()
                    .name(format!("mytheclipse-compute-{index}"))
                    .spawn(move || {
                        ON_WORKER.with(|flag| flag.set(true));
                        loop {
                            // The lock is held only for the hand-off, never
                            // while the job runs.
                            let job = queue.lock().expect("compute queue poisoned").recv();
                            match job {
                                Ok(job) => job(),
                                Err(_) => break,
                            }
                        }
                    })
                    .expect("spawn a compute worker");
            }

            Pool { jobs }
        })
    }

    /// Queues `job`. Fails only if every worker is gone, which cannot happen
    /// while the pool is reachable.
    fn submit(&self, job: Job) {
        self.jobs
            .send(job)
            .expect("compute pool outlived its workers");
    }
}

/// `true` when the calling thread is one of this module's workers.
fn on_worker() -> bool {
    ON_WORKER.with(Cell::get)
}

/// Number of worker threads the parallel compute primitives use.
///
/// Defaults to one per logical core minus the one the caller occupies, which is
/// what [`crate::context::EngineContext::compute_threads`] resolves to as well.
/// Resolved once per process.
pub fn worker_count() -> usize {
    static WORKERS: OnceLock<usize> = OnceLock::new();
    *WORKERS.get_or_init(|| {
        std::thread::available_parallelism()
            .map(std::num::NonZeroUsize::get)
            .unwrap_or(1)
            .saturating_sub(1)
            .max(1)
    })
}

/// Runs `f` on the calling thread and converts a panic into
/// [`MytheclipseError::ComputePanic`].
///
/// Callers that need the work *off* their own thread should use
/// [`compute_map`]/[`compute_par_for_each`] (which hand it to the worker pool)
/// or `tokio::task::spawn_blocking`; this function is the panic-isolation
/// wrapper for a single closure and deliberately does not spawn.
///
/// # Panic-safety caveat
///
/// `f` is wrapped in [`AssertUnwindSafe`] so closures capturing ordinary
/// references or non-[`UnwindSafe`](std::panic::UnwindSafe) state can be
/// submitted without a compile error. It does not guarantee exception-safety of
/// state the closure captured by mutable reference: if `f` panics partway
/// through mutating a captured `&mut T`, that `T` may be left inconsistent.
pub fn compute<F, R>(f: F) -> Result<R, MytheclipseError>
where
    F: FnOnce() -> R,
{
    catch_unwind(AssertUnwindSafe(f))
        .map_err(|payload| MytheclipseError::ComputePanic(panic_payload_to_string(payload)))
}

fn panic_payload_to_string(payload: Box<dyn std::any::Any + Send>) -> String {
    if let Some(message) = payload.downcast_ref::<&str>() {
        (*message).to_string()
    } else if let Some(message) = payload.downcast_ref::<String>() {
        message.clone()
    } else {
        "compute closure panicked with a non-string payload".to_string()
    }
}

/// Errors collected while mapping over items on the CPU compute threads.
///
/// Each entry is one item's error message (from an `Err` return or a panic).
/// Unlike [`crate::aggregate_error::AggregateError`], this lives under the
/// `compute` feature only and carries plain strings, so the compute primitives
/// compile without the `resiliency` feature.
#[derive(Debug, Default)]
pub struct ComputeErrors {
    errors: Vec<String>,
}

impl ComputeErrors {
    /// Number of items that failed (returned `Err` or panicked).
    pub fn len(&self) -> usize {
        self.errors.len()
    }

    /// `true` when every item succeeded.
    pub fn is_empty(&self) -> bool {
        self.errors.is_empty()
    }

    /// Iterate over the error messages.
    pub fn iter(&self) -> impl Iterator<Item = &str> {
        self.errors.iter().map(String::as_str)
    }
}

impl std::fmt::Display for ComputeErrors {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} compute item(s) failed:", self.errors.len())?;
        for e in &self.errors {
            write!(f, "\n  - {e}")?;
        }
        Ok(())
    }
}

impl std::error::Error for ComputeErrors {}

/// Splits `items` into at most `workers` contiguous, owned chunks.
///
/// Contiguity is what keeps [`compute_map`]'s output in input order: chunk `i`
/// always covers the items before chunk `i + 1`, so reassembling the per-chunk
/// results by chunk index restores the original sequence.
fn into_chunks<T>(items: Vec<T>, workers: usize) -> Vec<Vec<T>> {
    let len = items.len();
    if len == 0 {
        return Vec::new();
    }
    let workers = workers.max(1).min(len);
    let chunk_len = len.div_ceil(workers);

    let mut chunks = Vec::with_capacity(workers);
    let mut iter = items.into_iter();
    loop {
        let chunk: Vec<T> = iter.by_ref().take(chunk_len).collect();
        if chunk.is_empty() {
            return chunks;
        }
        chunks.push(chunk);
    }
}

/// Applies `f` to every item of every chunk, spreading the chunks over the pool
/// and returning the per-chunk results **in chunk order**.
///
/// Runs inline — on the calling thread, chunk after chunk — when there is only
/// one chunk or when the caller is already a pool worker.
fn run_chunks<T, R, F>(chunks: Vec<Vec<T>>, f: Arc<F>) -> Vec<Vec<R>>
where
    T: Send + 'static,
    R: Send + 'static,
    F: Fn(T) -> R + Send + Sync + 'static,
{
    if chunks.len() <= 1 || on_worker() {
        return chunks
            .into_iter()
            .map(|chunk| chunk.into_iter().map(|item| f(item)).collect())
            .collect();
    }

    let (results, completed) = channel::<(usize, Vec<R>)>();
    for (index, chunk) in chunks.into_iter().enumerate() {
        let results = results.clone();
        let f = Arc::clone(&f);
        Pool::global().submit(Box::new(move || {
            let out: Vec<R> = chunk.into_iter().map(|item| f(item)).collect();
            let _ = results.send((index, out));
        }));
    }
    drop(results);

    // Workers finish out of order; the index is what restores input order.
    let mut tagged: Vec<(usize, Vec<R>)> = completed.iter().collect();
    tagged.sort_unstable_by_key(|(index, _)| *index);
    tagged.into_iter().map(|(_, out)| out).collect()
}

/// Runs `f` over every item in parallel, returning the results **in input
/// order**.
///
/// Work is split into one contiguous chunk per worker thread, each item's
/// closure runs inside [`std::panic::catch_unwind`], and every failure —
/// returned or panicked — is collected into one [`ComputeErrors`].
///
/// The closure and the items must be `'static` because they are handed to
/// long-lived worker threads.
///
/// ```
/// use mytheclipse::compute::compute_map;
///
/// let squares = compute_map(vec![1u32, 2, 3, 4], |x| Ok::<_, String>(x * x)).unwrap();
/// assert_eq!(squares, vec![1, 4, 9, 16]);
/// ```
pub fn compute_map<I, T, F>(items: I, f: F) -> Result<Vec<T>, ComputeErrors>
where
    I: IntoIterator,
    I::Item: Send + 'static,
    T: Send + 'static,
    F: Fn(I::Item) -> Result<T, String> + Send + Sync + 'static,
{
    let chunks = into_chunks(items.into_iter().collect(), worker_count());
    let per_chunk = run_chunks(
        chunks,
        Arc::new(move |item| {
            catch_unwind(AssertUnwindSafe(|| f(item)))
                .unwrap_or_else(|payload| Err(panic_payload_to_string(payload)))
        }),
    );

    let total: usize = per_chunk.iter().map(Vec::len).sum();
    let mut values = Vec::with_capacity(total);
    let mut errors = Vec::new();
    for result in per_chunk.into_iter().flatten() {
        match result {
            Ok(value) => values.push(value),
            Err(e) => errors.push(e),
        }
    }

    if errors.is_empty() {
        Ok(values)
    } else {
        Err(ComputeErrors { errors })
    }
}

/// Runs two closures in parallel, returning both results.
///
/// The first branch goes to the worker pool while the second runs on the
/// calling thread, so a single free worker is enough to make progress. Each
/// branch is panic-isolated: if one panics, its panic becomes a
/// [`MytheclipseError::ComputePanic`] while the other branch still runs to
/// completion.
///
/// ```
/// use mytheclipse::compute::compute_join;
///
/// let (a, b) = compute_join(
///     || (0..1_000_000u64).sum::<u64>(),
///     || (1_000_000..2_000_000u64).sum::<u64>(),
/// ).unwrap();
/// assert_eq!(a + b, (0..2_000_000u64).sum::<u64>());
/// ```
pub fn compute_join<A, RA, B, RB>(a: A, b: B) -> Result<(RA, RB), MytheclipseError>
where
    A: FnOnce() -> RA + Send + 'static,
    RA: Send + 'static,
    B: FnOnce() -> RB + Send + 'static,
    RB: Send + 'static,
{
    if on_worker() {
        let left = catch_unwind(AssertUnwindSafe(a))
            .map_err(|payload| MytheclipseError::ComputePanic(panic_payload_to_string(payload)));
        let right = catch_unwind(AssertUnwindSafe(b))
            .map_err(|payload| MytheclipseError::ComputePanic(panic_payload_to_string(payload)));
        return Ok((left?, right?));
    }

    let (left, completed) = channel::<Result<RA, String>>();
    Pool::global().submit(Box::new(move || {
        let _ = left.send(catch_unwind(AssertUnwindSafe(a)).map_err(panic_payload_to_string));
    }));

    let right = catch_unwind(AssertUnwindSafe(b))
        .map_err(|payload| MytheclipseError::ComputePanic(panic_payload_to_string(payload)));
    let left = completed
        .recv()
        .expect("compute worker dropped a join branch")
        .map_err(MytheclipseError::ComputePanic);

    Ok((left?, right?))
}

/// Runs `f` over every item in parallel, discarding return values (side effects
/// only), collecting errors and panics.
///
/// ```
/// use std::sync::atomic::{AtomicUsize, Ordering};
/// use std::sync::Arc;
/// use mytheclipse::compute::compute_par_for_each;
///
/// let count = Arc::new(AtomicUsize::new(0));
/// let c = Arc::clone(&count);
/// compute_par_for_each(0..100, move |x| {
///     c.fetch_add(x as usize, Ordering::SeqCst);
///     Ok::<_, String>(())
/// }).unwrap();
/// assert_eq!(count.load(Ordering::SeqCst), 4950);
/// ```
pub fn compute_par_for_each<I, F>(items: I, f: F) -> Result<(), ComputeErrors>
where
    I: IntoIterator,
    I::Item: Send + 'static,
    F: Fn(I::Item) -> Result<(), String> + Send + Sync + 'static,
{
    let chunks = into_chunks(items.into_iter().collect(), worker_count());
    let per_chunk = run_chunks(
        chunks,
        Arc::new(move |item| {
            catch_unwind(AssertUnwindSafe(|| f(item)))
                .unwrap_or_else(|payload| Err(panic_payload_to_string(payload)))
        }),
    );

    let errors: Vec<String> = per_chunk
        .into_iter()
        .flatten()
        .filter_map(Result::err)
        .collect();

    if errors.is_empty() {
        Ok(())
    } else {
        Err(ComputeErrors { errors })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn compute_panic_is_isolated() {
        let panicked: Result<u32, MytheclipseError> = compute(|| panic!("boom"));
        assert!(matches!(panicked, Err(MytheclipseError::ComputePanic(_))));

        assert_eq!(compute(|| 1 + 1).unwrap(), 2);
    }

    #[test]
    fn compute_map_ordered_and_aggregates_errors() {
        let squares = compute_map(vec![1u32, 2, 3, 4], |x| Ok::<_, String>(x * x)).unwrap();
        assert_eq!(squares, vec![1, 4, 9, 16]);

        let err = compute_map(vec![1u32, 2, 3], |x| {
            if x == 2 {
                Err("boom".to_string())
            } else {
                Ok(x * 10)
            }
        })
        .unwrap_err();
        assert_eq!(err.len(), 1);
        assert_eq!(err.iter().next().unwrap(), "boom");
    }

    #[test]
    fn compute_map_preserves_order_across_chunk_boundaries() {
        let items: Vec<u32> = (0..1000).collect();
        let out = compute_map(items.clone(), |x| Ok::<_, String>(x * 2)).unwrap();
        assert_eq!(out, items.iter().map(|x| x * 2).collect::<Vec<_>>());
    }

    #[test]
    fn compute_map_panic_is_isolated_and_collected() {
        let err = compute_map(vec![1u32, 2, 3], |x| {
            if x == 2 {
                panic!("item panic")
            } else {
                Ok::<_, String>(x)
            }
        })
        .unwrap_err();
        assert_eq!(err.len(), 1);
        assert!(err.iter().next().unwrap().contains("item panic"));

        let recovered = compute_map(vec![1u32], |x| Ok::<_, String>(x + 1)).unwrap();
        assert_eq!(recovered, vec![2]);
    }

    #[test]
    fn compute_map_empty_input_is_ok() {
        let out = compute_map(Vec::<u32>::new(), Ok::<_, String>).unwrap();
        assert!(out.is_empty());
    }

    /// A nested parallel map runs on a worker, so it must take the inline path
    /// rather than block on a pool it is itself occupying.
    #[test]
    fn nested_compute_map_does_not_deadlock() {
        let out = compute_map(0..64u32, |x| {
            let inner =
                compute_map(0..4u32, move |y| Ok::<_, String>(x * y)).map_err(|e| e.to_string())?;
            Ok::<_, String>(inner.into_iter().sum::<u32>())
        })
        .unwrap();

        assert_eq!(out, (0..64u32).map(|x| x * 6).collect::<Vec<_>>());
    }

    #[test]
    fn compute_join_runs_both_branches() {
        let (a, b) = compute_join(
            || (0..100_000u64).sum::<u64>(),
            || (100_000..200_000u64).sum::<u64>(),
        )
        .unwrap();
        assert_eq!(a + b, (0..200_000u64).sum::<u64>());
    }

    #[test]
    fn compute_join_panic_is_isolated() {
        let a = compute_join(|| panic!("branch a"), || 42u32);
        assert!(matches!(a, Err(MytheclipseError::ComputePanic(_))));

        let ok = compute_join(|| 1u32, || 2u32).unwrap();
        assert_eq!(ok, (1, 2));
    }

    /// Nested joins run inline, so a branch that itself joins cannot starve the
    /// pool.
    #[test]
    fn nested_compute_join_does_not_deadlock() {
        let (a, b) = compute_join(
            || compute_join(|| 1u32, || 2u32).unwrap(),
            || compute_join(|| 3u32, || 4u32).unwrap(),
        )
        .unwrap();
        assert_eq!(a, (1, 2));
        assert_eq!(b, (3, 4));
    }

    #[test]
    fn compute_par_for_each_runs_all_side_effects() {
        use std::sync::atomic::{AtomicUsize, Ordering};

        let count = Arc::new(AtomicUsize::new(0));
        let c = Arc::clone(&count);
        compute_par_for_each(0..100, move |x| {
            c.fetch_add(x as usize, Ordering::SeqCst);
            Ok::<_, String>(())
        })
        .unwrap();
        assert_eq!(count.load(Ordering::SeqCst), 4950);
    }

    #[test]
    fn compute_par_for_each_collects_errors() {
        let err = compute_par_for_each(0..10u32, |x| {
            if x % 2 == 0 {
                Err(format!("even {x}"))
            } else {
                Ok(())
            }
        })
        .unwrap_err();
        assert_eq!(err.len(), 5);
    }

    #[test]
    fn worker_count_is_positive() {
        assert!(worker_count() >= 1);
    }
}
