//! Bounded parallel map over collections (feature `resiliency`).
//!
//! [`parallel_map`], [`parallel_map_unordered`] fan work out across a
//! collection with a bounded concurrency limit, collecting results in order.
//! [`parallel_for_each`] is a streaming variant that never materializes the
//! whole input in memory. These remove the manual `Semaphore + join_all` +
//! error-aggregation boilerplate that races easily when done by hand.
//!
//! Both spawn exactly `concurrency` worker tasks that pull items from a shared
//! iterator instead of one task per item: the bound then limits *task
//! creation* as well as running work, and the per-item cost is a claim from a
//! shared iterator (amortized over a small batch) rather than a
//! `tokio::spawn`.

use std::future::Future;
use std::sync::{Arc, Mutex};

use crate::aggregate_error::AggregateError;

/// Resolves a concurrency hint into an actual bound.
///
/// Pass an explicit `usize` for a fixed bound, or `()` to auto-size from the
/// host CPU (`std::thread::available_parallelism`).
///
/// ```
/// use mytheclipse::parallel_map::{parallel_map, ParallelConcurrency};
///
/// #[tokio::main]
/// async fn main() {
///     let items = vec![1u32, 2, 3, 4];
///     let out = parallel_map(items, (), |x| async move { Ok::<_, std::io::Error>(x * 2) })
///         .await
///         .unwrap();
///     assert_eq!(out, vec![2, 4, 6, 8]);
/// }
/// ```
pub trait ParallelConcurrency {
    /// Turns the hint into a concrete positive concurrency bound.
    fn resolve(self) -> usize;
}

impl ParallelConcurrency for usize {
    fn resolve(self) -> usize {
        self.max(1)
    }
}

impl ParallelConcurrency for () {
    fn resolve(self) -> usize {
        std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(1)
    }
}

/// How many items one worker claims per lock acquisition.
///
/// One item per lock makes the shared mutex the bottleneck: with N workers
/// hammering it, every item pays a cache-line transfer. Claiming a small batch
/// amortizes that, while staying small enough that a worker never sits on more
/// than a few percent of the input (which would starve the others of work when
/// item costs are uneven).
fn handoff_batch(len: usize, workers: usize) -> usize {
    (len / (workers * 16)).clamp(1, 8)
}

/// Claim size for [`parallel_for_each`], whose stream length is unknown.
const STREAM_BATCH: usize = 8;

/// Runs `f` over every element of `items`, with at most `concurrency`
/// futures in flight, and returns the results **in input order**.
///
/// If any future fails, its error is aggregated into a single
/// [`AggregateError`]; every other item still gets processed (fan-out
/// semantics) so a failure doesn't stop in-flight work.
///
/// Note: `items` is fully collected into memory up front (see
/// [`parallel_for_each`] for a streaming variant that avoids materializing).
///
/// `concurrency` accepts an explicit `usize` or `()` to auto-size from the
/// host CPU (see [`ParallelConcurrency`]).
///
/// ```
/// use mytheclipse::parallel_map::parallel_map;
///
/// #[tokio::main]
/// async fn main() {
///     let items = vec![1u32, 2, 3, 4, 5];
///     // Auto concurrency: `()` resolved to available_parallelism().
///     let doubled = parallel_map(items, (), |x| async move { Ok::<_, std::io::Error>(x * 2) })
///         .await
///         .unwrap();
///     assert_eq!(doubled, vec![2, 4, 6, 8, 10]);
/// }
/// ```
pub async fn parallel_map<I, T, F, Fut, E, C>(
    items: I,
    concurrency: C,
    f: F,
) -> Result<Vec<T>, AggregateError>
where
    I: IntoIterator,
    I::Item: Send + 'static,
    T: Send + 'static,
    F: Fn(I::Item) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<T, E>> + Send + 'static,
    E: std::error::Error + Send + Sync + 'static,
    C: ParallelConcurrency,
{
    let items: Vec<I::Item> = items.into_iter().collect();
    if items.is_empty() {
        return Ok(Vec::new());
    }
    let len = items.len();
    let workers = concurrency.resolve().min(len);

    // Workers take the next item under a short lock and hand the result back
    // tagged with its index, so ordering needs no shared output buffer.
    let queue = Arc::new(Mutex::new(items.into_iter().enumerate()));
    let f = Arc::new(f);
    let batch = handoff_batch(len, workers);

    let mut handles = Vec::with_capacity(workers);
    for _ in 0..workers {
        let queue = Arc::clone(&queue);
        let f = Arc::clone(&f);
        handles.push(tokio::spawn(async move {
            // One slot per item this worker is expected to handle, so the
            // result vector does not reallocate on the way to `len / workers`.
            let mut done: Vec<(usize, Result<T, E>)> = Vec::with_capacity(len / workers + batch);
            let mut claimed: Vec<(usize, I::Item)> = Vec::with_capacity(batch);
            loop {
                {
                    let mut q = queue.lock().expect("item queue poisoned");
                    claimed.extend(q.by_ref().take(batch));
                }
                if claimed.is_empty() {
                    break;
                }
                for (index, item) in claimed.drain(..) {
                    done.push((index, f(item).await));
                }
            }
            done
        }));
    }

    let mut slots: Vec<Option<T>> = Vec::new();
    slots.resize_with(len, || None);
    let mut errors = AggregateError::empty();
    for handle in handles {
        match handle.await {
            Ok(done) => {
                for (index, result) in done {
                    match result {
                        Ok(value) => slots[index] = Some(value),
                        Err(e) => errors.push(Box::new(e)),
                    }
                }
            }
            Err(join_err) => errors.push(Box::new(join_err)),
        }
    }

    if !errors.is_empty() {
        return Err(errors);
    }
    Ok(slots
        .into_iter()
        .map(|slot| slot.expect("every index is written by exactly one worker"))
        .collect())
}

/// Alias of [`parallel_map`] kept for API symmetry.
///
/// Results still come back in **input order**: workers tag each result with
/// its index, so completion order never leaks into the output.
pub async fn parallel_map_unordered<I, T, F, Fut, E, C>(
    items: I,
    concurrency: C,
    f: F,
) -> Result<Vec<T>, AggregateError>
where
    I: IntoIterator,
    I::Item: Send + 'static,
    T: Send + 'static,
    F: Fn(I::Item) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<T, E>> + Send + 'static,
    E: std::error::Error + Send + Sync + 'static,
    C: ParallelConcurrency,
{
    parallel_map(items, concurrency, f).await
}

/// Streaming bounded parallel fan-out: runs `f` over each item with at most
/// `concurrency` futures in flight, **without materializing the whole input
/// collection in memory first**.
///
/// This is the right choice for large/streaming inputs (e.g. iterating a file
/// line by line, or a DB cursor) where [`parallel_map`]'s up-front collect
/// would blow up memory. The iterator is pulled on demand — a worker takes the
/// next item only when it is free — so the producer can never run ahead.
///
/// Errors are aggregated into a single [`AggregateError`].
///
/// `concurrency` accepts an explicit `usize` or `()` to auto-size from the
/// host CPU (see [`ParallelConcurrency`]).
///
/// ```
/// use std::sync::atomic::{AtomicUsize, Ordering};
/// use std::sync::Arc;
/// use mytheclipse::parallel_map::parallel_for_each;
///
/// #[tokio::main]
/// async fn main() {
///     let seen = Arc::new(AtomicUsize::new(0));
///     let s = Arc::clone(&seen);
///     parallel_for_each(0u32..100, (), move |x| {
///         let s = Arc::clone(&s);
///         async move {
///             s.fetch_add(x as usize, Ordering::SeqCst);
///             Ok::<_, std::io::Error>(())
///         }
///     })
///     .await
///     .unwrap();
///     assert_eq!(seen.load(Ordering::SeqCst), 4950); // sum 0..100
/// }
/// ```
pub async fn parallel_for_each<I, F, Fut, E, C>(
    items: I,
    concurrency: C,
    f: F,
) -> Result<(), AggregateError>
where
    I: IntoIterator + Send + 'static,
    I::Item: Send + 'static,
    I::IntoIter: Send,
    F: Fn(I::Item) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<(), E>> + Send + 'static,
    E: std::error::Error + Send + Sync + 'static,
    C: ParallelConcurrency,
{
    let workers = concurrency.resolve();
    let queue = Arc::new(Mutex::new(items.into_iter()));
    let f = Arc::new(f);
    // The stream length is unknown, so the claim size is fixed: big enough to
    // amortize the hand-off lock, small enough to keep read-ahead bounded.
    let batch = STREAM_BATCH;

    let mut handles = Vec::with_capacity(workers);
    for _ in 0..workers {
        let queue = Arc::clone(&queue);
        let f = Arc::clone(&f);
        handles.push(tokio::spawn(async move {
            let mut errors: Vec<Box<dyn std::error::Error + Send + Sync>> = Vec::new();
            let mut claimed: Vec<I::Item> = Vec::with_capacity(batch);
            loop {
                {
                    let mut q = queue.lock().expect("item queue poisoned");
                    claimed.extend(q.by_ref().take(batch));
                }
                if claimed.is_empty() {
                    break;
                }
                for item in claimed.drain(..) {
                    if let Err(e) = f(item).await {
                        errors.push(Box::new(e));
                    }
                }
            }
            errors
        }));
    }

    let mut errors = AggregateError::empty();
    for handle in handles {
        match handle.await {
            Ok(worker_errors) => {
                for error in worker_errors {
                    errors.push(error);
                }
            }
            Err(join_err) => errors.push(Box::new(join_err)),
        }
    }

    if errors.is_empty() {
        Ok(())
    } else {
        Err(errors)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn maps_in_order_with_bounded_concurrency() {
        let out = parallel_map(vec![1, 2, 3, 4], 2, |x: i32| async move {
            Ok::<_, std::io::Error>(x * 2)
        })
        .await
        .unwrap();
        assert_eq!(out, vec![2, 4, 6, 8]);
    }

    #[tokio::test]
    async fn aggregates_errors_from_failing_tasks() {
        let out = parallel_map(vec![1, 2, 3], 4, |x: i32| async move {
            if x == 2 {
                Err(std::io::Error::other("boom"))
            } else {
                Ok::<_, std::io::Error>(x)
            }
        })
        .await;
        assert!(out.is_err());
        assert_eq!(out.unwrap_err().len(), 1);
    }

    #[tokio::test]
    async fn empty_input_returns_empty() {
        let out: Result<Vec<i32>, AggregateError> =
            parallel_map(Vec::<i32>::new(), 4, |x: i32| async move {
                Ok::<_, std::io::Error>(x)
            })
            .await;
        assert_eq!(out.unwrap(), vec![]);
    }

    #[tokio::test]
    async fn for_each_processes_all_items() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        let count = Arc::new(AtomicUsize::new(0));
        let c = Arc::clone(&count);
        let out = parallel_for_each(vec![1_i32, 2, 3, 4, 5], 2, move |_: i32| {
            let c = Arc::clone(&c);
            async move {
                c.fetch_add(1, Ordering::SeqCst);
                Ok::<_, std::io::Error>(())
            }
        })
        .await;
        assert!(out.is_ok());
        assert_eq!(count.load(Ordering::SeqCst), 5);
    }

    /// The bound must cap futures in flight, not just task creation.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrency_bound_is_respected() {
        use std::sync::atomic::{AtomicUsize, Ordering};

        let in_flight = Arc::new(AtomicUsize::new(0));
        let peak = Arc::new(AtomicUsize::new(0));
        let (i, p) = (Arc::clone(&in_flight), Arc::clone(&peak));

        parallel_map(0..64u32, 4, move |x| {
            let (i, p) = (Arc::clone(&i), Arc::clone(&p));
            async move {
                let now = i.fetch_add(1, Ordering::SeqCst) + 1;
                p.fetch_max(now, Ordering::SeqCst);
                tokio::task::yield_now().await;
                i.fetch_sub(1, Ordering::SeqCst);
                Ok::<_, std::io::Error>(x)
            }
        })
        .await
        .unwrap();

        let observed = peak.load(Ordering::SeqCst);
        assert!(observed <= 4, "peak in-flight {observed} exceeded bound 4");
    }

    /// Every index must be written exactly once and stay in input order even
    /// when completion order is scrambled by yielding.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn all_results_land_in_input_order() {
        let out = parallel_map(0..500u32, 8, |x| async move {
            tokio::task::yield_now().await;
            Ok::<_, std::io::Error>(x)
        })
        .await
        .unwrap();
        assert_eq!(out, (0..500).collect::<Vec<_>>());
    }

    #[tokio::test]
    async fn for_each_aggregates_errors_and_keeps_going() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        let seen = Arc::new(AtomicUsize::new(0));
        let s = Arc::clone(&seen);
        let out = parallel_for_each(0..10u32, 2, move |x| {
            let s = Arc::clone(&s);
            async move {
                s.fetch_add(1, Ordering::SeqCst);
                if x % 2 == 0 {
                    Err(std::io::Error::other("even"))
                } else {
                    Ok(())
                }
            }
        })
        .await;
        assert_eq!(out.unwrap_err().len(), 5);
        assert_eq!(seen.load(Ordering::SeqCst), 10);
    }
}
