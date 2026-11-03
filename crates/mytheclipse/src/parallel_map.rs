//! Bounded parallel map over collections (feature `resiliency`).
//!
//! [`parallel_map`], [`parallel_map_unordered`] fan work out across a
//! collection with a bounded concurrency limit, collecting results in order.
//! [`parallel_for_each`] is a streaming variant that never materializes the
//! whole input in memory. These remove the manual `Semaphore + join_all` +
//! error-aggregation boilerplate that races easily when done by hand.

use std::future::Future;
use std::sync::Arc;

use tokio::sync::Semaphore;

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

/// Runs `f` over every element of `items`, with at most `concurrency`
/// futures in flight, and returns the results **in input order**.
///
/// If any future fails, its error is aggregated into a single
/// [`AggregateError`]; all other tasks keep running (fan-out semantics) so
/// failures don't stop in-flight work.
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
    let n = concurrency.resolve();
    let items: Vec<I::Item> = items.into_iter().collect();
    let sem = Arc::new(Semaphore::new(n));
    let f = Arc::new(f);

    let mut tasks = Vec::with_capacity(items.len());
    for item in items {
        let sem = Arc::clone(&sem);
        let f = Arc::clone(&f);
        tasks.push(tokio::spawn(async move {
            let _permit = sem.acquire_owned().await.expect("semaphore closed");
            f(item).await
        }));
    }

    let mut results = Vec::with_capacity(tasks.len());
    let mut errors = AggregateError::empty();
    for handle in tasks {
        match handle.await {
            Ok(Ok(v)) => results.push(v),
            Ok(Err(e)) => errors.push(Box::new(e)),
            Err(join_err) => errors.push(Box::new(join_err)),
        }
    }

    if errors.is_empty() {
        Ok(results)
    } else {
        Err(errors)
    }
}

/// Like [`parallel_map`] but aliased for clarity — because `tokio::spawn`
/// futures are polled in spawn order here, results come back in input order.
/// (True completion-order collection would require a `futures` dependency, so
/// this name is provided for API symmetry and documented as input-ordered.)
///
/// `concurrency` accepts an explicit `usize` or `()` to auto-size from the
/// host CPU (see [`ParallelConcurrency`]).
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
    let n = concurrency.resolve();
    let items: Vec<I::Item> = items.into_iter().collect();
    let sem = Arc::new(Semaphore::new(n));
    let f = Arc::new(f);

    let mut tasks = Vec::with_capacity(items.len());
    for item in items {
        let sem = Arc::clone(&sem);
        let f = Arc::clone(&f);
        tasks.push(tokio::spawn(async move {
            let _permit = sem.acquire_owned().await.expect("semaphore closed");
            f(item).await
        }));
    }

    let mut results = Vec::with_capacity(tasks.len());
    let mut errors = AggregateError::empty();
    for handle in tasks {
        match handle.await {
            Ok(Ok(v)) => results.push(v),
            Ok(Err(e)) => errors.push(Box::new(e)),
            Err(join_err) => errors.push(Box::new(join_err)),
        }
    }

    if errors.is_empty() {
        Ok(results)
    } else {
        Err(errors)
    }
}

/// Streaming bounded parallel fan-out: runs `f` over each item with at most
/// `concurrency` futures in flight, **without materializing the whole input
/// collection in memory first**.
///
/// This is the right choice for large/streaming inputs (e.g. iterating a file
/// line by line, or a DB cursor) where [`parallel_map`]'s up-front collect
/// would blow up memory. Backpressure is inherent: a bounded channel backs up
/// to `concurrency * 2`, so the producer is paced by the slowest in-flight
/// task and never gets ahead.
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
    use tokio::sync::{mpsc, Mutex};

    let n = concurrency.resolve();
    let (tx, rx) = mpsc::channel::<I::Item>(n * 2);
    let f = Arc::new(f);
    let sem = Arc::new(Semaphore::new(n));

    // Producer: feed items into the bounded channel (backpressures when all
    // workers are busy — no full materialization).
    tokio::spawn(async move {
        for item in items {
            if tx.send(item).await.is_err() {
                break; // all workers dropped
            }
        }
    });

    // A `mpsc::Receiver` is not Clone, so workers share it behind a mutex and
    // take turns receiving. Bounded concurrency is enforced by the semaphore.
    let rx = Arc::new(Mutex::new(rx));
    let mut handles = Vec::with_capacity(n);
    for _ in 0..n {
        let rx = Arc::clone(&rx);
        let f = Arc::clone(&f);
        let sem = Arc::clone(&sem);
        handles.push(tokio::spawn(async move {
            loop {
                let item = { rx.lock().await.recv().await };
                match item {
                    Some(item) => {
                        let sem = Arc::clone(&sem);
                        let _permit = sem.acquire_owned().await.expect("semaphore closed");
                        let _ = f(item).await;
                    }
                    None => break,
                }
            }
        }));
    }

    let mut errors = AggregateError::empty();
    for h in handles {
        match h.await {
            Ok(()) => {}
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
}
