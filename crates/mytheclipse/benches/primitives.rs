//! Criterion benchmarks for the high-level primitives.
//!
//! Run: `cargo bench -p mytheclipse --bench primitives --all-features`
//!
//! These measure the *overhead* of the race-safe abstractions — the cost you
//! pay for auto thread/core allocation, bounded fan-out, and exactly-once
//! cleanup. Numbers (per-op) should be tiny; anything in the µs+ range for a
//! single primitive would signal a regression.

use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::Duration;

use criterion::{black_box, criterion_group, criterion_main, Criterion};
use tokio::runtime::Runtime;

use mytheclipse::aggregate_error::AggregateError;
use mytheclipse::parallel_map::{parallel_for_each, parallel_map};
use mytheclipse::pool::{Pool, SemaphorePool};
use mytheclipse::ratelimit::RateLimiter;
use mytheclipse::retry::RetryConfig;
use mytheclipse::retry_ext::RetryExt;
use mytheclipse::shutdown_guard::ShutdownGuard;

fn rt() -> Runtime {
    Runtime::new().expect("tokio runtime")
}

fn bench_parallel_map(c: &mut Criterion) {
    let rt = rt();
    c.bench_function("parallel_map/1000x8", |b| {
        b.to_async(&rt).iter(|| async {
            let items: Vec<u32> = (0..1000).collect();
            let out = parallel_map(items, 8, |x| async move { Ok::<_, std::io::Error>(x * 2) })
                .await
                .unwrap();
            let _ = black_box(out);
        });
    });

    // Sequential baseline for comparison.
    c.bench_function("sequential/map_1000", |b| {
        b.iter(|| {
            let items: Vec<u32> = (0..1000).collect();
            let out: Vec<u32> = items.into_iter().map(|x| x * 2).collect();
            black_box(out);
        });
    });
}

fn bench_parallel_for_each(c: &mut Criterion) {
    let rt = rt();
    c.bench_function("parallel_for_each/1000x8", |b| {
        b.to_async(&rt).iter(|| async {
            parallel_for_each(
                0u32..1000,
                8,
                |_| async move { Ok::<_, std::io::Error>(()) },
            )
            .await
            .unwrap();
            black_box(());
        });
    });
}

fn bench_retry_ext(c: &mut Criterion) {
    let rt = rt();
    c.bench_function("retry_ext/success_first", |b| {
        b.to_async(&rt).iter(|| async {
            let op = || async { Ok::<u32, std::io::Error>(42) };
            let out = async { Ok::<u32, std::io::Error>(42) }
                .retry(
                    RetryConfig {
                        max_attempts: 3,
                        base_delay: Duration::from_millis(1),
                        ..RetryConfig::default()
                    },
                    |_: &std::io::Error| true,
                    op,
                )
                .await
                .unwrap();
            black_box(out);
        });
    });

    c.bench_function("retry_ext/retries_twice", |b| {
        b.to_async(&rt).iter(|| async {
            let attempts = Arc::new(AtomicU32::new(0));
            let a = Arc::clone(&attempts);
            let op = move || {
                let a = Arc::clone(&a);
                async move {
                    if a.fetch_add(1, Ordering::SeqCst) < 2 {
                        Err::<u32, std::io::Error>(std::io::Error::other("x"))
                    } else {
                        Ok(42)
                    }
                }
            };
            let out = async { Err::<u32, std::io::Error>(std::io::Error::other("first")) }
                .retry(
                    RetryConfig {
                        max_attempts: 3,
                        base_delay: Duration::from_millis(1),
                        ..RetryConfig::default()
                    },
                    |_: &std::io::Error| true,
                    op,
                )
                .await
                .unwrap();
            black_box(out);
        });
    });
}

fn bench_rate_limiter(c: &mut Criterion) {
    c.bench_function("rate_limiter/try_acquire", |b| {
        let rl = RateLimiter::new(1_000_000.0, 10_000_000);
        b.iter(|| {
            let _ = black_box(rl.try_acquire());
        });
    });
}

fn bench_semaphore_pool(c: &mut Criterion) {
    let rt = rt();
    c.bench_function("semaphore_pool/acquire_release", |b| {
        b.to_async(&rt).iter(|| async {
            let pool = SemaphorePool::new(vec![1u32, 2, 3, 4, 5]);
            let item = pool.acquire().await.unwrap();
            black_box(item.resource);
        });
    });
}

fn bench_aggregate_error(c: &mut Criterion) {
    c.bench_function("aggregate_error/all_ok_1000", |b| {
        b.iter(|| {
            let results: Vec<Result<u32, std::io::Error>> = (0..1000).map(Ok).collect();
            let out = AggregateError::from_results(black_box(results)).unwrap();
            black_box(out);
        });
    });

    c.bench_function("aggregate_error/500err_1000", |b| {
        b.iter(|| {
            let results: Vec<Result<u32, std::io::Error>> = (0..1000)
                .map(|i| {
                    if i % 2 == 0 {
                        Err(std::io::Error::other("x"))
                    } else {
                        Ok(i)
                    }
                })
                .collect();
            let out = AggregateError::from_results(black_box(results));
            let _ = black_box(out);
        });
    });
}

fn bench_shutdown_guard(c: &mut Criterion) {
    c.bench_function("shutdown_guard/new_drop", |b| {
        b.iter(|| {
            let g = ShutdownGuard::new(|| {});
            black_box(&g);
        });
    });
}

criterion_group!(
    benches,
    bench_parallel_map,
    bench_parallel_for_each,
    bench_retry_ext,
    bench_rate_limiter,
    bench_semaphore_pool,
    bench_aggregate_error,
    bench_shutdown_guard,
);
criterion_main!(benches);
