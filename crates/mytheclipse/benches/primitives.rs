//! Criterion benchmarks for the high-level primitives.
//!
//! Run: `cargo bench -p mytheclipse --bench primitives --all-features`
//!
//! These measure the *overhead* of the race-safe abstractions — the cost you
//! pay for auto thread/core allocation, bounded fan-out, and exactly-once
//! cleanup. Numbers (per-op) should be tiny; anything in the µs+ range for a
//! single primitive would signal a regression.

use std::sync::atomic::{AtomicU32, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use criterion::{black_box, criterion_group, criterion_main, Criterion};
use tokio::runtime::Runtime;

use mytheclipse::aggregate_error::AggregateError;
use mytheclipse::compute::{compute_join, compute_map, compute_par_for_each};
use mytheclipse::cron::CronTime;
use mytheclipse::parallel_map::{parallel_for_each, parallel_map};
use mytheclipse::pool::{Pool, SemaphorePool};
use mytheclipse::ratelimit::RateLimiter;
use mytheclipse::retry::RetryConfig;
use mytheclipse::retry_ext::RetryExt;
use mytheclipse::shutdown_guard::ShutdownGuard;
use mytheclipse::{
    BackpressureQueue, CircuitBreaker, CircuitBreakerConfig, ConcurrencyLimiter, CronSchedule,
    DistributedLock, InProcLock, MetricsCollector, OverflowPolicy,
};

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

    // Fixed cost of the abstraction: 8 items through 8 workers is the
    // worst case for fan-out overhead, because every worker is spawned and
    // claims exactly one item.
    c.bench_function("parallel_map/8x8", |b| {
        b.to_async(&rt).iter(|| async {
            let items: Vec<u32> = (0..8).collect();
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
    c.bench_function("parallel_for_each/8x8", |b| {
        b.to_async(&rt).iter(|| async {
            parallel_for_each(0u32..8, 8, |x| async move {
                let _ = black_box(x);
                Ok::<_, std::io::Error>(())
            })
            .await
            .unwrap();
        });
    });
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

fn bench_metrics(c: &mut Criterion) {
    let mut g = c.benchmark_group("metrics");
    let hot = MetricsCollector::new();
    hot.inc_counter("hot", 1);

    g.bench_function("inc_counter_existing", |b| {
        b.iter(|| hot.inc_counter("hot", 1));
    });

    // Same operation on a collector that already carries 64 series, which is
    // the realistic steady state of an app reporting per-route metrics.
    let wide = MetricsCollector::new();
    for i in 0..64 {
        wide.inc_counter(&format!("series_{i}"), 1);
    }
    g.bench_function("inc_counter_among_64", |b| {
        b.iter(|| wide.inc_counter("series_63", 1));
    });

    g.bench_function("set_gauge_existing", |b| {
        b.iter(|| hot.set_gauge("g", 1.5));
    });
    g.bench_function("observe_existing", |b| {
        b.iter(|| hot.observe("lat", Duration::from_micros(120)));
    });
    g.bench_function("export_prometheus_64", |b| {
        b.iter(|| black_box(wide.export_prometheus()));
    });
    g.finish();
}

fn bench_circuit_breaker(c: &mut Criterion) {
    let mut g = c.benchmark_group("circuit_breaker");
    let closed = CircuitBreaker::new(CircuitBreakerConfig::default());
    g.bench_function("closed_allow_record_success", |b| {
        b.iter(|| {
            if closed.allow_request() {
                closed.record_success();
            }
        });
    });

    let open = CircuitBreaker::new(CircuitBreakerConfig {
        failure_threshold: 1,
        open_timeout: Duration::from_secs(3600),
        ..CircuitBreakerConfig::default()
    });
    open.record_failure();
    g.bench_function("open_reject", |b| {
        b.iter(|| black_box(open.allow_request()));
    });
    g.finish();
}

fn bench_dlock(c: &mut Criterion) {
    let rt = rt();
    let mut g = c.benchmark_group("dlock");

    let single = InProcLock::new();
    g.bench_function("acquire_release_same_key", |b| {
        b.to_async(&rt).iter(|| async {
            let guard = single
                .acquire("k", Duration::from_secs(60), Duration::from_millis(1))
                .await
                .unwrap();
            drop(guard);
        });
    });

    // A thousand live leases in the map: acquisition must stay O(1), so this
    // should track `acquire_release_same_key` rather than grow with map size.
    let many = InProcLock::new();
    let keys: Vec<String> = (0..1000).map(|i| format!("key_{i}")).collect();
    let next = AtomicUsize::new(0);
    g.bench_function("acquire_release_1000_keys", |b| {
        b.to_async(&rt).iter(|| async {
            let i = next.fetch_add(1, Ordering::Relaxed) % keys.len();
            let guard = many
                .acquire(&keys[i], Duration::from_secs(60), Duration::from_millis(1))
                .await
                .unwrap();
            drop(guard);
        });
    });
    g.finish();
}

fn bench_cron(c: &mut Criterion) {
    let mut g = c.benchmark_group("cron");
    g.bench_function("parse_5_field", |b| {
        b.iter(|| CronSchedule::parse("*/5 * * * *").unwrap());
    });

    // A fixed instant, not `CronTime::now()`: the cost of `next_after` grows
    // with how far ahead the answer lies, so anchoring to the wall clock made
    // this bench report anything from 30 ns to 1.3 µs depending on the minute it
    // happened to run in.
    let from = CronTime {
        year: 2026,
        month: 11,
        day: 3,
        hour: 10,
        minute: 17,
        second: 0,
    };
    let hourly = CronSchedule::parse("0 * * * *").unwrap();
    g.bench_function("next_after_hourly", |b| {
        b.iter(|| black_box(hourly.next_after(from)));
    });

    // Worst case for the scan: a schedule that matches once every four years.
    let yearly = CronSchedule::parse("0 12 29 2 *").unwrap();
    g.bench_function("next_after_yearly", |b| {
        b.iter(|| black_box(yearly.next_after(from)));
    });
    g.finish();
}

fn bench_concurrency_limiter(c: &mut Criterion) {
    let limiter = ConcurrencyLimiter::new(8);
    c.bench_function("concurrency_limiter/try_acquire_release", |b| {
        b.iter(|| {
            let permit = limiter.try_acquire().expect("free slot");
            drop(permit);
        });
    });
}

fn bench_backpressure(c: &mut Criterion) {
    let queue = BackpressureQueue::new(1024, OverflowPolicy::DropOldest);
    c.bench_function("backpressure/push_pop", |b| {
        b.iter(|| {
            let _ = queue.try_push(1u64);
            black_box(queue.try_pop());
        });
    });
}

fn bench_compute(c: &mut Criterion) {
    let items: Vec<u64> = (0..1000).collect();
    c.bench_function("compute_map/1000", |b| {
        b.iter(|| {
            let out = compute_map(items.clone(), |x| Ok::<_, String>(x * 2)).unwrap();
            let _ = black_box(out);
        });
    });

    let small: Vec<u64> = (0..8).collect();
    c.bench_function("compute_map/8", |b| {
        b.iter(|| {
            let out = compute_map(small.clone(), |x| Ok::<_, String>(x * 2)).unwrap();
            let _ = black_box(out);
        });
    });

    c.bench_function("compute_par_for_each/1000", |b| {
        b.iter(|| {
            compute_par_for_each(0u64..1000, |x| {
                black_box(x);
                Ok::<_, String>(())
            })
            .unwrap();
        });
    });

    c.bench_function("compute_join", |b| {
        b.iter(|| {
            let out = compute_join(
                || (0..1000u64).sum::<u64>(),
                || (1000..2000u64).sum::<u64>(),
            )
            .unwrap();
            let _ = black_box(out);
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
    bench_metrics,
    bench_circuit_breaker,
    bench_dlock,
    bench_cron,
    bench_concurrency_limiter,
    bench_backpressure,
    bench_compute,
);
criterion_main!(benches);
