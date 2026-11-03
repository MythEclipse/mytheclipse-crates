//! End-to-end demo of the high-level (round 7-15) abstractions in `mytheclipse`.
//!
//! Run with: `cargo run -p mytheclipse --features full --example high_level`
//!
//! Demonstrates, wired together in one realistic flow:
//!   - `RuntimeConfig::auto()`  — auto thread/core sizing from the host CPU
//!   - `parallel_map`           — bounded fan-out with AggregateError
//!   - `RetryExt`               — ergonomic `.retry()` on a Future
//!   - `ShutdownGuard`          — RAII cleanup guaranteed even on panic
//!   - `AutoReconnectPool`      — self-healing resource pool
//!   - `AutoMetricsServiceBuilder` — service calls that auto-record metrics

use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::Duration;

use mytheclipse::{
    aggregate_error::AggregateError,
    auto_metrics_service::AutoMetricsServiceBuilder,
    parallel_map::parallel_map,
    pool::{AutoReconnectPool, Pool, Reconnectable, SemaphorePool},
    retry_ext::RetryExt,
    runtime_auto::RuntimeConfig,
    service_builder::ServiceConfig,
    shutdown_guard::ShutdownGuard,
};

#[tokio::main]
async fn main() {
    println!("== mytheclipse high-level abstractions demo ==");

    // 1. Auto thread/core allocation from host CPU topology.
    let cfg = RuntimeConfig::auto();
    println!(
        "1. RuntimeConfig::auto() -> worker={}, blocking={}, compute={}, io={}",
        cfg.worker_threads, cfg.max_blocking_threads, cfg.compute_threads, cfg.io_threads
    );

    // 2. Bounded parallel fan-out with automatic error aggregation.
    let results: Result<Vec<i32>, AggregateError> =
        parallel_map(vec![1, 2, 3, 4, 5], 2, |x| async move {
            tokio::time::sleep(Duration::from_millis(10)).await;
            Ok::<_, std::io::Error>(x * 10)
        })
        .await;
    println!("2. parallel_map -> {:?}", results.unwrap());

    // 3. Ergonomic retry on a Future.
    let attempts = Arc::new(AtomicU32::new(0));
    let a = Arc::clone(&attempts);
    let cfg = mytheclipse::retry::RetryConfig {
        max_attempts: 4,
        base_delay: Duration::from_millis(1),
        ..Default::default()
    };
    let fut = async { Err::<u32, String>("boom".into()) };
    let op = move || {
        let a = Arc::clone(&a);
        async move {
            let n = a.fetch_add(1, Ordering::SeqCst);
            if n < 3 {
                Err::<u32, String>("transient".into())
            } else {
                Ok(42u32)
            }
        }
    };
    let value = fut.retry(cfg, |_: &String| true, op).await.unwrap();
    println!(
        "3. RetryExt with {} attempts -> {value}",
        attempts.load(Ordering::SeqCst)
    );

    // 4. RAII ShutdownGuard — callback runs exactly once on drop, panic-safe.
    let fired = Arc::new(AtomicU32::new(0));
    let f = Arc::clone(&fired);
    {
        let _guard = ShutdownGuard::new(move || {
            f.fetch_add(1, Ordering::SeqCst);
        });
    } // guard dropped -> callback fires
    println!(
        "4. ShutdownGuard fired {}x (exactly-once, even on unwind)",
        fired.load(Ordering::SeqCst)
    );

    // 5. AutoReconnectPool — transparently replaces dead resources.
    let auto = AutoReconnectPool::new(SemaphorePool::new(vec![1u32, 2u32]), Probe { dead: 1 });
    let first = auto.acquire().await.unwrap().resource;
    // Never the dead value (1) once the probe rejects it.
    println!("5. AutoReconnectPool first acquire -> {first}");

    // 6. AutoMetricsServiceBuilder — auto records latency + outcome counters.
    let svc_cfg = ServiceConfig {
        max_attempts: 2,
        ..Default::default()
    };
    let svc = AutoMetricsServiceBuilder::new("demo_op", svc_cfg);
    let n = Arc::new(AtomicU32::new(0));
    let n2 = Arc::clone(&n);
    let _: Result<u32, mytheclipse::service_builder::RunError<()>> = svc
        .run(|| {
            let n2 = Arc::clone(&n2);
            Box::pin(async move {
                tokio::time::sleep(Duration::from_millis(5)).await;
                let v = n2.fetch_add(1, Ordering::SeqCst);
                if v < 1 {
                    Err(())
                } else {
                    Ok(7u32)
                }
            })
        })
        .await;
    let snap = svc.collector().snapshot();
    println!(
        "6. AutoMetrics -> {} counters, {} histograms",
        snap.counters.len(),
        snap.histograms.len()
    );

    println!("== demo complete ==");
}

/// A [`Reconnectable`] probe that rejects any value matching `dead`.
struct Probe {
    dead: u32,
}

#[async_trait::async_trait]
impl Reconnectable for Probe {
    type Item = u32;

    fn is_healthy(&self, item: &Self::Item) -> bool {
        *item != self.dead
    }

    async fn reconnect(&self) -> Result<Self::Item, Box<dyn std::error::Error + Send + Sync>> {
        Ok(999)
    }
}
