# mytheclipse

[![Crates.io](https://img.shields.io/crates/v/mytheclipse.svg)](https://crates.io/crates/mytheclipse)
[![Documentation](https://docs.rs/mytheclipse/badge.svg)](https://docs.rs/mytheclipse)
[![License](https://img.shields.io/badge/license-MIT%20OR%20Apache--2.0-blue.svg)](LICENSE-MIT)

Resource-aware execution primitives and reliability abstractions for Rust: async
I/O, heavy compute, background queue management, resiliency, traffic control,
lifecycle management, and observability. Sized automatically from the host's
logical core count and exposed through a single, lazily-initialized engine
context alongside self-contained, constructible utilities.

## Resource Sizing

Given `N` logical cores (`std::thread::available_parallelism()`):

| Subsystem | Sizing | On 8 cores | Backing primitive |
| :--- | :--- | :--- | :--- |
| **Async I/O** | `N` | 8 | Ambient `tokio::spawn` + `tracing` span |
| **Compute** | `max(1, N - 1)` | 7 | Process-wide worker pool + `catch_unwind` |
| **Background Queue** | `max(2, N / 2)` | 4 | `tokio::sync::Semaphore` + `tokio::spawn` |

The three execution primitives (`io`, `compute`, `bg`) are sized from the host's
CPU core count through `mytheclipse::context()`. Resiliency, traffic control,
lifecycle, and observability utilities are constructible per-instance without
global state.

## Features

Nothing is enabled by default (`default = []`), so you only pull in the
dependencies your application actually uses.

- **`io`** — `spawn_io`, instrumented async task spawning.
- **`compute`** — `compute`, `compute_join`, `compute_map`,
  `compute_par_for_each`: panic-isolated execution on a sized worker pool.
  The pool is process-wide and starts on first use, so a call pays a hand-off
  rather than a thread creation. Because the work goes to long-lived threads,
  closures, item types and results must be `'static`; a call made from inside a
  pool worker runs inline instead of submitting, so nesting cannot deadlock.
- **`bg`** — `spawn_bg`, semaphore-bounded background tasks.
- **`resiliency`** — fault tolerance:
  - `retry` / `retry_ext` — exponential backoff with full/equal jitter, plus a
    `.retry()` extension on any future.
  - `circuit_breaker` — Closed/Open/HalfOpen state machine with bounded probe
    admission.
  - `timeout` — `with_timeout`, `timeout`, `Timeout`.
  - `parallel_map` / `parallel_map_unordered` / `parallel_for_each` — bounded
    fan-out over a collection. Pass a `usize` for an explicit worker count, or
    `()` to size it from the host's CPU count.
  - `aggregate_error` — collect every failure instead of only the first.
  - `service_builder` — assemble a runnable service from these primitives.
- **`traffic`** — traffic and load control:
  - `ratelimit` — token-bucket limiter with burst capacity.
  - `backpressure` — bounded queue with `DropOldest`, `Reject`, and `Block`
    overflow policies.
  - `concurrency` — synchronous RAII permit capping concurrent operations.
  - `pool` — `SemaphorePool` and `AutoReconnectPool`.
- **`lifecycle`** — startup, shutdown, and scheduling:
  - `shutdown` / `shutdown_guard` — SIGINT/SIGTERM catching and graceful task
    draining.
  - `cron` — self-contained 5-field cron parser and async timer scheduler.
  - `health` / `leader` — health registry and in-process leader election.
  - `dlock` — `DistributedLock` with lease expiry.
  - `runtime_auto` — worker-thread sizing from the host CPU count.
- **`observability`** — runtime visibility:
  - `metrics` — thread-safe statistics collector with Prometheus text
    exposition format export.
  - `metrics_bridge` — expose health checks as metrics.
  - `panic_tracker` — non-fatal panic logging with tracing context.
- **`full`** — all of the above.

Three modules need a pair of features: `auto_metrics_service` and `middleware`
require `observability` + `resiliency`, and `pool_health` requires
`observability` + `traffic`.

## Quick Start

Add to your `Cargo.toml`:

```toml
[dependencies]
mytheclipse = { version = "4", features = ["full"] }
```

### 1. Execution Primitives

```rust
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let ctx = mytheclipse::init();

    // Async I/O (instrumented with tracing)
    let io = mytheclipse::spawn_io(async { 42 });

    // Heavy Compute (isolated from worker panics)
    let sum = mytheclipse::compute(|| (1..=1_000_000u64).sum::<u64>())?;

    // Background Queue (concurrency-bounded) — the await on spawn_bg itself
    // waits for a free slot, so it is the backpressure point.
    let bg = mytheclipse::spawn_bg(async { /* task */ }).await;

    let _ = (io.await, bg.await);
    Ok(())
}
```

Bounded fan-out, for when you have a collection rather than one task:

```rust
use mytheclipse::{parallel_for_each, parallel_map};

// 4 workers, results in input order.
let doubled = parallel_map(items, 4, |x| async move { Ok::<_, std::io::Error>(x * 2) }).await?;

// `()` sizes the bound from the host CPU count; order does not matter here.
parallel_for_each(urls, (), |url| async move { fetch(url).await }).await?;
```

### 2. Resiliency & Fault Tolerance

```rust
use std::time::Duration;

// Auto-Retry with Exponential Backoff + Jitter
let result = mytheclipse::retry(
    mytheclipse::RetryConfig::default(),
    |err| err.is_transient(),
    || async { make_network_request().await },
)
.await?;

// Circuit Breaker
let breaker = mytheclipse::CircuitBreaker::new(mytheclipse::CircuitBreakerConfig::default());
let value = breaker.call(|| fetch_remote_resource())?;

// Timeout & Deadlines
let value = mytheclipse::with_timeout(Duration::from_secs(5), async {
    long_running_task().await
})
.await?;
```

### 3. Traffic & Resource Control

```rust
// Rate Limiter (Token Bucket)
let limiter = mytheclipse::RateLimiter::new(100.0, 10);
limiter.acquire().await?;

// Concurrency Limiter (Sync Semaphore)
let limiter = mytheclipse::ConcurrencyLimiter::new(10);
let _permit = limiter.acquire(); // released on drop

// Backpressure Queue (Graceful Degradation)
let queue = mytheclipse::BackpressureQueue::new(100, mytheclipse::OverflowPolicy::DropOldest);
queue.push(job).await?;
let next_job = queue.pop().await;
```

### 4. Lifecycle & State Management

```rust
// Graceful Shutdown Manager
let shutdown = mytheclipse::ShutdownManager::new();
let sig = shutdown.handle();
tokio::spawn(async move {
    let mut sig = sig;
    tokio::select! {
        _ = sig.wait() => { /* clean up */ }
        _ = worker_loop() => {}
    }
});
shutdown.drain(Duration::from_secs(10)).await;

// Cron Periodic Job Scheduler (self-contained, no external crates)
let cron = mytheclipse::CronSchedule::parse("0 1 * * *")?; // 1 AM daily
let job = mytheclipse::schedule("0 1 * * *", || async {
    clean_cache().await;
})?;
```

### 5. Telemetry & Observability

```rust
// Centralized Metrics Collector (Prometheus text exposition format)
let metrics = mytheclipse::MetricsCollector::new();
metrics.record_task(Duration::from_millis(15));
metrics.inc_counter("http_requests_total", 1);
let prometheus_output = metrics.export_prometheus();

// Panic Isolation Tracker
let guard = mytheclipse::PanicTracker::install(); // logs panics with tracing span
let result = mytheclipse::PanicTracker::catch(|| risky_operation());
```

## Running the Examples

```bash
cargo run --example main --features full
cargo run --example high_level --features full
cargo run --example scaling_demo --features full
```

## Benchmarks

Hot-path timings for the primitives in this crate (and the reference numbers
they are compared against) live in the workspace README.

```bash
cargo bench -p mytheclipse --bench primitives --all-features
```

## License

Licensed under either of:

- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE) or <http://www.apache.org/licenses/LICENSE-2.0>)
- MIT license ([LICENSE-MIT](LICENSE-MIT) or <http://opensource.org/licenses/MIT>)

at your option.
