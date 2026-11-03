# mytheclipse

A personal collection of Rust abstractions for building resource-aware,
resilient, and well-instrumented applications without hand-rolling the same
plumbing every time — organized as a Cargo workspace, one focused crate per
concern.

[![License](https://img.shields.io/badge/license-MIT%20OR%20Apache--2.0-blue.svg)](crates/mytheclipse/LICENSE-MIT)

## Crates

| Crate | Description | Docs |
| :--- | :--- | :--- |
| [`mytheclipse`](crates/mytheclipse) | Resource-aware execution primitives (async I/O, compute, background queues), resiliency (retry, circuit breaker, timeout), traffic control (rate limiter, backpressure, concurrency limiter), lifecycle (graceful shutdown, cron, async lifecycle manager, distributed lock), and observability (metrics, panic tracking, metrics-to-health bridge). | [README](crates/mytheclipse/README.md) |
| [`mytheclipse-cache`](crates/mytheclipse-cache) | Unified multi-layer (L1/L2) cache abstraction: in-memory or Moka L1, Redis/Valkey L2, cache-aside read-through. | [README](crates/mytheclipse-cache/README.md) |
| [`mytheclipse-event`](crates/mytheclipse-event) | Unified events & message bus abstraction: in-memory pub/sub dispatcher plus RabbitMQ and NATS broker adapters behind one trait. | [README](crates/mytheclipse-event/README.md) |
| [`mytheclipse-queue`](crates/mytheclipse-queue) | Job queue abstraction with WorkerPool executor, retry/backoff, and dead-letter support. In-memory backend only; `Queue` is a trait for adding another transport. | [README](crates/mytheclipse-queue/README.md) |
| [`mytheclipse-tracing`](crates/mytheclipse-tracing) | Pre-built `tracing-subscriber` fmt layer with `RUST_LOG` filtering, installed in one call. | [README](crates/mytheclipse-tracing/README.md) |

Every crate follows the same philosophy: **one small interface, pluggable
backends behind feature flags, and a working default that needs no external
service to build or test.** Distributed backends (Redis, RabbitMQ, NATS) are
feature-gated and their integration tests are `#[ignore]`d unless
the corresponding environment variables point at a live service.

## Getting started

Each crate is published independently; add the ones you need:

```toml
[dependencies]
mytheclipse = { version = "1", features = ["full"] }
mytheclipse-cache = "1"
mytheclipse-event = { version = "1", features = ["nats"] }
```

See each crate's own README (linked above) for usage examples and the full
feature-flag list.

## Performance

Four criterion suites cover the hot primitives. None is a correctness gate —
run them before and after a change to the code they measure:

```bash
cargo bench -p mytheclipse --bench primitives --all-features
cargo bench -p mytheclipse-cache --bench cache
cargo bench -p mytheclipse-queue --bench queue
cargo bench -p mytheclipse-event --bench event
```

Reference numbers on an Intel i5-13500HX (Linux, release profile with
`lto = "thin"` + `codegen-units = 1`, set in the workspace `Cargo.toml`). Treat
them as a regression baseline, not an absolute target: run-to-run variance is
up to ±10% on this hardware (worse for the benches that spawn tasks), so only
read a change above that as real.

| primitive | time |
| --- | --- |
| `circuit_breaker` closed allow+record | 1.7 ns |
| `metrics` inc_counter / observe | 17 ns / 17 ns |
| `queue` job id | 16 ns |
| `shutdown_guard` new+drop | 25 ns |
| `cache` get hit / miss | 26 ns / 20 ns |
| `concurrency_limiter` try_acquire+release | 29 ns |
| `rate_limiter` try_acquire | 30 ns |
| `backpressure` push+pop | 34 ns |
| `queue` len / enqueue+dequeue | 37 ns / 154 ns |
| `cache` set / set at capacity (evict) | 44 ns / 112 ns |
| `event` publish, no subscriber / with | 50 ns / 73 ns |
| `semaphore_pool` acquire+release | 91 ns |
| `dlock` acquire+release (1k live keys) | 122 ns |
| `cron` parse / next_after hourly / yearly | 282 ns / 1.0 µs / 2.0 µs |
| `metrics` export_prometheus (64 series) | 7.3 µs |
| `compute_join` | 5.0 µs |
| `parallel_map` 8 items @ 8 | 5.5 µs |
| `compute_map` 8 items | 6.5 µs |
| `parallel_for_each` 1000 items @ 8 | 12 µs |
| `compute_par_for_each` 1000 items | 21 µs |
| `compute_map` 1000 items | 23 µs |
| `parallel_map` 1000 items @ 8 | 26 µs |

`parallel_map` and `parallel_for_each` spawn `concurrency` workers that pull
from a shared iterator rather than one task per item, and each worker claims a
small batch of items per lock acquisition: the bound then caps task creation
*and* futures in flight, while the hand-off lock is amortized instead of paid
per item.

The `compute` primitives hand their chunks to a process-wide pool of worker
threads that starts on first use. That pool is the whole point: a thread costs
tens of microseconds to create, so spawning workers per call would cost more
than the work itself — measured at 100 µs for an eight-item map before the pool
existed, against 6.5 µs after.

### On `-C target-cpu=native`

Not enabled, because it does not help what this workspace does. Every
primitive here is dominated by pointer chasing — hash probes, atomic RMWs,
allocation, task hand-off — not by arithmetic. Measured on this machine,
`RUSTFLAGS="-C target-cpu=native"` moves the primitives by roughly ±10% in
both directions, i.e. inside run-to-run noise. The one vectorizable loop in
the suite tells the real story: `sequential/map_1000` (`x * 2` over 1000
`u32`) gets **41% faster**, because the feature that helps is AVX2 and only
arithmetic-heavy code can use it. Enable it for numeric workloads; never bake
it into a published crate, where it makes the artifact non-portable.


## Development

This is a Cargo workspace; run commands from the repository root:

```bash
cargo build --workspace --all-features
cargo test --workspace --all-features
cargo clippy --workspace --all-features -- -D warnings
cargo fmt --all --check
```

Or target a single crate with `-p <name>`, e.g. `cargo test -p mytheclipse-cache`.

## License

Licensed under either of:

- Apache License, Version 2.0 ([LICENSE-APACHE](crates/mytheclipse/LICENSE-APACHE) or <http://www.apache.org/licenses/LICENSE-2.0>)
- MIT license ([LICENSE-MIT](crates/mytheclipse/LICENSE-MIT) or <http://opensource.org/licenses/MIT>)

at your option.
