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
| [`mytheclipse-config`](crates/mytheclipse-config) | Type-safe, dynamic configuration engine: load `.env`/YAML/JSON/TOML into typed structs, with hot-reload and typed validation. | [README](crates/mytheclipse-config/README.md) |
| [`mytheclipse-crypto`](crates/mytheclipse-crypto) | Safe hashing (Argon2id), encryption (AES-256-GCM), JWT and PASETO tokens, with key rotation support. | [README](crates/mytheclipse-crypto/README.md) |
| [`mytheclipse-queue`](crates/mytheclipse-queue) | Unified job queue abstraction with WorkerPool executor, retry/backoff, and dead-letter support. Backends: in-memory, Redis, NATS, PostgreSQL. | [README](crates/mytheclipse-queue/README.md) |
| [`mytheclipse-tracing`](crates/mytheclipse-tracing) | Pre-built tracing subscriber layers with env filtering and optional OTLP/Jaeger/Zipkin export. | [README](crates/mytheclipse-tracing/README.md) |
| [`mytheclipse-cli`](crates/mytheclipse-cli) | CLI framework for mytheclipse applications with built-in subcommands (serve, worker, migrate, health, version). | [README](crates/mytheclipse-cli/README.md) |

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
mytheclipse-cache = "0.1"
mytheclipse-event = { version = "0.1", features = ["nats"] }
mytheclipse-config = "0.1"
mytheclipse-crypto = "0.1"
```

See each crate's own README (linked above) for usage examples and the full
feature-flag list.

## Performance

Two criterion suites cover the hot primitives. Neither is a correctness gate —
run them before and after a change to the code they measure:

```bash
cargo bench -p mytheclipse --bench primitives --all-features
cargo bench -p mytheclipse-cache --bench cache
```

Reference numbers on an AMD Ryzen 9 (Linux, release profile). Treat them as a
regression baseline, not an absolute target:

| primitive | time |
| --- | --- |
| `circuit_breaker` closed allow+record | 4.8 ns |
| `metrics` inc_counter / observe | 20 ns |
| `shutdown_guard` new+drop | 27 ns |
| `backpressure` push+pop | 35 ns |
| `rate_limiter` try_acquire | 36 ns |
| `cache` get hit / miss | 59 ns / 45 ns |
| `semaphore_pool` acquire+release | 114 ns |
| `dlock` acquire+release (1k live keys) | 147 ns |
| `cache` set at capacity (evict) | 130 ns |
| `cron` next_after, hourly / yearly | 0.62 µs / 2.0 µs |
| `metrics` export_prometheus (64 series) | 11 µs |
| `parallel_map` 1000 items @ 8 | 120 µs |

`parallel_map` and `parallel_for_each` spawn `concurrency` workers that pull
from a shared iterator rather than one task per item, which is what keeps the
1000-item case at 120 µs instead of 527 µs.

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
