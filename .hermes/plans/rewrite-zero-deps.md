# Rewrite plan — dependency-free core

Status: proposal. Nothing here is implemented yet.

Measured against `e5e6272` (post-rebase, 188 tests green, 6 crates, ~10k src lines).

## 0. Three targets, not one

"Without dependencies" is three different projects. Pick one before writing code.

| Target | Meaning | Effort | Ships as |
| --- | --- | --- | --- |
| **A. Dead weight out** | Delete deps that no code path uses | 5 manifest lines | patch |
| **B. Runtime-only** | Zero third-party deps except the async runtime (`tokio`) | ~2-3 days | minor (major if `async-trait` goes) |
| **C. Truly zero** | No `tokio` either → the library becomes synchronous | 3-6 weeks, full API break | major |

C is not a dependency cleanup, it is a different library: `async fn` everywhere in
the public surface (`Cache`, `Queue`, `EventBus`, `EngineContext`) is `tokio`'s
model. Dropping the runtime means every method becomes blocking, every `Notify`
becomes a condvar, and all 188 tests plus every downstream caller get rewritten.
Recommendation: **do A now, then B. Do not do C.**

## 1. Inventory

Every non-dev dependency, what it actually does, and what removing it costs.

### Already dead — delete now, zero code change

| Crate | Dependency | Proof |
| --- | --- | --- |
| `mytheclipse-queue` | `serde`, `serde_json` | no `serde` token anywhere in `src/` |
| `mytheclipse-event` | `tracing` | no `tracing` token anywhere in `src/` |
| `mytheclipse-cli` | `tracing`, dev-`tokio` | same |

`serde` + `serde_json` alone pull `serde_derive`, `itoa`, `ryu`, `memchr` and two
proc-macro crates.

### Cheap — replace with std, no API change

| Dependency | Where | Replacement | Lines |
| --- | --- | --- | --- |
| `num_cpus` | `context.rs:41` (one call) | `std::thread::available_parallelism()` — already used in `runtime_auto.rs:91` and `parallel_map.rs:50`, so this crate is a leftover | 1 |
| `thiserror` | `pool.rs:14` (one `#[derive]` in the whole workspace; the other 16 error types are already hand-written) | `impl Display + Error` by hand | ~15 |
| `uuid` | `job.rs:13` (one call) | 16 bytes from the entropy source below, hex-encoded | ~25 |
| `rand` | 4 sites, all retry jitter | `xoshiro256**` seeded from `RandomState` + clock | ~35 |

### Bounded — needs care, may break the public surface

| Dependency | Where | Replacement | Risk |
| --- | --- | --- | --- |
| `async-trait` | 46 sites, 17 files | native `async fn` in traits (MSRV 1.75) + hand-written boxed futures for the 4 `dyn` traits (`Cache`, `EventBus`, `Queue`, pool) | **breaks external implementors** — they must write `Pin<Box<dyn Future>>` instead of `async fn`. `AFIT` is not dyn-compatible, so the boxed form cannot be avoided |
| `rayon` | `compute` feature only | `std::thread::scope` + chunked work distribution (~80 lines) | low; `parallel_map` already owns the distribution logic |
| `clap` | `mytheclipse-cli` (84 lines, 5 subcommands, unpublished) | hand parser (~150 lines) — or delete the crate, which removes `clap` and its 10 transitive crates outright | low |
| `moka` | `l1-moka` feature | delete the feature; `MemoryCache` already covers L1 | low, breaking for feature users |

### Large — real work, questionable payoff

| Dependency | Where | Replacement | Estimate |
| --- | --- | --- | --- |
| `tracing` + `tracing-subscriber` | all crates + `mytheclipse-tracing` (126 lines) | own facade + subscriber + env filter + formatter | 800-1500 lines |
| `redis` | `l2-redis` feature | RESP3 client (`SET/GET/DEL/EXPIRE`, connection pooling) | ~600 lines |
| `serde`/`serde_json` (live use) | `event/src/typed.rs` | minimal JSON codec, or drop typed events | ~800 lines / breaking |

### The wall

`tokio` is used in 25+ shapes: `spawn`, `task::JoinSet`, `time::{sleep, sleep_until, timeout, interval}`, `sync::{broadcast, mpsc, watch, Semaphore, Notify}`, `select!`, `pin!`, `signal`, `runtime::Builder`. Reimplementing that is 20-40k lines and would still be worse than `tokio`. This is why target C is a different library, not a cleanup.

`lapin`, `async-nats`, `futures-lite`, `futures-util`, `bytes` are optional and only reachable through the `amqp`/`nats` features — they disappear with those features, they do not need replacing.

## 2. Order of work

1. **A — dead weight.** Delete the 5 unused manifest entries. Prove with `cargo tree -e normal --depth 1` per crate and a full test run.
2. **B1 — std swaps.** `num_cpus`, `thiserror`, `uuid`, `rand`. No public API change; the only new code is one small RNG + one hex encoder, both `pub(crate)`.
3. **B2 — drop unused features.** `l1-moka` (and `mytheclipse-cli` if nobody uses it) → `moka` and/or `clap` leave the tree.
4. **B3 — `async-trait`.** Do it last of the B work because it is the only step that changes how downstream code writes an impl. Land it alone, behind the `race_stress` test.
5. **B4 — `rayon`.** `std::thread::scope` in `compute`; benchmark `parallel_map` before/after (the README numbers are the contract).
6. **Do not start C** without a written decision that the library is allowed to stop being async.

## 3. Acceptance criteria

- `cargo tree -p <crate> -e normal --depth 1` lists only workspace members and `tokio` (and `tracing`, if it survives B).
- `cargo test --workspace --all-features` stays at 188 passing.
- `cargo clippy --workspace --all-features --all-targets -- -D warnings` clean.
- Benchmarks: `parallel_map`, `cache`, `queue`, `event` within run-to-run noise (±10%) of the README table.
- `Cargo.lock` shrinks; record the before/after crate count in the commit message.

## 4. What breaks for users

| Change | Breakage |
| --- | --- |
| `async-trait` removed | anyone implementing `Cache`/`Queue`/`EventBus` by hand must write the boxed signature |
| `l1-moka` / `mytheclipse-cli` deleted | feature or crate disappears from the next release |
| `rand`/`uuid` removed | none, both are internal |
| Target C | everything |

`mytheclipse-config` is already gone (`e2499f3`); its published versions remain on
crates.io and can be yanked separately.
