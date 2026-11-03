## [1.21.4](https://github.com/MythEclipse/mytheclipse-crates/compare/v1.21.3...v1.21.4) (2026-11-03)


### Bug Fixes

* **ci:** publish crates from the release workflow ([eb44987](https://github.com/MythEclipse/mytheclipse-crates/commit/eb449876302ddb023b2dc75b321ad32d775a83bc))

## [1.21.3](https://github.com/MythEclipse/mytheclipse-crates/compare/v1.21.2...v1.21.3) (2026-11-03)


### Bug Fixes

* **cache:** hold cache state under one lock and evict in O(1) ([6fbaec0](https://github.com/MythEclipse/mytheclipse-crates/commit/6fbaec0c4f427f30b6e9e946b136dbf420fd1b88))
* **lifecycle:** fence lock leases and release them without a runtime ([41267ca](https://github.com/MythEclipse/mytheclipse-crates/commit/41267ca0a583be3fea7292a8a74efdce56919838))
* **lifecycle:** skip impossible days when finding the next cron fire ([2af626e](https://github.com/MythEclipse/mytheclipse-crates/commit/2af626eed8b33e69cd658a0f789d713ef4883ed8))
* **observability:** stop allocating per metrics record ([7409b9c](https://github.com/MythEclipse/mytheclipse-crates/commit/7409b9c19d0028b342cd5ba802c12316880188a6))
* **queue:** hold concurrency permits for the whole job ([be5d5c9](https://github.com/MythEclipse/mytheclipse-crates/commit/be5d5c95522f04492d2cc377f124a1990d7e4472))
* **resiliency:** make half-open probe accounting atomic ([1ec8d8d](https://github.com/MythEclipse/mytheclipse-crates/commit/1ec8d8d0b8dd75b99180004da3a554fa4ffdfe1f))
* **traffic:** give each semaphore pool its own rotation cursor ([9858d94](https://github.com/MythEclipse/mytheclipse-crates/commit/9858d9431513fa1287acf312ce2b749445d623a7))
* **traffic:** guard the rate limiter bucket with one lock ([3a9f6e2](https://github.com/MythEclipse/mytheclipse-crates/commit/3a9f6e267b37fe70ebdacc6d102f3692064d13f9))

## [1.21.2](https://github.com/asepharyana/mytheclipse/compare/v1.21.1...v1.21.2) (2026-08-30)


### Bug Fixes

* **cache:** align redis dep to 0.32 for deadpool unification ([901fc51](https://github.com/asepharyana/mytheclipse/commit/901fc51f7b3bf4b2ab433db2e35303722778cc76))

## [1.21.1](https://github.com/asepharyana/mytheclipse/compare/v1.21.0...v1.21.1) (2026-08-29)


### Bug Fixes

* **ci:** restore full CI green — test-matrix, clippy, rustfmt, and rustdoc gates ([1dfc6d6](https://github.com/asepharyana/mytheclipse/commit/1dfc6d68657e5b002896f400d744b052800c9285))

# [1.21.0](https://github.com/asepharyana/mytheclipse/compare/v1.20.0...v1.21.0) (2026-08-29)


### Features

* CPU parallel compute primitives — compute_map, compute_join, compute_par_for_each ([e148944](https://github.com/asepharyana/mytheclipse/commit/e14894479d8ca716a9decf2c1403359fdc376717))

# [1.20.0](https://github.com/asepharyana/mytheclipse/compare/v1.19.0...v1.20.0) (2026-08-29)


### Features

* ParallelConcurrency — auto-size concurrency from CPU cores ([d00881c](https://github.com/asepharyana/mytheclipse/commit/d00881c2b96fa29ee0eaa5f43a7c7af90983e802))

# [1.19.0](https://github.com/asepharyana/mytheclipse/compare/v1.18.0...v1.19.0) (2026-08-29)


### Features

* criterion benchmarks proving primitive overhead is negligible ([5df04d7](https://github.com/asepharyana/mytheclipse/commit/5df04d75126d71d0790028c4a215acb571f25616))

# [1.18.0](https://github.com/asepharyana/mytheclipse/compare/v1.17.0...v1.18.0) (2026-08-29)


### Features

* round-15 abstractions — parallel_for_each streaming fan-out ([730245e](https://github.com/asepharyana/mytheclipse/commit/730245e9c02c5b6839b9342320b81002ea8421a3))

# [1.17.0](https://github.com/asepharyana/mytheclipse/compare/v1.16.0...v1.17.0) (2026-08-29)


### Features

* round-14 abstractions — parallel_map bounded fan-out ([bb4998d](https://github.com/asepharyana/mytheclipse/commit/bb4998d8fa68e25415ea79a245313f17a2792a06))

# [1.16.0](https://github.com/asepharyana/mytheclipse/compare/v1.15.0...v1.16.0) (2026-08-29)


### Features

* round-13 abstractions — AggregateError for parallel fan-out ([ff5fbe4](https://github.com/asepharyana/mytheclipse/commit/ff5fbe49dc1ff06c287306835110a6652d6b24b9))

# [1.15.0](https://github.com/asepharyana/mytheclipse/compare/v1.14.0...v1.15.0) (2026-08-29)


### Features

* round-12 abstractions — AutoReconnectPool, Reconnectable ([ddb2c2f](https://github.com/asepharyana/mytheclipse/commit/ddb2c2fd2d43e07b0d26b2901746ffcc3fe8b284))

# [1.14.0](https://github.com/asepharyana/mytheclipse/compare/v1.13.0...v1.14.0) (2026-08-29)


### Features

* round-11 abstractions — RuntimeConfig auto thread/core, ShutdownGuard RAII ([74abe71](https://github.com/asepharyana/mytheclipse/commit/74abe7172f0d72b41cd808d53f85021edc15b8e0))

# [1.13.0](https://github.com/asepharyana/mytheclipse/compare/v1.12.0...v1.13.0) (2026-08-29)


### Features

* round-10 abstractions — AutoMetricsServiceBuilder, RateLimitedWorkerPool ([2f445d3](https://github.com/asepharyana/mytheclipse/commit/2f445d3b8539c89f819224e421a555bc605aac91))

# [1.12.0](https://github.com/asepharyana/mytheclipse/compare/v1.11.0...v1.12.0) (2026-08-29)


### Features

* round-9 abstractions — RetryExt ergonomic retry, ResilientHttpClient ([7a063fa](https://github.com/asepharyana/mytheclipse/commit/7a063fa75c6ca243d1e76a485e117a3b334b25e9))

# [1.11.0](https://github.com/asepharyana/mytheclipse/compare/v1.10.0...v1.11.0) (2026-08-29)


### Features

* round-8 abstractions — ResilientHttpClient, MiddlewarePipeline, BgJoiner ([076b0bb](https://github.com/asepharyana/mytheclipse/commit/076b0bb75789ecf7f19f1b4078a3260d33218fb6))

# [1.10.0](https://github.com/asepharyana/mytheclipse/compare/v1.9.0...v1.10.0) (2026-08-29)


### Features

* round-7 abstractions — BgJoiner, MiddlewarePipeline, RateLimitedQueue ([851c8c4](https://github.com/asepharyana/mytheclipse/commit/851c8c4ebbe465cecd89cfea78c1b00bb47c07c2))

# [1.9.0](https://github.com/asepharyana/mytheclipse/compare/v1.8.0...v1.9.0) (2026-08-29)


### Features

* round-6 abstractions — HealthCheckedPool, HkdfKeyDeriver, BackpressureEnforcer ([a03db38](https://github.com/asepharyana/mytheclipse/commit/a03db38c5ccabead51fa49d2001b0cd94a9dd66e))

# [1.8.0](https://github.com/asepharyana/mytheclipse/compare/v1.7.0...v1.8.0) (2026-08-29)


### Features

* round-5 abstractions — BatchProcessor, CircuitBreakerHealthCheck, TypedKeyRegistry, MetricsHttpHandler ([97b5e02](https://github.com/asepharyana/mytheclipse/commit/97b5e02820674a5b61a2d396f95df07f2b4fd735))

# [1.7.0](https://github.com/asepharyana/mytheclipse/compare/v1.6.0...v1.7.0) (2026-08-29)


### Features

* round-5 abstractions — CircuitBreakerHealthCheck, TypedKeyRegistry, MetricsHttpHandler ([510aadc](https://github.com/asepharyana/mytheclipse/commit/510aadc066a428c1627a38bdb22e4f0440cc01b3))

# [1.6.0](https://github.com/asepharyana/mytheclipse/compare/v1.5.0...v1.6.0) (2026-08-29)


### Features

* round-4 metrics for circuit breaker + retry stats + lifecycle fixes ([717e690](https://github.com/asepharyana/mytheclipse/commit/717e6905cd7a3f7389b455d01054a2c2cc28befd))

# [1.5.0](https://github.com/asepharyana/mytheclipse/compare/v1.4.1...v1.5.0) (2026-08-29)


### Features

* round-3 abstractions — ConfigValidator, AsyncLifecycleManager, MetricsBridge, rate limiter pre-acquire ([1ea3b35](https://github.com/asepharyana/mytheclipse/commit/1ea3b3558143bd07168c3be89653fbeb9c38930a))

## [1.4.1](https://github.com/asepharyana/mytheclipse/compare/v1.4.0...v1.4.1) (2026-08-29)


### Bug Fixes

* clippy clean for round-2 (pipeline module export, lint cleanup) ([1981544](https://github.com/asepharyana/mytheclipse/commit/198154442c4297c94e8743caec81294b478c0d3a))

# [1.4.0](https://github.com/asepharyana/mytheclipse/compare/v1.3.5...v1.4.0) (2026-08-29)


### Features

* add 4 new crates (queue, tracing, http, cli) + enhancements to existing crates ([8106f89](https://github.com/asepharyana/mytheclipse/commit/8106f8943ebe83f7348b9fde3fbd2e347018604e))

## [1.3.5](https://github.com/asepharyana/mytheclipse/compare/v1.3.4...v1.3.5) (2026-08-28)


### Bug Fixes

* **cache:** honor sub-second Redis TTL via PSETEX + document clear() safety ([2c9367a](https://github.com/asepharyana/mytheclipse/commit/2c9367a83c2dd01b1e197ec33215a6c7d3755fa2))

## [1.3.4](https://github.com/asepharyana/mytheclipse/compare/v1.3.3...v1.3.4) (2026-08-28)


### Bug Fixes

* **cache,storage:** harden cache bounds + atomic disk writes ([b6f138b](https://github.com/asepharyana/mytheclipse/commit/b6f138b90d67c9531b5a58993e8ec750e5eec57f))

## [1.3.3](https://github.com/asepharyana/mytheclipse/compare/v1.3.2...v1.3.3) (2026-08-28)


### Bug Fixes

* **publish:** trim mytheclipse keywords to 5 to satisfy crates.io limit ([5f1e3ac](https://github.com/asepharyana/mytheclipse/commit/5f1e3ace5c8cb10881e30f550f51b7d845b24edd))

## [1.3.2](https://github.com/asepharyana/mytheclipse/compare/v1.3.1...v1.3.2) (2026-08-28)


### Bug Fixes

* **ci:** gate cache & storage crate doctests behind their features ([5717f8a](https://github.com/asepharyana/mytheclipse/commit/5717f8aaaae34cb66cdbfc31f4982c93816ee5a3))

## [1.3.1](https://github.com/asepharyana/mytheclipse/compare/v1.3.0...v1.3.1) (2026-08-28)


### Bug Fixes

* **ci:** gate event crate doctest behind mem feature and apply rustfmt ([1994115](https://github.com/asepharyana/mytheclipse/commit/19941156b43ec58370d0d1369174ec84400e9bc9))

# [1.3.0](https://github.com/asepharyana/mytheclipse/compare/v1.2.0...v1.3.0) (2026-08-28)


### Features

* add corex-storage crate for unified storage abstraction ([db4f277](https://github.com/asepharyana/mytheclipse/commit/db4f277336d1e32cb6a2ddd86ac37ae9789fa4f8))

# [1.2.0](https://github.com/asepharyana/mytheclipse/compare/v1.1.0...v1.2.0) (2026-08-28)


### Features

* add panic tracking and logging with PanicTracker ([d119821](https://github.com/asepharyana/mytheclipse/commit/d1198213391710ccc6f2c108b15aa4526380423d))
