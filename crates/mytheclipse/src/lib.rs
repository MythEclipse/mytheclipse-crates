//! # mytheclipse
//!
//! Resource-aware abstractions for async I/O, heavy compute, background queue
//! management, resiliency, traffic control, lifecycle, and observability —
//! built on a single lazily-initialized global engine context plus a set of
//! self-contained, constructible utilities.
//!
//! Call [`init`] once at startup (or let the first call to any entry point
//! trigger it lazily) and then use whichever feature-gated entry points your
//! workload needs:
//!
//! - [`spawn_io`] (feature `io`) — spawn an async I/O task, tracing-instrumented.
//! - [`compute()`] (feature `compute`) — run CPU-bound work on a sized worker pool, panic-isolated.
//! - [`spawn_bg`] (feature `bg`) — spawn a background task under bounded concurrency.
//! - [`retry()`] / [`CircuitBreaker`] / [`timeout()`] (feature `resiliency`) — fault tolerance.
//! - [`RateLimiter`] / [`BackpressureQueue`] / [`ConcurrencyLimiter`] (feature `traffic`) — load control.
//! - [`SemaphorePool`] (feature `traffic`) — shared bounded resource pool.
//! - [`ShutdownManager`] / [`CronSchedule`] (feature `lifecycle`) — lifecycle + scheduling.
//! - [`HealthRegistry`] / [`LeaderElection`] (feature `lifecycle`) — health checks + leader election.
//! - [`MetricsCollector`] / [`PanicTracker`] (feature `observability`) — runtime visibility.
//!
//! Enable the `full` feature to pull in all of the above at once.

#![deny(missing_docs)]
pub mod context;
pub mod error;

#[cfg(feature = "bg")]
pub mod bg;
#[cfg(feature = "compute")]
pub mod compute;
#[cfg(feature = "io")]
pub mod io;

#[cfg(feature = "resiliency")]
pub mod aggregate_error;
#[cfg(feature = "resiliency")]
pub mod parallel_map;
#[cfg(feature = "resiliency")]
pub mod retry;
#[cfg(feature = "resiliency")]
pub mod retry_ext;
#[cfg(feature = "resiliency")]
pub mod simple_rng;
#[cfg(feature = "resiliency")]
pub use aggregate_error::AggregateError;
#[cfg(feature = "resiliency")]
pub use parallel_map::{
    parallel_for_each, parallel_map, parallel_map_unordered, ParallelConcurrency,
};
#[cfg(feature = "resiliency")]
pub use retry_ext::RetryExt;
#[cfg(all(feature = "observability", feature = "resiliency"))]
pub mod auto_metrics_service;
#[cfg(all(feature = "observability", feature = "resiliency"))]
pub use auto_metrics_service::AutoMetricsServiceBuilder;
#[cfg(feature = "resiliency")]
pub mod circuit_breaker;
#[cfg(feature = "resiliency")]
pub mod timeout;

#[cfg(feature = "traffic")]
pub mod backpressure;
#[cfg(feature = "traffic")]
pub mod concurrency;
#[cfg(feature = "traffic")]
pub mod pool;
#[cfg(feature = "traffic")]
pub mod ratelimit;

#[cfg(feature = "lifecycle")]
pub mod cron;
#[cfg(feature = "lifecycle")]
pub mod health;
#[cfg(feature = "lifecycle")]
pub mod leader;
#[cfg(feature = "lifecycle")]
pub mod lifecycle;
#[cfg(all(feature = "observability", feature = "traffic"))]
pub mod pool_health;
#[cfg(feature = "lifecycle")]
pub mod shutdown;

#[cfg(feature = "lifecycle")]
pub mod bg_join;
#[cfg(feature = "lifecycle")]
pub mod runtime_auto;
#[cfg(feature = "lifecycle")]
pub use runtime_auto::{available_parallelism, RuntimeConfig};

#[cfg(feature = "lifecycle")]
pub mod shutdown_guard;
#[cfg(feature = "lifecycle")]
pub use shutdown_guard::ShutdownGuard;

#[cfg(all(feature = "observability", feature = "resiliency"))]
pub mod middleware;

#[cfg(feature = "observability")]
pub mod metrics;
#[cfg(feature = "observability")]
pub mod metrics_bridge;
#[cfg(feature = "observability")]
pub mod panic_tracker;

#[cfg(feature = "lifecycle")]
pub mod dlock;
#[cfg(feature = "resiliency")]
pub mod service_builder;

pub use context::{context, EngineContext};
pub use error::MytheclipseError;

#[cfg(feature = "bg")]
pub use bg::spawn_bg;
#[cfg(feature = "compute")]
pub use compute::{compute, compute_join, compute_map, compute_par_for_each, ComputeErrors};
#[cfg(feature = "io")]
pub use io::spawn_io;

#[cfg(feature = "resiliency")]
pub use circuit_breaker::{CircuitBreaker, CircuitBreakerConfig, CircuitError, CircuitState};
#[cfg(feature = "resiliency")]
pub use retry::{retry, JitterKind, RetryConfig, RetryError};
#[cfg(feature = "resiliency")]
pub use timeout::{timeout, with_timeout, Timeout, TimeoutError};

/// Re-export of `async-trait` so implementing [`pool::Reconnectable`] (and
/// other async traits) doesn't require users to add their own `async-trait`
/// dependency.
pub use async_trait::async_trait;
#[cfg(feature = "traffic")]
pub use backpressure::{BackpressureError, BackpressureQueue, OverflowPolicy};
#[cfg(feature = "traffic")]
pub use concurrency::{ConcurrencyLimiter, ConcurrencyPermit};
#[cfg(feature = "traffic")]
pub use pool::{AutoReconnectPool, Pool, PoolError, Pooled, Reconnectable, SemaphorePool};
#[cfg(feature = "traffic")]
pub use ratelimit::{RateLimitError, RateLimiter};

#[cfg(feature = "lifecycle")]
pub use cron::{schedule, CronError, CronJob, CronParseError, CronSchedule};
#[cfg(feature = "lifecycle")]
pub use health::{HealthCheck, HealthRegistry, HealthStatus};
#[cfg(feature = "lifecycle")]
pub use leader::{InProcLeaderElection, LeaderElection};
#[cfg(feature = "lifecycle")]
pub use shutdown::{ShutdownManager, ShutdownSignal};

#[cfg(feature = "resiliency")]
pub use service_builder::{RunError, ServiceBuilder, ServiceConfig};

#[cfg(feature = "lifecycle")]
pub use dlock::{DistributedLock, InProcLock, LockError, LockGuard};
#[cfg(feature = "lifecycle")]
pub use lifecycle::AsyncLifecycleManager;

#[cfg(feature = "observability")]
pub use metrics::{MetricsCollector, MetricsSnapshot};
#[cfg(feature = "observability")]
pub use metrics_bridge::{MetricsBridge, MetricsHealthCheck};

/// Re-export of [`metrics_bridge::CircuitBreakerHealthCheck`].
/// Only compiled when both `observability` and `resiliency` are enabled.
#[cfg(all(feature = "observability", feature = "resiliency"))]
pub use metrics_bridge::CircuitBreakerHealthCheck;

/// Re-export of [`pool_health::HealthCheckedPool`].
/// Only compiled when both `observability` and `traffic` are enabled.
#[cfg(all(feature = "observability", feature = "traffic"))]
pub use pool_health::HealthCheckedPool;

#[cfg(feature = "lifecycle")]
pub use bg_join::BgJoiner;

#[cfg(all(feature = "observability", feature = "resiliency"))]
pub use middleware::{mw, BoxMiddleware, MiddlewarePipeline, PipelineError};
#[cfg(feature = "observability")]
pub use panic_tracker::{PanicGuard, PanicInfo, PanicTracker};

/// Bootstraps the global [`EngineContext`].
pub fn init() -> &'static EngineContext {
    context::init()
}

/// The common case, in one import.
///
/// ```
/// use mytheclipse::prelude::*;
/// ```
///
/// The traits are the reason this exists. [`Pool`], [`Reconnectable`] and
/// [`ParallelConcurrency`] must be in scope for their methods to be callable,
/// and an import list that names only the concrete types compiles right up
/// until the first method call.
///
/// [`init`] is deliberately absent: every primitive lazily sizes its own pool
/// on first use, so calling it is an optimisation, not setup.
pub mod prelude {
    pub use crate::MytheclipseError;

    #[cfg(feature = "bg")]
    pub use crate::spawn_bg;
    #[cfg(feature = "io")]
    pub use crate::spawn_io;
    #[cfg(feature = "compute")]
    pub use crate::{compute, compute_join, compute_map, compute_par_for_each, ComputeErrors};

    #[cfg(feature = "resiliency")]
    pub use crate::{
        parallel_for_each, parallel_map, parallel_map_unordered, retry, with_timeout,
        AggregateError, CircuitBreaker, CircuitBreakerConfig, ParallelConcurrency, RetryConfig,
        RetryExt, ServiceBuilder, ServiceConfig, Timeout,
    };

    #[cfg(feature = "traffic")]
    pub use crate::{
        BackpressureQueue, ConcurrencyLimiter, OverflowPolicy, Pool, RateLimiter, Reconnectable,
        SemaphorePool,
    };

    #[cfg(feature = "lifecycle")]
    pub use crate::{
        CronSchedule, DistributedLock, HealthRegistry, HealthStatus, ShutdownGuard, ShutdownManager,
    };

    #[cfg(feature = "observability")]
    pub use crate::{MetricsCollector, PanicTracker};
}
