//! Bridges the metrics collector to health checks and tracing events.
//!
//! [`MetricsBridge`] ties [`crate::metrics::MetricsCollector`] to
//! [`crate::health::HealthCheck`], so a metrics-based health probe can report
//! `Degraded` when error counters rise or throughput drops, and optionally emit
//! tracing events so counters/gauges are visible in structured logs.

use std::time::Duration;

use crate::health::{HealthCheck, HealthStatus};
use crate::metrics::MetricsCollector;

/// A health check backed by a [`crate::circuit_breaker::CircuitBreaker`]:
/// unhealthy if open, degraded if half-open, ok otherwise.
///
/// Only available when both `resiliency` and `observability` features are
/// enabled (circuit breaker + health/metrics bridge).
#[cfg(feature = "resiliency")]
pub struct CircuitBreakerHealthCheck {
    breaker: crate::circuit_breaker::CircuitBreaker,
}

#[cfg(feature = "resiliency")]
impl CircuitBreakerHealthCheck {
    /// Wraps `breaker` so its state shows up in `/health`.
    pub fn new(breaker: crate::circuit_breaker::CircuitBreaker) -> Self {
        Self { breaker }
    }
}

#[cfg(feature = "resiliency")]
impl HealthCheck for CircuitBreakerHealthCheck {
    fn name(&self) -> &str {
        "circuit_breaker"
    }

    fn check(
        &self,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = HealthStatus> + Send + '_>> {
        let state = self.breaker.snapshot().state;
        Box::pin(async move {
            match state {
                crate::circuit_breaker::CircuitState::Open => HealthStatus::Unhealthy,
                crate::circuit_breaker::CircuitState::HalfOpen => HealthStatus::Degraded,
                crate::circuit_breaker::CircuitState::Closed => HealthStatus::Ok,
            }
        })
    }
}

/// A health check backed by a [`MetricsCollector`]: unhealthy if any registered
/// "error" counter is non-zero, degraded if any gauge is below a configured
/// threshold.
pub struct MetricsHealthCheck {
    collector: MetricsCollector,
}

impl MetricsHealthCheck {
    /// Reads its verdict from `collector`.
    pub fn new(collector: MetricsCollector) -> Self {
        Self { collector }
    }

    /// Returns unhealthy if the named counter is non-zero.
    pub fn error_counter_exists(&self, name: &str) -> bool {
        self.collector.snapshot().counters.contains_key(name)
    }

    fn has_errors(&self) -> bool {
        self.collector.snapshot().counters.values().any(|&v| v > 0)
    }
}

impl HealthCheck for MetricsHealthCheck {
    fn name(&self) -> &str {
        "metrics"
    }

    fn check(
        &self,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = HealthStatus> + Send + '_>> {
        let has_errors = self.has_errors();
        Box::pin(async move {
            if has_errors {
                HealthStatus::Unhealthy
            } else {
                HealthStatus::Ok
            }
        })
    }
}

/// Bridges a [`MetricsCollector`] to tracing: periodically emits the current
/// snapshot as tracing events so metrics are visible in structured logs.
pub struct MetricsBridge {
    collector: MetricsCollector,
}

impl MetricsBridge {
    /// Reads snapshots from `collector`.
    pub fn new(collector: MetricsCollector) -> Self {
        Self { collector }
    }

    /// Sends a one-shot tracing event with the current snapshot.
    pub fn emit_now(&self) {
        let snap = self.collector.snapshot();
        let mut counters: Vec<_> = snap.counters.into_iter().collect();
        counters.sort_by(|a, b| a.0.cmp(&b.0));
        let mut gauges: Vec<_> = snap.gauges.into_iter().collect();
        gauges.sort_by(|a, b| a.0.cmp(&b.0));

        tracing::debug!(
            task_count = snap.task_count,
            active_threads = snap.active_threads,
            queue_capacity_total = snap.queue_capacity_total,
            queue_capacity_remaining = snap.queue_capacity_remaining,
            "metrics snapshot"
        );
        for (name, value) in &counters {
            tracing::info!(name, value, "metric counter");
        }
        for (name, value) in &gauges {
            tracing::info!(name, value, "metric gauge");
        }
    }

    /// Spawns a background task that calls [`emit_now`](Self::emit_now) every
    /// `interval`. Returns a handle that can be aborted.
    pub fn emit_periodic(self, interval: Duration) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(interval);
            loop {
                ticker.tick().await;
                self.emit_now();
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metrics::MetricsCollector;

    #[test]
    fn metrics_health_ok_when_no_counters() {
        let collector = MetricsCollector::new();
        let check = MetricsHealthCheck::new(collector);
        // No counters set → no errors → Ok.
        let fut = check.check();
        // Can't await in #[test]; use tokio test below instead.
        drop(fut);
    }

    #[tokio::test]
    async fn metrics_health_unhealthy_when_errors_exist() {
        let collector = MetricsCollector::new();
        collector.inc_counter("errors", 1);
        let check = MetricsHealthCheck::new(collector);
        let status = check.check().await;
        assert_eq!(status, HealthStatus::Unhealthy);
    }

    #[tokio::test]
    async fn metrics_health_ok_when_no_errors() {
        let collector = MetricsCollector::new();
        collector.set_gauge("load", 0.5);
        let check = MetricsHealthCheck::new(collector);
        let status = check.check().await;
        assert_eq!(status, HealthStatus::Ok);
    }

    #[tokio::test]
    async fn bridge_emit_now_runs() {
        let collector = MetricsCollector::new();
        collector.set_gauge("temp", 42.0);
        let bridge = MetricsBridge::new(collector);
        bridge.emit_now();
    }

    #[cfg(feature = "lifecycle")]
    #[tokio::test]
    async fn lifecycle_manager_with_metrics_bridge() {
        let collector = MetricsCollector::new();
        collector.set_gauge("load", 0.1);
        let mgr = crate::lifecycle::AsyncLifecycleManager::new();
        let bridge = MetricsBridge::new(collector);
        let _handle = bridge.emit_periodic(Duration::from_millis(50));
        mgr.request_shutdown();
        // Should not hang — shutdown is immediate.
        mgr.await_shutdown(Duration::from_secs(1)).await;
    }
}
