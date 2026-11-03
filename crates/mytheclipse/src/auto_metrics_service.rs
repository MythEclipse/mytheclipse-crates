//! Auto-metrics service builder (feature `observability`).
//!
//! [`AutoMetricsServiceBuilder`] composes a [`crate::ServiceBuilder`] with a
//! [`crate::metrics::MetricsCollector`] so that every call recorded through
//! `.run()` automatically:
//!
//!  - increments a `mytheclipse_service_calls_total` counter (labelled by
//!    outcome `ok` / `err` / `timeout` / `circuit_open` / `rate_limited`),
//!  - observes a `mytheclipse_service_duration_seconds` histogram,
//!  - forwards the result to a [`crate::metrics_bridge::MetricsBridge`] when
//!    one is attached (e.g. for OpenTelemetry export).
//!
//! This removes the need for callers to hand-wire tracing/metering at every
//! call site.

use std::time::Duration;

use crate::metrics::MetricsCollector;
use crate::service_builder::{RunError, ServiceBuilder, ServiceConfig};

/// A [`ServiceBuilder`] wrapper that auto-records latency and outcome metrics.
///
/// ```
/// use std::time::Duration;
/// use mytheclipse::auto_metrics_service::AutoMetricsServiceBuilder;
/// use mytheclipse::service_builder::ServiceConfig;
///
/// let cfg = ServiceConfig {
///     max_attempts: 2,
///     timeout: Duration::from_millis(1),
///     ..ServiceConfig::default()
/// };
/// let svc = AutoMetricsServiceBuilder::new("checkout", cfg);
/// // Every `.run()` call now auto-records outcome + latency on the shared
/// // MetricsCollector (retrievable via `collector()`).
/// let _ = svc.collector();
/// ```
pub struct AutoMetricsServiceBuilder {
    inner: ServiceBuilder,
    metrics: MetricsCollector,
    #[cfg(feature = "resiliency")]
    bridge: Option<crate::metrics_bridge::MetricsBridge>,
    service_name: String,
}

impl AutoMetricsServiceBuilder {
    /// Creates a new auto-metrics builder around a base [`ServiceConfig`].
    pub fn new(service_name: impl Into<String>, config: ServiceConfig) -> Self {
        Self {
            inner: ServiceBuilder::new(config),
            metrics: MetricsCollector::new(),
            #[cfg(feature = "resiliency")]
            bridge: None,
            service_name: service_name.into(),
        }
    }

    /// Sets the underlying [`ServiceBuilder`] (e.g. to attach a circuit
    /// breaker) and returns a fresh [`AutoMetricsServiceBuilder`].
    pub fn with_builders(self, inner: ServiceBuilder) -> Self {
        Self {
            inner,
            metrics: self.metrics,
            #[cfg(feature = "resiliency")]
            bridge: self.bridge,
            service_name: self.service_name,
        }
    }

    /// Attaches a [`MetricsCollector`] to share with the caller (so the caller
    /// can scrape/export the same counters it records here).
    pub fn with_collector(mut self, m: MetricsCollector) -> Self {
        self.metrics = m;
        self
    }

    /// Attaches a [`crate::metrics_bridge::MetricsBridge`] to forward snapshots
    /// downstream (requires the `resiliency` feature which pulls in the bridge).
    #[cfg(feature = "resiliency")]
    pub fn with_bridge(mut self, bridge: crate::metrics_bridge::MetricsBridge) -> Self {
        self.bridge = Some(bridge);
        self
    }

    /// Returns a shared [`MetricsCollector`] handle.
    pub fn collector(&self) -> MetricsCollector {
        self.metrics.clone()
    }

    /// Runs a service call, auto-recording metrics around the outcome.
    pub async fn run<F, T, E>(&self, f: F) -> Result<T, RunError<E>>
    where
        F: FnMut() -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<T, E>> + Send>>,
        E: std::fmt::Debug,
    {
        let start = std::time::Instant::now();
        let result = self.inner.run(f).await;
        let dur: Duration = start.elapsed();

        let outcome = match &result {
            Ok(_) => "ok",
            Err(RunError::Inner(_)) => "err",
            Err(RunError::Timeout) => "timeout",
            Err(RunError::CircuitOpen) => "circuit_open",
            #[cfg(feature = "traffic")]
            Err(RunError::RateLimited) => "rate_limited",
            #[allow(unreachable_patterns)]
            Err(_) => "other",
        };

        self.metrics.inc_counter(
            &format!(
                "mytheclipse_service_calls_total{{service=\"{}\",outcome=\"{}\"}}",
                self.service_name, outcome
            ),
            1,
        );
        self.metrics
            .observe("mytheclipse_service_duration_seconds", dur);

        #[cfg(feature = "resiliency")]
        if let Some(b) = &self.bridge {
            b.emit_now();
        }

        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU32, Ordering};
    use std::sync::Arc;

    #[tokio::test]
    async fn auto_metrics_records_call() {
        let cfg = ServiceConfig {
            max_attempts: 3,
            ..Default::default()
        };
        let builder = AutoMetricsServiceBuilder::new("test_svc", cfg);

        let attempts = Arc::new(AtomicU32::new(0));
        let a = Arc::clone(&attempts);
        let result: Result<u32, RunError<()>> = builder
            .run(|| {
                let a = Arc::clone(&a);
                Box::pin(async move {
                    let n = a.fetch_add(1, Ordering::SeqCst);
                    if n < 2 {
                        Err(())
                    } else {
                        Ok(42u32)
                    }
                })
            })
            .await;
        assert_eq!(result.unwrap(), 42);
        assert_eq!(attempts.load(Ordering::SeqCst), 3);
        let snap = builder.collector().snapshot();
        assert!(!snap.counters.is_empty());
    }
}
