//! Async lifecycle manager composing shutdown, health checks, and periodic tasks.
//!
//! [`AsyncLifecycleManager`] ties together [`ShutdownManager`], [`HealthRegistry`],
//! and an optional periodic health-check ticker into a single orchestrator so
//! applications don't need to wire three separate primitives together.

use std::sync::Arc;
use std::time::Duration;

use crate::health::{HealthCheck, HealthRegistry, HealthStatus};
use crate::shutdown::ShutdownManager;

/// Coordinates graceful shutdown, health-check registration, and an optional
/// periodic health poll loop.
///
/// Typical usage:
/// ```ignore
/// let mgr = mytheclipse::AsyncLifecycleManager::new();
/// mgr.register_health_check("db", my_db_check());
/// let handle = mgr.start_health_loop(std::time::Duration::from_secs(30));
/// mgr.await_shutdown(std::time::Duration::from_secs(10)).await;
/// ```
#[derive(Clone)]
pub struct AsyncLifecycleManager {
    shutdown: ShutdownManager,
    health: Arc<HealthRegistry>,
}

impl AsyncLifecycleManager {
    pub fn new() -> Self {
        Self {
            shutdown: ShutdownManager::new(),
            health: Arc::new(HealthRegistry::new()),
        }
    }

    /// Returns a clone of the underlying shutdown manager.
    pub fn shutdown(&self) -> &ShutdownManager {
        &self.shutdown
    }

    /// Returns a clone of the underlying health registry.
    pub fn health(&self) -> &HealthRegistry {
        &self.health
    }

    /// Registers a named health check.
    pub async fn register_health_check(
        &self,
        name: impl Into<String>,
        check: impl HealthCheck + 'static,
    ) {
        self.health.register(name, check).await;
    }

    /// Runs all registered health checks once and returns their statuses.
    pub async fn check_health(&self) -> Vec<(String, HealthStatus)> {
        self.health.check_all().await
    }

    /// Returns a shutdown signal for long-running tasks to observe.
    pub fn shutdown_signal(&self) -> crate::shutdown::ShutdownSignal {
        self.shutdown.handle()
    }

    /// Starts a background task that polls health checks at `interval` and
    /// emits tracing events. Returns a [`tokio::task::JoinHandle`] that can
    /// be aborted on shutdown.
    pub fn start_health_loop(&self, interval: Duration) -> tokio::task::JoinHandle<()> {
        let health = self.health.clone();
        let signal = self.shutdown_signal();
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(interval);
            let mut sig = signal;
            loop {
                // Stop when shutdown is requested.
                if sig.is_shutdown() {
                    tracing::info_span!("mytheclipse_health_loop",);
                    return;
                }
                tokio::select! {
                    _ = sig.wait() => {
                        return;
                    }
                    _ = ticker.tick() => {
                        let results = health.check_all().await;
                        for (name, status) in &results {
                            match status {
                                HealthStatus::Ok => tracing::debug!(name, "health check ok"),
                                HealthStatus::Degraded => tracing::warn!(name, "health check degraded"),
                                HealthStatus::Unhealthy => tracing::error!(name, "health check unhealthy"),
                            }
                        }
                        if results.iter().any(|(_, s)| matches!(s, HealthStatus::Unhealthy)) {
                            tracing::error!("unhealthy component detected; requesting shutdown");
                            return;
                        }
                    }
                }
            }
        })
    }

    /// Waits for shutdown (OS signal or explicit `request()`) then drains all
    /// registered tasks with a `grace` timeout per task.
    pub async fn await_shutdown(&self, grace: Duration) {
        self.shutdown.wait_for_shutdown().await;
        self.shutdown.drain(grace).await;
    }

    /// Requests shutdown programmatically (safe to call multiple times).
    pub fn request_shutdown(&self) {
        self.shutdown.request();
    }
}

impl Default for AsyncLifecycleManager {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct AlwaysOk;
    impl HealthCheck for AlwaysOk {
        fn name(&self) -> &str {
            "always-ok"
        }
        fn check(
            &self,
        ) -> std::pin::Pin<Box<dyn std::future::Future<Output = HealthStatus> + Send + '_>>
        {
            Box::pin(async { HealthStatus::Ok })
        }
    }

    #[tokio::test]
    async fn new_manager_has_no_checks() {
        let mgr = AsyncLifecycleManager::new();
        let results = mgr.check_health().await;
        assert!(results.is_empty());
    }

    #[tokio::test]
    async fn registers_and_checks_health() {
        let mgr = AsyncLifecycleManager::new();
        mgr.register_health_check("ok", AlwaysOk).await;
        let results = mgr.check_health().await;
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].0, "ok");
        assert_eq!(results[0].1, HealthStatus::Ok);
    }

    #[tokio::test]
    async fn shutdown_signal_fires_on_request() {
        let mgr = AsyncLifecycleManager::new();
        let sig = mgr.shutdown_signal();
        assert!(!sig.is_shutdown());
        mgr.request_shutdown();
        assert!(sig.is_shutdown());
    }
}
