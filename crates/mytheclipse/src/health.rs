//! Health check registry for reporting component status.

use std::fmt;
use std::sync::Arc;

use tokio::sync::RwLock;

/// Status levels returned by health checks.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HealthStatus {
    /// Serving normally.
    Ok,
    /// Serving, but something upstream is impaired.
    Degraded,
    /// Not able to serve.
    Unhealthy,
}

impl fmt::Display for HealthStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            HealthStatus::Ok => write!(f, "ok"),
            HealthStatus::Degraded => write!(f, "degraded"),
            HealthStatus::Unhealthy => write!(f, "unhealthy"),
        }
    }
}

/// A single health check.
pub trait HealthCheck: Send + Sync {
    /// The name this check reports under, e.g. `connection-pool:primary`.
    fn name(&self) -> &str;

    /// Runs the check and reports the current status.
    fn check(
        &self,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = HealthStatus> + Send + '_>>;
}

/// A registered health check with its name and trait object.
struct RegisteredCheck {
    name: String,
    check: Arc<dyn HealthCheck>,
}

/// Registry of health checks for aggregated /health reporting.
#[derive(Default, Clone)]
pub struct HealthRegistry {
    checks: Arc<RwLock<Vec<RegisteredCheck>>>,
}

impl HealthRegistry {
    /// Creates a registry with no checks registered.
    pub fn new() -> Self {
        Self {
            checks: Arc::new(RwLock::new(Vec::new())),
        }
    }

    /// Adds `check` under `name`.
    ///
    /// Re-registering the same name adds a second entry rather than replacing
    /// the first; [`check_all`](Self::check_all) reports both.
    pub async fn register(&self, name: impl Into<String>, check: impl HealthCheck + 'static) {
        let mut checks = self.checks.write().await;
        checks.push(RegisteredCheck {
            name: name.into(),
            check: Arc::new(check),
        });
    }

    /// Runs all checks and returns aggregated results.
    pub async fn check_all(&self) -> Vec<(String, HealthStatus)> {
        let checks = self.checks.read().await;
        let mut results = Vec::new();
        for registered in checks.iter() {
            let status = registered.check.check().await;
            results.push((registered.name.clone(), status));
        }
        results
    }
}
