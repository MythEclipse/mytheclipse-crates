//! Health-checked resource pool (feature `observability` + `traffic`).
//!
//! [`HealthCheckedPool`] composes a [`SemaphorePool`] with a [`HealthRegistry`]:
//! a background probe periodically logs how many pooled items still validate,
//! a registered `HealthCheck` reports pool liveliness on demand, and
//! [`acquire_healthy`](HealthCheckedPool::acquire_healthy) refuses to hand out
//! an item the validator rejects.

use std::sync::Arc;
use std::time::Duration;

use crate::health::{HealthCheck, HealthRegistry, HealthStatus};
use crate::pool::{Pool, PoolError, Pooled, SemaphorePool};

/// A health check backed by a closure.
struct ClosureCheck {
    name: String,
    check: Arc<dyn Fn() -> HealthStatus + Send + Sync>,
}

impl HealthCheck for ClosureCheck {
    fn name(&self) -> &str {
        &self.name
    }

    fn check(
        &self,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = HealthStatus> + Send + '_>> {
        let status = (self.check)();
        Box::pin(async move { status })
    }
}

/// A resource pool with integrated health reporting.
pub struct HealthCheckedPool<T: Clone + Send + Sync + 'static> {
    pub(crate) inner: SemaphorePool<T>,
    validator: Arc<dyn Fn(&T) -> bool + Send + Sync>,
}

impl<T: Clone + Send + Sync + 'static + std::fmt::Debug> HealthCheckedPool<T> {
    /// Creates a new health-checked pool.
    ///
    /// `validator` decides whether a pooled item is usable. The registered
    /// health check reports `Ok` while any item passes it, and
    /// [`acquire_healthy`](Self::acquire_healthy) skips items that fail.
    pub async fn new(
        items: Vec<T>,
        registry: &HealthRegistry,
        name: impl Into<String>,
        check_interval: Duration,
        validator: impl Fn(&T) -> bool + Send + Sync + 'static,
    ) -> Self {
        let name = name.into();
        let pool = SemaphorePool::new(items);
        // One snapshot of the items, shared by the health check and the probe
        // rather than cloned once per closure.
        let snapshot = Arc::new(pool.items().to_vec());
        let validator: Arc<dyn Fn(&T) -> bool + Send + Sync> = Arc::new(validator);

        let check = ClosureCheck {
            name: format!("connection-pool:{name}"),
            check: {
                let snapshot = Arc::clone(&snapshot);
                let validator = Arc::clone(&validator);
                Arc::new(move || {
                    if snapshot.iter().any(|item| validator(item)) {
                        HealthStatus::Ok
                    } else {
                        HealthStatus::Unhealthy
                    }
                })
            },
        };
        // Awaited rather than spawned: a caller that reads `/health` right after
        // `new` returns must not race the registration.
        registry.register(check.name.clone(), check).await;

        let probe_validator = Arc::clone(&validator);
        tokio::task::spawn(async move {
            let mut ticker = tokio::time::interval(check_interval);
            loop {
                ticker.tick().await;
                let up = snapshot.iter().filter(|item| probe_validator(item)).count();
                tracing::debug!(pool = %name, up, total = snapshot.len(), "pool health probe");
            }
        });

        Self {
            inner: pool,
            validator,
        }
    }

    /// Acquires an item the validator accepts.
    ///
    /// Each retry returns a different item — the pool hands them out
    /// round-robin — so trying `size()` times covers the whole pool. Returns
    /// [`PoolError::Exhausted`] when every item fails validation.
    pub async fn acquire_healthy(&self) -> Result<Pooled<T>, PoolError> {
        for _ in 0..self.size() {
            let pooled = self.inner.acquire().await?;
            if (self.validator)(&pooled.resource) {
                return Ok(pooled);
            }
        }
        Err(PoolError::Exhausted)
    }

    /// Number of items in the pool.
    pub fn size(&self) -> usize {
        self.inner.items().len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn acquires_resource() {
        let reg = HealthRegistry::new();
        let pool = HealthCheckedPool::new(
            vec![42u32, 84u32],
            &reg,
            "test",
            Duration::from_secs(5),
            |_| true,
        )
        .await;
        let item = pool.acquire_healthy().await.unwrap();
        assert!(item.resource == 42 || item.resource == 84);
        assert_eq!(pool.size(), 2);
    }

    #[tokio::test]
    async fn skips_items_the_validator_rejects() {
        let reg = HealthRegistry::new();
        let pool = HealthCheckedPool::new(
            vec![0u32, 0u32, 7u32],
            &reg,
            "test",
            Duration::from_secs(5),
            |x| *x > 0,
        )
        .await;
        // Round-robin retry reaches the one healthy item within `size` tries.
        let item = pool.acquire_healthy().await.unwrap();
        assert_eq!(item.resource, 7);
    }

    #[tokio::test]
    async fn reports_exhausted_when_nothing_validates() {
        let reg = HealthRegistry::new();
        let pool = HealthCheckedPool::new(
            vec![0u32, 0u32],
            &reg,
            "test",
            Duration::from_secs(5),
            |_| false,
        )
        .await;
        assert!(matches!(
            pool.acquire_healthy().await,
            Err(PoolError::Exhausted)
        ));
    }

    #[tokio::test]
    async fn registers_the_health_check_before_returning() {
        let reg = HealthRegistry::new();
        let _pool =
            HealthCheckedPool::new(vec![1u32], &reg, "test", Duration::from_secs(5), |_| true)
                .await;
        // No await point between construction and the read: registration is
        // complete by the time `new` returns.
        let results = reg.check_all().await;
        assert!(results
            .iter()
            .any(|(name, _)| name == "connection-pool:test"));
    }
}
