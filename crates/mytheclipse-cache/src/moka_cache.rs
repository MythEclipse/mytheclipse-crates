//! A high-performance in-process cache backed by Moka (L1, `l1-moka`).
//!
//! Moka provides automatic max-capacity and (optionally) TTL-based eviction,
//! so this L1 is well-suited to workloads where memory bounds matter.

use std::time::Duration;

use async_trait::async_trait;
use moka::future::Cache as MokaCache;

use crate::traits::{Cache, CacheError};

/// A Moka-backed [`Cache`] for L1 caching.
#[derive(Clone)]
pub struct MokaL1 {
    inner: MokaCache<String, Vec<u8>>,
}

impl MokaL1 {
    /// Builds a Moka cache with `max_capacity` entries and an optional default
    /// `ttl`.
    ///
    /// # Panics
    ///
    /// Panics if `max_capacity` is `0`. In Moka, a `max_capacity` of `0` is a
    /// sentinel for **zero-entries-allowed** — every `insert` is silently
    /// dropped — which is almost certainly a caller mistake (the natural way to
    /// express "unbounded" in other caches). Pass `1..=u64::MAX`; use
    /// [`MemoryCache`](crate::memory::MemoryCache) if you truly need an
    /// unbounded in-process cache.
    pub fn new(max_capacity: u64, ttl: Option<Duration>) -> Self {
        assert!(
            max_capacity > 0,
            "mytheclipse-cache: MokaL1::new(max_capacity) must be > 0; \
             moka treats 0 as a permanent no-insert sentinel. \
             Use MemoryCache for an unbounded cache."
        );
        let mut builder = MokaCache::builder().max_capacity(max_capacity);
        if let Some(ttl) = ttl {
            builder = builder.time_to_live(ttl);
        }
        Self {
            inner: builder.build(),
        }
    }
}

#[async_trait]
impl Cache for MokaL1 {
    async fn get(&self, key: &str) -> Result<Option<Vec<u8>>, CacheError> {
        Ok(self.inner.get(key).await)
    }

    /// Inserts `value`, using the cache's configured TTL policy. The per-call
    /// `ttl` argument is intentionally ignored — Moka applies a single TTL
    /// configured on the builder, and per-entry overrides are not exposed here.
    async fn set(
        &self,
        key: &str,
        value: Vec<u8>,
        _ttl: Option<Duration>,
    ) -> Result<(), CacheError> {
        self.inner.insert(key.to_string(), value).await;
        Ok(())
    }

    async fn invalidate(&self, key: &str) -> Result<(), CacheError> {
        self.inner.invalidate(key).await;
        Ok(())
    }

    async fn clear(&self) -> Result<(), CacheError> {
        self.inner.invalidate_all();
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn set_get_roundtrip() {
        let c = MokaL1::new(100, None);
        c.set("k", b"v".to_vec(), None).await.unwrap();
        assert_eq!(c.get("k").await.unwrap(), Some(b"v".to_vec()));
        assert_eq!(c.get("missing").await.unwrap(), None);
    }

    #[tokio::test]
    async fn invalidate_and_clear() {
        let c = MokaL1::new(100, None);
        c.set("a", b"1".to_vec(), None).await.unwrap();
        c.set("b", b"2".to_vec(), None).await.unwrap();
        c.invalidate("a").await.unwrap();
        assert_eq!(c.get("a").await.unwrap(), None);
        assert_eq!(c.get("b").await.unwrap(), Some(b"2".to_vec()));
        c.clear().await.unwrap();
        assert_eq!(c.get("b").await.unwrap(), None);
    }

    /// Asserts that `max_capacity == 0` panics with a clear message, rather
    /// than silently creating a cache that never accepts entries.
    #[test]
    #[should_panic(expected = "must be > 0")]
    fn zero_capacity_panics() {
        let _ = MokaL1::new(0, None);
    }

    #[tokio::test]
    async fn ttl_does_expire() {
        // Keep a firm TTL assertion; sleep well past the expiry window.
        let c = MokaL1::new(100, Some(Duration::from_millis(40)));
        c.set("k", b"v".to_vec(), None).await.unwrap();
        assert_eq!(c.get("k").await.unwrap(), Some(b"v".to_vec()));
        tokio::time::sleep(Duration::from_millis(120)).await;
        let v = c.get("k").await.unwrap();
        assert!(v.is_none());
    }
}
