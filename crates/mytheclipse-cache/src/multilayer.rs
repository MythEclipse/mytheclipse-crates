//! Multi-layer (L1/L2) caching behind a single [`Cache`] face.
//!
//! [`MultiLayerCache`] layers a fast in-process L1 over a slower but larger
//! L2 (e.g. Redis). Reads are L1-first with an L2 fallback; a hit on L2 is
//! backfilled into L1. Writes and invalidations go to both layers.

use std::time::Duration;

use async_trait::async_trait;

use crate::traits::{Cache, CacheError};

/// A read-through, write-through composition of an L1 and L2 cache.
///
/// `L1` is typically [`crate::memory::MemoryCache`]; `L2` is typically a
/// distributed cache such as a Redis backend. Order of layers fixed: `L1` is
/// consulted first.
#[derive(Clone)]
pub struct MultiLayerCache<L1, L2> {
    l1: L1,
    l2: L2,
    /// When `true`, an L2 hit is written back into L1 (default `true`).
    populate_l1: bool,
}

impl<L1, L2> MultiLayerCache<L1, L2>
where
    L1: Cache,
    L2: Cache,
{
    /// Builds a two-layer cache with L1-backfill enabled.
    pub fn new(l1: L1, l2: L2) -> Self {
        Self {
            l1,
            l2,
            populate_l1: true,
        }
    }

    /// Disables L1 backfill-on-read.
    pub fn without_l1_backfill(mut self) -> Self {
        self.populate_l1 = false;
        self
    }

    /// Returns a reference to the L1 layer.
    pub fn l1(&self) -> &L1 {
        &self.l1
    }

    /// Returns a reference to the L2 layer.
    pub fn l2(&self) -> &L2 {
        &self.l2
    }
}

#[async_trait]
impl<L1, L2> Cache for MultiLayerCache<L1, L2>
where
    L1: Cache,
    L2: Cache,
{
    async fn get(&self, key: &str) -> Result<Option<Vec<u8>>, CacheError> {
        // L1 first.
        if let Some(value) = self.l1.get(key).await? {
            return Ok(Some(value));
        }
        // L2 fallback.
        if let Some(value) = self.l2.get(key).await? {
            if self.populate_l1 {
                self.l1.set(key, value.clone(), None).await?;
            }
            return Ok(Some(value));
        }
        Ok(None)
    }

    async fn set(
        &self,
        key: &str,
        value: Vec<u8>,
        ttl: Option<Duration>,
    ) -> Result<(), CacheError> {
        self.l1.set(key, value.clone(), ttl).await?;
        self.l2.set(key, value, ttl).await
    }

    async fn invalidate(&self, key: &str) -> Result<(), CacheError> {
        self.l1.invalidate(key).await?;
        self.l2.invalidate(key).await
    }

    async fn clear(&self) -> Result<(), CacheError> {
        self.l1.clear().await?;
        self.l2.clear().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::memory::MemoryCache;

    #[tokio::test]
    async fn read_through_populates_l1() {
        let l2 = MemoryCache::new();
        l2.set("k", b"l2-value".to_vec(), None).await.unwrap();

        let layered = MultiLayerCache::new(MemoryCache::new(), l2);
        assert_eq!(layered.l1().get("k").await.unwrap(), None);
        assert_eq!(layered.get("k").await.unwrap(), Some(b"l2-value".to_vec()));
        // L2 hit should have populated L1.
        assert_eq!(
            layered.l1().get("k").await.unwrap(),
            Some(b"l2-value".to_vec())
        );
    }

    #[tokio::test]
    async fn write_goes_to_both() {
        let l1 = MemoryCache::new();
        let l2 = MemoryCache::new();
        let layered = MultiLayerCache::new(l1, l2.clone());
        layered.set("k", b"v".to_vec(), None).await.unwrap();
        assert_eq!(layered.l1().get("k").await.unwrap(), Some(b"v".to_vec()));
        assert_eq!(l2.get("k").await.unwrap(), Some(b"v".to_vec()));
    }

    #[tokio::test]
    async fn invalidate_clears_both() {
        let l1 = MemoryCache::new();
        let l2 = MemoryCache::new();
        let layered = MultiLayerCache::new(l1, l2);
        layered.set("k", b"v".to_vec(), None).await.unwrap();
        layered.invalidate("k").await.unwrap();
        assert_eq!(layered.l1().get("k").await.unwrap(), None);
        assert_eq!(layered.l2().get("k").await.unwrap(), None);
    }
}
