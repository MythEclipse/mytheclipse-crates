//! A simple, dependency-free in-process cache (L1, `l1-memory`).
//!
//! Backed by a `HashMap<String, Entry>` guarded by a single `RwLock`: reads
//! take the lock shared, so readers never wait on each other, while every
//! mutation (insert, evict, expire) holds it exclusively and therefore sees a
//! consistent map + insertion order. Entries are lazily expired on
//! access by comparing against `Instant`; a monotonic clock keeps TTLs robust
//! against wall-clock discontinuities, and an entry with no TTL never reads
//! the clock at all.

use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

use async_trait::async_trait;

use crate::traits::{Cache, CacheError};

/// A stored value with its optional expiry and its insertion sequence number.
#[derive(Debug)]
struct Entry {
    value: Vec<u8>,
    /// `None` means the value never expires.
    expires: Option<Instant>,
    /// Monotonic insert order, used to detect stale `order` slots.
    seq: u64,
}

/// The cache's mutable state: the map and (when bounded) its insertion order
/// live behind one lock, so they can never drift out of step.
#[derive(Debug)]
struct State {
    map: HashMap<String, Entry>,
    /// Insertion order as `(seq, key)`; only maintained when the cache is
    /// bounded. A slot whose `seq` no longer matches the map entry is stale
    /// (overwritten, invalidated or expired) and is skipped during eviction.
    order: VecDeque<(u64, String)>,
    next_seq: u64,
}

/// Compact the order queue once stale slots outnumber live ones, keeping it
/// proportional to the number of live entries.
const COMPACT_FACTOR: usize = 4;

impl State {
    fn new() -> Self {
        Self {
            map: HashMap::new(),
            order: VecDeque::new(),
            next_seq: 0,
        }
    }

    /// Evicts oldest-first until the map is back within `cap`, then compacts
    /// the order queue if it accumulated too many stale slots.
    fn evict_down_to(&mut self, cap: usize) {
        while self.map.len() > cap {
            match self.order.pop_front() {
                // Only remove when this slot is the key's live insertion.
                Some((seq, key)) => {
                    if self.map.get(&key).is_some_and(|entry| entry.seq == seq) {
                        self.map.remove(&key);
                    }
                }
                None => return,
            }
        }
        if self.order.len() > cap.saturating_mul(COMPACT_FACTOR) {
            let Self { map, order, .. } = self;
            order.retain(|(seq, key)| map.get(key).is_some_and(|entry| entry.seq == *seq));
        }
    }
}

/// An in-process [`Cache`] for L1 caching.
///
/// Default instance is **unbounded** — it grows until the process runs out of
/// memory. For memory-constrained workloads, use [`MemoryCache::with_max_entries`]
/// to install a simple LRU-style cap: when the cap is exceeded, the oldest
/// (least-recently-inserted) entry is evicted.
#[derive(Debug, Clone)]
pub struct MemoryCache {
    state: Arc<RwLock<State>>,
    /// When `Some(n)`, the cache refuses more than `n` live entries and evicts
    /// the oldest on overflow. `None` = unbounded (legacy default).
    max_entries: Option<usize>,
}

impl Default for MemoryCache {
    fn default() -> Self {
        Self {
            state: Arc::new(RwLock::new(State::new())),
            max_entries: None,
        }
    }
}

impl MemoryCache {
    /// Builds an empty in-memory cache (unbounded by default).
    pub fn new() -> Self {
        Self::default()
    }

    /// Pre-allocates space for `capacity` entries to reduce reallocation.
    pub fn with_capacity(self, capacity: usize) -> Self {
        self.state.write().unwrap().map.reserve(capacity);
        self
    }

    /// Installs a bounded LRU-style cap. When the cache exceeds `max`, the
    /// oldest (least-recently-inserted) entry is evicted on each `set`.
    ///
    /// This is the recommended constructor for production L1 caches: a
    /// [`MemoryCache::new()`] (unbounded) left unmanaged can grow without bound
    /// and exhaust process memory.
    pub fn with_max_entries(mut self, max: usize) -> Self {
        assert!(max > 0, "mytheclipse-cache: with_max_entries must be > 0");
        self.max_entries = Some(max);
        self
    }

    /// The configured max entries, if any.
    pub fn max_entries(&self) -> Option<usize> {
        self.max_entries
    }
}

#[async_trait]
impl Cache for MemoryCache {
    async fn get(&self, key: &str) -> Result<Option<Vec<u8>>, CacheError> {
        // Fast path: one probe under a shared lock. The clock is only read for
        // entries that actually carry a TTL, so a cache of immortal entries
        // never pays for `Instant::now`.
        {
            let state = self.state.read().unwrap();
            match state.map.get(key) {
                None => return Ok(None),
                Some(entry) => {
                    let fresh = match entry.expires {
                        None => true,
                        Some(at) => at > Instant::now(),
                    };
                    if fresh {
                        return Ok(Some(entry.value.clone()));
                    }
                }
            }
        }

        // Slow path: the entry is stale, so drop it under the exclusive lock.
        // A racing `set` may have replaced it in between — re-check before
        // removing so a fresh value is never evicted by a stale observation.
        let mut state = self.state.write().unwrap();
        if state
            .map
            .get(key)
            .is_some_and(|entry| entry.expires.is_some_and(|at| at <= Instant::now()))
        {
            state.map.remove(key);
        }
        Ok(None)
    }

    async fn set(
        &self,
        key: &str,
        value: Vec<u8>,
        ttl: Option<Duration>,
    ) -> Result<(), CacheError> {
        let expires = ttl.map(|d| Instant::now() + d);
        let mut state = self.state.write().unwrap();
        let seq = state.next_seq;
        state.next_seq += 1;
        state.map.insert(
            key.to_string(),
            Entry {
                value,
                expires,
                seq,
            },
        );
        // Only bounded caches need an eviction order at all: tracking it on an
        // unbounded cache would just duplicate every key forever.
        if let Some(cap) = self.max_entries {
            state.order.push_back((seq, key.to_string()));
            state.evict_down_to(cap);
        }
        Ok(())
    }

    async fn invalidate(&self, key: &str) -> Result<(), CacheError> {
        // O(1): the key's order slot becomes stale and is skipped on eviction.
        self.state.write().unwrap().map.remove(key);
        Ok(())
    }

    async fn clear(&self) -> Result<(), CacheError> {
        let mut state = self.state.write().unwrap();
        state.map.clear();
        state.order.clear();
        Ok(())
    }
}

/// A typed view over a byte cache using `serde`-compatible (JSON) encoding.
///
/// Only enabled with the `cache-aside` feature, which pulls in `serde`.
#[cfg(feature = "cache-aside")]
pub mod typed {
    use serde::{de::DeserializeOwned, Serialize};

    use super::*;

    /// Wraps a [`Cache`] with JSON-based typed get/set.
    #[derive(Clone)]
    pub struct TypedCache<C> {
        inner: C,
    }

    impl<C: Cache> TypedCache<C> {
        /// Wraps `inner`.
        pub fn new(inner: C) -> Self {
            Self { inner }
        }

        /// Fetches and deserializes a value.
        pub async fn get<T: DeserializeOwned>(&self, key: &str) -> Result<Option<T>, CacheError> {
            match self.inner.get(key).await? {
                Some(bytes) => serde_json::from_slice(&bytes)
                    .map(Some)
                    .map_err(|e| CacheError::Serialization(e.to_string())),
                None => Ok(None),
            }
        }

        /// Serializes and stores a value.
        pub async fn set<T: Serialize>(
            &self,
            key: &str,
            value: &T,
            ttl: Option<Duration>,
        ) -> Result<(), CacheError> {
            let bytes =
                serde_json::to_vec(value).map_err(|e| CacheError::Serialization(e.to_string()))?;
            self.inner.set(key, bytes, ttl).await
        }

        /// Returns the underlying byte cache.
        pub fn into_inner(self) -> C {
            self.inner
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn set_get_roundtrip() {
        let c = MemoryCache::new();
        c.set("k", b"v".to_vec(), None).await.unwrap();
        assert_eq!(c.get("k").await.unwrap(), Some(b"v".to_vec()));
        assert_eq!(c.get("missing").await.unwrap(), None);
    }

    #[tokio::test]
    async fn ttl_expires_entry() {
        let c = MemoryCache::new();
        c.set("k", b"v".to_vec(), Some(Duration::from_millis(30)))
            .await
            .unwrap();
        assert_eq!(c.get("k").await.unwrap(), Some(b"v".to_vec()));
        tokio::time::sleep(Duration::from_millis(60)).await;
        assert_eq!(c.get("k").await.unwrap(), None);
    }

    #[tokio::test]
    async fn invalidate_and_clear() {
        let c = MemoryCache::new();
        c.set("a", b"1".to_vec(), None).await.unwrap();
        c.set("b", b"2".to_vec(), None).await.unwrap();
        c.invalidate("a").await.unwrap();
        assert_eq!(c.get("a").await.unwrap(), None);
        assert_eq!(c.get("b").await.unwrap(), Some(b"2".to_vec()));
        c.clear().await.unwrap();
        assert_eq!(c.get("b").await.unwrap(), None);
    }

    /// Asserts that an unbounded `MemoryCache::with_max_entries(0)` panics,
    /// preventing a no-op cache that accepts zero entries.
    #[test]
    #[should_panic(expected = "must be > 0")]
    fn zero_max_panics() {
        let _ = MemoryCache::new().with_max_entries(0);
    }

    #[tokio::test]
    async fn bounded_cache_evicts_oldest() {
        let c = MemoryCache::new().with_max_entries(2);
        c.set("a", b"1".to_vec(), None).await.unwrap();
        c.set("b", b"2".to_vec(), None).await.unwrap();
        c.set("c", b"3".to_vec(), None).await.unwrap();
        // "a" (oldest) should have been evicted.
        assert_eq!(c.get("a").await.unwrap(), None);
        assert_eq!(c.get("b").await.unwrap(), Some(b"2".to_vec()));
        assert_eq!(c.get("c").await.unwrap(), Some(b"3".to_vec()));
    }

    #[tokio::test]
    async fn bounded_cache_keeps_cap_under_hot_overwrites() {
        let c = MemoryCache::new().with_max_entries(2);
        for i in 0..100 {
            c.set("hot", format!("v{i}").into_bytes(), None)
                .await
                .unwrap();
        }
        c.set("other", b"o".to_vec(), None).await.unwrap();
        c.set("third", b"t".to_vec(), None).await.unwrap();

        let state = c.state.read().unwrap();
        assert_eq!(state.map.len(), 2);
        assert!(state.order.len() <= 2 * COMPACT_FACTOR);
        // "hot" holds the oldest live insertion, so it is the evicted one.
        assert!(!state.map.contains_key("hot"));
        assert!(state.map.contains_key("other"));
        assert!(state.map.contains_key("third"));
    }

    #[tokio::test]
    async fn unbounded_cache_does_not_track_order() {
        let c = MemoryCache::new();
        for i in 0..10 {
            c.set(&format!("k{i}"), b"v".to_vec(), None).await.unwrap();
        }
        assert!(c.state.read().unwrap().order.is_empty());
    }

    #[cfg(feature = "cache-aside")]
    #[tokio::test]
    async fn typed_cache_roundtrip() {
        use typed::TypedCache;
        #[derive(serde::Serialize, serde::Deserialize, Debug, PartialEq)]
        struct User {
            id: u64,
            name: String,
        }
        let typed = TypedCache::new(MemoryCache::new());
        typed
            .set(
                "u",
                &User {
                    id: 1,
                    name: "alice".into(),
                },
                None,
            )
            .await
            .unwrap();
        let got: User = typed.get("u").await.unwrap().unwrap();
        assert_eq!(
            got,
            User {
                id: 1,
                name: "alice".into()
            }
        );
    }
}
