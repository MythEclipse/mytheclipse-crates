//! # mytheclipse-cache
//!
//! A unified multi-layer cache abstraction that keeps your application from
//! being locked to any single cache provider.
//!
//! - **L1 (in-process) caches**: [`memory::MemoryCache`] (zero-dependency,
//!   default).
//! - **L2 (distributed) caches**: [`redis::RedisCache`] backed by Redis/Valkey.
//! - **Multi-layer composition**: [`multilayer::MultiLayerCache`] layers an L1
//!   over an L2 behind one [`Cache`] face; reads fall through to L2 and
//!   backfill L1.
//! - **Cache-aside / auto-refresh**: [`cache_aside::CacheAside`] reads through
//!   to a data source on a miss and caches the result.
//!
//! The core [`Cache`] trait is byte-oriented; typed convenience (JSON) is
//! layered on top via [`memory::typed::TypedCache`].
//!
//! ## Example
//!
//! Multi-layer + cache-aside composition (default features):
//!
//! ```no_run
//! # #[cfg(all(feature = "l1-memory", feature = "cache-aside"))]
//! # async fn run() {
//! use mytheclipse_cache::{Cache, MemoryCache, MultiLayerCache, CacheAside};
//! let l1 = MemoryCache::new();
//! let l2 = MemoryCache::new(); // in a real app: a RedisCache
//! let cache = MultiLayerCache::new(l1, l2);
//!
//! cache.set("user:1", b"payload".to_vec(), None).await.unwrap();
//! assert_eq!(cache.get("user:1").await.unwrap(), Some(b"payload".to_vec()));
//!
//! // Cache-aside: fill misses from a source of truth.
//! let aside = CacheAside::new(
//!     MemoryCache::new(),
//!     |key| async move { Some(format!("data-for-{key}").into_bytes()) },
//! );
//! let _v = aside.get("orders:42").await.unwrap();
//! # }
//! # #[cfg(not(all(feature = "l1-memory", feature = "cache-aside")))]
//! # fn run() {}
//! ```

#![forbid(unsafe_code)]

pub mod traits;

#[cfg(feature = "l1-memory")]
pub mod memory;

#[cfg(feature = "l2-redis")]
pub mod redis;

#[cfg(feature = "cache-aside")]
pub mod cache_aside;

#[cfg(feature = "cache-aside")]
pub mod multilayer;

#[cfg(feature = "cache-aside")]
pub mod auto_refresh;

#[cfg(feature = "cache-aside")]
pub mod metrics;

pub use traits::{Cache, CacheError};

#[cfg(feature = "l1-memory")]
pub use memory::MemoryCache;

#[cfg(feature = "l2-redis")]
pub use redis::RedisCache;

#[cfg(feature = "cache-aside")]
pub use cache_aside::CacheAside;

#[cfg(feature = "cache-aside")]
pub use multilayer::MultiLayerCache;
