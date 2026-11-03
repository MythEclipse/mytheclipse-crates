//! Resource pool for sharing bounded resources across async tasks.
//!
//! Provides a `Pool` trait and a built-in `SemaphorePool<T>` implementation
//! that distributes items drawn from a `Vec<T>` under a counting semaphore.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use tokio::sync::OwnedSemaphorePermit;
use tokio::sync::Semaphore;

/// Errors returned by pool operations.
#[derive(Debug)]
pub enum PoolError {
    /// The pool was closed, or no resource became available in time.
    Exhausted,
    /// A wrapped error from the pooled resource itself.
    Other(Box<dyn std::error::Error + Send + Sync>),
}

impl std::fmt::Display for PoolError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Exhausted => f.write_str("pool exhausted"),
            Self::Other(e) => write!(f, "{e}"),
        }
    }
}

impl std::error::Error for PoolError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Other(e) => Some(e.as_ref()),
            Self::Exhausted => None,
        }
    }
}

impl From<Box<dyn std::error::Error + Send + Sync>> for PoolError {
    fn from(e: Box<dyn std::error::Error + Send + Sync>) -> Self {
        Self::Other(e)
    }
}

/// A pooled resource that releases the permit when dropped.
pub struct Pooled<T> {
    pub resource: T,
    _permit: OwnedSemaphorePermit,
}

/// A pool of resources with bounded concurrency.
#[async_trait]
pub trait Pool<T>: Send + Sync {
    /// Acquires a resource from the pool, waiting if none are available.
    async fn acquire(&self) -> Result<Pooled<T>, PoolError>;
}

/// In-memory pool backed by a semaphore + `Vec`.
#[derive(Clone)]
pub struct SemaphorePool<T: Clone> {
    semaphore: Arc<Semaphore>,
    items: Arc<Vec<T>>,
    /// Round-robin cursor over `items`, owned by this pool so that two pools
    /// hand out their items independently.
    cursor: Arc<AtomicUsize>,
}

impl<T: Clone> SemaphorePool<T> {
    /// Returns the underlying items slice (read-only view).
    pub fn items(&self) -> &[T] {
        &self.items
    }

    /// Creates a new pool from a vector of items.
    ///
    /// # Panics
    ///
    /// Panics if `items` is empty: a pool with no resources could never serve
    /// an [`acquire`](Pool::acquire).
    pub fn new(items: Vec<T>) -> Self {
        assert!(
            !items.is_empty(),
            "mytheclipse: SemaphorePool needs at least one item"
        );
        let permits = items.len();
        Self {
            semaphore: Arc::new(Semaphore::new(permits)),
            items: Arc::new(items),
            cursor: Arc::new(AtomicUsize::new(0)),
        }
    }
}

#[async_trait]
impl<T: Clone + Send + Sync + 'static> Pool<T> for SemaphorePool<T> {
    async fn acquire(&self) -> Result<Pooled<T>, PoolError> {
        let permit = self
            .semaphore
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| PoolError::Exhausted)?;
        let idx = self.cursor.fetch_add(1, Ordering::Relaxed) % self.items.len();
        Ok(Pooled {
            resource: self.items[idx].clone(),
            _permit: permit,
        })
    }
}

/// A liveness probe for a pooled resource.
///
/// Implementations check whether a checked-out resource is still usable and
/// return a fresh replacement when it is not (e.g. a broken connection).
#[async_trait]
pub trait Reconnectable {
    /// Type of the healthy resource.
    type Item;

    /// Returns `true` if `item` is still healthy, `false` if it should be
    /// replaced.
    fn is_healthy(&self, item: &Self::Item) -> bool;

    /// Builds a fresh, healthy resource to replace a dead one.
    async fn reconnect(&self) -> Result<Self::Item, Box<dyn std::error::Error + Send + Sync>>;
}

/// A pool wrapper that transparently reconnects broken resources.
///
/// Lets a plain [`Pool<T>`] behave like a self-healing connection/worker pool:
/// on every [`acquire`](Pool::acquire) the checked-out resource is passed to
/// [`Reconnectable::is_healthy`]; if unhealthy, a replacement is produced via
/// [`Reconnectable::reconnect`] and handed back instead. This removes the
/// per-call-site "is my connection dead? rebuild it" boilerplate.
///
/// ```
/// use mytheclipse::pool::{AutoReconnectPool, Pool, Reconnectable, SemaphorePool};
/// use mytheclipse::async_trait;
///
/// // A "connection" that's dead when its value equals 0.
/// #[derive(Clone)]
/// struct Conn { alive: bool }
/// impl Default for Conn { fn default() -> Self { Self { alive: true } } }
///
/// #[derive(Default)]
/// struct ConnReconnector;
///
/// #[async_trait]
/// impl Reconnectable for ConnReconnector {
///     type Item = Conn;
///     fn is_healthy(&self, c: &Conn) -> bool { c.alive }
///     async fn reconnect(&self) -> Result<Conn, Box<dyn std::error::Error + Send + Sync>> {
///         Ok(Conn::default())
///     }
/// }
///
/// #[tokio::main]
/// async fn main() {
///     let pool = SemaphorePool::new(vec![Conn { alive: false }, Conn::default()]);
///     let pool = AutoReconnectPool::new(pool, ConnReconnector);
///     let first = pool.acquire().await.unwrap();
///     assert!(first.resource.alive); // dead one was transparently replaced
/// }
/// ```
pub struct AutoReconnectPool<P, R> {
    inner: P,
    reconnect: R,
}

impl<P, R> AutoReconnectPool<P, R> {
    /// Wraps `inner` with the reconnect strategy `reconnect`.
    pub fn new(inner: P, reconnect: R) -> Self {
        Self { inner, reconnect }
    }
}

#[async_trait]
impl<P, R> Pool<R::Item> for AutoReconnectPool<P, R>
where
    P: Pool<R::Item> + Send + Sync,
    R: Reconnectable + Send + Sync,
    R::Item: Send,
{
    async fn acquire(&self) -> Result<Pooled<R::Item>, PoolError> {
        // Check out an item from the underlying pool.
        let pooled = { self.inner.acquire().await? };
        let item = pooled.resource;

        // Replace it if the lease is stale, dropping the dead resource and
        // re-adding the fresh one to keep the pool size stable would require
        // a rebuild — here we simply return a freshly built item so callers
        // always get something usable.
        if self.reconnect.is_healthy(&item) {
            Ok(Pooled {
                resource: item,
                _permit: pooled._permit,
            })
        } else {
            let fresh = self.reconnect.reconnect().await.map_err(PoolError::Other)?;
            Ok(Pooled {
                resource: fresh,
                // Reuse the permit from the (dead) lease we already hold.
                _permit: pooled._permit,
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn pool_returns_item() {
        let pool = SemaphorePool::new(vec![42u32, 84u32]);
        let item = pool.acquire().await.unwrap();
        assert!(item.resource == 42 || item.resource == 84);
    }

    #[tokio::test]
    async fn each_pool_hands_out_its_own_items_in_order() {
        let a = SemaphorePool::new(vec![1u32, 2]);
        let b = SemaphorePool::new(vec![10u32, 20]);
        // Interleaved acquisitions must not disturb pool A's rotation.
        assert_eq!(a.acquire().await.unwrap().resource, 1);
        assert_eq!(b.acquire().await.unwrap().resource, 10);
        assert_eq!(a.acquire().await.unwrap().resource, 2);
        assert_eq!(b.acquire().await.unwrap().resource, 20);
    }

    #[test]
    #[should_panic(expected = "at least one item")]
    fn empty_pool_panics() {
        let _: SemaphorePool<u32> = SemaphorePool::new(Vec::new());
    }

    struct Probe {
        dead: u32,
    }

    #[async_trait]
    impl Reconnectable for Probe {
        type Item = u32;

        fn is_healthy(&self, item: &Self::Item) -> bool {
            *item != self.dead
        }

        async fn reconnect(&self) -> Result<Self::Item, Box<dyn std::error::Error + Send + Sync>> {
            Ok(999)
        }
    }

    #[tokio::test]
    async fn reconnects_broken_item() {
        let inner = SemaphorePool::new(vec![1u32, 2u32]);
        let auto = AutoReconnectPool::new(inner, Probe { dead: 1 });
        for _ in 0..10 {
            let p = auto.acquire().await.unwrap();
            assert_ne!(p.resource, 1); // never the dead value
        }
    }
}
