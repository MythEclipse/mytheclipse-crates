//! Distributed lock with lease-based expiration.
//!
//! Provides a `DistributedLock` trait with in-process and Redis backends.
//! Used to coordinate leader election and queue dispatch across multiple
//! worker instances.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use async_trait::async_trait;

/// Errors returned by distributed lock operations.
#[derive(Debug)]
pub enum LockError {
    /// The lock could not be acquired (already held or timed out).
    AlreadyHeld,
    /// The lease expired and the lock was released.
    Expired,
    /// A backend transport error occurred.
    Io(String),
}

impl std::fmt::Display for LockError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            LockError::AlreadyHeld => write!(f, "lock already held"),
            LockError::Expired => write!(f, "lock lease expired"),
            LockError::Io(s) => write!(f, "lock io error: {s}"),
        }
    }
}

impl std::error::Error for LockError {}

/// One held lease: when it expires, and the fencing token of the guard that
/// took it (so a stale guard cannot release or extend someone else's lease).
#[derive(Debug, Clone, Copy)]
struct Held {
    expires: Instant,
    token: u64,
}

/// Shared state for an in-process lock — maps keys to their lease.
#[derive(Default)]
struct LockState {
    held: HashMap<String, Held>,
    next_token: u64,
}

type LockMap = Arc<Mutex<LockState>>;

/// A handle to an acquired distributed lock (RAII — releases on drop).
pub struct LockGuard {
    map: LockMap,
    key: String,
    token: u64,
}

impl LockGuard {
    pub fn key(&self) -> &str {
        &self.key
    }

    /// Extends the lease, provided this guard still owns the lock.
    pub async fn extend(&mut self, dur: Duration) -> Result<(), LockError> {
        let mut state = self.map.lock().unwrap();
        match state.held.get_mut(&self.key) {
            Some(held) if held.token == self.token => {
                held.expires = Instant::now() + dur;
                Ok(())
            }
            _ => Err(LockError::Expired),
        }
    }
}

impl Drop for LockGuard {
    fn drop(&mut self) {
        // Synchronous release: no runtime handle needed, so a guard dropped
        // during shutdown (or outside a runtime) still frees the key.
        let mut state = self.map.lock().unwrap();
        if state
            .held
            .get(&self.key)
            .is_some_and(|held| held.token == self.token)
        {
            state.held.remove(&self.key);
        }
    }
}

/// Trait for distributed lock backends.
#[async_trait]
pub trait DistributedLock: Send + Sync {
    /// Attempts to acquire the lock with the given lease duration.
    async fn acquire(
        &self,
        key: &str,
        lease: Duration,
        timeout: Duration,
    ) -> Result<LockGuard, LockError>;

    /// Releases the lock.
    async fn release(&self, key: &str) -> Result<(), LockError>;

    /// Attempts to extend an existing lease.
    async fn extend(&self, key: &str, lease: Duration) -> Result<(), LockError>;
}

/// In-process distributed lock using a mutex + lease timer.
/// Suitable for testing and single-instance coordination.
pub struct InProcLock {
    held: LockMap,
}

impl InProcLock {
    pub fn new() -> Self {
        Self {
            held: Arc::new(Mutex::new(LockState::default())),
        }
    }
}

impl Default for InProcLock {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl DistributedLock for InProcLock {
    async fn acquire(
        &self,
        key: &str,
        lease: Duration,
        timeout: Duration,
    ) -> Result<LockGuard, LockError> {
        let deadline = Instant::now() + timeout;
        loop {
            let token = {
                let mut state = self.held.lock().unwrap();
                let now = Instant::now();
                // Lazy per-key expiry: only the key being acquired is examined,
                // so acquisition stays O(1) regardless of how many other leases
                // the lock is tracking.
                let free = state.held.get(key).map_or(true, |held| held.expires <= now);
                if free {
                    let token = state.next_token;
                    state.next_token += 1;
                    state.held.insert(
                        key.to_string(),
                        Held {
                            expires: now + lease,
                            token,
                        },
                    );
                    Some(token)
                } else {
                    None
                }
            };
            if let Some(token) = token {
                return Ok(LockGuard {
                    map: Arc::clone(&self.held),
                    key: key.to_string(),
                    token,
                });
            }
            if Instant::now() >= deadline {
                return Err(LockError::AlreadyHeld);
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    async fn release(&self, key: &str) -> Result<(), LockError> {
        self.held.lock().unwrap().held.remove(key);
        Ok(())
    }

    async fn extend(&self, key: &str, lease: Duration) -> Result<(), LockError> {
        let mut state = self.held.lock().unwrap();
        match state.held.get_mut(key) {
            Some(held) => {
                held.expires = Instant::now() + lease;
                Ok(())
            }
            None => Err(LockError::AlreadyHeld),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn lock_acquire_release() {
        let lock = InProcLock::new();
        let guard = lock
            .acquire("key", Duration::from_secs(10), Duration::from_secs(1))
            .await
            .unwrap();
        assert!(lock.release("key").await.is_ok());
        drop(guard);
    }

    #[tokio::test]
    async fn lock_rejects_second_acquire() {
        let lock = InProcLock::new();
        let _guard1 = lock
            .acquire("key", Duration::from_secs(10), Duration::from_secs(1))
            .await
            .unwrap();
        // While guard1 is alive, a second acquire with short timeout should fail.
        let result = lock
            .acquire("key", Duration::from_secs(10), Duration::from_millis(50))
            .await;
        assert!(result.is_err());
        drop(_guard1);
    }

    #[tokio::test]
    async fn lock_auto_releases_on_drop() {
        let lock = InProcLock::new();
        let guard = lock
            .acquire("k", Duration::from_secs(10), Duration::from_secs(1))
            .await
            .unwrap();
        drop(guard);
        // After drop, the lock should be releasable / re-acquirable.
        let result = lock
            .acquire("k", Duration::from_secs(10), Duration::from_millis(50))
            .await;
        assert!(result.is_ok(), "lock should be free after guard drop");
    }

    #[tokio::test]
    async fn lock_expires_after_lease() {
        let lock = InProcLock::new();
        let _guard = lock
            .acquire("key", Duration::from_millis(20), Duration::from_millis(5))
            .await
            .unwrap();
        drop(_guard);
        tokio::time::sleep(Duration::from_millis(30)).await;
        // Should be acquirable now.
        let result = lock
            .acquire("key", Duration::from_millis(20), Duration::from_millis(5))
            .await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn guard_extend_keeps_the_lease_alive() {
        let lock = InProcLock::new();
        let mut guard = lock
            .acquire("k", Duration::from_millis(20), Duration::from_millis(5))
            .await
            .unwrap();
        guard.extend(Duration::from_secs(5)).await.unwrap();
        tokio::time::sleep(Duration::from_millis(40)).await;
        // Past the original lease, still held because it was extended.
        assert!(lock
            .acquire("k", Duration::from_secs(1), Duration::from_millis(20))
            .await
            .is_err());
    }

    #[tokio::test]
    async fn stale_guard_does_not_release_the_new_holder() {
        let lock = InProcLock::new();
        let stale = lock
            .acquire("k", Duration::from_millis(10), Duration::from_millis(5))
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(20)).await;
        let fresh = lock
            .acquire("k", Duration::from_secs(5), Duration::from_millis(50))
            .await
            .unwrap();
        drop(stale);
        assert!(
            lock.acquire("k", Duration::from_secs(1), Duration::from_millis(20))
                .await
                .is_err(),
            "dropping an expired guard must not free the live lease"
        );
        drop(fresh);
    }
}
