//! Distributed leader election via Redis or in-process fallback.

use std::sync::Arc;

use async_trait::async_trait;
use tokio::sync::Notify;

/// Trait for leader election backends.
#[async_trait]
pub trait LeaderElection: Send + Sync {
    /// Attempts to acquire leadership. Returns true if elected.
    async fn try_acquire(&self) -> bool;
    /// Releases leadership if currently held.
    async fn release(&self);
    /// Returns true if this instance currently holds leadership.
    async fn is_leader(&self) -> bool;
}

/// In-process leader election using a shared atomic flag.
#[derive(Clone)]
pub struct InProcLeaderElection {
    leader: Arc<tokio::sync::Mutex<bool>>,
    notify: Arc<Notify>,
}

impl InProcLeaderElection {
    /// Creates an election nobody has won yet.
    pub fn new() -> Self {
        Self {
            leader: Arc::new(tokio::sync::Mutex::new(false)),
            notify: Arc::new(Notify::new()),
        }
    }
}

#[async_trait]
impl LeaderElection for InProcLeaderElection {
    async fn try_acquire(&self) -> bool {
        let mut leader = self.leader.lock().await;
        if *leader {
            false
        } else {
            *leader = true;
            true
        }
    }

    async fn release(&self) {
        let mut leader = self.leader.lock().await;
        *leader = false;
        self.notify.notify_waiters();
    }

    async fn is_leader(&self) -> bool {
        *self.leader.lock().await
    }
}

impl Default for InProcLeaderElection {
    fn default() -> Self {
        Self::new()
    }
}
