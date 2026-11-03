//! Rate-limited queue wrapper.
//!
//! [`RateLimitedQueue`] wraps any [`Queue`] implementation and applies a
//! token-bucket rate limiter before enqueuing. If the bucket is empty the
//! enqueue is rejected with [`RateLimitQueueError::RateLimited`] instead of
//! blocking.

use std::sync::Arc;
use std::time::Duration;

use tokio::sync::Mutex;
use tokio::time::Instant;

use crate::error::QueueError;
use crate::traits::Queue;
use async_trait::async_trait;

/// Error returned by [`RateLimitedQueue::enqueue`].
#[derive(Debug)]
pub enum RateLimitQueueError {
    /// The limiter refused the job; nothing was enqueued.
    RateLimited,
    /// The wrapped queue rejected the job.
    Other(QueueError),
}

impl std::fmt::Display for RateLimitQueueError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::RateLimited => write!(f, "rate limited: capacity exhausted"),
            Self::Other(e) => write!(f, "queue error: {e}"),
        }
    }
}

impl std::error::Error for RateLimitQueueError {}

impl From<QueueError> for RateLimitQueueError {
    fn from(e: QueueError) -> Self {
        Self::Other(e)
    }
}

impl From<RateLimitQueueError> for QueueError {
    fn from(e: RateLimitQueueError) -> Self {
        match e {
            RateLimitQueueError::RateLimited => Self::RateLimit("capacity exhausted".into()),
            RateLimitQueueError::Other(e) => e,
        }
    }
}

/// Token-bucket rate limiter (no extra deps beyond tokio).
struct TokenBucket {
    /// Maximum burst capacity.
    capacity: u32,
    /// Current tokens (float for fractional refill).
    tokens: f64,
    /// Refill rate (tokens per second).
    rate: f64,
    /// Last refill timestamp.
    last: Instant,
}

impl TokenBucket {
    fn new(rate_per_sec: f64, burst: u32) -> Self {
        Self {
            capacity: burst.max(1),
            tokens: burst as f64,
            rate: rate_per_sec.max(0.0),
            last: Instant::now(),
        }
    }

    /// Attempts to consume one token. Returns true on success.
    fn try_consume(&mut self) -> bool {
        let now = Instant::now();
        let elapsed = now.saturating_duration_since(self.last).as_secs_f64();
        self.tokens = (self.tokens + elapsed * self.rate).min(self.capacity as f64);
        self.last = now;
        if self.tokens >= 1.0 {
            self.tokens -= 1.0;
            true
        } else {
            false
        }
    }
}

/// A queue decorator that enforces a rate limit on enqueue.
pub struct RateLimitedQueue<Q: ?Sized> {
    inner: Arc<Q>,
    bucket: Arc<Mutex<TokenBucket>>,
}

impl<Q: Queue + 'static> RateLimitedQueue<Q> {
    /// Creates a new rate-limited wrapper around `inner`.
    pub fn new(inner: Q, rate_per_sec: f64, burst: u32) -> Self {
        Self {
            inner: Arc::new(inner),
            bucket: Arc::new(Mutex::new(TokenBucket::new(rate_per_sec, burst))),
        }
    }
}

#[async_trait]
impl<Q: Queue + ?Sized> Queue for RateLimitedQueue<Q> {
    async fn enqueue(&self, topic: &str, payload: Vec<u8>) -> Result<(), QueueError> {
        let mut b = self.bucket.lock().await;
        if !b.try_consume() {
            return Err(RateLimitQueueError::RateLimited.into());
        }
        self.inner.enqueue(topic, payload).await
    }

    async fn dequeue(
        &self,
        topic: &str,
        timeout: Duration,
    ) -> Result<Option<crate::job::Job>, QueueError> {
        self.inner.dequeue(topic, timeout).await
    }

    async fn ack(&self, job: &crate::job::Job) -> Result<(), crate::error::JobError> {
        self.inner.ack(job).await
    }

    async fn nack(
        &self,
        job: &crate::job::Job,
        requeue: bool,
    ) -> Result<(), crate::error::JobError> {
        self.inner.nack(job, requeue).await
    }

    async fn dlq_move(&self, topic: &str, job: crate::job::Job) -> Result<(), QueueError> {
        self.inner.dlq_move(topic, job).await
    }

    async fn len(&self, topic: &str) -> Result<u64, QueueError> {
        self.inner.len(topic).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::in_memory::InMemoryQueue;

    #[tokio::test]
    async fn allows_enqueue_within_rate() {
        let inner = InMemoryQueue::new();
        let rl = RateLimitedQueue::new(inner, 100.0, 10);
        assert!(rl.enqueue("t", b"x".to_vec()).await.is_ok());
    }

    #[tokio::test]
    async fn rejects_when_bucket_empty() {
        let inner = InMemoryQueue::new();
        let rl = RateLimitedQueue::new(inner, 0.0, 1); // 0 tokens/sec, 1 burst
                                                       // consume the single burst token
        let _ = rl.enqueue("t", b"x".to_vec()).await;
        // next should be rate limited (no refill)
        let result = rl.enqueue("t", b"y".to_vec()).await;
        assert!(matches!(result, Err(QueueError::RateLimit(_))));
    }
}
