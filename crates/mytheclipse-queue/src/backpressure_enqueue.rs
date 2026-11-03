//! Backpressure-aware enqueuer (in-memory backend).
//!
//! [`BackpressureEnforcer`] tracks un-acked jobs per topic and caps how many may
//! be outstanding at once, returning [`BackpressureError`] instead of blocking
//! when the cap is exceeded.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use crate::error::QueueError;
use crate::traits::Queue;

/// Errors returned by [`BackpressureEnforcer`].
#[derive(Debug)]
pub enum BackpressureError {
    /// The configured per-topic cap was reached; enqueue rejected.
    LimitReached {
        /// The topic whose cap was reached.
        topic: String,
    },
    /// The underlying queue rejected the enqueue.
    Enqueue(QueueError),
}

impl std::fmt::Display for BackpressureError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::LimitReached { topic } => {
                write!(f, "backpressure: in-flight limit reached for topic {topic}")
            }
            Self::Enqueue(e) => write!(f, "backpressure: enqueue failed: {e}"),
        }
    }
}

impl std::error::Error for BackpressureError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::LimitReached { .. } => None,
            Self::Enqueue(e) => Some(e),
        }
    }
}

/// Caps how many un-acked jobs a topic may have outstanding.
///
/// A slot is taken by [`try_enqueue`](Self::try_enqueue) and returned by
/// [`dec_finished`](Self::dec_finished), so the cap holds only if the caller
/// pairs them. Nothing here releases a slot on its own: the enforcer cannot know
/// when a handler finished.
pub struct BackpressureEnforcer {
    /// Maximum un-acked jobs allowed per topic.
    max_per_topic: u64,
    /// Per-topic slot counter.
    counters: Mutex<HashMap<String, Arc<AtomicU64>>>,
}

impl BackpressureEnforcer {
    /// Creates an enforcer that allows `max_inflight` un-acked jobs per topic.
    ///
    /// A cap of zero would reject every enqueue, so it is clamped to one.
    pub fn new(max_inflight: usize) -> Self {
        Self {
            max_per_topic: max_inflight.max(1) as u64,
            counters: Mutex::new(HashMap::new()),
        }
    }

    /// Returns the counter for `topic`, creating it on first use.
    fn counter(&self, topic: &str) -> Arc<AtomicU64> {
        let mut map = self.counters.lock().expect("counter map poisoned");
        Arc::clone(map.entry(topic.to_owned()).or_default())
    }

    /// Takes one slot for `topic` without blocking.
    ///
    /// The compare-exchange loop is what makes the cap hold: two enqueues racing
    /// for the last slot cannot both see room and both take it.
    fn take_slot(&self, topic: &str) -> Result<Arc<AtomicU64>, BackpressureError> {
        let counter = self.counter(topic);
        let mut current = counter.load(Ordering::Acquire);
        loop {
            if current >= self.max_per_topic {
                return Err(BackpressureError::LimitReached {
                    topic: topic.to_owned(),
                });
            }
            match counter.compare_exchange_weak(
                current,
                current + 1,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return Ok(counter),
                Err(actual) => current = actual,
            }
        }
    }

    /// Enqueues `payload` if the topic has room, returning
    /// [`BackpressureError::LimitReached`] otherwise.
    ///
    /// The slot is released again if the queue itself rejects the job, so a
    /// failing backend cannot leak capacity.
    pub async fn try_enqueue<Q: Queue + ?Sized>(
        &self,
        queue: &Q,
        topic: &str,
        payload: Vec<u8>,
    ) -> Result<(), BackpressureError> {
        let counter = self.take_slot(topic)?;
        match queue.enqueue(topic, payload).await {
            Ok(()) => Ok(()),
            Err(e) => {
                counter.fetch_sub(1, Ordering::AcqRel);
                Err(BackpressureError::Enqueue(e))
            }
        }
    }

    /// Returns one slot for `topic` after a job is acked or nacked.
    ///
    /// Saturates at zero, so an unmatched call cannot wrap the counter and hand
    /// the topic unlimited capacity.
    pub fn dec_finished(&self, topic: &str) {
        let counter = self.counter(topic);
        let mut current = counter.load(Ordering::Acquire);
        while current > 0 {
            match counter.compare_exchange_weak(
                current,
                current - 1,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return,
                Err(actual) => current = actual,
            }
        }
    }

    /// Slots currently held for `topic`, i.e. un-acked jobs.
    pub fn inflight(&self, topic: &str) -> u64 {
        self.counter(topic).load(Ordering::Acquire)
    }
}

/// Enqueues every payload, counting the ones rejected for lack of room.
///
/// A queue failure is returned rather than counted: it is not backpressure, and
/// continuing past it would silently drop the rest of the batch.
pub async fn enqueue_with_backpressure<Q: Queue + ?Sized>(
    enforcer: &BackpressureEnforcer,
    queue: &Q,
    topic: &str,
    payloads: Vec<Vec<u8>>,
) -> Result<usize, BackpressureError> {
    let mut rejected = 0;
    for payload in payloads {
        match enforcer.try_enqueue(queue, topic, payload).await {
            Ok(()) => {}
            Err(BackpressureError::LimitReached { .. }) => rejected += 1,
            Err(e) => return Err(e),
        }
    }
    Ok(rejected)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::in_memory::InMemoryQueue;

    #[tokio::test]
    async fn try_enqueue_within_limit_succeeds() {
        let reg = BackpressureEnforcer::new(2);
        let queue = InMemoryQueue::new();
        assert!(reg.try_enqueue(&queue, "t", b"x".to_vec()).await.is_ok());
        assert_eq!(reg.inflight("t"), 1);
    }

    #[tokio::test]
    async fn try_enqueue_rejects_when_full() {
        let reg = BackpressureEnforcer::new(1);
        let queue = InMemoryQueue::new();

        reg.try_enqueue(&queue, "t", b"x".to_vec()).await.unwrap();
        assert!(matches!(
            reg.try_enqueue(&queue, "t", b"y".to_vec()).await,
            Err(BackpressureError::LimitReached { .. })
        ));
    }

    #[tokio::test]
    async fn finished_jobs_release_capacity() {
        let reg = BackpressureEnforcer::new(1);
        let queue = InMemoryQueue::new();

        reg.try_enqueue(&queue, "t", b"x".to_vec()).await.unwrap();
        reg.dec_finished("t");
        assert_eq!(reg.inflight("t"), 0);
        assert!(reg.try_enqueue(&queue, "t", b"y".to_vec()).await.is_ok());
    }

    #[tokio::test]
    async fn topics_are_capped_independently() {
        let reg = BackpressureEnforcer::new(1);
        let queue = InMemoryQueue::new();

        reg.try_enqueue(&queue, "a", b"x".to_vec()).await.unwrap();
        assert!(reg.try_enqueue(&queue, "b", b"y".to_vec()).await.is_ok());
    }

    #[tokio::test]
    async fn unmatched_finish_does_not_wrap_the_counter() {
        let reg = BackpressureEnforcer::new(1);
        reg.dec_finished("t");
        reg.dec_finished("t");
        assert_eq!(reg.inflight("t"), 0);
    }
}
