//! In-process job queue: a topic map guarded by an `RwLock` plus one
//! `Mutex<Vec<Job>>` + `Notify` per topic.
//!
//! Both locks are synchronous. Neither is ever held across an `.await`, so
//! there is nothing for an async mutex to buy here — and a `std` mutex avoids
//! the waker-list bookkeeping an async one pays on every acquisition. Topic
//! lookup takes the map's read lock, so producers and consumers on the same
//! topic do not serialize on a global lock.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, RwLock};
use std::time::Duration;

use async_trait::async_trait;
use tokio::sync::Notify;

use crate::error::{JobError, QueueError};
use crate::job::{Job, JobId};
use crate::traits::Queue;

struct TopicQueue {
    /// Jobs waiting to be delivered. Never locked across an `.await`.
    jobs: Mutex<Vec<Job>>,
    notify: Notify,
}

impl TopicQueue {
    fn new() -> Self {
        Self {
            jobs: Mutex::new(Vec::new()),
            notify: Notify::new(),
        }
    }

    /// Pushes `job` and wakes one waiting consumer.
    fn push(&self, job: Job) {
        self.jobs.lock().expect("job queue poisoned").push(job);
        self.notify.notify_one();
    }
}

impl std::fmt::Debug for TopicQueue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TopicQueue")
            .field(
                "jobs_len",
                &self.jobs.try_lock().map(|j| j.len()).unwrap_or(0),
            )
            .finish()
    }
}

/// An in-memory queue. Each topic is a shared mutex-protected vector + Notify.
#[derive(Clone)]
pub struct InMemoryQueue {
    topics: Arc<RwLock<HashMap<String, Arc<TopicQueue>>>>,
}

impl std::fmt::Debug for InMemoryQueue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("InMemoryQueue").finish()
    }
}

impl Default for InMemoryQueue {
    fn default() -> Self {
        Self::new()
    }
}

impl InMemoryQueue {
    pub fn new() -> Self {
        Self {
            topics: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// The queue for `topic`, created on first use. Existing topics are served
    /// from the read lock and without allocating a key.
    fn topic(&self, topic: &str) -> Arc<TopicQueue> {
        let existing = self.topics.read().unwrap().get(topic).cloned();
        if let Some(tq) = existing {
            return tq;
        }
        let mut topics = self.topics.write().unwrap();
        Arc::clone(
            topics
                .entry(topic.to_owned())
                .or_insert_with(|| Arc::new(TopicQueue::new())),
        )
    }
}

#[async_trait]
impl Queue for InMemoryQueue {
    async fn enqueue(&self, topic: &str, payload: Vec<u8>) -> Result<(), QueueError> {
        let job = Job::new(JobId::generate(), topic, payload);
        self.topic(topic).push(job);
        Ok(())
    }

    async fn dequeue(&self, topic: &str, timeout: Duration) -> Result<Option<Job>, QueueError> {
        let tq = self.topic(topic);
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            if let Some(job) = tq.jobs.lock().expect("job queue poisoned").pop() {
                return Ok(Some(job));
            }
            // One deadline for the whole call, so a spurious wake-up does not
            // restart the wait.
            tokio::select! {
                _ = tq.notify.notified() => {}
                _ = tokio::time::sleep_until(deadline) => {
                    return Ok(tq.jobs.lock().expect("job queue poisoned").pop());
                }
            }
        }
    }

    async fn ack(&self, _job: &Job) -> Result<(), JobError> {
        // Dequeue already removed the job, so there is nothing left to
        // acknowledge; the in-flight copy is the handler's own.
        Ok(())
    }

    async fn nack(&self, job: &Job, requeue: bool) -> Result<(), JobError> {
        if requeue {
            let tq = self.topic(&job.topic);
            // Requeue the *same* job (same id) with its attempt count advanced:
            // a fresh `Job::new` would reset the counter and make the worker's
            // retry budget unenforceable (endless redelivery, never a DLQ).
            let retried = Job {
                attempt: job.attempt + 1,
                delivered_at: Some(std::time::SystemTime::now()),
                ..job.clone()
            };
            tq.push(retried);
        }
        Ok(())
    }

    async fn dlq_move(&self, topic: &str, job: Job) -> Result<(), QueueError> {
        let dlq_topic = format!("dlq:{topic}");
        self.topic(&dlq_topic).push(job);
        Ok(())
    }

    async fn len(&self, topic: &str) -> Result<u64, QueueError> {
        let tq = self.topic(topic);
        let guard = tq.jobs.lock().expect("job queue poisoned");
        Ok(guard.len() as u64)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn enqueue_dequeue_roundtrip() {
        let q = InMemoryQueue::new();
        q.enqueue("test", b"hello".to_vec()).await.unwrap();
        let job = q
            .dequeue("test", Duration::from_millis(500))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(job.payload, b"hello");
        assert_eq!(job.topic, "test");
    }

    #[tokio::test]
    async fn empty_returns_none() {
        let q = InMemoryQueue::new();
        let result = q.dequeue("none", Duration::from_millis(50)).await.unwrap();
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn len_tracks_jobs() {
        let q = InMemoryQueue::new();
        q.enqueue("t", vec![1]).await.unwrap();
        q.enqueue("t", vec![2]).await.unwrap();
        assert_eq!(q.len("t").await.unwrap(), 2);
    }

    #[tokio::test]
    async fn requeue_advances_attempt_on_the_same_job() {
        let q = InMemoryQueue::new();
        q.enqueue("t", vec![1]).await.unwrap();
        let job = q
            .dequeue("t", Duration::from_millis(50))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(job.attempt, 0);

        q.nack(&job, true).await.unwrap();
        let again = q
            .dequeue("t", Duration::from_millis(50))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(again.id, job.id, "retry must keep the job identity");
        assert_eq!(again.attempt, 1, "retry must advance the attempt counter");
    }
}
