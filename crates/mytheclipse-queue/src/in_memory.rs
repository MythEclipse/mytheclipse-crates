//! In-process job queue using `tokio::sync::Mutex` + `Notify`.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use tokio::sync::{Mutex, Notify};

use crate::error::{JobError, QueueError};
use crate::job::{Job, JobId};
use crate::traits::Queue;

struct TopicQueue {
    jobs: Mutex<Vec<Job>>,
    notify: Notify,
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

struct Inner {
    topics: std::collections::HashMap<String, Arc<TopicQueue>>,
}

impl std::fmt::Debug for Inner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let keys: Vec<&String> = self.topics.keys().collect();
        f.debug_struct("Inner").field("topics", &keys).finish()
    }
}

/// An in-memory queue. Each topic is a shared mutex-protected vector + Notify.
#[derive(Clone)]
pub struct InMemoryQueue {
    inner: Arc<Mutex<Inner>>,
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
            inner: Arc::new(Mutex::new(Inner {
                topics: std::collections::HashMap::new(),
            })),
        }
    }

    async fn get_topic(&self, topic: &str) -> Arc<TopicQueue> {
        let mut inner = self.inner.lock().await;
        inner
            .topics
            .entry(topic.to_string())
            .or_insert_with(|| {
                Arc::new(TopicQueue {
                    jobs: Mutex::new(Vec::new()),
                    notify: Notify::new(),
                })
            })
            .clone()
    }
}

#[async_trait]
impl Queue for InMemoryQueue {
    async fn enqueue(&self, topic: &str, payload: Vec<u8>) -> Result<(), QueueError> {
        let tq = self.get_topic(topic).await;
        tq.jobs
            .lock()
            .await
            .push(Job::new(JobId::generate(), topic, payload));
        tq.notify.notify_one();
        Ok(())
    }

    async fn dequeue(&self, topic: &str, timeout: Duration) -> Result<Option<Job>, QueueError> {
        let tq = self.get_topic(topic).await;
        let tq2 = Arc::clone(&tq);
        loop {
            if let Some(job) = tq.jobs.lock().await.pop() {
                return Ok(Some(job));
            }
            tokio::select! {
                _ = tq2.notify.notified() => {}
                _ = tokio::time::sleep(timeout) => {
                    if let Some(job) = tq.jobs.lock().await.pop() {
                        return Ok(Some(job));
                    }
                    return Ok(None);
                }
            }
        }
    }

    async fn ack(&self, _job: &Job) -> Result<(), JobError> {
        Ok(())
    }

    async fn nack(&self, job: &Job, requeue: bool) -> Result<(), JobError> {
        if requeue {
            let tq = self.get_topic(&job.topic).await;
            // Requeue the *same* job (same id) with its attempt count advanced:
            // a fresh `Job::new` would reset the counter and make the worker's
            // retry budget unenforceable (endless redelivery, never a DLQ).
            let retried = Job {
                attempt: job.attempt + 1,
                delivered_at: Some(std::time::SystemTime::now()),
                ..job.clone()
            };
            tq.jobs.lock().await.push(retried);
            tq.notify.notify_one();
        }
        Ok(())
    }

    async fn dlq_move(&self, topic: &str, job: Job) -> Result<(), QueueError> {
        let dlq_topic = format!("dlq:{topic}");
        let tq = self.get_topic(&dlq_topic).await;
        tq.jobs.lock().await.push(job);
        tq.notify.notify_one();
        Ok(())
    }

    async fn len(&self, topic: &str) -> Result<u64, QueueError> {
        let tq = self.get_topic(topic).await;
        let guard = tq.jobs.lock().await;
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
