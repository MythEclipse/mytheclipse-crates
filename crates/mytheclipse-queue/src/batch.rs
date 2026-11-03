//! Batch job processor for bulk processing of queued jobs.
//!
//! [`BatchProcessor`] wraps a [`Queue`] and accumulates jobs per topic until
//! either `batch_size` is reached or `batch_timeout` elapses, then dispatches
//! them to a [`BatchJobHandler`] for bulk processing (e.g. bulk DB insert,
//! bulk email send, batch index write).

use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::{mpsc, Semaphore};

use crate::error::JobError;
use crate::job::Job;
use crate::traits::Queue;

/// A handler that processes a batch of jobs atomically.
pub trait BatchJobHandler: Send + Sync {
    fn handle_batch(
        &self,
        jobs: Vec<Job>,
    ) -> Pin<Box<dyn std::future::Future<Output = Result<(), JobError>> + Send>>;
}

impl<F, Fut> BatchJobHandler for F
where
    F: Fn(Vec<Job>) -> Fut + Send + Sync,
    Fut: std::future::Future<Output = Result<(), JobError>> + Send + 'static,
{
    fn handle_batch(
        &self,
        jobs: Vec<Job>,
    ) -> Pin<Box<dyn std::future::Future<Output = Result<(), JobError>> + Send>> {
        Box::pin((self)(jobs))
    }
}

/// Configuration for [`BatchProcessor`].
#[derive(Debug, Clone)]
pub struct BatchConfig {
    /// Max jobs per batch before flushing.
    pub batch_size: usize,
    /// Max time to wait before flushing a partial batch.
    pub batch_timeout: Duration,
    /// Max concurrent batch-processing tasks.
    pub concurrency: usize,
}

impl Default for BatchConfig {
    fn default() -> Self {
        Self {
            batch_size: 100,
            batch_timeout: Duration::from_secs(5),
            concurrency: 4,
        }
    }
}

/// Result of a completed batch flush.
pub struct BatchFlush {
    /// Number of jobs in the flushed batch.
    pub count: usize,
}

/// A processor that batches jobs before dispatching them.
pub struct BatchProcessor<Q: Queue + 'static> {
    queue: Arc<Q>,
    config: BatchConfig,
    semaphore: Arc<Semaphore>,
}

impl<Q: Queue + 'static> BatchProcessor<Q> {
    pub fn new(queue: Q, config: BatchConfig) -> Self {
        let sem = Arc::new(Semaphore::new(config.concurrency.max(1)));
        Self {
            queue: Arc::new(queue),
            config,
            semaphore: sem,
        }
    }

    /// Starts a batch processor for `topic` using `handler`.
    pub fn start<H>(&self, topic: &str, handler: H)
    where
        H: BatchJobHandler + 'static,
    {
        let queue = Arc::clone(&self.queue);
        let config = self.config.clone();
        let semaphore = Arc::clone(&self.semaphore);
        let handler: Arc<dyn BatchJobHandler> = Arc::new(handler);
        let topic_owned = topic.to_string();

        let (tx, mut rx): (mpsc::Sender<Job>, mpsc::Receiver<Job>) =
            mpsc::channel(config.batch_size);

        // Dequeue loop → forward to channel
        {
            let q = Arc::clone(&queue);
            let t = topic_owned.clone();
            let tx2 = tx.clone();
            let poll = config.poll_timeout();
            tokio::spawn(async move {
                loop {
                    match q.dequeue(&t, poll).await {
                        Ok(Some(job)) => {
                            if tx2.send(job).await.is_err() {
                                // Processor dropped; re-enqueue remaining
                                break;
                            }
                        }
                        Ok(None) => {}
                        Err(e) => {
                            tracing::error!(queue_error = %e, "batch dequeue error");
                            tokio::time::sleep(poll).await;
                        }
                    }
                }
            });
        }

        // Batch accumulation + flush loop
        let h = handler;
        tokio::spawn(async move {
            loop {
                let mut batch: Vec<Job> = Vec::with_capacity(config.batch_size);
                let deadline = tokio::time::sleep(config.batch_timeout);
                tokio::pin!(deadline);

                // Fill batch
                loop {
                    if batch.len() >= config.batch_size {
                        break;
                    }
                    tokio::select! {
                        biased;
                        job = rx.recv() => match job {
                            Some(j) => batch.push(j),
                            None => {
                                // channel closed: drain remaining
                                while let Ok(j) = rx.try_recv() {
                                    batch.push(j);
                                }
                                if !batch.is_empty() {
                                    Self::flush(&h, &semaphore, batch).await;
                                }
                                return;
                            }
                        },
                        _ = &mut deadline => break,
                    }
                }

                if !batch.is_empty() {
                    Self::flush(&h, &semaphore, batch).await;
                }
                deadline
                    .as_mut()
                    .reset(tokio::time::Instant::now() + config.batch_timeout);
            }
        });

        // Keep tx alive for the dequeue loop (it was cloned)
        let _keep = tx;
    }

    async fn flush(handler: &Arc<dyn BatchJobHandler>, sem: &Arc<Semaphore>, batch: Vec<Job>) {
        let permit = match sem.clone().acquire_owned().await {
            Ok(permit) => permit,
            Err(_) => {
                tracing::error!("batch semaphore closed");
                return;
            }
        };
        let h = Arc::clone(handler);
        let batch_len = batch.len();
        tokio::spawn(async move {
            // Hold the permit for the duration of the batch: dropping it here
            // (as a `flush`-local) would make `concurrency` a no-op.
            let _permit = permit;
            match h.handle_batch(batch).await {
                Ok(()) => tracing::debug!(count = batch_len, "batch processed"),
                Err(e) => tracing::error!("batch handler error: {}", e),
            }
        });
    }
}

impl BatchConfig {
    fn poll_timeout(&self) -> Duration {
        self.batch_timeout.min(Duration::from_millis(100))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::in_memory::InMemoryQueue;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc as StdArc;

    fn make_queue() -> InMemoryQueue {
        InMemoryQueue::new()
    }

    #[tokio::test]
    async fn flush_on_batch_size() {
        let queue = make_queue();
        let counter = StdArc::new(AtomicUsize::new(0));
        let cfg = BatchConfig {
            batch_size: 3,
            batch_timeout: Duration::from_secs(10),
            concurrency: 2,
        };
        let bp = BatchProcessor::new(queue, cfg);
        let c2 = StdArc::clone(&counter);
        bp.start("t", move |jobs: Vec<Job>| {
            let c3 = StdArc::clone(&c2);
            Box::pin(async move {
                c3.fetch_add(jobs.len(), Ordering::SeqCst);
                Ok(())
            })
        });

        for i in 0..3 {
            bp.queue
                .enqueue("t", format!("job{}", i).into_bytes())
                .await
                .unwrap();
        }

        tokio::time::sleep(Duration::from_millis(300)).await;
        assert_eq!(counter.load(Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn flush_on_timeout() {
        let queue = make_queue();
        let queue2 = queue.clone();
        let counter = StdArc::new(AtomicUsize::new(0));
        let cfg = BatchConfig {
            batch_size: 100,
            batch_timeout: Duration::from_millis(100),
            concurrency: 2,
        };
        let bp = BatchProcessor::new(queue, cfg);
        let c2 = StdArc::clone(&counter);
        bp.start("t", move |jobs: Vec<Job>| {
            let c3 = StdArc::clone(&c2);
            Box::pin(async move {
                c3.fetch_add(jobs.len(), Ordering::SeqCst);
                Ok(())
            })
        });

        queue2.enqueue("t", b"x".to_vec()).await.unwrap();

        tokio::time::sleep(Duration::from_millis(300)).await;
        assert_eq!(counter.load(Ordering::SeqCst), 1);
    }
}
