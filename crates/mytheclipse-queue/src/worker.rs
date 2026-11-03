//! Worker pool for processing queued jobs with retry, backoff, and graceful shutdown.

use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::Semaphore;

use crate::error::JobError;
use crate::job::Job;
use crate::traits::Queue;

/// A future returned by a job handler.
pub type JobFuture = Pin<Box<dyn std::future::Future<Output = Result<(), JobError>> + Send>>;

/// A handler for processing a single job.
pub trait JobHandler: Send + Sync {
    fn handle(&self, job: Job) -> JobFuture;
}

impl<F, Fut> JobHandler for F
where
    F: Fn(Job) -> Fut + Send + Sync,
    Fut: std::future::Future<Output = Result<(), JobError>> + Send + 'static,
{
    fn handle(&self, job: Job) -> JobFuture {
        Box::pin((self)(job))
    }
}

/// Configuration for the worker pool.
#[derive(Debug, Clone)]
pub struct WorkerConfig {
    /// Maximum concurrent job handlers.
    pub concurrency: usize,
    /// Maximum number of retry attempts (0 = no retries).
    pub max_retries: u32,
    /// Base delay for exponential backoff between retries.
    pub retry_base_delay: Duration,
    /// Maximum delay cap for retry backoff.
    pub retry_max_delay: Duration,
    /// Backoff multiplier.
    pub retry_factor: f64,
    /// Visibility timeout for in-progress jobs.
    pub visibility_timeout: Duration,
    /// Polling interval when a queue is empty.
    pub poll_interval: Duration,
}

impl Default for WorkerConfig {
    fn default() -> Self {
        Self {
            concurrency: 4,
            max_retries: 3,
            retry_base_delay: Duration::from_millis(500),
            retry_max_delay: Duration::from_secs(10),
            retry_factor: 2.0,
            visibility_timeout: Duration::from_secs(30),
            poll_interval: Duration::from_millis(100),
        }
    }
}

/// A pool of workers consuming jobs from a `Queue`.
pub struct WorkerPool<Q: Queue + 'static> {
    queue: Arc<Q>,
    config: WorkerConfig,
    semaphore: Arc<Semaphore>,
}

impl<Q: Queue + 'static> WorkerPool<Q> {
    /// Creates a new worker pool with the given concurrency.
    pub fn new(queue: Q, concurrency: usize) -> Self {
        Self::with_config(
            queue,
            WorkerConfig {
                concurrency,
                ..Default::default()
            },
        )
    }

    /// Creates a new worker pool with explicit configuration.
    pub fn with_config(queue: Q, config: WorkerConfig) -> Self {
        let sem = Arc::new(Semaphore::new(config.concurrency.max(1)));
        Self {
            queue: Arc::new(queue),
            config,
            semaphore: sem,
        }
    }

    /// Starts `concurrency` workers consuming from `topic`.
    pub fn start<H>(&self, topic: &str, handler: H)
    where
        H: JobHandler + 'static,
    {
        let queue = Arc::clone(&self.queue);
        let config = self.config.clone();
        let semaphore = Arc::clone(&self.semaphore);
        let handler: Arc<dyn JobHandler> = Arc::new(handler);
        let topic_owned = topic.to_string();

        for _ in 0..config.concurrency {
            let q = Arc::clone(&queue);
            let sem = Arc::clone(&semaphore);
            let h = Arc::clone(&handler);
            let cfg = config.clone();
            let topic_inner = topic_owned.clone();

            tokio::spawn(async move {
                loop {
                    // Take a concurrency slot *before* pulling work, then hand
                    // it to the job task: the permit must live as long as the
                    // handler runs, otherwise the cap never binds.
                    let permit = match sem.clone().acquire_owned().await {
                        Ok(permit) => permit,
                        Err(_) => break, // pool shutting down
                    };
                    match q.dequeue(&topic_inner, cfg.poll_interval).await {
                        Ok(Some(job)) => {
                            let q2 = Arc::clone(&q);
                            let h2 = Arc::clone(&h);
                            let cfg2 = cfg.clone();
                            let t2 = topic_inner.clone();
                            let j2 = job.clone();

                            tokio::spawn(async move {
                                let _permit = permit;
                                let fut = h2.handle(j2.clone());
                                match fut.await {
                                    Ok(()) => {
                                        let _ = q2.ack(&j2).await;
                                    }
                                    Err(e) => {
                                        if j2.attempt < cfg2.max_retries {
                                            let delay = retry_delay(&cfg2, j2.attempt);
                                            tracing::warn!(
                                                job = %j2.id,
                                                attempt = j2.attempt + 1,
                                                max_retries = cfg2.max_retries,
                                                delay_ms = delay.as_millis() as u64,
                                                error = %e,
                                                "job failed; retrying after backoff"
                                            );
                                            tokio::time::sleep(delay).await;
                                            let _ = q2.nack(&j2, true).await;
                                        } else {
                                            let _ = q2.dlq_move(&t2, j2.clone()).await;
                                            let _ = q2.nack(&j2, false).await;
                                        }
                                    }
                                }
                            });
                        }
                        Ok(None) => {}
                        Err(e) => {
                            tracing::error!("queue error: {e}");
                            tokio::time::sleep(cfg.poll_interval).await;
                        }
                    }
                }
            });
        }
    }
}

/// Computes the (capped) exponential backoff delay.
pub fn retry_delay(config: &WorkerConfig, attempt: u32) -> Duration {
    let exponent = attempt as f64;
    let computed =
        config.retry_base_delay.as_millis() as f64 * config.retry_factor.powf(exponent.max(0.0));
    let capped = computed.min(config.retry_max_delay.as_millis() as f64);
    Duration::from_millis(capped as u64)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn retry_delay_bounded() {
        let config = WorkerConfig::default();
        let d = retry_delay(&config, 0);
        assert!(d <= config.retry_max_delay);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn worker_respects_configured_concurrency() {
        use crate::in_memory::InMemoryQueue;
        use std::sync::atomic::{AtomicUsize, Ordering};

        let queue = InMemoryQueue::new();
        for i in 0..6u8 {
            queue.enqueue("t", vec![i]).await.unwrap();
        }

        let in_flight = Arc::new(AtomicUsize::new(0));
        let peak = Arc::new(AtomicUsize::new(0));
        let pool = WorkerPool::new(queue, 2);

        let (live, high) = (Arc::clone(&in_flight), Arc::clone(&peak));
        pool.start("t", move |_job: Job| {
            let live = Arc::clone(&live);
            let high = Arc::clone(&high);
            async move {
                let now = live.fetch_add(1, Ordering::SeqCst) + 1;
                high.fetch_max(now, Ordering::SeqCst);
                tokio::time::sleep(Duration::from_millis(20)).await;
                live.fetch_sub(1, Ordering::SeqCst);
                Ok(())
            }
        });

        tokio::time::sleep(Duration::from_millis(400)).await;
        let peak = peak.load(Ordering::SeqCst);
        assert!(peak <= 2, "handlers ran {peak} at a time, cap was 2");
        assert_eq!(in_flight.load(Ordering::SeqCst), 0);
    }
}
