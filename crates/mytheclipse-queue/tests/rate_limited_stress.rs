//! Race-safety stress tests for the queue crate's rate-limited primitives.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use mytheclipse_queue::in_memory::InMemoryQueue;
use mytheclipse_queue::rate_limited::RateLimitedQueue;
use mytheclipse_queue::traits::Queue;

const TASKS: usize = 8;
const PER_TASK: usize = 1000;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rate_limited_queue_no_item_loss_under_contention() {
    // Enqueue 8000 items, dequeue them all concurrently: nothing may vanish.
    let inner = InMemoryQueue::new();
    let q = Arc::new(RateLimitedQueue::new(inner, 1_000_000.0, 10_000));

    let producer: tokio::task::JoinHandle<()> = tokio::spawn({
        let q = Arc::clone(&q);
        async move {
            for i in 0..(TASKS * PER_TASK) {
                q.enqueue("stress", format!("item-{i}").into_bytes())
                    .await
                    .unwrap();
            }
        }
    });

    // Drain concurrently with the producer.
    let seen = Arc::new(AtomicUsize::new(0));
    let mut workers = Vec::new();
    for _ in 0..TASKS {
        let q = Arc::clone(&q);
        let s = Arc::clone(&seen);
        workers.push(tokio::spawn(async move {
            while let Some(job) = q
                .dequeue("stress", Duration::from_millis(50))
                .await
                .unwrap()
            {
                let _ = String::from_utf8(job.payload).unwrap();
                s.fetch_add(1, Ordering::SeqCst);
            }
        }));
    }

    producer.await.unwrap();
    for w in workers {
        w.await.unwrap();
    }

    // Slight subtlety: a worker may observe None while producer still has
    // items in flight (producer finished above, but enqueue is async —
    // actually producer.await guarantees all enqueues completed). Since the
    // producer finished, any None means truly drained.
    assert_eq!(
        seen.load(Ordering::SeqCst),
        TASKS * PER_TASK,
        "items lost under contention"
    );
}
