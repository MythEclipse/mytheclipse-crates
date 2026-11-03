//! Criterion benchmarks for the in-memory queue.
//!
//! Run: `cargo bench -p mytheclipse-queue --bench queue`
//!
//! The numbers that matter are `enqueue` (id generation + topic lookup + push)
//! and `enqueue_dequeue` (one full job cycle, which is what a worker loop pays
//! per job). `len` isolates the topic-lookup + lock cost that every operation
//! shares.

use std::time::Duration;

use criterion::{black_box, criterion_group, criterion_main, BatchSize, Criterion};
use tokio::runtime::Runtime;

use mytheclipse_queue::{InMemoryQueue, JobId, Queue};

fn rt() -> Runtime {
    Runtime::new().expect("tokio runtime")
}

fn bench_job_id(c: &mut Criterion) {
    c.bench_function("job_id/generate", |b| {
        b.iter(|| black_box(JobId::generate()));
    });
}

fn bench_in_memory_queue(c: &mut Criterion) {
    let rt = rt();
    let mut g = c.benchmark_group("in_memory_queue");
    let payload = vec![0u8; 64];

    // Enqueue measured alone: the queue is created per iteration as setup, so
    // the queue stays empty. Enqueueing into one growing queue would measure
    // the allocator (and eventually memory pressure), not the queue.
    g.bench_function("enqueue", |b| {
        b.to_async(&rt).iter_batched(
            InMemoryQueue::new,
            |q| {
                let payload = payload.clone();
                async move {
                    q.enqueue("hot", payload).await.unwrap();
                    black_box(q);
                }
            },
            BatchSize::SmallInput,
        );
    });

    // One full job cycle, on a topic that already exists: enqueue + dequeue.
    let hot = InMemoryQueue::new();
    rt.block_on(async { hot.enqueue("hot", payload.clone()).await.unwrap() });
    g.bench_function("enqueue_dequeue", |b| {
        b.to_async(&rt).iter(|| async {
            hot.enqueue("hot", payload.clone()).await.unwrap();
            black_box(hot.dequeue("hot", Duration::from_millis(1)).await.unwrap());
        });
    });

    g.bench_function("len", |b| {
        b.to_async(&rt).iter(|| async {
            black_box(hot.len("hot").await.unwrap());
        });
    });

    // Many topics: the topic lookup must not degrade as the map grows. Each
    // iteration drains what it pushed, so the queue stays at a steady size.
    let wide = InMemoryQueue::new();
    let topics: Vec<String> = (0..1000).map(|i| format!("topic_{i}")).collect();
    rt.block_on(async {
        for topic in &topics {
            wide.enqueue(topic, payload.clone()).await.unwrap();
            wide.dequeue(topic, Duration::from_millis(1)).await.unwrap();
        }
    });
    let next = std::sync::atomic::AtomicUsize::new(0);
    g.bench_function("enqueue_1000_topics", |b| {
        b.to_async(&rt).iter(|| async {
            let i = next.fetch_add(1, std::sync::atomic::Ordering::Relaxed) % topics.len();
            wide.enqueue(&topics[i], payload.clone()).await.unwrap();
            wide.dequeue(&topics[i], Duration::from_millis(1))
                .await
                .unwrap();
        });
    });
    g.finish();
}

criterion_group!(benches, bench_job_id, bench_in_memory_queue);
criterion_main!(benches);
