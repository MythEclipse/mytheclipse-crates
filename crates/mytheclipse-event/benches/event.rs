//! Criterion benchmarks for the in-memory event bus.
//!
//! Run: `cargo bench -p mytheclipse-event --bench event`
//!
//! `publish` is the per-message cost every producer pays, so the topic lookup
//! on that path must stay allocation-free and must not serialize producers
//! behind one exclusive lock.

use criterion::{black_box, criterion_group, criterion_main, Criterion};
use tokio::runtime::Runtime;

use mytheclipse_event::{EventBus, InMemoryEventBus};

fn rt() -> Runtime {
    Runtime::new().expect("tokio runtime")
}

fn bench_publish(c: &mut Criterion) {
    let rt = rt();
    let mut g = c.benchmark_group("in_memory_bus");
    let payload = vec![0u8; 64];

    // A live subscriber exists, so the send actually writes into a channel
    // ring buffer and wakes a receiver.
    let bus = InMemoryEventBus::default();
    let sub = rt.block_on(async { bus.subscribe("hot").await.unwrap() });
    g.bench_function("publish_with_subscriber", |b| {
        b.to_async(&rt).iter(|| async {
            bus.publish("hot", payload.clone()).await.unwrap();
        });
    });
    drop(sub);

    let lonely = InMemoryEventBus::default();
    rt.block_on(async { lonely.publish("hot", payload.clone()).await.unwrap() });
    g.bench_function("publish_no_subscriber", |b| {
        b.to_async(&rt).iter(|| async {
            lonely.publish("hot", payload.clone()).await.unwrap();
        });
    });

    // Many topics: publishing must not degrade as the topic map grows.
    let wide = InMemoryEventBus::default();
    let topics: Vec<String> = (0..1000).map(|i| format!("topic_{i}")).collect();
    let subs: Vec<_> = rt.block_on(async {
        let mut subs = Vec::new();
        for topic in &topics {
            subs.push(wide.subscribe(topic).await.unwrap());
        }
        subs
    });
    let next = std::sync::atomic::AtomicUsize::new(0);
    g.bench_function("publish_1000_topics", |b| {
        b.to_async(&rt).iter(|| async {
            let i = next.fetch_add(1, std::sync::atomic::Ordering::Relaxed) % topics.len();
            wide.publish(&topics[i], payload.clone()).await.unwrap();
        });
    });
    black_box(&subs);
    g.finish();
}

criterion_group!(benches, bench_publish);
criterion_main!(benches);
