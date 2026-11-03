//! Criterion benchmarks for the in-process cache.
//!
//! Run: `cargo bench -p mytheclipse-cache --bench cache`
//!
//! The interesting numbers are `get_hit` (the path every read-through takes)
//! and `set_at_capacity_evict` (the path that decides whether a full cache
//! stays O(1) or degrades to a scan).

use std::sync::atomic::{AtomicU64, Ordering};

use criterion::{black_box, criterion_group, criterion_main, Criterion};
use tokio::runtime::Runtime;

use mytheclipse_cache::{Cache, MemoryCache};

fn rt() -> Runtime {
    Runtime::new().expect("tokio runtime")
}

fn bench_memory_cache(c: &mut Criterion) {
    let rt = rt();
    let mut g = c.benchmark_group("memory_cache");
    let value = vec![0u8; 64];

    let cache = MemoryCache::new();
    rt.block_on(async {
        cache.set("hot", value.clone(), None).await.unwrap();
    });

    g.bench_function("get_hit", |b| {
        b.to_async(&rt).iter(|| async {
            black_box(cache.get("hot").await.unwrap());
        });
    });

    g.bench_function("get_miss", |b| {
        b.to_async(&rt).iter(|| async {
            black_box(cache.get("absent").await.unwrap());
        });
    });

    g.bench_function("set_overwrite", |b| {
        b.to_async(&rt).iter(|| async {
            cache.set("hot", value.clone(), None).await.unwrap();
        });
    });

    g.bench_function("set_then_invalidate", |b| {
        b.to_async(&rt).iter(|| async {
            cache.set("churn", value.clone(), None).await.unwrap();
            cache.invalidate("churn").await.unwrap();
        });
    });

    // A full cache: every `set` of a new key must evict one entry. If eviction
    // is a scan, this grows with capacity; it must stay flat.
    let bounded = MemoryCache::new().with_max_entries(1024);
    rt.block_on(async {
        for i in 0..1024 {
            bounded
                .set(&format!("fill_{i}"), value.clone(), None)
                .await
                .unwrap();
        }
    });
    let next = AtomicU64::new(0);
    g.bench_function("set_at_capacity_evict", |b| {
        b.to_async(&rt).iter(|| async {
            let i = next.fetch_add(1, Ordering::Relaxed);
            bounded
                .set(&format!("evict_{i}"), value.clone(), None)
                .await
                .unwrap();
        });
    });

    g.finish();
}

criterion_group!(benches, bench_memory_cache);
criterion_main!(benches);
