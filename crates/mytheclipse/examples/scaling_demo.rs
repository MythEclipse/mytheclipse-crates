//! Demonstrates the scaling model: bounded concurrency, no over-allocation.
//!
//! Run: `cargo run -p mytheclipse --features full --example scaling_demo`
//!
//! Shows both modes:
//! 1. Explicit concurrency (`4`) — never more than 4 futures in flight.
//! 2. Auto concurrency (`()`) — sized from host CPU (`available_parallelism`),
//!    still bounded (it never spawns one task per item).

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use mytheclipse::parallel_map::{parallel_for_each, ParallelConcurrency};

#[tokio::main]
async fn main() {
    let total = 100u32;
    println!(
        "host available_parallelism = {}",
        <() as ParallelConcurrency>::resolve(())
    );
    println!("total items = {total}");
    println!();

    run("explicit concurrency=4", 4, total).await;
    println!();
    run("auto           concurrency=()", (), total).await;
}

async fn run(label: &str, concurrency: impl ParallelConcurrency + Copy, total: u32) {
    let in_flight = Arc::new(AtomicUsize::new(0));
    let peak = Arc::new(AtomicUsize::new(0));
    let resolved = concurrency.resolve();

    let t = Arc::clone(&in_flight);
    let p = Arc::clone(&peak);
    let start = Instant::now();
    parallel_for_each(0..total, concurrency, move |_| {
        let t = Arc::clone(&t);
        let p = Arc::clone(&p);
        async move {
            let now = t.fetch_add(1, Ordering::SeqCst) + 1;
            p.fetch_max(now, Ordering::SeqCst);
            tokio::time::sleep(Duration::from_millis(1)).await;
            t.fetch_sub(1, Ordering::SeqCst);
            Ok::<_, std::io::Error>(())
        }
    })
    .await
    .unwrap();
    let elapsed = start.elapsed();

    println!("{label}  resolved={resolved}");
    println!(
        "  peak in-flight = {} (bounded, never {total})",
        peak.load(Ordering::SeqCst)
    );
    println!("  elapsed        = {elapsed:?}  (sequential ~{}ms)", total);
}
