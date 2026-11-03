# mytheclipse-queue

A unified job queue abstraction so your background work isn't locked to one
transport. Provides a single `Queue` trait, `Job` type, and `WorkerPool` executor
with configurable retry/backoff, concurrency, and a dead-letter queue.

Today there is **one backend: in-memory** (`tokio::sync::mpsc` + task spawning,
no external service). `Queue` is a trait, so another transport can implement it
without touching `WorkerPool`.

## Features

| Feature | Default | Backend |
| :--- | :---: | :--- |
| `in-memory` | yes | `tokio::sync` — `InMemoryQueue`. |

## Usage

```rust
use mytheclipse_queue::{InMemoryQueue, WorkerPool, Job, JobHandler, JobFuture};

fn print_handler() -> impl JobHandler {
    struct PrintHandler;
    impl JobHandler for PrintHandler {
        fn handle(&self, job: Job) -> JobFuture {
            Box::pin(async move {
                println!("payload: {:?}", job.payload);
                Ok(())
            })
        }
    }
    PrintHandler
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let queue = InMemoryQueue::new();
    queue.enqueue("email", b"hello".to_vec()).await?;

    let pool = WorkerPool::new(queue, 4);
    pool.start("email", print_handler());

    Ok(())
}
```

Any future backend only has to implement `Queue`; the `WorkerPool` driver is
already generic over it, so swapping is a one-line change at construction time.
