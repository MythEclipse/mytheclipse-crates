//! # mytheclipse-queue
//!
//! A unified job queue abstraction so your background work isn't locked to one
//! transport. Provides a single [`Queue`] trait, [`Job`] type, and `WorkerPool`
//! executor with configurable retry/backoff, concurrency, and a dead-letter
//! queue.
//!
//! The in-memory backend (`InMemoryQueue`, feature `in-memory`) is the one that
//! exists today: `tokio::sync::mpsc` plus task spawning, no external service.
//! The `redis`, `nats`, and `postgres` features are declared but have no backend
//! implementation yet — see the README.

pub mod error;
#[cfg(feature = "in-memory")]
pub mod in_memory;
pub mod job;
pub mod traits;
#[cfg(feature = "in-memory")]
pub mod worker;

pub use error::{JobError, QueueError};
#[cfg(feature = "in-memory")]
pub use in_memory::InMemoryQueue;
pub use job::{Job, JobId};
pub use traits::Queue;
#[cfg(feature = "in-memory")]
pub use worker::{JobFuture, JobHandler, WorkerConfig, WorkerPool};

#[cfg(feature = "in-memory")]
pub mod backpressure_enqueue;
#[cfg(feature = "in-memory")]
pub mod batch;
#[cfg(feature = "in-memory")]
pub mod rate_limited;
#[cfg(feature = "in-memory")]
pub mod worker_rate_limited;
#[cfg(feature = "in-memory")]
pub use backpressure_enqueue::{
    enqueue_with_backpressure, BackpressureEnforcer, BackpressureError,
};
#[cfg(feature = "in-memory")]
pub use rate_limited::{RateLimitQueueError, RateLimitedQueue};
#[cfg(feature = "in-memory")]
pub use worker_rate_limited::RateLimitedWorkerPool;

#[cfg(feature = "in-memory")]
pub mod pipeline;
#[cfg(feature = "in-memory")]
pub use pipeline::{Stage, StageError, StageRunner};
