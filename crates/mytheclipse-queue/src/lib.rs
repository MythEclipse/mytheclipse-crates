//! # mytheclipse-queue
//!
//! A job queue abstraction so your background work isn't locked to one
//! transport. Provides a single [`Queue`] trait, [`Job`] type, and `WorkerPool`
//! executor with configurable retry/backoff, concurrency, and a dead-letter
//! queue.
//!
//! The backend that ships is in-memory (`InMemoryQueue`, feature `in-memory`):
//! one `Mutex<Vec<Job>>` + `Notify` per topic, no external service. `Queue` is a
//! trait, so another transport can implement it without touching `WorkerPool`.

pub mod error;
#[cfg(feature = "in-memory")]
mod host;
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
