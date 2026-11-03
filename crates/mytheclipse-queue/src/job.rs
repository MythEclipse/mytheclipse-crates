//! The `Job` type and its metadata.
//!
//! A minimal, dependency-free in-process job queue using `tokio::sync::mpsc`.

use std::time::{Duration, SystemTime};

/// A unique identifier for a queued job.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct JobId(pub String);

impl JobId {
    pub fn generate() -> Self {
        Self(uuid::Uuid::new_v4().to_string())
    }
}

impl std::fmt::Display for JobId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// A unit of queued work.
#[derive(Debug, Clone)]
pub struct Job {
    /// The unique ID of this job.
    pub id: JobId,
    /// The topic/queue name the job was delivered from.
    pub topic: String,
    /// The raw payload bytes.
    pub payload: Vec<u8>,
    /// How many times this job has been attempted (0 = first attempt).
    pub attempt: u32,
    /// When the job was first enqueued.
    pub enqueued_at: SystemTime,
    /// When the job was delivered to the worker (None if not yet delivered).
    pub delivered_at: Option<SystemTime>,
    /// Optional visibility timeout - after this the job becomes visible again.
    pub visibility_timeout: Option<Duration>,
}

impl Job {
    pub fn new(id: JobId, topic: &str, payload: Vec<u8>) -> Self {
        // One clock read for both timestamps: they describe the same instant,
        // and `SystemTime::now()` is not free.
        let now = SystemTime::now();
        Self {
            id,
            topic: topic.to_string(),
            payload,
            attempt: 0,
            enqueued_at: now,
            delivered_at: Some(now),
            visibility_timeout: None,
        }
    }
}
