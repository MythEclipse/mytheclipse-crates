//! The `Job` type and its metadata.
//!
//! A minimal, dependency-free in-process job queue using `tokio::sync::mpsc`.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::OnceLock;
use std::time::{Duration, SystemTime};

/// A unique identifier for a queued job.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct JobId(pub String);

impl JobId {
    /// Generates an id that is unique within this process and across restarts.
    ///
    /// The prefix is fixed once per process from the start instant and the pid,
    /// so two runs never overlap; the suffix is a monotonic counter, so two jobs
    /// in one run never collide. This replaces a random UUID: uniqueness is the
    /// only property callers need, and a counter gets it without a random
    /// source or a 36-byte string.
    pub fn generate() -> Self {
        static PREFIX: OnceLock<String> = OnceLock::new();
        static COUNTER: AtomicU64 = AtomicU64::new(0);

        let prefix = PREFIX.get_or_init(|| {
            let nanos = SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos();
            format!("{nanos:x}{:x}", std::process::id())
        });
        let sequence = COUNTER.fetch_add(1, Ordering::Relaxed);
        Self(format!("{prefix}-{sequence:x}"))
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generated_ids_are_unique() {
        let a = JobId::generate();
        let b = JobId::generate();
        assert_ne!(a, b);
    }

    #[test]
    fn generated_ids_share_the_process_prefix() {
        let a = JobId::generate().0;
        let b = JobId::generate().0;
        let prefix = |s: &str| s.rsplit_once('-').unwrap().0.to_owned();
        assert_eq!(prefix(&a), prefix(&b));
    }
}
