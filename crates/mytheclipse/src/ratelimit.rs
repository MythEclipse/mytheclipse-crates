//! A token-bucket rate limiter (feature `traffic`).
//!
//! [`RateLimiter`] caps how many tokens can be consumed per unit time, so a
//! caller/request stream is throttled (e.g. at most 100 jobs per second).
//! Tokens are refilled lazily from elapsed time rather than by a background
//! task, and a configurable burst capacity permits short bursts beyond the
//! steady-state rate.

use std::sync::Mutex;
use std::time::{Duration, Instant};

use tracing::Instrument;

use crate::error::MytheclipseError;

/// The error returned by rate-limit acquisition.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RateLimitError {
    /// The limiter's capacity was exceeded and no retry path was taken.
    ///
    /// Returned by [`RateLimiter::try_acquire`] when no token is available, or
    /// wrapped into the shared [`MytheclipseError::RateLimited`] variant.
    Limited,
}

impl std::fmt::Display for RateLimitError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Limited => write!(f, "rate limit exceeded"),
        }
    }
}

impl std::error::Error for RateLimitError {}

/// Token bucket state, guarded as one unit so `tokens` and `last_refill` can
/// never be observed out of step with each other.
struct Bucket {
    tokens: f64,
    last_refill: Instant,
}

struct Inner {
    capacity: u64,
    refill_per_sec: f64,
    bucket: Mutex<Bucket>,
}

/// A thread-safe, async-compatible token-bucket rate limiter.
///
/// Construct with [`RateLimiter::new`], then [`RateLimiter::acquire`] to await
/// a token or [`RateLimiter::try_acquire`] to fail fast when exhausted.
#[derive(Clone)]
pub struct RateLimiter {
    inner: std::sync::Arc<Inner>,
}

impl RateLimiter {
    /// Builds a limiter refilling at `rate_per_sec` tokens/second with a burst
    /// capacity of `burst_capacity`.
    ///
    /// # Panics
    ///
    /// Panics if `rate_per_sec` is not finite and positive, or if
    /// `burst_capacity` is zero.
    pub fn new(rate_per_sec: f64, burst_capacity: u64) -> Self {
        assert!(
            rate_per_sec.is_finite() && rate_per_sec > 0.0,
            "rate must be positive"
        );
        assert!(burst_capacity > 0, "burst capacity must be > 0");
        Self {
            inner: std::sync::Arc::new(Inner {
                capacity: burst_capacity,
                refill_per_sec: rate_per_sec,
                bucket: Mutex::new(Bucket {
                    tokens: burst_capacity as f64,
                    last_refill: Instant::now(),
                }),
            }),
        }
    }

    /// Awaits until a token is available and consumes one.
    ///
    /// Returns [`RateLimitError::Limited`] (in practice, never for `acquire`)
    /// on closure; the suspended waiter is woken when the bucket refills. The
    /// wait is wrapped in a `mytheclipse_ratelimit_task` tracing span.
    pub async fn acquire(&self) -> Result<(), RateLimitError> {
        loop {
            if self.try_acquire().is_ok() {
                return Ok(());
            }
            let span = tracing::info_span!("mytheclipse_ratelimit_task");
            // Sleep for the time it takes to refill a single token.
            let sleep = Duration::from_secs_f64(1.0 / self.inner.refill_per_sec);
            tokio::time::sleep(sleep).instrument(span).await;
        }
    }

    /// Consumes one token immediately if one is available.
    ///
    /// Returns `Err(RateLimitError::Limited)` (or the caller may map it to
    /// [`MytheclipseError::RateLimited`]) when the bucket is empty.
    pub fn try_acquire(&self) -> Result<(), RateLimitError> {
        let mut bucket = self.inner.bucket.lock().unwrap();
        self.refill(&mut bucket);

        if bucket.tokens >= 1.0 {
            bucket.tokens -= 1.0;
            Ok(())
        } else {
            Err(RateLimitError::Limited)
        }
    }

    /// The approximate number of tokens currently available (including burst
    /// headroom), for metrics/observability purposes.
    pub fn available_tokens(&self) -> u64 {
        let mut bucket = self.inner.bucket.lock().unwrap();
        self.refill(&mut bucket);
        bucket.tokens.floor() as u64
    }

    fn refill(&self, bucket: &mut Bucket) {
        let now = Instant::now();
        let elapsed_secs = now
            .saturating_duration_since(bucket.last_refill)
            .as_secs_f64();
        let added = elapsed_secs * self.inner.refill_per_sec;
        bucket.tokens = (bucket.tokens + added).min(self.inner.capacity as f64);
        bucket.last_refill = now;
    }

    /// `acquire` mapped onto the shared error type, e.g. for code returning
    /// [`MytheclipseError`].
    pub async fn acquire_err(&self) -> Result<(), MytheclipseError> {
        self.acquire()
            .await
            .map_err(|_| MytheclipseError::RateLimited)
    }

    /// `try_acquire` mapped onto the shared error type.
    pub fn try_acquire_err(&self) -> Result<(), MytheclipseError> {
        self.try_acquire()
            .map_err(|_| MytheclipseError::RateLimited)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn burst_capacity_consumed_immediately() {
        let limiter = RateLimiter::new(1000.0, 3);
        assert!(limiter.try_acquire().is_ok());
        assert!(limiter.try_acquire().is_ok());
        assert!(limiter.try_acquire().is_ok());
        assert_eq!(limiter.try_acquire(), Err(RateLimitError::Limited));
    }

    #[test]
    fn available_tokens_bounded_by_capacity() {
        let limiter = RateLimiter::new(1000.0, 5);
        assert_eq!(limiter.available_tokens(), 5);
        let _ = limiter.try_acquire();
        assert_eq!(limiter.available_tokens(), 4);
    }

    #[tokio::test]
    async fn acquire_waits_for_refill() {
        let limiter = RateLimiter::new(1000.0, 1);
        assert!(limiter.try_acquire().is_ok());
        let start = Instant::now();
        limiter
            .acquire()
            .await
            .expect("acquire should eventually succeed");
        assert!(start.elapsed() >= Duration::from_millis(1));
    }

    #[tokio::test]
    async fn try_acquire_err_maps_to_shared_error() {
        let limiter = RateLimiter::new(1000.0, 1);
        let _ = limiter.try_acquire();
        assert_eq!(
            limiter.try_acquire_err(),
            Err(MytheclipseError::RateLimited)
        );
    }

    #[test]
    #[should_panic]
    fn zero_burst_panics() {
        let _ = RateLimiter::new(1.0, 0);
    }
}
