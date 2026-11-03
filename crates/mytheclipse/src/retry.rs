//! Automatic retry with exponential backoff and jitter (feature `resiliency`).
//!
//! [`retry`] re-runs a fallible async operation according to a
//! [`RetryConfig`], sleeping an exponentially-growing, optionally-jittered
//! delay between attempts. A predicate decides which errors are retryable, so
//! permanent failures (e.g. a 4xx response) short-circuit immediately while
//! transient ones (network hiccups, connection refused) are retried.

use std::future::Future;
use std::time::Duration;

use tracing::Instrument;

use crate::simple_rng::{random_rng, Rng};

/// How much random jitter to apply to each backoff delay.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JitterKind {
    /// No randomness: the delay is exactly `base_delay * factor^n`.
    None,
    /// Half fixed, half random: the delay lands in `[d/2, d]`.
    Equal,
    /// Fully random: the delay lands in `[0, d]`, which spreads a thundering
    /// herd the hardest.
    Full,
}

/// Configuration driving [`retry`].
#[derive(Debug, Clone)]
pub struct RetryConfig {
    /// Total attempts, including the first. `3` means two retries.
    pub max_attempts: u32,
    /// Delay before the second attempt.
    pub base_delay: Duration,
    /// Ceiling the exponential growth saturates at.
    pub max_delay: Duration,
    /// Multiplier per attempt: attempt `n` waits `base_delay * factor^n`.
    pub factor: f64,
    /// How much randomness to add to each delay.
    pub jitter: JitterKind,
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            max_attempts: 3,
            base_delay: Duration::from_millis(500),
            max_delay: Duration::from_secs(10),
            factor: 2.0,
            jitter: JitterKind::Full,
        }
    }
}

/// The error returned by [`retry`] once it gives up.
#[derive(Debug)]
pub enum RetryError<E> {
    /// Every attempt failed.
    Exhausted {
        /// Attempts made, including the first.
        attempts: u32,
        /// Error from the final attempt.
        last: E,
    },
}

impl<E> RetryError<E> {
    /// The error from the final attempt.
    pub fn last(&self) -> &E {
        match self {
            Self::Exhausted { last, .. } => last,
        }
    }
}

impl<E: std::fmt::Display> std::fmt::Display for RetryError<E> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Exhausted { attempts, last } => {
                write!(f, "retry exhausted after {attempts} attempts: {last}")
            }
        }
    }
}

impl<E: std::fmt::Debug + std::fmt::Display> std::error::Error for RetryError<E> {}

/// Statistics collected during a [`retry`] call.
#[derive(Debug, Clone, Default)]
pub struct RetryStats {
    /// Attempts made, including the first.
    pub attempts: u32,
    /// Attempts that followed a failure.
    pub retries: u32,
    /// Rendered error from the final attempt, when any attempt failed.
    pub last_error: Option<String>,
}

/// Runs `op` until it succeeds or `config.max_attempts` is spent, returning
/// the statistics alongside the outcome.
///
/// Same loop as [`retry`], but the caller also gets [`RetryStats`] — useful
/// when retries are a metric rather than a detail.
pub async fn retry_with_stats<T, E, F, Fut, P>(
    config: RetryConfig,
    filter: P,
    mut op: F,
) -> (Result<T, RetryError<E>>, RetryStats)
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, E>>,
    P: Fn(&E) -> bool,
    E: std::fmt::Display,
{
    let mut attempt: u32 = 0;
    let mut last_error: Option<String> = None;
    loop {
        attempt += 1;
        let span = tracing::info_span!(
            "mytheclipse_retry_task",
            attempt,
            max_attempts = config.max_attempts
        );
        let result = op().instrument(span).await;

        match result {
            Ok(value) => {
                return (
                    Ok(value),
                    RetryStats {
                        attempts: attempt,
                        retries: attempt.saturating_sub(1),
                        last_error,
                    },
                );
            }
            Err(err) => {
                last_error = Some(err.to_string());
                let retryable = filter(&err);
                if !retryable || attempt >= config.max_attempts {
                    let stats = RetryStats {
                        attempts: attempt,
                        retries: attempt.saturating_sub(1),
                        last_error,
                    };
                    return (
                        Err(RetryError::Exhausted {
                            attempts: attempt,
                            last: err,
                        }),
                        stats,
                    );
                }
                let delay = backoff_delay(&config, attempt, &mut random_rng());
                tokio::time::sleep(delay).await;
            }
        }
    }
}

/// Runs `op` until it succeeds, `filter` rejects the error, or
/// `config.max_attempts` is spent.
///
/// `filter` decides whether a given error is worth retrying: return `true` to
/// try again, `false` to give up immediately with
/// [`RetryError::Exhausted`] carrying that first error.
///
/// ```
/// use std::time::Duration;
/// use mytheclipse::prelude::*;
///
/// # async fn run() -> Result<(), Box<dyn std::error::Error>> {
/// let mut attempt = 0;
/// let value = retry(
///     RetryConfig {
///         max_attempts: 3,
///         base_delay: Duration::from_millis(1),
///         ..Default::default()
///     },
///     |e: &std::io::Error| e.kind() == std::io::ErrorKind::TimedOut,
///     || {
///         attempt += 1;
///         async move {
///             if attempt < 3 {
///                 Err(std::io::Error::from(std::io::ErrorKind::TimedOut))
///             } else {
///                 Ok(7)
///             }
///         }
///     },
/// )
/// .await?;
/// assert_eq!(value, 7);
/// # Ok(())
/// # }
/// ```
pub async fn retry<T, E, F, Fut, P>(
    config: RetryConfig,
    filter: P,
    mut op: F,
) -> Result<T, RetryError<E>>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, E>>,
    P: Fn(&E) -> bool,
{
    let mut attempt: u32 = 0;
    loop {
        attempt += 1;
        let span = tracing::info_span!(
            "mytheclipse_retry_task",
            attempt,
            max_attempts = config.max_attempts
        );
        let result = op().instrument(span).await;

        match result {
            Ok(value) => return Ok(value),
            Err(err) => {
                let retryable = filter(&err);
                if !retryable || attempt >= config.max_attempts {
                    return Err(RetryError::Exhausted {
                        attempts: attempt,
                        last: err,
                    });
                }
                let delay = backoff_delay(&config, attempt, &mut random_rng());
                tokio::time::sleep(delay).await;
            }
        }
    }
}

pub(crate) fn backoff_delay<R: Rng>(config: &RetryConfig, attempt: u32, rng: &mut R) -> Duration {
    let exponent = attempt.saturating_sub(1) as f64;
    let computed = config.base_delay.as_millis() as f64 * config.factor.powf(exponent);
    let max_ms = config.max_delay.as_millis() as f64;
    let capped = computed.min(max_ms);

    let millis = match config.jitter {
        JitterKind::None => capped,
        JitterKind::Equal => capped / 2.0 + rng.next_f64() * (capped / 2.0),
        JitterKind::Full => rng.next_f64() * capped,
    };

    Duration::from_millis(millis.clamp(0.0, max_ms) as u64)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::Cell;

    fn always<E>(_: &E) -> bool {
        true
    }

    #[tokio::test]
    async fn succeeds_on_first_attempt() {
        let calls = Cell::new(0u32);
        let result = retry(RetryConfig::default(), always, || async {
            calls.set(calls.get() + 1);
            Ok::<_, ()>(42u32)
        })
        .await;
        assert_eq!(result.unwrap(), 42);
        assert_eq!(calls.get(), 1);
    }

    #[tokio::test]
    async fn succeeds_after_transient_failures() {
        let config = RetryConfig {
            max_attempts: 5,
            base_delay: Duration::from_millis(1),
            ..RetryConfig::default()
        };
        let calls = Cell::new(0u32);
        let result = retry(config, always, || async {
            calls.set(calls.get() + 1);
            if calls.get() < 3 {
                Err::<u32, u8>(9)
            } else {
                Ok(7u32)
            }
        })
        .await;
        assert_eq!(result.unwrap(), 7);
        assert_eq!(calls.get(), 3);
    }

    #[tokio::test]
    async fn exhausts_after_max_attempts() {
        let config = RetryConfig {
            max_attempts: 3,
            base_delay: Duration::from_millis(1),
            ..RetryConfig::default()
        };
        let calls = Cell::new(0u32);
        let result = retry(config, always, || async {
            calls.set(calls.get() + 1);
            Err::<u32, u8>(42)
        })
        .await;
        assert!(matches!(
            result,
            Err(RetryError::Exhausted { attempts: 3, .. })
        ));
        assert_eq!(result.unwrap_err().last(), &42);
    }

    #[tokio::test]
    async fn non_retryable_error_short_circuits() {
        let config = RetryConfig {
            max_attempts: 10,
            base_delay: Duration::from_millis(1),
            ..RetryConfig::default()
        };
        let calls = Cell::new(0u32);
        let result = retry(
            config,
            |e: &u16| *e != 403,
            || async {
                calls.set(calls.get() + 1);
                Err::<u32, u16>(403)
            },
        )
        .await;
        assert!(matches!(
            result,
            Err(RetryError::Exhausted { attempts: 1, .. })
        ));
        assert_eq!(calls.get(), 1);
    }

    #[tokio::test]
    async fn retry_with_stats_succeeds_with_counts() {
        let config = RetryConfig {
            max_attempts: 5,
            base_delay: Duration::from_millis(1),
            ..RetryConfig::default()
        };
        let calls = Cell::new(0u32);
        let (result, stats) = retry_with_stats(
            config,
            |_| true,
            || async {
                calls.set(calls.get() + 1);
                if calls.get() < 3 {
                    Err::<u32, &str>("fail")
                } else {
                    Ok(42u32)
                }
            },
        )
        .await;
        assert_eq!(result.unwrap(), 42);
        assert_eq!(stats.attempts, 3);
        assert_eq!(stats.retries, 2);
    }

    #[test]
    fn full_jitter_is_within_bounds_and_capped() {
        let config = RetryConfig {
            max_attempts: 3,
            base_delay: Duration::from_secs(2),
            max_delay: Duration::from_secs(4),
            factor: 10.0,
            jitter: JitterKind::Full,
        };
        let mut rng = crate::simple_rng::Xoshiro256::new(0);
        for _ in 0..1000 {
            let d = backoff_delay(&config, 2, &mut rng);
            assert!(d <= config.max_delay);
        }
    }
}
