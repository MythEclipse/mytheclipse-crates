//! Shared error type for mytheclipse execution primitives.

/// Errors surfaced by mytheclipse's execution primitives.
///
/// Marked `#[non_exhaustive]` so new variants can be added without a
/// breaking change; downstream `match` expressions should include a
/// wildcard arm.
#[non_exhaustive]
#[derive(Debug, PartialEq, Eq)]
pub enum MytheclipseError {
    /// A closure submitted to [`crate::compute::compute`] panicked.
    ///
    /// The contained string is a best-effort rendering of the panic
    /// payload; the compute thread pool itself remains usable afterward.
    ComputePanic(String),
    /// A deadline elapsed before the wrapped future completed
    /// ([`crate::timeout::with_timeout`]).
    Timeout,
    /// A request was refused because the circuit breaker is open
    /// ([`crate::circuit_breaker`]).
    CircuitOpen,
    /// A retry loop gave up after exhausting its maximum attempts
    /// ([`crate::retry()`]). The contained count is the number of attempts made.
    RetryExhausted {
        /// Attempts made, including the first.
        attempts: u32,
    },
    /// A request was refused because the rate limiter had no tokens left
    /// ([`crate::ratelimit`]).
    RateLimited,
    /// A queue was full and the configured overflow policy rejected the item
    /// ([`crate::backpressure`]).
    QueueFull(String),
    /// A concurrency limiter was already at its maximum and refused a request
    /// ([`crate::concurrency`]).
    ConcurrencyLimitExceeded,
    /// Shutdown has been requested for the process or manager
    /// ([`crate::shutdown`]).
    Shutdown,
    /// An observability operation failed, e.g. metric export
    /// ([`crate::metrics`]).
    Metrics(String),
    /// Invalid configuration or input, e.g. a malformed cron expression
    /// ([`crate::cron`]).
    Config(String),
}

impl std::fmt::Display for MytheclipseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ComputePanic(message) => {
                write!(f, "compute closure panicked: {message}")
            }
            Self::Timeout => write!(f, "operation timed out"),
            Self::CircuitOpen => write!(f, "circuit breaker is open"),
            Self::RetryExhausted { attempts } => {
                write!(f, "retry exhausted after {attempts} attempts")
            }
            Self::RateLimited => write!(f, "rate limit exceeded"),
            Self::QueueFull(detail) => write!(f, "queue is full: {detail}"),
            Self::ConcurrencyLimitExceeded => {
                write!(f, "concurrency limit exceeded")
            }
            Self::Shutdown => write!(f, "shutdown requested"),
            Self::Metrics(detail) => write!(f, "metrics error: {detail}"),
            Self::Config(detail) => write!(f, "configuration error: {detail}"),
        }
    }
}

impl std::error::Error for MytheclipseError {}
