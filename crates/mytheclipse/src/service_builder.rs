//! Service builder that composes resiliency primitives.
//!
//! Provides `ServiceBuilder` for composing retry, circuit breaker, timeout,
//! and rate limiting around async service calls.

use std::future::Future;
use std::pin::Pin;
use std::time::Duration;

use tracing::Instrument;

#[cfg(feature = "resiliency")]
use crate::simple_rng::random_rng;

#[cfg(feature = "resiliency")]
use crate::circuit_breaker::CircuitBreaker;
#[cfg(feature = "traffic")]
use crate::ratelimit::RateLimiter;
#[cfg(feature = "resiliency")]
use crate::retry::{retry, RetryConfig, RetryError};

/// Error returned by [`ServiceBuilder::run`].
#[derive(Debug)]
pub enum RunError<E> {
    Inner(E),
    Retry(RetryError<E>),
    CircuitOpen,
    Timeout,
    RateLimited,
}

impl<E: std::fmt::Display> std::fmt::Display for RunError<E> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Inner(e) => write!(f, "service failed: {e}"),
            Self::Retry(e) => write!(f, "retry exhausted: {e}"),
            Self::CircuitOpen => write!(f, "circuit breaker open"),
            Self::Timeout => write!(f, "operation timed out"),
            Self::RateLimited => write!(f, "rate limited"),
        }
    }
}

#[cfg(feature = "resiliency")]
impl<E: std::fmt::Debug + std::fmt::Display + std::error::Error> std::error::Error for RunError<E> {}
#[cfg(not(feature = "resiliency"))]
impl<E: std::fmt::Debug + std::fmt::Display> std::error::Error for RunError<E> {}

// Config ------------------------------------------------------------------

#[cfg(not(feature = "traffic"))]
#[derive(Clone)]
pub struct ServiceConfig {
    pub max_attempts: u32,
    pub timeout: Duration,
}

#[cfg(not(feature = "traffic"))]
impl Default for ServiceConfig {
    fn default() -> Self {
        Self {
            max_attempts: 0,
            timeout: Duration::ZERO,
        }
    }
}

#[cfg(feature = "traffic")]
#[derive(Clone)]
pub struct ServiceConfig {
    pub max_attempts: u32,
    pub timeout: Duration,
    pub rate_per_sec: f64,
    pub rate_burst: u64,
}

#[cfg(feature = "traffic")]
impl Default for ServiceConfig {
    fn default() -> Self {
        Self {
            max_attempts: 0,
            timeout: Duration::ZERO,
            rate_per_sec: 0.0,
            rate_burst: 0,
        }
    }
}

// Builder -----------------------------------------------------------------

pub struct ServiceBuilder {
    #[cfg(feature = "resiliency")]
    retry_cfg: Option<RetryConfig>,
    #[cfg(feature = "resiliency")]
    circuit: Option<CircuitBreaker>,
    #[cfg(feature = "traffic")]
    rate_limiter: Option<RateLimiter>,
    timeout: Duration,
}

impl ServiceBuilder {
    pub fn new(config: ServiceConfig) -> Self {
        #[cfg(feature = "resiliency")]
        let retry_cfg = (config.max_attempts > 0).then(|| RetryConfig {
            max_attempts: config.max_attempts,
            ..RetryConfig::default()
        });

        #[cfg(feature = "traffic")]
        let rate_limiter = {
            if config.rate_per_sec > 0.0 && config.rate_burst > 0 {
                Some(RateLimiter::new(config.rate_per_sec, config.rate_burst))
            } else {
                None
            }
        };

        Self {
            #[cfg(feature = "resiliency")]
            retry_cfg,
            #[cfg(feature = "resiliency")]
            circuit: None,
            #[cfg(feature = "traffic")]
            rate_limiter,
            timeout: config.timeout,
        }
    }

    #[cfg(feature = "resiliency")]
    pub fn with_circuit_breaker(mut self, cb: CircuitBreaker) -> Self {
        self.circuit = Some(cb);
        self
    }

    #[cfg(feature = "traffic")]
    pub fn with_rate_limiter(mut self, rl: RateLimiter) -> Self {
        self.rate_limiter = Some(rl);
        self
    }

    fn check_pre<E>(&self) -> Result<(), RunError<E>> {
        #[cfg(feature = "resiliency")]
        if let Some(cb) = &self.circuit {
            if !cb.allow_request() {
                return Err(RunError::CircuitOpen);
            }
        }
        #[cfg(feature = "traffic")]
        if let Some(rl) = &self.rate_limiter {
            if rl.try_acquire().is_err() {
                return Err(RunError::RateLimited);
            }
        }
        Ok(())
    }

    #[cfg(feature = "resiliency")]
    fn record(&self, ok: bool) {
        if let Some(cb) = &self.circuit {
            if ok {
                cb.record_success();
            } else {
                cb.record_failure();
            }
        }
    }

    pub async fn run<F, T, E>(&self, f: F) -> Result<T, RunError<E>>
    where
        F: FnMut() -> Pin<Box<dyn Future<Output = Result<T, E>> + Send>>,
        E: std::fmt::Debug,
    {
        self.check_pre()?;

        let dur = self.timeout;

        #[cfg(feature = "resiliency")]
        {
            if let Some(retry_cfg) = &self.retry_cfg {
                let op = f;
                let result: Result<T, RunError<E>> = if dur > Duration::ZERO {
                    // We can't easily combine retry + timeout with FnMut due to
                    // closure capture rules, so use a manual retry loop instead:
                    let cfg = retry_cfg.clone();
                    let mut attempt_no: u32 = 0;
                    let mut op_ref = op;
                    loop {
                        attempt_no += 1;
                        let span =
                            tracing::info_span!("mytheclipse_service_call", attempt = attempt_no);
                        let fut = op_ref();
                        let attempt_result = tokio::time::timeout(dur, fut.instrument(span)).await;
                        match attempt_result {
                            Ok(Ok(v)) => {
                                self.record(true);
                                return Ok(v);
                            }
                            Ok(Err(e)) => {
                                self.record(false);
                                if attempt_no >= cfg.max_attempts {
                                    return Err(RunError::Inner(e));
                                }
                                // retryable — backoff and retry
                                let delay = crate::retry::backoff_delay(
                                    &cfg,
                                    attempt_no,
                                    &mut random_rng(),
                                );
                                tokio::time::sleep(delay).await;
                            }
                            Err(_) => {
                                self.record(false);
                                if attempt_no >= cfg.max_attempts {
                                    return Err(RunError::Timeout);
                                }
                                // retryable timeout — backoff and retry
                                let delay = crate::retry::backoff_delay(
                                    &cfg,
                                    attempt_no,
                                    &mut random_rng(),
                                );
                                tokio::time::sleep(delay).await;
                            }
                        }
                    }
                } else {
                    // retry() expects FnMut() -> Fut (not boxed), so adapt.
                    let mut inner_op = op;
                    retry(
                        retry_cfg.clone(),
                        |_: &E| true,
                        || {
                            let span = tracing::info_span!("mytheclipse_service_call");
                            let fut = inner_op();
                            async move { fut.instrument(span).await }
                        },
                    )
                    .await
                    .map_err(|e| {
                        self.record(false);
                        RunError::Retry(e)
                    })
                    .map(|v| {
                        self.record(true);
                        v
                    })
                };
                result
            } else {
                // No retry: just timeout or plain
                let mut op = f;
                let span = tracing::info_span!("mytheclipse_service_call");
                let result = if dur > Duration::ZERO {
                    tokio::time::timeout(dur, op().instrument(span))
                        .await
                        .map_err(|_| RunError::Timeout)?
                        .map_err(RunError::Inner)
                } else {
                    op().instrument(span).await.map_err(RunError::Inner)
                };
                match &result {
                    Ok(_) => self.record(true),
                    Err(_) => self.record(false),
                }
                result
            }
        }

        #[cfg(not(feature = "resiliency"))]
        {
            let _ = dur;
            let mut op = f;
            let span = tracing::info_span!("mytheclipse_service_call");
            op().instrument(span).await.map_err(RunError::Inner)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    #[tokio::test]
    async fn no_layers_passes_through() {
        let builder = ServiceBuilder::new(ServiceConfig::default());
        let result = builder.run(|| Box::pin(async { Ok::<_, ()>(42u32) })).await;
        assert_eq!(result.unwrap(), 42);
    }

    #[cfg(feature = "resiliency")]
    #[tokio::test]
    async fn retry_succeeds_after_transient_failure() {
        let cfg = ServiceConfig {
            max_attempts: 3,
            ..Default::default()
        };
        let builder = ServiceBuilder::new(cfg);
        let attempts = Arc::new(std::sync::atomic::AtomicU32::new(0));
        let result = builder
            .run(|| {
                let a = Arc::clone(&attempts);
                Box::pin(async move {
                    let n = a.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    if n < 2 {
                        Err::<u32, _>(())
                    } else {
                        Ok::<u32, _>(42)
                    }
                })
            })
            .await;
        assert_eq!(result.unwrap(), 42);
    }

    #[tokio::test]
    async fn timeout_returns_timeout_error() {
        let cfg = ServiceConfig {
            timeout: Duration::from_millis(5),
            ..Default::default()
        };
        let builder = ServiceBuilder::new(cfg);
        let result = builder
            .run(|| {
                Box::pin(async {
                    tokio::time::sleep(Duration::from_secs(1)).await;
                    Ok::<_, ()>(42u32)
                })
            })
            .await;
        assert!(matches!(result, Err(RunError::Timeout)));
    }
}
