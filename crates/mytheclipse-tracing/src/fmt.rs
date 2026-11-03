//! Formatted tracing subscriber with environment filtering.
//!
//! Logs go to stderr, never stdout: a library that installs global logging must
//! not interleave with the program's own output.

use std::io::IsTerminal;

use tracing_subscriber::prelude::*;
use tracing_subscriber::{fmt, EnvFilter};

/// Filter used when `RUST_LOG` is unset or unparsable.
const DEFAULT_FILTER: &str = "info";

/// Namespace for installing the pre-configured subscriber.
///
/// This is not a `tracing_subscriber::Layer`; use [`TracingLayer::layer`] when
/// composing a subscriber by hand.
#[derive(Clone)]
pub struct TracingLayer;

impl TracingLayer {
    /// Installs the global subscriber, ignoring the case where one is already
    /// set.
    ///
    /// Use [`TracingLayer::try_install`] when the caller needs to know whether
    /// this actually took effect.
    pub fn install() {
        let _ = Self::try_install();
    }

    /// Installs the global subscriber, reporting why it did not take effect.
    ///
    /// Fails if a global subscriber is already installed, so a second call is a
    /// no-op rather than a panic.
    pub fn try_install() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        fmt::Subscriber::builder()
            .with_env_filter(env_filter())
            .with_ansi(use_ansi())
            .with_writer(std::io::stderr)
            .try_init()
    }

    /// Returns a formatted layer for manual composition.
    ///
    /// ```no_run
    /// use mytheclipse_tracing::TracingLayer;
    /// use tracing_subscriber::prelude::*;
    ///
    /// let subscriber = tracing_subscriber::registry().with(TracingLayer::layer());
    /// tracing::subscriber::set_global_default(subscriber).unwrap();
    /// ```
    pub fn layer() -> impl tracing_subscriber::layer::Layer<tracing_subscriber::Registry> {
        fmt::layer()
            .with_ansi(use_ansi())
            .with_writer(std::io::stderr)
            .with_filter(env_filter())
    }
}

/// Reads `RUST_LOG`, falling back to [`DEFAULT_FILTER`] when it is unset or
/// invalid.
fn env_filter() -> EnvFilter {
    filter_from(std::env::var("RUST_LOG").ok().as_deref())
}

/// Parses a filter directive, falling back to [`DEFAULT_FILTER`].
///
/// Separated from [`env_filter`] so the fallback is testable without touching
/// process-wide environment state.
fn filter_from(spec: Option<&str>) -> EnvFilter {
    spec.and_then(|s| EnvFilter::try_new(s).ok())
        .unwrap_or_else(|| EnvFilter::new(DEFAULT_FILTER))
}

/// ANSI escapes only when stderr is a terminal, so redirected logs stay readable.
fn use_ansi() -> bool {
    std::io::stderr().is_terminal()
}

#[cfg(test)]
mod tests {
    use super::*;
    use tracing::level_filters::LevelFilter;

    #[test]
    fn unset_rust_log_falls_back_to_info() {
        assert_eq!(filter_from(None).max_level_hint(), Some(LevelFilter::INFO));
    }

    #[test]
    fn unparsable_rust_log_falls_back_to_info() {
        assert_eq!(
            filter_from(Some("==not a directive==")).max_level_hint(),
            Some(LevelFilter::INFO)
        );
    }

    #[test]
    fn rust_log_is_honoured() {
        assert_eq!(
            filter_from(Some("debug")).max_level_hint(),
            Some(LevelFilter::DEBUG)
        );
    }

    #[test]
    fn layer_builds() {
        let _ = TracingLayer::layer();
    }

    #[test]
    fn second_install_reports_that_it_did_nothing() {
        assert!(TracingLayer::try_install().is_ok());
        assert!(TracingLayer::try_install().is_err());
    }
}
