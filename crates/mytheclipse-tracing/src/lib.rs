//! # mytheclipse-tracing
//!
//! A pre-built `tracing` subscriber with environment filtering, installed in
//! one call.

#[cfg(feature = "env")]
pub mod fmt;

#[cfg(feature = "env")]
pub use fmt::TracingLayer;
