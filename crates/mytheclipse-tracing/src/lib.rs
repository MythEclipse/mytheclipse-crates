//! # mytheclipse-tracing
//!
//! A pre-built `tracing` subscriber with environment filtering, installed in
//! one call.

#![deny(missing_docs)]
#[cfg(feature = "env")]
pub mod fmt;

#[cfg(feature = "env")]
pub use fmt::TracingLayer;
