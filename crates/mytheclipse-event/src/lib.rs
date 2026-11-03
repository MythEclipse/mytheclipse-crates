//! # mytheclipse-event
//!
//! A unified events & message bus abstraction so component-to-component (or
//! service-to-service) communication doesn't get locked to one transport.
//!
//! - **In-memory dispatcher** ([`memory::InMemoryEventBus`], feature `mem`,
//!   default): a `tokio::sync::broadcast`-backed pub/sub bus for
//!   intra-process communication.
//! - **Distributed broker backends**: [`amqp::AmqpEventBus`] (RabbitMQ,
//!   feature `amqp`) and [`nats::NatsEventBus`] (NATS, feature `nats`) behind
//!   the same [`EventBus`] trait, so swapping the backend does not change
//!   handler code.
//! - **Typed convenience**: [`typed::TypedEventBus`] layers JSON
//!   (de)serialization on top of the byte-oriented [`EventBus`].
//!
//! ## Example
//!
//! The in-memory bus (`mem` feature, on by default) carries raw bytes; the
//! `typed` feature adds JSON (de)serialization on top of it.
//!
//! ```no_run
//! #[cfg(feature = "mem")]
//! #[tokio::main]
//! async fn main() {
//!     use mytheclipse_event::{EventBus, InMemoryEventBus};
//!     let bus = InMemoryEventBus::default();
//!     let mut sub = bus.subscribe("orders").await.unwrap();
//!     bus.publish("orders", b"{\"id\":42}".to_vec()).await.unwrap();
//!     let payload = sub.recv().await.unwrap();
//!     assert_eq!(payload, b"{\"id\":42}");
//! }
//!
//! #[cfg(not(feature = "mem"))]
//! fn main() {}
//! ```

#![deny(missing_docs)]
pub mod traits;

#[cfg(feature = "mem")]
pub mod memory;

#[cfg(feature = "typed")]
pub mod typed;

#[cfg(feature = "amqp")]
pub mod amqp;

#[cfg(feature = "nats")]
pub mod nats;

pub use traits::{EventBus, EventError, Subscription};

#[cfg(feature = "mem")]
pub use memory::InMemoryEventBus;

#[cfg(feature = "typed")]
pub use typed::{Event, TypedEventBus, TypedSubscription};

#[cfg(feature = "amqp")]
pub use amqp::AmqpEventBus;

#[cfg(feature = "nats")]
pub use nats::NatsEventBus;

/// The common case, in one import.
///
/// ```
/// use mytheclipse_event::prelude::*;
/// ```
///
/// [`EventBus`] is the reason this exists: `subscribe` and `publish` are trait
/// methods, so they are not callable until the trait is in scope.
pub mod prelude {
    pub use crate::{EventBus, EventError, Subscription};

    #[cfg(feature = "amqp")]
    pub use crate::AmqpEventBus;
    #[cfg(feature = "mem")]
    pub use crate::InMemoryEventBus;
    #[cfg(feature = "nats")]
    pub use crate::NatsEventBus;
    #[cfg(feature = "typed")]
    pub use crate::{Event, TypedEventBus, TypedSubscription};
}
