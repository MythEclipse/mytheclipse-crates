//! JSON-typed convenience over the byte-oriented [`EventBus`].
//!
//! ```no_run
//! use serde::{Deserialize, Serialize};
//!
//! #[derive(Debug, Serialize, Deserialize, PartialEq)]
//! struct OrderCreated { id: u64 }
//!
//! #[cfg(feature = "mem")]
//! #[tokio::main]
//! async fn main() {
//!     use mytheclipse_event::{InMemoryEventBus, TypedEventBus};
//!     let bus = TypedEventBus::new(InMemoryEventBus::default());
//!     let mut sub = bus.subscribe::<OrderCreated>("orders").await.unwrap();
//!     bus.publish("orders", &OrderCreated { id: 42 }).await.unwrap();
//!     let event = sub.recv().await.unwrap();
//!     assert_eq!(event, OrderCreated { id: 42 });
//! }
//!
//! #[cfg(not(feature = "mem"))]
//! fn main() {}
//! ```

use std::marker::PhantomData;
use std::sync::Arc;

use serde::de::DeserializeOwned;
use serde::Serialize;

use crate::traits::{EventBus, EventError, Subscription};

/// A marker trait for event payload types: anything JSON-(de)serializable.
pub trait Event: Serialize + DeserializeOwned + Send + Sync + 'static {}
impl<T> Event for T where T: Serialize + DeserializeOwned + Send + Sync + 'static {}

/// Wraps a byte-oriented [`EventBus`] with typed publish/subscribe using JSON
/// encoding.
#[derive(Clone)]
pub struct TypedEventBus<B> {
    inner: Arc<B>,
}

impl<B: EventBus> TypedEventBus<B> {
    /// Wraps `inner`.
    pub fn new(inner: B) -> Self {
        Self {
            inner: Arc::new(inner),
        }
    }

    /// Serializes and publishes `event` to `topic`.
    pub async fn publish<E: Event>(&self, topic: &str, event: &E) -> Result<(), EventError> {
        let bytes =
            serde_json::to_vec(event).map_err(|e| EventError::Serialization(e.to_string()))?;
        self.inner.publish(topic, bytes).await
    }

    /// Subscribes to `topic`, returning a [`TypedSubscription<E>`].
    pub async fn subscribe<E: Event>(
        &self,
        topic: &str,
    ) -> Result<TypedSubscription<E>, EventError> {
        let inner = self.inner.subscribe(topic).await?;
        Ok(TypedSubscription {
            inner,
            _marker: PhantomData,
        })
    }

    /// Returns the underlying byte-oriented bus.
    pub fn inner(&self) -> &B {
        &self.inner
    }
}

/// A typed view over a byte [`Subscription`], deserializing each message.
pub struct TypedSubscription<E> {
    inner: Subscription,
    _marker: PhantomData<fn() -> E>,
}

impl<E: Event> TypedSubscription<E> {
    /// Awaits and deserializes the next event.
    pub async fn recv(&mut self) -> Result<E, EventError> {
        let bytes = self.inner.recv().await?;
        serde_json::from_slice(&bytes).map_err(|e| EventError::Serialization(e.to_string()))
    }
}

#[cfg(all(test, feature = "mem"))]
mod tests {
    use super::*;
    use crate::memory::InMemoryEventBus;
    use serde::Deserialize;

    #[derive(Debug, Serialize, Deserialize, PartialEq)]
    struct OrderCreated {
        id: u64,
        total_cents: u64,
    }

    #[tokio::test]
    async fn typed_publish_subscribe_roundtrip() {
        let bus = TypedEventBus::new(InMemoryEventBus::default());
        let mut sub = bus.subscribe::<OrderCreated>("orders").await.unwrap();
        bus.publish(
            "orders",
            &OrderCreated {
                id: 1,
                total_cents: 4999,
            },
        )
        .await
        .unwrap();
        let event = sub.recv().await.unwrap();
        assert_eq!(
            event,
            OrderCreated {
                id: 1,
                total_cents: 4999
            }
        );
    }
}
