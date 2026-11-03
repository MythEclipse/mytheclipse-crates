//! In-memory pub/sub dispatcher (feature `mem`), backed by
//! [`tokio::sync::broadcast`].
//!
//! Suitable for communication between components inside a single process
//! (monolith). Each topic lazily gets its own broadcast channel on first use.
//!
//! The topic map is guarded by an `RwLock`: publishing takes it shared and
//! clones the channel sender out, so concurrent producers do not serialize on
//! one exclusive lock and the steady-state path allocates nothing.

use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use async_trait::async_trait;
use tokio::sync::broadcast;

use crate::traits::{EventBus, EventError, Subscription, SubscriptionInner};

const DEFAULT_CAPACITY: usize = 256;

/// An in-process [`EventBus`] backed by a `tokio::sync::broadcast` channel per
/// topic.
#[derive(Clone)]
pub struct InMemoryEventBus {
    channels: Arc<RwLock<HashMap<String, broadcast::Sender<Vec<u8>>>>>,
    capacity: usize,
}

impl Default for InMemoryEventBus {
    fn default() -> Self {
        Self::new(DEFAULT_CAPACITY)
    }
}

impl InMemoryEventBus {
    /// Builds a bus whose per-topic channels buffer up to `capacity` messages
    /// for slow subscribers before older ones are dropped (lagged).
    pub fn new(capacity: usize) -> Self {
        Self {
            channels: Arc::new(RwLock::new(HashMap::new())),
            capacity,
        }
    }

    /// The sender for `topic`, creating its channel on first use. Existing
    /// topics are served from the read lock without allocating a key.
    fn sender_for(&self, topic: &str) -> broadcast::Sender<Vec<u8>> {
        let existing = self.channels.read().unwrap().get(topic).cloned();
        if let Some(tx) = existing {
            return tx;
        }
        let mut channels = self.channels.write().unwrap();
        channels
            .entry(topic.to_owned())
            .or_insert_with(|| broadcast::channel(self.capacity).0)
            .clone()
    }

    /// The number of currently subscribed receivers for `topic` (0 if the
    /// topic has never been touched or has no active subscribers).
    pub fn subscriber_count(&self, topic: &str) -> usize {
        self.channels
            .read()
            .unwrap()
            .get(topic)
            .map(|tx| tx.receiver_count())
            .unwrap_or(0)
    }
}

#[async_trait]
impl EventBus for InMemoryEventBus {
    async fn publish(&self, topic: &str, payload: Vec<u8>) -> Result<(), EventError> {
        let tx = self.sender_for(topic);
        // A `SendError` here just means there are currently no subscribers,
        // which is not an error condition for a pub/sub bus.
        let _ = tx.send(payload);
        Ok(())
    }

    async fn subscribe(&self, topic: &str) -> Result<Subscription, EventError> {
        let rx = self.sender_for(topic).subscribe();
        Ok(Subscription::new(Box::new(BroadcastSubscription(rx))))
    }
}

struct BroadcastSubscription(broadcast::Receiver<Vec<u8>>);

#[async_trait]
impl SubscriptionInner for BroadcastSubscription {
    async fn recv(&mut self) -> Result<Vec<u8>, EventError> {
        loop {
            match self.0.recv().await {
                Ok(payload) => return Ok(payload),
                // A slow subscriber missed some messages; skip ahead rather
                // than erroring, matching typical broadcast semantics.
                Err(broadcast::error::RecvError::Lagged(_)) => continue,
                Err(broadcast::error::RecvError::Closed) => return Err(EventError::Closed),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn publish_subscribe_roundtrip() {
        let bus = InMemoryEventBus::default();
        let mut sub = bus.subscribe("orders").await.unwrap();
        bus.publish("orders", b"order-created".to_vec())
            .await
            .unwrap();
        let msg = sub.recv().await.unwrap();
        assert_eq!(msg, b"order-created");
    }

    #[tokio::test]
    async fn multiple_subscribers_all_receive() {
        let bus = InMemoryEventBus::default();
        let mut sub_a = bus.subscribe("t").await.unwrap();
        let mut sub_b = bus.subscribe("t").await.unwrap();
        bus.publish("t", b"hello".to_vec()).await.unwrap();
        assert_eq!(sub_a.recv().await.unwrap(), b"hello");
        assert_eq!(sub_b.recv().await.unwrap(), b"hello");
    }

    #[tokio::test]
    async fn publish_with_no_subscribers_is_ok() {
        let bus = InMemoryEventBus::default();
        assert!(bus.publish("nobody-listening", b"x".to_vec()).await.is_ok());
    }

    #[tokio::test]
    async fn different_topics_are_isolated() {
        let bus = InMemoryEventBus::default();
        let mut sub = bus.subscribe("a").await.unwrap();
        bus.publish("b", b"for-b".to_vec()).await.unwrap();
        // Nothing arrives on `a`'s subscription; use a short timeout to prove it.
        let result = tokio::time::timeout(std::time::Duration::from_millis(30), sub.recv()).await;
        assert!(
            result.is_err(),
            "subscription on `a` should not receive `b`'s event"
        );
    }

    #[tokio::test]
    async fn subscriber_count_tracks_active_subscriptions() {
        let bus = InMemoryEventBus::default();
        assert_eq!(bus.subscriber_count("t"), 0);
        let sub = bus.subscribe("t").await.unwrap();
        assert_eq!(bus.subscriber_count("t"), 1);
        drop(sub);
        assert_eq!(bus.subscriber_count("t"), 0);
    }
}
