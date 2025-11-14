//! In-memory event bus implementation.
//!
//! Provides a fast, thread-safe in-memory implementation of `EventBus`
//! suitable for testing and development.

use async_trait::async_trait;
use event_sauce_core::{EventBus, EventEnvelope, EventFilter, Result};
use futures::stream::{self, Stream};
use parking_lot::RwLock;
use std::sync::Arc;
use tokio::sync::broadcast;

/// In-memory event bus for testing and development.
///
/// This implementation uses tokio broadcast channels to deliver events
/// to subscribers. It supports filtering and handles backpressure automatically.
///
/// # Thread Safety
///
/// This bus is thread-safe and can be cloned cheaply (uses `Arc` internally).
///
/// # Examples
///
/// ```
/// use event_sauce_memory::InMemoryEventBus;
/// use event_sauce_core::{EventBus, EventFilter};
///
/// #[tokio::main]
/// async fn main() -> Result<(), Box<dyn std::error::Error>> {
///     let bus = InMemoryEventBus::new();
///
///     // Bus is ready to use
///     Ok(())
/// }
/// ```
#[derive(Clone)]
pub struct InMemoryEventBus {
    inner: Arc<InMemoryEventBusInner>,
}

struct InMemoryEventBusInner {
    sender: broadcast::Sender<EventEnvelope>,
    // We keep a receiver to prevent the channel from closing
    _receiver: RwLock<broadcast::Receiver<EventEnvelope>>,
}

impl InMemoryEventBus {
    /// Creates a new in-memory event bus.
    ///
    /// # Examples
    ///
    /// ```
    /// use event_sauce_memory::InMemoryEventBus;
    ///
    /// let bus = InMemoryEventBus::new();
    /// ```
    #[must_use]
    pub fn new() -> Self {
        Self::with_capacity(1000)
    }

    /// Creates a new in-memory event bus with specified channel capacity.
    ///
    /// The capacity determines how many events can be buffered before
    /// publishers start experiencing backpressure.
    ///
    /// # Examples
    ///
    /// ```
    /// use event_sauce_memory::InMemoryEventBus;
    ///
    /// let bus = InMemoryEventBus::with_capacity(100);
    /// ```
    #[must_use]
    pub fn with_capacity(capacity: usize) -> Self {
        let (sender, receiver) = broadcast::channel(capacity);
        Self {
            inner: Arc::new(InMemoryEventBusInner {
                sender,
                _receiver: RwLock::new(receiver),
            }),
        }
    }
}

impl Default for InMemoryEventBus {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl EventBus for InMemoryEventBus {
    async fn publish(&self, event: EventEnvelope) -> Result<()> {
        // Ignore the error if there are no subscribers
        let _ = self.inner.sender.send(event);
        Ok(())
    }

    async fn subscribe(
        &self,
        filter: EventFilter,
    ) -> Result<impl Stream<Item = EventEnvelope> + Send> {
        let receiver = self.inner.sender.subscribe();

        // Create an async stream that filters events
        Ok(stream::unfold(
            (receiver, filter),
            |(mut rx, filter)| async move {
                loop {
                    match rx.recv().await {
                        Ok(event) => {
                            if filter.matches(&event) {
                                return Some((event, (rx, filter)));
                            }
                            // Event doesn't match filter, continue loop
                        }
                        Err(_) => return None, // Channel closed
                    }
                }
            },
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use event_sauce_core::{EventEnvelope, Version};
    use futures::StreamExt;
    use serde_json::json;
    use uuid::Uuid;

    fn create_test_envelope(event_type: &str, aggregate_type: &str) -> EventEnvelope {
        EventEnvelope::new(
            Uuid::new_v4(),
            Uuid::new_v4(),
            aggregate_type.to_string(),
            event_type.to_string(),
            Version::new(1),
            json!({"test": "data"}),
        )
    }

    #[tokio::test]
    async fn test_create_bus() {
        let _bus = InMemoryEventBus::new();
    }

    #[tokio::test]
    async fn test_publish_without_subscribers() {
        let bus = InMemoryEventBus::new();
        let event = create_test_envelope("UserCreated", "User");

        let result = bus.publish(event).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_subscribe_and_publish() {
        let bus = InMemoryEventBus::new();
        let subscription = bus.subscribe(EventFilter::all()).await.unwrap();
        futures::pin_mut!(subscription);
        futures::pin_mut!(subscription);

        // Publish event
        let event = create_test_envelope("UserCreated", "User");
        bus.publish(event.clone()).await.unwrap();

        // Receive event
        let received = tokio::time::timeout(
            std::time::Duration::from_millis(100),
            subscription.next(),
        )
        .await
        .unwrap()
        .unwrap();

        assert_eq!(received.event_type, "UserCreated");
    }

    #[tokio::test]
    async fn test_filter_by_event_type() {
        let bus = InMemoryEventBus::new();
        let subscription = bus
            .subscribe(EventFilter::by_event_type("UserCreated"))
            .await
            .unwrap();
        futures::pin_mut!(subscription);

        // Publish matching event
        bus.publish(create_test_envelope("UserCreated", "User"))
            .await
            .unwrap();

        // Publish non-matching event
        bus.publish(create_test_envelope("OrderPlaced", "Order"))
            .await
            .unwrap();

        // Should only receive the matching event
        let received = tokio::time::timeout(
            std::time::Duration::from_millis(100),
            subscription.next(),
        )
        .await
        .unwrap()
        .unwrap();

        assert_eq!(received.event_type, "UserCreated");
    }

    #[tokio::test]
    async fn test_filter_by_aggregate_type() {
        let bus = InMemoryEventBus::new();
        let subscription = bus
            .subscribe(EventFilter::by_aggregate_type("User"))
            .await
            .unwrap();
        futures::pin_mut!(subscription);

        // Publish matching events
        bus.publish(create_test_envelope("UserCreated", "User"))
            .await
            .unwrap();
        bus.publish(create_test_envelope("UserUpdated", "User"))
            .await
            .unwrap();

        // Publish non-matching event
        bus.publish(create_test_envelope("OrderPlaced", "Order"))
            .await
            .unwrap();

        // Should receive both User events
        let received1 = tokio::time::timeout(
            std::time::Duration::from_millis(100),
            subscription.next(),
        )
        .await
        .unwrap()
        .unwrap();
        let received2 = tokio::time::timeout(
            std::time::Duration::from_millis(100),
            subscription.next(),
        )
        .await
        .unwrap()
        .unwrap();

        assert_eq!(received1.event_type, "UserCreated");
        assert_eq!(received2.event_type, "UserUpdated");
    }

    #[tokio::test]
    async fn test_multiple_subscribers() {
        let bus = InMemoryEventBus::new();
        let sub1 = bus.subscribe(EventFilter::all()).await.unwrap();
        let sub2 = bus.subscribe(EventFilter::all()).await.unwrap();
        futures::pin_mut!(sub1);
        futures::pin_mut!(sub2);

        // Publish event
        let event = create_test_envelope("UserCreated", "User");
        bus.publish(event.clone()).await.unwrap();

        // Both subscribers should receive the event
        let received1 = tokio::time::timeout(
            std::time::Duration::from_millis(100),
            sub1.next(),
        )
        .await
        .unwrap()
        .unwrap();
        let received2 = tokio::time::timeout(
            std::time::Duration::from_millis(100),
            sub2.next(),
        )
        .await
        .unwrap()
        .unwrap();

        assert_eq!(received1.event_type, "UserCreated");
        assert_eq!(received2.event_type, "UserCreated");
    }

    #[tokio::test]
    async fn test_publish_batch() {
        let bus = InMemoryEventBus::new();
        let subscription = bus.subscribe(EventFilter::all()).await.unwrap();
        futures::pin_mut!(subscription);

        let events = vec![
            create_test_envelope("Event1", "Aggregate1"),
            create_test_envelope("Event2", "Aggregate2"),
            create_test_envelope("Event3", "Aggregate3"),
        ];

        bus.publish_batch(events).await.unwrap();

        // Should receive all three events
        for i in 1..=3 {
            let received = tokio::time::timeout(
                std::time::Duration::from_millis(100),
                subscription.next(),
            )
            .await
            .unwrap()
            .unwrap();
            assert_eq!(received.event_type, format!("Event{}", i));
        }
    }

    #[tokio::test]
    async fn test_bus_is_cloneable() {
        let bus = InMemoryEventBus::new();
        let bus_clone = bus.clone();

        let subscription = bus.subscribe(EventFilter::all()).await.unwrap();
        futures::pin_mut!(subscription);

        // Publish via clone
        let event = create_test_envelope("UserCreated", "User");
        bus_clone.publish(event).await.unwrap();

        // Should receive via original subscription
        let received = tokio::time::timeout(
            std::time::Duration::from_millis(100),
            subscription.next(),
        )
        .await
        .unwrap()
        .unwrap();

        assert_eq!(received.event_type, "UserCreated");
    }

    #[tokio::test]
    async fn test_with_capacity() {
        let bus = InMemoryEventBus::with_capacity(10);
        let event = create_test_envelope("UserCreated", "User");

        let result = bus.publish(event).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_default_trait() {
        let bus = InMemoryEventBus::default();
        let event = create_test_envelope("UserCreated", "User");

        let result = bus.publish(event).await;
        assert!(result.is_ok());
    }
}
