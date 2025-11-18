//! `PostgreSQL` event bus implementation using `LISTEN`/`NOTIFY`.
//!
//! Provides a production-ready `PostgreSQL` implementation of `EventBus`.

use async_trait::async_trait;
use event_sauce_core::{Error, EventBus, EventEnvelope, EventFilter, Result};
use futures::stream::{self, Stream};
use sqlx::postgres::{PgListener, PgPool};
use std::sync::Arc;

/// `PostgreSQL` event bus implementation using `LISTEN`/`NOTIFY`.
///
/// This bus provides real-time event delivery using `PostgreSQL`'s native
/// `LISTEN`/`NOTIFY` mechanism. Events are serialized as JSON and delivered
/// to all active subscribers matching the filter.
///
/// # Features
///
/// - Real-time event delivery
/// - Multiple concurrent subscribers
/// - Filtering support
/// - Connection pooling
/// - Automatic reconnection
///
/// # Examples
///
/// ```ignore
/// use event_sauce_postgres::PostgresEventBus;
/// use event_sauce_core::{EventBus, EventFilter};
/// use sqlx::PgPool;
///
/// #[tokio::main]
/// async fn main() -> Result<(), Box<dyn std::error::Error>> {
///     let pool = PgPool::connect("postgresql://localhost/events").await?;
///     let bus = PostgresEventBus::new(pool)?;
///
///     // Subscribe to events
///     let filter = EventFilter::by_event_type("UserCreated");
///     let mut subscription = bus.subscribe(filter).await?;
///
///     Ok(())
/// }
/// ```
#[derive(Clone)]
pub struct PostgresEventBus {
    inner: Arc<PostgresEventBusInner>,
}

struct PostgresEventBusInner {
    pool: PgPool,
    channel: String,
}

impl PostgresEventBus {
    /// Creates a new `PostgreSQL` event bus.
    ///
    /// Uses the default channel name `"event_sauce_events"`.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// use event_sauce_postgres::PostgresEventBus;
    /// use sqlx::PgPool;
    ///
    /// let pool = PgPool::connect("postgresql://localhost/events").await?;
    /// let bus = PostgresEventBus::new(pool)?;
    /// ```
    ///
    /// # Errors
    ///
    /// Returns an error if the event bus cannot be created.
    pub fn new(pool: PgPool) -> Result<Self> {
        Self::with_channel(pool, "event_sauce_events")
    }

    /// Creates a new `PostgreSQL` event bus with a custom channel name.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// use event_sauce_postgres::PostgresEventBus;
    /// use sqlx::PgPool;
    ///
    /// let pool = PgPool::connect("postgresql://localhost/events").await?;
    /// let bus = PostgresEventBus::with_channel(pool, "my_events")?;
    /// ```
    ///
    /// # Errors
    ///
    /// Returns an error if the event bus cannot be created.
    pub fn with_channel(pool: PgPool, channel: impl Into<String>) -> Result<Self> {
        Ok(Self {
            inner: Arc::new(PostgresEventBusInner {
                pool,
                channel: channel.into(),
            }),
        })
    }

    /// Returns a reference to the underlying connection pool.
    #[must_use]
    pub fn pool(&self) -> &PgPool {
        &self.inner.pool
    }
}

#[async_trait]
impl EventBus for PostgresEventBus {
    async fn publish(&self, event: EventEnvelope) -> Result<()> {
        // Serialize event to JSON
        let payload = serde_json::to_string(&event)
            .map_err(|e| Error::custom(format!("Failed to serialize event: {e}")))?;

        // Send NOTIFY with event payload
        // Note: NOTIFY does not support parameterized queries, so we must escape the payload
        let escaped_payload = payload.replace('\\', "\\\\").replace('\'', "''");
        sqlx::query(&format!(
            "NOTIFY {}, '{}'",
            self.inner.channel, escaped_payload
        ))
        .execute(&self.inner.pool)
        .await
        .map_err(|e| Error::custom(format!("Failed to publish event: {e}")))?;

        Ok(())
    }

    async fn subscribe(
        &self,
        filter: EventFilter,
    ) -> Result<impl Stream<Item = EventEnvelope> + Send> {
        // Create a new listener for this subscription
        let mut listener = PgListener::connect_with(&self.inner.pool)
            .await
            .map_err(|e| Error::custom(format!("Failed to create listener: {e}")))?;

        // Listen to the channel
        listener
            .listen(&self.inner.channel)
            .await
            .map_err(|e| Error::custom(format!("Failed to listen on channel: {e}")))?;

        // Create stream that filters events
        Ok(stream::unfold(
            (listener, filter),
            |(mut listener, filter)| async move {
                loop {
                    match listener.recv().await {
                        Ok(notification) => {
                            // Deserialize event
                            if let Ok(event) =
                                serde_json::from_str::<EventEnvelope>(notification.payload())
                            {
                                // Check if event matches filter
                                if filter.matches(&event) {
                                    return Some((event, (listener, filter)));
                                }
                                // Event doesn't match filter, continue loop
                            }
                            // Failed to deserialize, skip this event
                        }
                        Err(_) => {
                            // Connection error or channel closed
                            return None;
                        }
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
    use sqlx::PgPool;
    use testcontainers::ImageExt;
    use testcontainers_modules::{postgres::Postgres, testcontainers::runners::AsyncRunner};
    use uuid::Uuid;

    /// Test database helper using testcontainers for isolated PostgreSQL testing.
    struct TestDatabase {
        pool: PgPool,
        #[allow(dead_code)]
        container: testcontainers_modules::testcontainers::ContainerAsync<Postgres>,
    }

    impl TestDatabase {
        /// Creates a new test database with testcontainers.
        async fn new() -> Result<Self> {
            // Start PostgreSQL container
            let container = Postgres::default()
                .with_tag("16-alpine")
                .start()
                .await
                .map_err(|e| Error::custom(format!("Failed to start PostgreSQL container: {e}")))?;

            // Get connection string
            let host = container
                .get_host()
                .await
                .map_err(|e| Error::custom(format!("Failed to get container host: {e}")))?;
            let port = container
                .get_host_port_ipv4(5432)
                .await
                .map_err(|e| Error::custom(format!("Failed to get container port: {e}")))?;

            let connection_string =
                format!("postgresql://postgres:postgres@{}:{}/postgres", host, port);

            // Connect to database
            let pool = PgPool::connect(&connection_string)
                .await
                .map_err(|e| Error::custom(format!("Failed to connect to test database: {e}")))?;

            Ok(Self { pool, container })
        }

        fn pool(&self) -> &PgPool {
            &self.pool
        }
    }

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
        let db = TestDatabase::new().await.unwrap();
        let _bus = PostgresEventBus::new(db.pool().clone()).unwrap();
    }

    #[tokio::test]
    async fn test_publish_without_subscribers() {
        let db = TestDatabase::new().await.unwrap();
        let bus = PostgresEventBus::new(db.pool().clone()).unwrap();
        let event = create_test_envelope("UserCreated", "User");

        let result = bus.publish(event).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_subscribe_and_publish() {
        let db = TestDatabase::new().await.unwrap();
        let bus = PostgresEventBus::new(db.pool().clone()).unwrap();
        let subscription = bus.subscribe(EventFilter::all()).await.unwrap();
        futures::pin_mut!(subscription);

        // Publish event
        let event = create_test_envelope("UserCreated", "User");
        bus.publish(event.clone()).await.unwrap();

        // Receive event
        let received =
            tokio::time::timeout(std::time::Duration::from_millis(1000), subscription.next())
                .await
                .unwrap()
                .unwrap();

        assert_eq!(received.event_type, "UserCreated");
    }

    #[tokio::test]
    async fn test_filter_by_event_type() {
        let db = TestDatabase::new().await.unwrap();
        let bus = PostgresEventBus::new(db.pool().clone()).unwrap();
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
        let received =
            tokio::time::timeout(std::time::Duration::from_millis(1000), subscription.next())
                .await
                .unwrap()
                .unwrap();

        assert_eq!(received.event_type, "UserCreated");
    }

    #[tokio::test]
    async fn test_filter_by_aggregate_type() {
        let db = TestDatabase::new().await.unwrap();
        let bus = PostgresEventBus::new(db.pool().clone()).unwrap();
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
        let received1 =
            tokio::time::timeout(std::time::Duration::from_millis(1000), subscription.next())
                .await
                .unwrap()
                .unwrap();
        let received2 =
            tokio::time::timeout(std::time::Duration::from_millis(1000), subscription.next())
                .await
                .unwrap()
                .unwrap();

        assert_eq!(received1.event_type, "UserCreated");
        assert_eq!(received2.event_type, "UserUpdated");
    }

    #[tokio::test]
    async fn test_multiple_subscribers() {
        let db = TestDatabase::new().await.unwrap();
        let bus = PostgresEventBus::new(db.pool().clone()).unwrap();
        let sub1 = bus.subscribe(EventFilter::all()).await.unwrap();
        let sub2 = bus.subscribe(EventFilter::all()).await.unwrap();
        futures::pin_mut!(sub1);
        futures::pin_mut!(sub2);

        // Publish event
        let event = create_test_envelope("UserCreated", "User");
        bus.publish(event.clone()).await.unwrap();

        // Both subscribers should receive the event
        let received1 = tokio::time::timeout(std::time::Duration::from_millis(1000), sub1.next())
            .await
            .unwrap()
            .unwrap();
        let received2 = tokio::time::timeout(std::time::Duration::from_millis(1000), sub2.next())
            .await
            .unwrap()
            .unwrap();

        assert_eq!(received1.event_type, "UserCreated");
        assert_eq!(received2.event_type, "UserCreated");
    }

    #[tokio::test]
    async fn test_publish_batch() {
        let db = TestDatabase::new().await.unwrap();
        let bus = PostgresEventBus::new(db.pool().clone()).unwrap();
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
            let received =
                tokio::time::timeout(std::time::Duration::from_millis(1000), subscription.next())
                    .await
                    .unwrap()
                    .unwrap();
            assert_eq!(received.event_type, format!("Event{}", i));
        }
    }

    #[tokio::test]
    async fn test_bus_is_cloneable() {
        let db = TestDatabase::new().await.unwrap();
        let bus = PostgresEventBus::new(db.pool().clone()).unwrap();
        let bus_clone = bus.clone();

        let subscription = bus.subscribe(EventFilter::all()).await.unwrap();
        futures::pin_mut!(subscription);

        // Publish via clone
        let event = create_test_envelope("UserCreated", "User");
        bus_clone.publish(event).await.unwrap();

        // Should receive via original subscription
        let received =
            tokio::time::timeout(std::time::Duration::from_millis(1000), subscription.next())
                .await
                .unwrap()
                .unwrap();

        assert_eq!(received.event_type, "UserCreated");
    }

    #[tokio::test]
    async fn test_with_custom_channel() {
        let db = TestDatabase::new().await.unwrap();
        let bus = PostgresEventBus::with_channel(db.pool().clone(), "custom_channel").unwrap();

        let subscription = bus.subscribe(EventFilter::all()).await.unwrap();
        futures::pin_mut!(subscription);

        let event = create_test_envelope("UserCreated", "User");
        bus.publish(event).await.unwrap();

        let received =
            tokio::time::timeout(std::time::Duration::from_millis(1000), subscription.next())
                .await
                .unwrap()
                .unwrap();

        assert_eq!(received.event_type, "UserCreated");
    }

    #[tokio::test]
    async fn test_subscribe_before_publish() {
        let db = TestDatabase::new().await.unwrap();
        let bus = PostgresEventBus::new(db.pool().clone()).unwrap();

        // Subscribe first
        let subscription = bus.subscribe(EventFilter::all()).await.unwrap();
        futures::pin_mut!(subscription);

        // Small delay to ensure subscription is established
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        // Then publish
        let event = create_test_envelope("UserCreated", "User");
        bus.publish(event).await.unwrap();

        // Should receive event
        let received =
            tokio::time::timeout(std::time::Duration::from_millis(1000), subscription.next())
                .await
                .unwrap()
                .unwrap();

        assert_eq!(received.event_type, "UserCreated");
    }
}
