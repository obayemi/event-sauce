//! `PostgreSQL` event store implementation.
//!
//! Provides a production-ready `PostgreSQL` implementation of `EventStore`.

use async_trait::async_trait;
use event_sauce_core::{
    Error, EventEnvelope, EventStore, Position, Result, Snapshot, StreamId, Version,
};
use futures::stream::{self, Stream};
use sqlx::PgPool;

/// `PostgreSQL` event store implementation.
///
/// This store provides durable event persistence using `PostgreSQL` with:
/// - Optimistic concurrency control
/// - Efficient streaming queries
/// - Snapshot support
/// - Full ACID guarantees
///
/// # Examples
///
/// ```ignore
/// use event_sauce_postgres::PostgresEventStore;
/// use sqlx::PgPool;
///
/// #[tokio::main]
/// async fn main() -> Result<(), Box<dyn std::error::Error>> {
///     let pool = PgPool::connect("postgresql://localhost/events").await?;
///     let store = PostgresEventStore::new(pool);
///
///     // Store is ready to use
///     Ok(())
/// }
/// ```
#[derive(Clone)]
pub struct PostgresEventStore {
    pool: PgPool,
}

impl PostgresEventStore {
    /// Creates a new `PostgreSQL` event store.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// use event_sauce_postgres::PostgresEventStore;
    /// use sqlx::PgPool;
    ///
    /// let pool = PgPool::connect("postgresql://localhost/events").await?;
    /// let store = PostgresEventStore::new(pool);
    /// ```
    #[must_use]
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    /// Returns a reference to the underlying connection pool.
    #[must_use]
    pub fn pool(&self) -> &PgPool {
        &self.pool
    }
}

#[async_trait]
impl EventStore for PostgresEventStore {
    async fn append(
        &self,
        stream_id: StreamId,
        events: Vec<EventEnvelope>,
        expected_version: Version,
    ) -> Result<()> {
        if events.is_empty() {
            return Ok(());
        }

        let mut tx = self.pool.begin().await.map_err(|e| {
            Error::custom(format!("Failed to start transaction: {e}"))
        })?;

        // Check current version
        let current_version: Option<i64> = sqlx::query_scalar(
            "SELECT MAX(stream_version) FROM events WHERE aggregate_id = $1 AND aggregate_type = $2"
        )
        .bind(stream_id.aggregate_id())
        .bind(stream_id.aggregate_type())
        .fetch_one(&mut *tx)
        .await
        .map_err(|e| Error::custom(format!("Failed to check version: {e}")))?;

        #[allow(clippy::cast_possible_truncation)]
        let current_version = Version::new((current_version.unwrap_or(-1) + 1) as i32);

        if current_version != expected_version {
            return Err(Error::concurrency_conflict(expected_version, current_version));
        }

        // Insert events
        for (idx, event) in events.iter().enumerate() {
            #[allow(clippy::cast_possible_wrap)]
            let stream_version = i64::from(expected_version.as_i32()) + idx as i64;

            sqlx::query(
                "INSERT INTO events (
                    event_id, aggregate_id, aggregate_type, event_type, event_version,
                    event_data, stream_version, created_by, correlation_id, causation_id, metadata
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)"
            )
            .bind(event.id)
            .bind(event.aggregate_id)
            .bind(&event.aggregate_type)
            .bind(&event.event_type)
            .bind(event.event_version.as_i32())
            .bind(&event.event_data)
            .bind(stream_version)
            .bind(event.created_by)
            .bind(event.metadata.as_ref().and_then(|m| m.correlation_id))
            .bind(event.metadata.as_ref().and_then(|m| m.causation_id))
            .bind(event.metadata.as_ref().and_then(|m| m.additional.clone()))
            .execute(&mut *tx)
            .await
            .map_err(|e| Error::custom(format!("Failed to insert event: {e}")))?;
        }

        tx.commit().await.map_err(|e| {
            Error::custom(format!("Failed to commit transaction: {e}"))
        })?;

        Ok(())
    }

    async fn load_stream(
        &self,
        stream_id: StreamId,
        from_version: Version,
    ) -> Result<impl Stream<Item = Result<EventEnvelope>> + Send> {
        let events: Vec<EventEnvelope> = sqlx::query_as::<_, EventRow>(
            "SELECT event_id, aggregate_id, aggregate_type, event_type, event_version,
                    event_data, created_by, created_at, correlation_id, causation_id, metadata
             FROM events
             WHERE aggregate_id = $1 AND aggregate_type = $2 AND stream_version >= $3
             ORDER BY stream_version ASC"
        )
        .bind(stream_id.aggregate_id())
        .bind(stream_id.aggregate_type())
        .bind(i64::from(from_version.as_i32()))
        .fetch_all(&self.pool)
        .await
        .map_err(|e| Error::custom(format!("Failed to load stream: {e}")))?
        .into_iter()
        .map(Into::into)
        .collect();

        Ok(stream::iter(events.into_iter().map(Ok)))
    }

    async fn stream_all(
        &self,
        from_position: Position,
    ) -> Result<impl Stream<Item = Result<EventEnvelope>> + Send> {
        let events: Vec<EventEnvelope> = sqlx::query_as::<_, EventRow>(
            "SELECT event_id, aggregate_id, aggregate_type, event_type, event_version,
                    event_data, created_by, created_at, correlation_id, causation_id, metadata
             FROM events
             WHERE id > $1
             ORDER BY id ASC"
        )
        .bind(from_position.as_i64())
        .fetch_all(&self.pool)
        .await
        .map_err(|e| Error::custom(format!("Failed to stream all: {e}")))?
        .into_iter()
        .map(Into::into)
        .collect();

        Ok(stream::iter(events.into_iter().map(Ok)))
    }

    async fn get_version(&self, stream_id: StreamId) -> Result<Version> {
        let version: Option<i64> = sqlx::query_scalar(
            "SELECT MAX(stream_version) FROM events WHERE aggregate_id = $1 AND aggregate_type = $2"
        )
        .bind(stream_id.aggregate_id())
        .bind(stream_id.aggregate_type())
        .fetch_one(&self.pool)
        .await
        .map_err(|e| Error::custom(format!("Failed to get version: {e}")))?;

        #[allow(clippy::cast_possible_truncation)]
        let next_version = version.map_or(0, |v| v + 1) as i32;
        Ok(Version::new(next_version))
    }

    async fn save_snapshot(&self, snapshot: Snapshot) -> Result<()> {
        sqlx::query(
            "INSERT INTO snapshots (aggregate_id, aggregate_type, snapshot_version, snapshot_data)
             VALUES ($1, $2, $3, $4)
             ON CONFLICT (aggregate_id, aggregate_type)
             DO UPDATE SET snapshot_version = $3, snapshot_data = $4, created_at = NOW()"
        )
        .bind(snapshot.aggregate_id)
        .bind(&snapshot.aggregate_type)
        .bind(i64::from(snapshot.snapshot_version.as_i32()))
        .bind(&snapshot.snapshot_data)
        .execute(&self.pool)
        .await
        .map_err(|e| Error::custom(format!("Failed to save snapshot: {e}")))?;

        Ok(())
    }

    async fn load_snapshot(&self, stream_id: StreamId) -> Result<Option<Snapshot>> {
        let row: Option<SnapshotRow> = sqlx::query_as::<_, SnapshotRow>(
            "SELECT aggregate_id, aggregate_type, snapshot_version, snapshot_data
             FROM snapshots
             WHERE aggregate_id = $1 AND aggregate_type = $2"
        )
        .bind(stream_id.aggregate_id())
        .bind(stream_id.aggregate_type())
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| Error::custom(format!("Failed to load snapshot: {e}")))?;

        Ok(row.map(Into::into))
    }
}

// Database row types
#[derive(sqlx::FromRow)]
struct EventRow {
    event_id: uuid::Uuid,
    aggregate_id: uuid::Uuid,
    aggregate_type: String,
    event_type: String,
    event_version: i32,
    event_data: serde_json::Value,
    created_by: Option<uuid::Uuid>,
    created_at: chrono::DateTime<chrono::Utc>,
    correlation_id: Option<uuid::Uuid>,
    causation_id: Option<uuid::Uuid>,
    metadata: Option<serde_json::Value>,
}

impl From<EventRow> for EventEnvelope {
    fn from(row: EventRow) -> Self {
        let metadata = if row.correlation_id.is_some() || row.causation_id.is_some() || row.metadata.is_some() {
            Some(event_sauce_core::EventMetadata {
                correlation_id: row.correlation_id,
                causation_id: row.causation_id,
                timestamp: row.created_at,
                additional: row.metadata,
            })
        } else {
            None
        };

        EventEnvelope {
            id: row.event_id,
            aggregate_id: row.aggregate_id,
            aggregate_type: row.aggregate_type,
            event_type: row.event_type,
            event_version: Version::new(row.event_version),
            event_data: row.event_data,
            created_by: row.created_by,
            created_at: row.created_at,
            metadata,
        }
    }
}

#[derive(sqlx::FromRow)]
struct SnapshotRow {
    aggregate_id: uuid::Uuid,
    aggregate_type: String,
    snapshot_version: i64,
    snapshot_data: serde_json::Value,
}

impl From<SnapshotRow> for Snapshot {
    fn from(row: SnapshotRow) -> Self {
        #[allow(clippy::cast_possible_truncation)]
        let snapshot_version = Version::new(row.snapshot_version as i32);

        Snapshot {
            aggregate_id: row.aggregate_id,
            aggregate_type: row.aggregate_type,
            snapshot_version,
            snapshot_data: row.snapshot_data,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use event_sauce_core::{
        EventEnvelope, EventStore, Position, Snapshot, StreamId, Version,
    };
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
            let host = container.get_host().await.map_err(|e| {
                Error::custom(format!("Failed to get container host: {e}"))
            })?;
            let port = container.get_host_port_ipv4(5432).await.map_err(|e| {
                Error::custom(format!("Failed to get container port: {e}"))
            })?;

            let connection_string = format!(
                "postgresql://postgres:postgres@{}:{}/postgres",
                host, port
            );

            // Connect to database
            let pool = PgPool::connect(&connection_string).await.map_err(|e| {
                Error::custom(format!("Failed to connect to test database: {e}"))
            })?;

            // Run migrations
            sqlx::migrate!("./migrations")
                .run(&pool)
                .await
                .map_err(|e| Error::custom(format!("Failed to run migrations: {e}")))?;

            Ok(Self { pool, container })
        }

        fn pool(&self) -> &PgPool {
            &self.pool
        }
    }

    fn create_test_envelope(event_type: &str, aggregate_id: Uuid) -> EventEnvelope {
        create_test_envelope_with_type(event_type, "User", aggregate_id)
    }

    fn create_test_envelope_with_type(event_type: &str, aggregate_type: &str, aggregate_id: Uuid) -> EventEnvelope {
        EventEnvelope::new(
            Uuid::new_v4(),
            aggregate_id,
            aggregate_type.to_string(),
            event_type.to_string(),
            Version::new(1),
            json!({"data": "test"}),
        )
    }

    #[tokio::test]
    async fn test_create_store() {
        let db = TestDatabase::new().await.unwrap();
        let _store = PostgresEventStore::new(db.pool().clone());
    }

    #[tokio::test]
    async fn test_append_to_new_stream() {
        let db = TestDatabase::new().await.unwrap();
        let store = PostgresEventStore::new(db.pool().clone());
        let stream_id = StreamId::new("User", Uuid::new_v4());
        let event = create_test_envelope("UserCreated", stream_id.aggregate_id());

        let result = store
            .append(stream_id, vec![event], Version::initial())
            .await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_load_stream() {
        let db = TestDatabase::new().await.unwrap();
        let store = PostgresEventStore::new(db.pool().clone());
        let aggregate_id = Uuid::new_v4();
        let stream_id = StreamId::new("User", aggregate_id);
        let event = create_test_envelope("UserCreated", aggregate_id);

        store
            .append(stream_id.clone(), vec![event.clone()], Version::initial())
            .await
            .unwrap();

        let mut stream = store
            .load_stream(stream_id, Version::initial())
            .await
            .unwrap();
        let loaded = stream.next().await.unwrap().unwrap();

        assert_eq!(loaded.event_type, "UserCreated");
    }

    #[tokio::test]
    async fn test_get_version() {
        let db = TestDatabase::new().await.unwrap();
        let store = PostgresEventStore::new(db.pool().clone());
        let stream_id = StreamId::new("User", Uuid::new_v4());

        // New stream should have initial version
        let version = store.get_version(stream_id.clone()).await.unwrap();
        assert_eq!(version, Version::initial());
    }

    #[tokio::test]
    async fn test_concurrency_conflict() {
        let db = TestDatabase::new().await.unwrap();
        let store = PostgresEventStore::new(db.pool().clone());
        let aggregate_id = Uuid::new_v4();
        let stream_id = StreamId::new("User", aggregate_id);
        let event1 = create_test_envelope("UserCreated", aggregate_id);
        let event2 = create_test_envelope("UserUpdated", aggregate_id);

        // Append first event
        store
            .append(stream_id.clone(), vec![event1], Version::initial())
            .await
            .unwrap();

        // Try to append with wrong version - should fail
        let result = store
            .append(stream_id, vec![event2], Version::initial())
            .await;
        assert!(result.is_err());
        assert!(result.unwrap_err().is_concurrency_conflict());
    }

    #[tokio::test]
    async fn test_stream_all() {
        let db = TestDatabase::new().await.unwrap();
        let store = PostgresEventStore::new(db.pool().clone());
        let id1 = Uuid::new_v4();
        let id2 = Uuid::new_v4();
        let stream1 = StreamId::new("User", id1);
        let stream2 = StreamId::new("Order", id2);

        store
            .append(
                stream1,
                vec![create_test_envelope("UserCreated", id1)],
                Version::initial(),
            )
            .await
            .unwrap();
        store
            .append(
                stream2,
                vec![create_test_envelope_with_type("OrderPlaced", "Order", id2)],
                Version::initial(),
            )
            .await
            .unwrap();

        let all_events = store.stream_all(Position::start()).await.unwrap();
        let events: Vec<_> = all_events.collect::<Vec<_>>().await;

        assert_eq!(events.len(), 2);
    }

    #[tokio::test]
    async fn test_save_and_load_snapshot() {
        let db = TestDatabase::new().await.unwrap();
        let store = PostgresEventStore::new(db.pool().clone());
        let aggregate_id = Uuid::new_v4();
        let stream_id = StreamId::new("User", aggregate_id);

        let snapshot = Snapshot::new(
            aggregate_id,
            "User".to_string(),
            Version::new(10),
            json!({"name": "Alice"}),
        );

        store.save_snapshot(snapshot.clone()).await.unwrap();
        let loaded = store.load_snapshot(stream_id).await.unwrap();

        assert!(loaded.is_some());
        assert_eq!(loaded.unwrap().snapshot_version, Version::new(10));
    }

    #[tokio::test]
    async fn test_append_multiple_events() {
        let db = TestDatabase::new().await.unwrap();
        let store = PostgresEventStore::new(db.pool().clone());
        let aggregate_id = Uuid::new_v4();
        let stream_id = StreamId::new("User", aggregate_id);

        let events = vec![
            create_test_envelope("UserCreated", aggregate_id),
            create_test_envelope("UserUpdated", aggregate_id),
            create_test_envelope("UserVerified", aggregate_id),
        ];

        store
            .append(stream_id.clone(), events, Version::initial())
            .await
            .unwrap();

        let version = store.get_version(stream_id).await.unwrap();
        assert_eq!(version, Version::new(3));
    }

    #[tokio::test]
    async fn test_load_stream_from_version() {
        let db = TestDatabase::new().await.unwrap();
        let store = PostgresEventStore::new(db.pool().clone());
        let aggregate_id = Uuid::new_v4();
        let stream_id = StreamId::new("User", aggregate_id);

        // Add 5 events
        for i in 0..5 {
            let event = create_test_envelope(&format!("Event{}", i), aggregate_id);
            store
                .append(stream_id.clone(), vec![event], Version::new(i))
                .await
                .unwrap();
        }

        // Load from version 2
        let stream = store
            .load_stream(stream_id, Version::new(2))
            .await
            .unwrap();
        let events: Vec<_> = stream.collect::<Vec<_>>().await;

        assert_eq!(events.len(), 3); // Should get events 2, 3, 4
    }

    #[tokio::test]
    async fn test_stream_all_from_position() {
        let db = TestDatabase::new().await.unwrap();
        let store = PostgresEventStore::new(db.pool().clone());

        for i in 0..5 {
            let aggregate_id = Uuid::new_v4();
            let stream_id = StreamId::new("User", aggregate_id);
            let event = create_test_envelope(&format!("Event{}", i), aggregate_id);

            store
                .append(stream_id, vec![event], Version::initial())
                .await
                .unwrap();
        }

        // Stream from position 2
        let stream = store.stream_all(Position::new(2)).await.unwrap();
        let events: Vec<_> = stream.collect::<Vec<_>>().await;

        assert_eq!(events.len(), 3); // Should get events at positions 2, 3, 4
    }

    #[tokio::test]
    async fn test_load_nonexistent_stream() {
        let db = TestDatabase::new().await.unwrap();
        let store = PostgresEventStore::new(db.pool().clone());
        let stream_id = StreamId::new("User", Uuid::new_v4());

        let stream = store
            .load_stream(stream_id, Version::initial())
            .await
            .unwrap();
        let events: Vec<_> = stream.collect::<Vec<_>>().await;

        assert_eq!(events.len(), 0);
    }

    #[tokio::test]
    async fn test_load_nonexistent_snapshot() {
        let db = TestDatabase::new().await.unwrap();
        let store = PostgresEventStore::new(db.pool().clone());
        let stream_id = StreamId::new("User", Uuid::new_v4());

        let snapshot = store.load_snapshot(stream_id).await.unwrap();
        assert!(snapshot.is_none());
    }

    #[tokio::test]
    async fn test_store_is_cloneable() {
        let db = TestDatabase::new().await.unwrap();
        let store = PostgresEventStore::new(db.pool().clone());
        let store_clone = store.clone();

        let aggregate_id = Uuid::new_v4();
        let stream_id = StreamId::new("User", aggregate_id);
        let event = create_test_envelope("UserCreated", aggregate_id);

        // Append via original store
        store
            .append(stream_id.clone(), vec![event], Version::initial())
            .await
            .unwrap();

        // Read via clone - should see the same data (same database)
        let version = store_clone.get_version(stream_id).await.unwrap();
        assert_eq!(version, Version::new(1));
    }

    #[tokio::test]
    async fn test_version_increments_correctly() {
        let db = TestDatabase::new().await.unwrap();
        let store = PostgresEventStore::new(db.pool().clone());
        let aggregate_id = Uuid::new_v4();
        let stream_id = StreamId::new("User", aggregate_id);

        // Initial version
        let v0 = store.get_version(stream_id.clone()).await.unwrap();
        assert_eq!(v0, Version::initial());

        // After first append
        store
            .append(
                stream_id.clone(),
                vec![create_test_envelope("Event1", aggregate_id)],
                Version::initial(),
            )
            .await
            .unwrap();
        let v1 = store.get_version(stream_id.clone()).await.unwrap();
        assert_eq!(v1, Version::new(1));

        // After second append
        store
            .append(
                stream_id.clone(),
                vec![create_test_envelope("Event2", aggregate_id)],
                Version::new(1),
            )
            .await
            .unwrap();
        let v2 = store.get_version(stream_id).await.unwrap();
        assert_eq!(v2, Version::new(2));
    }

    #[tokio::test]
    async fn test_snapshot_overwrites() {
        let db = TestDatabase::new().await.unwrap();
        let store = PostgresEventStore::new(db.pool().clone());
        let aggregate_id = Uuid::new_v4();
        let stream_id = StreamId::new("User", aggregate_id);

        // Save first snapshot
        let snapshot1 = Snapshot::new(
            aggregate_id,
            "User".to_string(),
            Version::new(5),
            json!({"version": 1}),
        );
        store.save_snapshot(snapshot1).await.unwrap();

        // Save second snapshot (should overwrite via UPSERT)
        let snapshot2 = Snapshot::new(
            aggregate_id,
            "User".to_string(),
            Version::new(10),
            json!({"version": 2}),
        );
        store.save_snapshot(snapshot2).await.unwrap();

        // Should get the latest snapshot
        let loaded = store.load_snapshot(stream_id).await.unwrap().unwrap();
        assert_eq!(loaded.snapshot_version, Version::new(10));
    }

    #[tokio::test]
    async fn test_append_empty_events() {
        let db = TestDatabase::new().await.unwrap();
        let store = PostgresEventStore::new(db.pool().clone());
        let stream_id = StreamId::new("User", Uuid::new_v4());

        // Appending empty events should succeed (no-op)
        let result = store.append(stream_id, vec![], Version::initial()).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_concurrent_appends_from_different_streams() {
        let db = TestDatabase::new().await.unwrap();
        let store = PostgresEventStore::new(db.pool().clone());

        let id1 = Uuid::new_v4();
        let id2 = Uuid::new_v4();
        let stream1 = StreamId::new("User", id1);
        let stream2 = StreamId::new("User", id2);

        // Concurrent appends to different streams should both succeed
        let result1 = store.append(
            stream1.clone(),
            vec![create_test_envelope("Event1", id1)],
            Version::initial(),
        );
        let result2 = store.append(
            stream2.clone(),
            vec![create_test_envelope("Event2", id2)],
            Version::initial(),
        );

        let (r1, r2) = tokio::join!(result1, result2);
        assert!(r1.is_ok());
        assert!(r2.is_ok());
    }
}
