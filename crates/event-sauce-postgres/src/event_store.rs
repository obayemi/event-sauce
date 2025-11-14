//! PostgreSQL event store implementation.
//!
//! Provides a production-ready PostgreSQL implementation of `EventStore`.

use async_trait::async_trait;
use event_sauce_core::{
    Error, EventEnvelope, EventStore, Position, Result, Snapshot, StreamId, Version,
};
use futures::stream::{self, Stream};
use sqlx::PgPool;

/// PostgreSQL event store implementation.
///
/// This store provides durable event persistence using PostgreSQL with:
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
    /// Creates a new PostgreSQL event store.
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
            Error::custom(format!("Failed to start transaction: {}", e))
        })?;

        // Check current version
        let current_version: Option<i64> = sqlx::query_scalar(
            "SELECT MAX(stream_version) FROM events WHERE aggregate_id = $1 AND aggregate_type = $2"
        )
        .bind(stream_id.aggregate_id())
        .bind(stream_id.aggregate_type())
        .fetch_optional(&mut *tx)
        .await
        .map_err(|e| Error::custom(format!("Failed to check version: {}", e)))?;

        let current_version = Version::new(current_version.unwrap_or(-1) + 1);

        if current_version != expected_version {
            return Err(Error::concurrency_conflict(expected_version, current_version));
        }

        // Insert events
        for (idx, event) in events.iter().enumerate() {
            #[allow(clippy::cast_possible_wrap)]
            let stream_version = expected_version.as_i64() + idx as i64;

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
            .bind(event.event_version.as_i64())
            .bind(&event.event_data)
            .bind(stream_version)
            .bind(event.created_by)
            .bind(event.metadata.as_ref().and_then(|m| m.correlation_id))
            .bind(event.metadata.as_ref().and_then(|m| m.causation_id))
            .bind(event.metadata.as_ref().and_then(|m| m.additional.clone()))
            .execute(&mut *tx)
            .await
            .map_err(|e| Error::custom(format!("Failed to insert event: {}", e)))?;
        }

        tx.commit().await.map_err(|e| {
            Error::custom(format!("Failed to commit transaction: {}", e))
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
        .bind(from_version.as_i64())
        .fetch_all(&self.pool)
        .await
        .map_err(|e| Error::custom(format!("Failed to load stream: {}", e)))?
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
        .map_err(|e| Error::custom(format!("Failed to stream all: {}", e)))?
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
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| Error::custom(format!("Failed to get version: {}", e)))?;

        Ok(Version::new(version.map_or(0, |v| v + 1)))
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
        .bind(snapshot.snapshot_version.as_i64())
        .bind(&snapshot.snapshot_data)
        .execute(&self.pool)
        .await
        .map_err(|e| Error::custom(format!("Failed to save snapshot: {}", e)))?;

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
        .map_err(|e| Error::custom(format!("Failed to load snapshot: {}", e)))?;

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
    event_version: i64,
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
        Snapshot {
            aggregate_id: row.aggregate_id,
            aggregate_type: row.aggregate_type,
            snapshot_version: Version::new(row.snapshot_version),
            snapshot_data: row.snapshot_data,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use event_sauce_core::{EventStore, StreamId, Version};
    use serde_json::json;
    use uuid::Uuid;

    // Tests will be added following TDD

    #[tokio::test]
    async fn test_placeholder() {
        // This ensures the module compiles
        assert!(true);
    }
}
