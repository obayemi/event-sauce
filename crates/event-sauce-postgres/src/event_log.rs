//! `PostgreSQL` event log query implementation.
//!
//! Provides paginated, filtered access to the global event log
//! stored in `PostgreSQL`.

use async_trait::async_trait;
use event_sauce_core::{
    AggregateType, EventEnvelope, EventLogEntry, EventLogPage, EventLogParams, EventLogQuery,
    EventMetadata, EventVersion, Position, Result,
};
use sqlx::PgPool;

/// `PostgreSQL` implementation of [`EventLogQuery`].
///
/// Queries the `{schema}.events` table with dynamic SQL filtering
/// and parameterized bind values.
///
/// # Examples
///
/// ```ignore
/// use event_sauce_postgres::PostgresEventLogQuery;
/// use event_sauce_core::{EventLogQuery, EventLogParams};
/// use sqlx::PgPool;
///
/// let pool = PgPool::connect("postgresql://localhost/events").await?;
/// let query = PostgresEventLogQuery::new(pool, "event_sauce".to_string());
///
/// let page = query.query_events(EventLogParams::default()).await?;
/// println!("Total events: {}", page.total_count);
/// ```
pub struct PostgresEventLogQuery {
    pool: PgPool,
    schema: String,
}

impl PostgresEventLogQuery {
    /// Creates a new `PostgresEventLogQuery`.
    ///
    /// # Arguments
    ///
    /// * `pool` - The `PostgreSQL` connection pool.
    /// * `schema` - The schema name where the events table lives (e.g., `"event_sauce"`).
    #[must_use]
    pub fn new(pool: PgPool, schema: String) -> Self {
        Self { pool, schema }
    }
}

#[async_trait]
impl EventLogQuery for PostgresEventLogQuery {
    async fn query_events(&self, params: EventLogParams) -> Result<EventLogPage> {
        let mut conditions = Vec::new();
        let mut bind_idx = 1u32;

        if params.aggregate_type.is_some() {
            conditions.push(format!("aggregate_type = ${bind_idx}"));
            bind_idx += 1;
        }
        if params.event_type.is_some() {
            conditions.push(format!("event_type = ${bind_idx}"));
            bind_idx += 1;
        }
        if params.aggregate_id.is_some() {
            conditions.push(format!("aggregate_id = ${bind_idx}"));
            bind_idx += 1;
        }
        if params.created_by.is_some() {
            conditions.push(format!("created_by = ${bind_idx}"));
            bind_idx += 1;
        }
        if params.from_date.is_some() {
            conditions.push(format!("created_at >= ${bind_idx}"));
            bind_idx += 1;
        }
        if params.to_date.is_some() {
            conditions.push(format!("created_at <= ${bind_idx}"));
            bind_idx += 1;
        }

        let where_clause = if conditions.is_empty() {
            String::new()
        } else {
            format!("WHERE {}", conditions.join(" AND "))
        };

        let count_sql = format!(
            "SELECT COUNT(*) FROM {schema}.events {where_clause}",
            schema = self.schema
        );
        let data_sql = format!(
            "SELECT id, event_id, aggregate_id, aggregate_type, event_type, \
             event_version, event_data, created_by, created_at, \
             correlation_id, causation_id, metadata \
             FROM {schema}.events {where_clause} \
             ORDER BY id DESC LIMIT ${bind_idx} OFFSET ${next}",
            schema = self.schema,
            next = bind_idx + 1,
        );

        // Build count query
        let mut count_query = sqlx::query_scalar::<_, i64>(&count_sql);
        let mut data_query = sqlx::query_as::<_, EventLogRow>(&data_sql);

        // Bind parameters in the same order
        if let Some(ref v) = params.aggregate_type {
            count_query = count_query.bind(v);
            data_query = data_query.bind(v);
        }
        if let Some(ref v) = params.event_type {
            count_query = count_query.bind(v);
            data_query = data_query.bind(v);
        }
        if let Some(v) = params.aggregate_id {
            count_query = count_query.bind(v);
            data_query = data_query.bind(v);
        }
        if let Some(v) = params.created_by {
            count_query = count_query.bind(v);
            data_query = data_query.bind(v);
        }
        if let Some(v) = params.from_date {
            count_query = count_query.bind(v);
            data_query = data_query.bind(v);
        }
        if let Some(v) = params.to_date {
            count_query = count_query.bind(v);
            data_query = data_query.bind(v);
        }

        #[allow(clippy::cast_possible_wrap)]
        let limit = params.per_page as i64;
        #[allow(clippy::cast_possible_wrap)]
        let offset = (params.page * params.per_page) as i64;
        data_query = data_query.bind(limit).bind(offset);

        #[allow(clippy::cast_sign_loss)]
        let total_count = count_query.fetch_one(&self.pool).await.map_err(|e| {
            event_sauce_core::Error::custom(format!("Event log count query failed: {e}"))
        })? as u64;

        let rows = data_query.fetch_all(&self.pool).await.map_err(|e| {
            event_sauce_core::Error::custom(format!("Event log data query failed: {e}"))
        })?;

        let entries = rows.into_iter().map(Into::into).collect();

        Ok(EventLogPage {
            entries,
            total_count,
            page: params.page,
            per_page: params.per_page,
        })
    }

    async fn distinct_aggregate_types(&self) -> Result<Vec<String>> {
        let rows: Vec<(String,)> = sqlx::query_as(&format!(
            "SELECT DISTINCT aggregate_type FROM {schema}.events ORDER BY aggregate_type",
            schema = self.schema
        ))
        .fetch_all(&self.pool)
        .await
        .map_err(|e| {
            event_sauce_core::Error::custom(format!(
                "Event log distinct_aggregate_types query failed: {e}"
            ))
        })?;

        Ok(rows.into_iter().map(|(t,)| t).collect())
    }

    async fn distinct_event_types(&self) -> Result<Vec<String>> {
        let rows: Vec<(String,)> = sqlx::query_as(&format!(
            "SELECT DISTINCT event_type FROM {schema}.events ORDER BY event_type",
            schema = self.schema
        ))
        .fetch_all(&self.pool)
        .await
        .map_err(|e| {
            event_sauce_core::Error::custom(format!(
                "Event log distinct_event_types query failed: {e}"
            ))
        })?;

        Ok(rows.into_iter().map(|(t,)| t).collect())
    }
}

#[derive(sqlx::FromRow)]
struct EventLogRow {
    id: i64,
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

impl From<EventLogRow> for EventLogEntry {
    fn from(row: EventLogRow) -> Self {
        let metadata =
            if row.correlation_id.is_some() || row.causation_id.is_some() || row.metadata.is_some()
            {
                Some(EventMetadata {
                    correlation_id: row.correlation_id,
                    causation_id: row.causation_id,
                    causation_chain: Vec::new(),
                    timestamp: row.created_at,
                    additional: row.metadata,
                })
            } else {
                None
            };

        Self {
            position: Position::new(row.id),
            envelope: EventEnvelope {
                id: row.event_id,
                aggregate_id: row.aggregate_id,
                aggregate_type: AggregateType::from_owned(row.aggregate_type),
                event_type: row.event_type,
                event_version: EventVersion::new(row.event_version),
                event_data: row.event_data,
                created_by: row.created_by,
                created_at: row.created_at,
                metadata,
            },
        }
    }
}
