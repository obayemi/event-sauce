//! Mapping between event rows and the domain types, on both sides of the
//! `events` table: [`EventRow`] reads, [`EventColumns`] writes.

use event_sauce_core::{EventEnvelope, EventMetadata, EventVersion, Position, StreamCommit};
use sqlx::postgres::PgArguments;
use sqlx::query::QueryScalar;
use sqlx::types::Json;
use sqlx::Postgres;

/// Every column [`EventRow`] decodes by name, including the global `id` —
/// shared by every query that reads events (per-stream, the global log, and
/// the log-query filters), so a query can never omit one.
pub(crate) const EVENT_COLUMNS: &str = "id, event_id, aggregate_id, aggregate_type, event_type, \
     event_version, event_data, created_by, created_at, correlation_id, causation_id, metadata, \
     causation_chain, tenant, command, correlation_path";

#[derive(sqlx::FromRow)]
pub(crate) struct EventRow {
    /// Global BIGSERIAL position, read solely through [`EventRow::log_entry`].
    id: i64,
    event_id: uuid::Uuid,
    aggregate_id: uuid::Uuid,
    aggregate_type: String,
    event_type: String,
    event_version: i64,
    event_data: serde_json::Value,
    created_by: Option<uuid::Uuid>,
    created_at: chrono::DateTime<chrono::Utc>,
    correlation_id: Option<String>,
    causation_id: Option<uuid::Uuid>,
    metadata: Option<serde_json::Value>,
    causation_chain: Option<Json<Vec<uuid::Uuid>>>,
    tenant: Option<String>,
    command: Option<String>,
    correlation_path: Option<Json<Vec<String>>>,
}

impl EventRow {
    /// Converts a row that includes the global `id` column into an
    /// [`EventLogEntry`](event_sauce_core::EventLogEntry), pairing the
    /// store-issued [`Position`] with the envelope.
    pub(crate) fn log_entry(self) -> event_sauce_core::EventLogEntry {
        event_sauce_core::EventLogEntry {
            position: Position::new(self.id),
            envelope: EventEnvelope::from(self),
        }
    }
}

impl From<EventRow> for EventEnvelope {
    fn from(row: EventRow) -> Self {
        let causation_chain = row.causation_chain.map(|chain| chain.0).unwrap_or_default();
        let correlation_path = row.correlation_path.map(|path| path.0).unwrap_or_default();
        let metadata = EventMetadata {
            correlation_id: row.correlation_id,
            causation_id: row.causation_id,
            causation_chain,
            tenant: row.tenant,
            command: row.command,
            correlation_path,
            timestamp: row.created_at,
            additional: row
                .metadata
                .and_then(event_sauce_core::additional_from_value),
        };

        EventEnvelope {
            id: row.event_id,
            aggregate_id: row.aggregate_id,
            aggregate_type: event_sauce_core::AggregateType::from_owned(row.aggregate_type),
            event_type: row.event_type,
            event_version: EventVersion::new(row.event_version),
            event_data: row.event_data,
            created_by: row.created_by,
            created_at: row.created_at,
            metadata: Some(metadata).filter(EventMetadata::carries_data),
        }
    }
}

/// One column vector per `events` column written on append, each indexed by
/// the event's position in the commit.
///
/// The column order of [`EventColumns::insert_sql`] and of
/// [`EventColumns::bind_all`] is written once, here.
pub(super) struct EventColumns {
    event_ids: Vec<uuid::Uuid>,
    aggregate_ids: Vec<uuid::Uuid>,
    aggregate_types: Vec<String>,
    event_types: Vec<String>,
    event_versions: Vec<i64>,
    event_datas: Vec<serde_json::Value>,
    stream_versions: Vec<i64>,
    created_bys: Vec<Option<uuid::Uuid>>,
    correlation_ids: Vec<Option<String>>,
    causation_ids: Vec<Option<uuid::Uuid>>,
    metadatas: Vec<Option<Json<serde_json::Map<String, serde_json::Value>>>>,
    causation_chains: Vec<Option<Json<Vec<uuid::Uuid>>>>,
    tenants: Vec<Option<String>>,
    commands: Vec<Option<String>>,
    correlation_paths: Vec<Option<Json<Vec<String>>>>,
}

fn metadata_column<T>(
    events: &[EventEnvelope],
    project: impl Fn(&EventMetadata) -> Option<T>,
) -> Vec<Option<T>> {
    events
        .iter()
        .map(|event| event.metadata.as_ref().and_then(&project))
        .collect()
}

fn non_empty_json<T: Clone>(items: &[T]) -> Option<Json<Vec<T>>> {
    (!items.is_empty()).then(|| Json(items.to_vec()))
}

impl EventColumns {
    /// Splits the events of `commit` into one column vector per `events`
    /// column, numbering their stream versions from the commit's expected
    /// version.
    pub(super) fn from_commit(commit: &StreamCommit) -> Self {
        let events = &commit.events;
        let aggregate_type = commit.stream_id.aggregate_type().as_str();
        Self {
            event_ids: events.iter().map(|e| e.id).collect(),
            aggregate_ids: vec![commit.stream_id.aggregate_id(); events.len()],
            aggregate_types: vec![aggregate_type.to_owned(); events.len()],
            event_types: events.iter().map(|e| e.event_type.clone()).collect(),
            event_versions: events.iter().map(|e| e.event_version.as_i64()).collect(),
            event_datas: events.iter().map(|e| e.event_data.clone()).collect(),
            stream_versions: (commit.expected_version.as_i64()..)
                .take(events.len())
                .collect(),
            created_bys: events.iter().map(|e| e.created_by).collect(),
            correlation_ids: metadata_column(events, |m| m.correlation_id.clone()),
            causation_ids: metadata_column(events, |m| m.causation_id),
            metadatas: metadata_column(events, |m| m.additional.clone().map(Json)),
            causation_chains: metadata_column(events, |m| non_empty_json(&m.causation_chain)),
            tenants: metadata_column(events, |m| m.tenant.clone()),
            commands: metadata_column(events, |m| m.command.clone()),
            correlation_paths: metadata_column(events, |m| non_empty_json(&m.correlation_path)),
        }
    }

    /// The statement inserting every column of one commit through `UNNEST`,
    /// returning the issued global ids. Its parameters are bound by
    /// [`EventColumns::bind_all`].
    pub(super) fn insert_sql(events_table: &str) -> String {
        format!(
            "INSERT INTO {events_table} (
                event_id, aggregate_id, aggregate_type, event_type, event_version,
                event_data, stream_version, created_by, correlation_id, causation_id, metadata,
                causation_chain, tenant, command, correlation_path
            )
            SELECT * FROM UNNEST(
                $1::uuid[], $2::uuid[], $3::text[], $4::text[], $5::bigint[],
                $6::jsonb[], $7::bigint[], $8::uuid[], $9::text[], $10::uuid[], $11::jsonb[], $12::jsonb[],
                $13::text[], $14::text[], $15::jsonb[]
            )
            RETURNING id"
        )
    }

    /// Binds every column in the order [`EventColumns::insert_sql`] expects.
    pub(super) fn bind_all(
        self,
        query: QueryScalar<'_, Postgres, i64, PgArguments>,
    ) -> QueryScalar<'_, Postgres, i64, PgArguments> {
        query
            .bind(self.event_ids)
            .bind(self.aggregate_ids)
            .bind(self.aggregate_types)
            .bind(self.event_types)
            .bind(self.event_versions)
            .bind(self.event_datas)
            .bind(self.stream_versions)
            .bind(self.created_bys)
            .bind(self.correlation_ids)
            .bind(self.causation_ids)
            .bind(self.metadatas)
            .bind(self.causation_chains)
            .bind(self.tenants)
            .bind(self.commands)
            .bind(self.correlation_paths)
    }
}

#[cfg(test)]
mod tests;
