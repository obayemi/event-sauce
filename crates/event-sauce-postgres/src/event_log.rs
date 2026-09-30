//! `PostgreSQL` event log query implementation.
//!
//! Provides paginated, filtered access to the global event log
//! stored in `PostgreSQL`.

use crate::event_store::EventRow;
use async_trait::async_trait;
use event_sauce_core::{EventLogPage, EventLogParams, EventLogQuery, Result};
use sqlx::{PgPool, Postgres, QueryBuilder};

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

    /// Returns the distinct values of `column` in the events table, sorted.
    /// `column` is never user input — always a literal at the call site — so
    /// interpolating it into the query text carries no injection risk.
    async fn distinct(&self, column: &'static str) -> Result<Vec<String>> {
        let events_table = crate::sql::qualify(&self.schema, "events");
        sqlx::query_scalar(&format!(
            "SELECT DISTINCT {column} FROM {events_table} ORDER BY {column}"
        ))
        .fetch_all(&self.pool)
        .await
        .map_err(|e| {
            event_sauce_core::Error::backend(format!("Event log distinct {column} query failed"), e)
        })
    }
}

/// Appends ` WHERE TRUE AND <cond> AND <cond> ...` for every filter `params`
/// sets, each as its own bound parameter. `QueryBuilder` numbers the
/// placeholders itself, so the count and data queries below can push the
/// same filters independently without a hand-maintained placeholder index
/// or two parallel bind chains kept in sync by hand. The unconditional
/// `TRUE` (folded away by the planner) means each filter is pushed the same
/// way whether or not any other filter is set, with no separate "is this
/// the first clause" bookkeeping.
fn push_filters(qb: &mut QueryBuilder<'_, Postgres>, params: &EventLogParams) {
    qb.push(" WHERE TRUE");
    if let Some(v) = params.aggregate_type.clone() {
        qb.push(" AND aggregate_type = ").push_bind(v);
    }
    if let Some(v) = params.event_type.clone() {
        qb.push(" AND event_type = ").push_bind(v);
    }
    if let Some(v) = params.aggregate_id {
        qb.push(" AND aggregate_id = ").push_bind(v);
    }
    if let Some(v) = params.created_by {
        qb.push(" AND created_by = ").push_bind(v);
    }
    if let Some(v) = params.from_date {
        qb.push(" AND created_at >= ").push_bind(v);
    }
    if let Some(v) = params.to_date {
        qb.push(" AND created_at <= ").push_bind(v);
    }
}

#[async_trait]
impl EventLogQuery for PostgresEventLogQuery {
    async fn query_events(&self, params: EventLogParams) -> Result<EventLogPage> {
        let events_table = crate::sql::qualify(&self.schema, "events");

        let mut count_qb: QueryBuilder<Postgres> =
            QueryBuilder::new(format!("SELECT COUNT(*) FROM {events_table}"));
        push_filters(&mut count_qb, &params);

        let mut data_qb: QueryBuilder<Postgres> = QueryBuilder::new(format!(
            "SELECT {columns} FROM {events_table}",
            columns = crate::event_store::EVENT_COLUMNS
        ));
        push_filters(&mut data_qb, &params);

        #[allow(clippy::cast_possible_wrap)]
        let limit = params.per_page as i64;
        #[allow(clippy::cast_possible_wrap)]
        let offset = (params.page * params.per_page) as i64;
        data_qb.push(format!(" ORDER BY {} LIMIT ", params.order_by.order_sql()));
        data_qb.push_bind(limit);
        data_qb.push(" OFFSET ");
        data_qb.push_bind(offset);

        #[allow(clippy::cast_sign_loss)]
        let total_count = count_qb
            .build_query_scalar::<i64>()
            .fetch_one(&self.pool)
            .await
            .map_err(|e| event_sauce_core::Error::backend("Event log count query failed", e))?
            as u64;

        let rows = data_qb
            .build_query_as::<EventRow>()
            .fetch_all(&self.pool)
            .await
            .map_err(|e| event_sauce_core::Error::backend("Event log data query failed", e))?;

        let entries = rows.into_iter().map(EventRow::log_entry).collect();

        Ok(EventLogPage {
            entries,
            total_count,
            page: params.page,
            per_page: params.per_page,
        })
    }

    async fn distinct_aggregate_types(&self) -> Result<Vec<String>> {
        self.distinct("aggregate_type").await
    }

    async fn distinct_event_types(&self) -> Result<Vec<String>> {
        self.distinct("event_type").await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::PostgresEventStore;
    use chrono::{Duration, Utc};
    use event_sauce_core::{
        AggregateVersion, EventEnvelope, EventLogOrder, EventLogParams, EventStore, EventVersion,
        StreamId,
    };
    use serde_json::json;
    use testcontainers_modules::postgres::Postgres;
    use uuid::Uuid;

    struct TestDb {
        pool: PgPool,
        _container: testcontainers_modules::testcontainers::ContainerAsync<Postgres>,
    }

    impl TestDb {
        async fn new() -> Self {
            let (container, url) = crate::test_support::start_postgres_url().await;
            let pool = PgPool::connect(&url).await.expect("connect");
            let store = PostgresEventStore::builder()
                .pool(pool.clone())
                .schema("public")
                .build()
                .expect("pool was set");
            store.migrate().await.expect("migrate");
            Self {
                pool,
                _container: container,
            }
        }

        fn query(&self) -> PostgresEventLogQuery {
            PostgresEventLogQuery::new(self.pool.clone(), "public".to_string())
        }

        fn store(&self) -> PostgresEventStore {
            PostgresEventStore::builder()
                .pool(self.pool.clone())
                .schema("public")
                .build()
                .expect("pool was set")
        }

        async fn seed(&self, event: EventEnvelope) {
            self.seed_stream(vec![event]).await;
        }

        /// Appends several events to the same stream (same aggregate), in
        /// order, as a real caller would.
        async fn seed_stream(&self, events: Vec<EventEnvelope>) {
            let first = events.first().expect("at least one event");
            let stream_id = StreamId::new(first.aggregate_type.clone(), first.aggregate_id);
            self.store()
                .append(
                    stream_id,
                    events,
                    AggregateVersion::initial(),
                    vec![],
                    false,
                )
                .await
                .expect("append seeded stream");
        }

        /// Backdates a seeded event's `created_at`. The store's own `INSERT`
        /// always timestamps with the database's `NOW()` (it does not bind
        /// `EventEnvelope::created_at`), so date-range tests reach around it
        /// with a direct `UPDATE`.
        async fn backdate(&self, event_id: Uuid, created_at: chrono::DateTime<Utc>) {
            sqlx::query("UPDATE events SET created_at = $1 WHERE event_id = $2")
                .bind(created_at)
                .bind(event_id)
                .execute(&self.pool)
                .await
                .expect("backdate seeded event");
        }
    }

    fn envelope(
        aggregate_type: &'static str,
        event_type: &str,
        aggregate_id: Uuid,
    ) -> EventEnvelope {
        EventEnvelope::new(
            Uuid::new_v4(),
            aggregate_id,
            aggregate_type,
            event_type.to_string(),
            EventVersion::new(1),
            json!({"k": "v"}),
        )
    }

    /// Default order is `id DESC`, so the later insert must come first.
    #[tokio::test]
    async fn test_query_events_no_filters_returns_all_newest_first() {
        let db = TestDb::new().await;
        db.seed(envelope("User", "User.Created", Uuid::new_v4()))
            .await;
        db.seed(envelope("User", "User.Updated", Uuid::new_v4()))
            .await;

        let page = db
            .query()
            .query_events(EventLogParams::default())
            .await
            .unwrap();
        assert_eq!(page.total_count, 2);
        assert_eq!(page.entries.len(), 2);
        assert_eq!(page.entries[0].envelope.event_type, "User.Updated");
        assert_eq!(page.entries[1].envelope.event_type, "User.Created");
    }

    #[tokio::test]
    async fn test_query_events_filters_by_aggregate_type() {
        let db = TestDb::new().await;
        db.seed(envelope("User", "User.Created", Uuid::new_v4()))
            .await;
        db.seed(envelope("Order", "Order.Placed", Uuid::new_v4()))
            .await;

        let page = db
            .query()
            .query_events(EventLogParams {
                aggregate_type: Some("Order".to_string()),
                ..EventLogParams::default()
            })
            .await
            .unwrap();

        assert_eq!(page.total_count, 1);
        assert_eq!(page.entries[0].envelope.event_type, "Order.Placed");
    }

    #[tokio::test]
    async fn test_query_events_filters_by_event_type() {
        let db = TestDb::new().await;
        db.seed(envelope("User", "User.Created", Uuid::new_v4()))
            .await;
        db.seed(envelope("User", "User.Updated", Uuid::new_v4()))
            .await;

        let page = db
            .query()
            .query_events(EventLogParams {
                event_type: Some("User.Updated".to_string()),
                ..EventLogParams::default()
            })
            .await
            .unwrap();

        assert_eq!(page.total_count, 1);
        assert_eq!(page.entries[0].envelope.event_type, "User.Updated");
    }

    /// Combining several filters must AND them together, not just apply the
    /// last one bound — the failure mode a hand-numbered placeholder chain
    /// (or a `QueryBuilder` misuse) would produce.
    #[tokio::test]
    async fn test_query_events_combines_multiple_filters() {
        let db = TestDb::new().await;
        let target = Uuid::new_v4();
        db.seed_stream(vec![
            envelope("User", "User.Created", target),
            envelope("User", "User.Updated", target),
        ])
        .await;
        db.seed(envelope("Order", "Order.Placed", Uuid::new_v4()))
            .await;

        let page = db
            .query()
            .query_events(EventLogParams {
                aggregate_type: Some("User".to_string()),
                aggregate_id: Some(target),
                event_type: Some("User.Updated".to_string()),
                ..EventLogParams::default()
            })
            .await
            .unwrap();

        assert_eq!(page.total_count, 1);
        assert_eq!(page.entries[0].envelope.event_type, "User.Updated");
        assert_eq!(page.entries[0].envelope.aggregate_id, target);
    }

    #[tokio::test]
    async fn test_query_events_filters_by_aggregate_id() {
        let db = TestDb::new().await;
        let target = Uuid::new_v4();
        db.seed(envelope("User", "User.Created", target)).await;
        db.seed(envelope("User", "User.Created", Uuid::new_v4()))
            .await;

        let page = db
            .query()
            .query_events(EventLogParams {
                aggregate_id: Some(target),
                ..EventLogParams::default()
            })
            .await
            .unwrap();

        assert_eq!(page.total_count, 1);
        assert_eq!(page.entries[0].envelope.aggregate_id, target);
    }

    #[tokio::test]
    async fn test_query_events_filters_by_created_by() {
        let db = TestDb::new().await;
        let user = Uuid::new_v4();
        db.seed(envelope("User", "User.Created", Uuid::new_v4()).with_created_by(user))
            .await;
        db.seed(envelope("User", "User.Created", Uuid::new_v4()))
            .await;

        let page = db
            .query()
            .query_events(EventLogParams {
                created_by: Some(user),
                ..EventLogParams::default()
            })
            .await
            .unwrap();

        assert_eq!(page.total_count, 1);
        assert_eq!(page.entries[0].envelope.created_by, Some(user));
    }

    #[tokio::test]
    async fn test_query_events_filters_by_date_range() {
        let db = TestDb::new().await;
        let now = Utc::now();

        let old = envelope("User", "User.Old", Uuid::new_v4());
        let old_id = old.id;
        db.seed(old).await;
        db.backdate(old_id, now - Duration::days(10)).await;

        let recent = envelope("User", "User.Recent", Uuid::new_v4());
        let recent_id = recent.id;
        db.seed(recent).await;
        db.backdate(recent_id, now).await;

        let page = db
            .query()
            .query_events(EventLogParams {
                from_date: Some(now - Duration::days(1)),
                ..EventLogParams::default()
            })
            .await
            .unwrap();

        assert_eq!(page.total_count, 1);
        assert_eq!(page.entries[0].envelope.event_type, "User.Recent");

        let page = db
            .query()
            .query_events(EventLogParams {
                to_date: Some(now - Duration::days(1)),
                ..EventLogParams::default()
            })
            .await
            .unwrap();

        assert_eq!(page.total_count, 1);
        assert_eq!(page.entries[0].envelope.event_type, "User.Old");
    }

    #[tokio::test]
    async fn test_query_events_paginates() {
        let db = TestDb::new().await;
        for i in 0..5 {
            db.seed(envelope("User", &format!("User.Event{i}"), Uuid::new_v4()))
                .await;
        }

        let page = db
            .query()
            .query_events(EventLogParams {
                per_page: 2,
                page: 1,
                order_by: EventLogOrder::ByIdAsc,
                ..EventLogParams::default()
            })
            .await
            .unwrap();

        assert_eq!(page.total_count, 5);
        assert_eq!(page.entries.len(), 2);
        assert_eq!(page.entries[0].envelope.event_type, "User.Event2");
        assert_eq!(page.entries[1].envelope.event_type, "User.Event3");
    }

    #[tokio::test]
    async fn test_distinct_aggregate_types() {
        let db = TestDb::new().await;
        db.seed(envelope("User", "User.Created", Uuid::new_v4()))
            .await;
        db.seed(envelope("Order", "Order.Placed", Uuid::new_v4()))
            .await;
        db.seed(envelope("Order", "Order.Shipped", Uuid::new_v4()))
            .await;

        let mut types = db.query().distinct_aggregate_types().await.unwrap();
        types.sort();
        assert_eq!(types, vec!["Order".to_string(), "User".to_string()]);
    }

    #[tokio::test]
    async fn test_distinct_event_types() {
        let db = TestDb::new().await;
        db.seed(envelope("User", "User.Created", Uuid::new_v4()))
            .await;
        db.seed(envelope("User", "User.Updated", Uuid::new_v4()))
            .await;
        db.seed(envelope("User", "User.Updated", Uuid::new_v4()))
            .await;

        let types = db.query().distinct_event_types().await.unwrap();
        assert_eq!(
            types,
            vec!["User.Created".to_string(), "User.Updated".to_string()]
        );
    }
}
