//! In-memory event log query implementation.
//!
//! Provides paginated, filtered access to the in-memory event store's
//! global event list. Suitable for testing.

use async_trait::async_trait;
use event_sauce_core::{
    EventLogEntry, EventLogPage, EventLogParams, EventLogQuery, Position, Result,
};

use crate::InMemoryEventStore;

/// In-memory implementation of [`EventLogQuery`].
///
/// Filters and paginates events from an [`InMemoryEventStore`]'s
/// global event list.
///
/// # Examples
///
/// ```
/// use event_sauce_memory::{InMemoryEventStore, InMemoryEventLogQuery};
/// use event_sauce_core::{EventLogQuery, EventLogParams};
///
/// # tokio_test::block_on(async {
/// let store = InMemoryEventStore::new();
/// let log_query = InMemoryEventLogQuery::new(store);
///
/// let page = log_query.query_events(EventLogParams::default()).await.unwrap();
/// assert_eq!(page.total_count, 0);
/// # });
/// ```
pub struct InMemoryEventLogQuery {
    store: InMemoryEventStore,
}

impl InMemoryEventLogQuery {
    /// Creates a new `InMemoryEventLogQuery` wrapping the given store.
    #[must_use]
    pub fn new(store: InMemoryEventStore) -> Self {
        Self { store }
    }
}

#[async_trait]
impl EventLogQuery for InMemoryEventLogQuery {
    async fn query_events(&self, params: EventLogParams) -> Result<EventLogPage> {
        let all_events = self.store.all_events();

        // Filter
        let filtered: Vec<_> = all_events
            .into_iter()
            .enumerate()
            .filter(|(_idx, env)| {
                if let Some(ref agg_type) = params.aggregate_type {
                    if env.aggregate_type.as_str() != agg_type.as_str() {
                        return false;
                    }
                }
                if let Some(ref evt_type) = params.event_type {
                    if env.event_type != *evt_type {
                        return false;
                    }
                }
                if let Some(agg_id) = params.aggregate_id {
                    if env.aggregate_id != agg_id {
                        return false;
                    }
                }
                if let Some(created_by) = params.created_by {
                    if env.created_by != Some(created_by) {
                        return false;
                    }
                }
                if let Some(from) = params.from_date {
                    if env.created_at < from {
                        return false;
                    }
                }
                if let Some(to) = params.to_date {
                    if env.created_at > to {
                        return false;
                    }
                }
                true
            })
            .collect();

        #[allow(clippy::cast_possible_truncation)]
        let total_count = filtered.len() as u64;

        // Sort by position descending (newest first), then paginate
        let mut sorted = filtered;
        sorted.reverse();

        #[allow(clippy::cast_possible_truncation)]
        let skip = (params.page * params.per_page) as usize;
        #[allow(clippy::cast_possible_truncation)]
        let take = params.per_page as usize;

        let entries = sorted
            .into_iter()
            .skip(skip)
            .take(take)
            .map(|(idx, envelope)| {
                #[allow(clippy::cast_possible_wrap)]
                let position = Position::new((idx + 1) as i64); // 1-indexed like BIGSERIAL
                EventLogEntry { position, envelope }
            })
            .collect();

        Ok(EventLogPage {
            entries,
            total_count,
            page: params.page,
            per_page: params.per_page,
        })
    }

    async fn distinct_aggregate_types(&self) -> Result<Vec<String>> {
        let all_events = self.store.all_events();
        let mut types: Vec<String> = all_events
            .iter()
            .map(|e| e.aggregate_type.to_string())
            .collect::<std::collections::BTreeSet<_>>()
            .into_iter()
            .collect();
        types.sort();
        Ok(types)
    }

    async fn distinct_event_types(&self) -> Result<Vec<String>> {
        let all_events = self.store.all_events();
        let mut types: Vec<String> = all_events
            .iter()
            .map(|e| e.event_type.clone())
            .collect::<std::collections::BTreeSet<_>>()
            .into_iter()
            .collect();
        types.sort();
        Ok(types)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use event_sauce_core::{AggregateVersion, EventEnvelope, EventStore, EventVersion, StreamId};
    use serde_json::json;
    use uuid::Uuid;

    fn create_envelope(
        aggregate_type: &str,
        event_type: &str,
        aggregate_id: Uuid,
    ) -> EventEnvelope {
        EventEnvelope::new(
            Uuid::new_v4(),
            aggregate_id,
            aggregate_type.to_string(),
            event_type.to_string(),
            EventVersion::new(1),
            json!({"data": "test"}),
        )
    }

    async fn setup_store_with_events() -> InMemoryEventStore {
        let store = InMemoryEventStore::new();
        let user_id = Uuid::new_v4();
        let order_id = Uuid::new_v4();

        // Add User events
        store
            .append(
                StreamId::new("User", user_id),
                vec![
                    create_envelope("User", "User.Created", user_id),
                    create_envelope("User", "User.Updated", user_id),
                ],
                AggregateVersion::initial(),
                vec![],
                false,
            )
            .await
            .unwrap();

        // Add Order events
        store
            .append(
                StreamId::new("Order", order_id),
                vec![create_envelope("Order", "Order.Placed", order_id)],
                AggregateVersion::initial(),
                vec![],
                false,
            )
            .await
            .unwrap();

        store
    }

    #[tokio::test]
    async fn test_query_all_events() {
        let store = setup_store_with_events().await;
        let query = InMemoryEventLogQuery::new(store);

        let page = query.query_events(EventLogParams::default()).await.unwrap();

        assert_eq!(page.total_count, 3);
        assert_eq!(page.entries.len(), 3);
        assert_eq!(page.page, 0);
        assert_eq!(page.per_page, 50);
    }

    #[tokio::test]
    async fn test_query_events_ordered_newest_first() {
        let store = setup_store_with_events().await;
        let query = InMemoryEventLogQuery::new(store);

        let page = query.query_events(EventLogParams::default()).await.unwrap();

        // Last inserted event should be first
        assert_eq!(page.entries[0].envelope.event_type, "Order.Placed");
        assert_eq!(page.entries[1].envelope.event_type, "User.Updated");
        assert_eq!(page.entries[2].envelope.event_type, "User.Created");
    }

    #[tokio::test]
    async fn test_filter_by_aggregate_type() {
        let store = setup_store_with_events().await;
        let query = InMemoryEventLogQuery::new(store);

        let page = query
            .query_events(EventLogParams {
                aggregate_type: Some("User".to_string()),
                ..Default::default()
            })
            .await
            .unwrap();

        assert_eq!(page.total_count, 2);
        for entry in &page.entries {
            assert_eq!(entry.envelope.aggregate_type.as_str(), "User");
        }
    }

    #[tokio::test]
    async fn test_filter_by_event_type() {
        let store = setup_store_with_events().await;
        let query = InMemoryEventLogQuery::new(store);

        let page = query
            .query_events(EventLogParams {
                event_type: Some("Order.Placed".to_string()),
                ..Default::default()
            })
            .await
            .unwrap();

        assert_eq!(page.total_count, 1);
        assert_eq!(page.entries[0].envelope.event_type, "Order.Placed");
    }

    #[tokio::test]
    async fn test_filter_by_aggregate_id() {
        let store = InMemoryEventStore::new();
        let id1 = Uuid::new_v4();
        let id2 = Uuid::new_v4();

        store
            .append(
                StreamId::new("User", id1),
                vec![create_envelope("User", "User.Created", id1)],
                AggregateVersion::initial(),
                vec![],
                false,
            )
            .await
            .unwrap();
        store
            .append(
                StreamId::new("User", id2),
                vec![create_envelope("User", "User.Created", id2)],
                AggregateVersion::initial(),
                vec![],
                false,
            )
            .await
            .unwrap();

        let query = InMemoryEventLogQuery::new(store);
        let page = query
            .query_events(EventLogParams {
                aggregate_id: Some(id1),
                ..Default::default()
            })
            .await
            .unwrap();

        assert_eq!(page.total_count, 1);
        assert_eq!(page.entries[0].envelope.aggregate_id, id1);
    }

    #[tokio::test]
    async fn test_filter_by_created_by() {
        let store = InMemoryEventStore::new();
        let agg_id = Uuid::new_v4();
        let actor_id = Uuid::new_v4();

        let mut env_with_actor = create_envelope("User", "User.Created", agg_id);
        env_with_actor = env_with_actor.with_created_by(actor_id);

        let env_without_actor = create_envelope("User", "User.Updated", agg_id);

        store
            .append(
                StreamId::new("User", agg_id),
                vec![env_with_actor, env_without_actor],
                AggregateVersion::initial(),
                vec![],
                false,
            )
            .await
            .unwrap();

        let query = InMemoryEventLogQuery::new(store);
        let page = query
            .query_events(EventLogParams {
                created_by: Some(actor_id),
                ..Default::default()
            })
            .await
            .unwrap();

        assert_eq!(page.total_count, 1);
        assert_eq!(page.entries[0].envelope.created_by, Some(actor_id));
    }

    #[tokio::test]
    async fn test_pagination() {
        let store = InMemoryEventStore::new();

        for i in 0..10 {
            let id = Uuid::new_v4();
            store
                .append(
                    StreamId::new("User", id),
                    vec![create_envelope("User", &format!("Event{i}"), id)],
                    AggregateVersion::initial(),
                    vec![],
                    false,
                )
                .await
                .unwrap();
        }

        let query = InMemoryEventLogQuery::new(store);

        // First page
        let page1 = query
            .query_events(EventLogParams {
                per_page: 3,
                page: 0,
                ..Default::default()
            })
            .await
            .unwrap();
        assert_eq!(page1.total_count, 10);
        assert_eq!(page1.entries.len(), 3);
        assert_eq!(page1.total_pages(), 4); // ceil(10/3)

        // Second page
        let page2 = query
            .query_events(EventLogParams {
                per_page: 3,
                page: 1,
                ..Default::default()
            })
            .await
            .unwrap();
        assert_eq!(page2.entries.len(), 3);

        // Last page (partial)
        let page4 = query
            .query_events(EventLogParams {
                per_page: 3,
                page: 3,
                ..Default::default()
            })
            .await
            .unwrap();
        assert_eq!(page4.entries.len(), 1);
    }

    #[tokio::test]
    async fn test_distinct_aggregate_types() {
        let store = setup_store_with_events().await;
        let query = InMemoryEventLogQuery::new(store);

        let types = query.distinct_aggregate_types().await.unwrap();
        assert_eq!(types, vec!["Order", "User"]);
    }

    #[tokio::test]
    async fn test_distinct_event_types() {
        let store = setup_store_with_events().await;
        let query = InMemoryEventLogQuery::new(store);

        let types = query.distinct_event_types().await.unwrap();
        assert_eq!(types, vec!["Order.Placed", "User.Created", "User.Updated"]);
    }

    #[tokio::test]
    async fn test_empty_store() {
        let store = InMemoryEventStore::new();
        let query = InMemoryEventLogQuery::new(store);

        let page = query.query_events(EventLogParams::default()).await.unwrap();
        assert_eq!(page.total_count, 0);
        assert!(page.entries.is_empty());

        let types = query.distinct_aggregate_types().await.unwrap();
        assert!(types.is_empty());

        let event_types = query.distinct_event_types().await.unwrap();
        assert!(event_types.is_empty());
    }

    #[tokio::test]
    async fn test_position_is_one_indexed() {
        let store = InMemoryEventStore::new();
        let id = Uuid::new_v4();

        store
            .append(
                StreamId::new("User", id),
                vec![create_envelope("User", "User.Created", id)],
                AggregateVersion::initial(),
                vec![],
                false,
            )
            .await
            .unwrap();

        let query = InMemoryEventLogQuery::new(store);
        let page = query.query_events(EventLogParams::default()).await.unwrap();

        assert_eq!(page.entries[0].position.as_i64(), 1);
    }
}
