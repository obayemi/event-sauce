//! The [`CommitSource`] trait shared by the event-sourced commit pipeline
//! and the state-stored save pipeline.

use crate::{
    Aggregate, AggregateClaim, AggregateRoot, AggregateType, AggregateVersion,
    DeletedAggregateRoot, EntityId,
};

/// An aggregate root that can be prepared for a commit or a save.
///
/// Implemented by [`AggregateRoot<A>`] (`IS_DELETED = false`) and
/// [`DeletedAggregateRoot<A>`] (`IS_DELETED = true`), which agree on every
/// axis this trait exposes except: which state gets serialized, whether the
/// write carries claims, and whether it clears them.
pub(crate) trait CommitSource<A: Aggregate> {
    /// Whether this root represents a deleted aggregate.
    const IS_DELETED: bool;

    /// This aggregate type's name, as recorded on every stream and snapshot.
    fn aggregate_type() -> AggregateType;

    /// Whether a previous `apply()` left this root inconsistent.
    fn is_poisoned(&self) -> bool;

    /// Pending events not yet committed, with their actor and metadata.
    fn pending_events_with_actors(&self) -> &[crate::aggregate_root::PendingEvent<A::Event>];

    /// This aggregate instance's id.
    fn entity_id(&self) -> EntityId;

    /// The version this root is at, including its pending events.
    fn version(&self) -> AggregateVersion;

    /// The version the store must currently hold for this root's write to
    /// apply: [`version`](Self::version) minus the pending events not yet
    /// persisted.
    fn committed_version(&self) -> AggregateVersion {
        let pending_len = self.pending_events_with_actors().len();
        let pending_count = i64::try_from(pending_len).unwrap_or(i64::MAX);
        AggregateVersion::new(self.version().as_i64().saturating_sub(pending_count))
    }

    /// Uniqueness claims to enforce transactionally (empty for a deletion).
    fn claims(&self) -> Vec<AggregateClaim>;

    /// Serializes the state a snapshot or stored row should hold: the entity
    /// for an active root, `A::DeletedState` for a deleted one.
    fn serialize_state(&self) -> std::result::Result<serde_json::Value, serde_json::Error>;
}

impl<A: Aggregate + serde::Serialize> CommitSource<A> for AggregateRoot<A> {
    const IS_DELETED: bool = false;

    fn aggregate_type() -> AggregateType {
        AggregateRoot::<A>::aggregate_type()
    }

    fn is_poisoned(&self) -> bool {
        AggregateRoot::is_poisoned(self)
    }

    fn pending_events_with_actors(&self) -> &[crate::aggregate_root::PendingEvent<A::Event>] {
        AggregateRoot::pending_events_with_actors(self)
    }

    fn entity_id(&self) -> EntityId {
        AggregateRoot::entity_id(self)
    }

    fn version(&self) -> AggregateVersion {
        AggregateRoot::version(self)
    }

    fn claims(&self) -> Vec<AggregateClaim> {
        self.entity().claims()
    }

    fn serialize_state(&self) -> std::result::Result<serde_json::Value, serde_json::Error> {
        serde_json::to_value(self.entity())
    }
}

impl<A: Aggregate> CommitSource<A> for DeletedAggregateRoot<A>
where
    A::DeletedState: serde::Serialize,
{
    const IS_DELETED: bool = true;

    fn aggregate_type() -> AggregateType {
        DeletedAggregateRoot::<A>::aggregate_type()
    }

    fn is_poisoned(&self) -> bool {
        DeletedAggregateRoot::is_poisoned(self)
    }

    fn pending_events_with_actors(&self) -> &[crate::aggregate_root::PendingEvent<A::Event>] {
        DeletedAggregateRoot::pending_events_with_actors(self)
    }

    fn entity_id(&self) -> EntityId {
        DeletedAggregateRoot::entity_id(self)
    }

    fn version(&self) -> AggregateVersion {
        DeletedAggregateRoot::version(self)
    }

    fn claims(&self) -> Vec<AggregateClaim> {
        vec![]
    }

    fn serialize_state(&self) -> std::result::Result<serde_json::Value, serde_json::Error> {
        serde_json::to_value(self.state())
    }
}

#[cfg(all(test, feature = "event-sourcing"))]
mod tests {
    use super::*;
    use crate::test_fixtures::{MockEventStore, SimpleTestEntity, SimpleTestEvent};
    use crate::EventStore;

    #[tokio::test]
    async fn test_committed_version_for_active_root() {
        let store = MockEventStore::new();
        let id = crate::EntityId::new();
        let mut agg = AggregateRoot::<SimpleTestEntity>::new(id);
        agg.apply(SimpleTestEvent::Created { value: 1 }).unwrap();
        store.commit(&mut agg).await.unwrap();

        agg.apply(SimpleTestEvent::Updated { value: 2 }).unwrap();

        assert_eq!(agg.version(), AggregateVersion::new(2));
        assert_eq!(
            CommitSource::committed_version(&agg),
            AggregateVersion::new(1),
            "committed_version must exclude the one pending event not yet persisted"
        );
    }

    #[tokio::test]
    async fn test_committed_version_for_deleted_root() {
        use crate::test_fixtures::SimpleTestDelete;

        let store = MockEventStore::new();
        let id = crate::EntityId::new();
        let mut agg = AggregateRoot::<SimpleTestEntity>::new(id);
        agg.apply(SimpleTestEvent::Created { value: 10 }).unwrap();
        store.commit(&mut agg).await.unwrap();

        let deleted = agg.apply_delete(SimpleTestDelete).unwrap();

        assert_eq!(deleted.version(), AggregateVersion::new(2));
        assert_eq!(
            CommitSource::committed_version(&deleted),
            AggregateVersion::new(1),
            "committed_version must exclude the one pending delete event not yet persisted"
        );
    }
}
