//! Event store access for a policy reaction, with automatic causation
//! tracking and buffered, atomically-flushed commits.

use std::sync::Arc;

use crate::event_store::PreparedCommit;
use crate::{
    Aggregate, AggregateId, AggregateRoot, DeletedAggregateRoot, EntityIdFor, EventEnvelope,
    EventMetadata, EventStore, Loaded, Result,
};

/// Provides event store access with automatic causation tracking.
///
/// `PolicyContext` wraps an event store and a source event. All load operations
/// delegate to the store, while commit operations automatically inject causation
/// metadata (correlation ID, causation ID, causation chain) into every pending
/// event before persisting.
///
/// # Causation Logic
///
/// When committing:
/// 1. `correlation_id` = source event's `correlation_id`, or source event's id if none
/// 2. `causation_id` = source event's id
/// 3. `causation_chain` = source event's chain + source event's id
/// 4. Depth check: `chain.len()` < `max_cascade_depth`
///
/// # Examples
///
/// ```ignore
/// async fn handle(&self, event: &EventEnvelope, ctx: &PolicyContext<impl EventStore>) -> Result<()> {
///     let mut group = ctx.load_as::<Group>(some_entity_id).await?;
///     group.apply(SomeEvent { ... })?;
///     ctx.commit(&mut group).await?;
///     Ok(())
/// }
/// ```
pub struct PolicyContext<S: EventStore> {
    store: Arc<S>,
    source_event: EventEnvelope,
    max_cascade_depth: usize,
    pending_commits: std::sync::Mutex<Vec<PreparedCommit>>,
}

impl<S: EventStore + 'static> PolicyContext<S> {
    /// Creates a new policy context.
    ///
    /// # Arguments
    ///
    /// - `store`: The event store for loading and committing aggregates.
    /// - `source_event`: The event being reacted to.
    /// - `max_cascade_depth`: Maximum allowed depth in the causation chain.
    #[must_use]
    pub fn new(store: Arc<S>, source_event: EventEnvelope, max_cascade_depth: usize) -> Self {
        Self {
            store,
            source_event,
            max_cascade_depth,
            pending_commits: std::sync::Mutex::new(Vec::new()),
        }
    }

    // === Load methods (read-only, delegate to store) ===

    /// Loads an aggregate using a typed ID.
    ///
    /// The aggregate type is inferred from the ID type.
    ///
    /// # Errors
    ///
    /// Returns an error if loading fails.
    pub async fn load<I: AggregateId>(&self, id: I) -> Result<AggregateRoot<I::Aggregate>>
    where
        I::Aggregate: Aggregate + serde::de::DeserializeOwned,
        <I::Aggregate as Aggregate>::DeletedState: serde::de::DeserializeOwned,
        <I::Aggregate as Aggregate>::Event: serde::de::DeserializeOwned,
    {
        crate::event_store::load(&*self.store, id.as_entity_id()).await
    }

    /// Loads an aggregate using a raw `EntityId` with explicit type.
    ///
    /// # Errors
    ///
    /// Returns an error if loading fails.
    pub async fn load_as<A: Aggregate + serde::de::DeserializeOwned>(
        &self,
        id: impl EntityIdFor<A>,
    ) -> Result<AggregateRoot<A>>
    where
        A::DeletedState: serde::de::DeserializeOwned,
        A::Event: serde::de::DeserializeOwned,
    {
        crate::event_store::load(&*self.store, id.entity_id()).await
    }

    /// Loads an aggregate returning its lifecycle state, using a typed ID.
    ///
    /// # Errors
    ///
    /// Returns an error if loading fails.
    pub async fn load_any<I: AggregateId>(&self, id: I) -> Result<Loaded<I::Aggregate>>
    where
        I::Aggregate: Aggregate + serde::de::DeserializeOwned,
        <I::Aggregate as Aggregate>::DeletedState: serde::de::DeserializeOwned,
        <I::Aggregate as Aggregate>::Event: serde::de::DeserializeOwned,
    {
        crate::event_store::load_any(&*self.store, id.as_entity_id()).await
    }

    /// Loads an aggregate returning its lifecycle state, with explicit type.
    ///
    /// # Errors
    ///
    /// Returns an error if loading fails.
    pub async fn load_any_as<A: Aggregate + serde::de::DeserializeOwned>(
        &self,
        id: impl EntityIdFor<A>,
    ) -> Result<Loaded<A>>
    where
        A::DeletedState: serde::de::DeserializeOwned,
        A::Event: serde::de::DeserializeOwned,
    {
        crate::event_store::load_any(&*self.store, id.entity_id()).await
    }

    /// Loads a deleted aggregate using a typed ID.
    ///
    /// # Errors
    ///
    /// Returns an error if loading fails or the aggregate is not deleted.
    pub async fn load_deleted<I: AggregateId>(
        &self,
        id: I,
    ) -> Result<DeletedAggregateRoot<I::Aggregate>>
    where
        I::Aggregate: Aggregate + serde::de::DeserializeOwned,
        <I::Aggregate as Aggregate>::DeletedState: serde::de::DeserializeOwned,
        <I::Aggregate as Aggregate>::Event: serde::de::DeserializeOwned,
    {
        crate::event_store::load_deleted(&*self.store, id.as_entity_id()).await
    }

    /// Checks if an aggregate exists, using a typed ID.
    ///
    /// # Errors
    ///
    /// Returns an error if the store operation fails.
    pub async fn exists<I: AggregateId>(&self, id: I) -> Result<bool>
    where
        I::Aggregate: Aggregate,
    {
        let stream_id =
            crate::StreamId::new(I::Aggregate::aggregate_type(), id.as_entity_id().as_uuid());
        self.store.stream_exists(stream_id).await
    }

    // === Commit methods (inject causation metadata) ===

    /// Commits an aggregate with automatic causation tracking.
    ///
    /// Injects causation metadata into all pending events and buffers the
    /// prepared commit. The buffered events are only persisted when
    /// `flush()` is called (typically by
    /// [`PolicyRunner`](crate::PolicyRunner) after the handler returns `Ok`).
    /// If the handler returns `Err`, the buffer is dropped and nothing is
    /// persisted.
    ///
    /// The aggregate's pending events are cleared as soon as they are
    /// buffered here, unlike [`EventStore::commit()`] which waits for the
    /// write to actually persist: this context's aggregates are discarded by
    /// the caller once the reaction returns (success or failure), so there is
    /// no retry path that needs them kept.
    ///
    /// # Errors
    ///
    /// Returns `Error::CascadeDepthExceeded` if the cascade depth exceeds the limit.
    /// Returns errors from serialization or encryption during preparation.
    ///
    /// # Panics
    ///
    /// Panics if the internal mutex is poisoned (only possible if a prior
    /// panic occurred while holding the lock).
    pub async fn commit<A>(&self, aggregate: &mut AggregateRoot<A>) -> Result<()>
    where
        A: Aggregate + serde::Serialize,
        A::Event: serde::Serialize,
    {
        let metadata = self.build_causation_metadata()?;
        aggregate.set_pending_metadata(&metadata);
        if let Some(prepared) = crate::event_store::prepare_commit(&*self.store, aggregate).await? {
            self.pending_commits.lock().unwrap().push(prepared);
            aggregate.clear_pending_events();
        }
        Ok(())
    }

    /// Commits a deleted aggregate with automatic causation tracking.
    ///
    /// Like [`commit()`](Self::commit), buffers the prepared commit for later
    /// flushing and clears the aggregate's pending events immediately.
    ///
    /// # Errors
    ///
    /// Returns `Error::CascadeDepthExceeded` if the cascade depth exceeds the limit.
    /// Returns errors from serialization or encryption during preparation.
    ///
    /// # Panics
    ///
    /// Panics if the internal mutex is poisoned (only possible if a prior
    /// panic occurred while holding the lock).
    pub async fn commit_deleted<A>(&self, aggregate: &mut DeletedAggregateRoot<A>) -> Result<()>
    where
        A: Aggregate + serde::Serialize,
        A::DeletedState: serde::Serialize,
        A::Event: serde::Serialize,
    {
        let metadata = self.build_causation_metadata()?;
        aggregate.set_pending_metadata(&metadata);
        if let Some(prepared) = crate::event_store::prepare_commit(&*self.store, aggregate).await? {
            self.pending_commits.lock().unwrap().push(prepared);
            aggregate.clear_pending_events();
        }
        Ok(())
    }

    /// Flushes all buffered commits to the event store as one logical write.
    ///
    /// Called by [`PolicyRunner`](crate::PolicyRunner) after a handler returns
    /// `Ok(())`. If the handler returns `Err`, this method is never called
    /// and the buffered commits are silently dropped.
    ///
    /// All buffered commits go through a single
    /// [`append_batch`](EventStore::append_batch) call, so a handler that reacts
    /// across several aggregates commits atomically on backends with real
    /// transactions (e.g. `PostgreSQL`): the whole reaction succeeds or none of it
    /// is persisted. On the in-memory backend the commits are applied per-stream.
    ///
    /// # Errors
    ///
    /// Returns an error if appending events or saving snapshots fails.
    pub(crate) async fn flush(&self) -> Result<()> {
        let commits: Vec<PreparedCommit> = self.pending_commits.lock().unwrap().drain(..).collect();
        crate::event_store::flush_prepared_batch(&*self.store, commits).await
    }

    // === Introspection ===

    /// Returns the event being reacted to.
    #[must_use]
    pub fn source_event(&self) -> &EventEnvelope {
        &self.source_event
    }

    /// Returns the current cascade depth.
    #[must_use]
    pub fn cascade_depth(&self) -> usize {
        self.source_event
            .metadata
            .as_ref()
            .map_or(0, EventMetadata::cascade_depth)
    }

    // === Internal ===

    /// Builds causation metadata from the source event.
    ///
    /// Returns `Error::CascadeDepthExceeded` if the chain would exceed the limit.
    fn build_causation_metadata(&self) -> Result<EventMetadata> {
        let source_id = self.source_event.id;

        let parent_metadata = self.source_event.metadata.clone().unwrap_or_default();
        let child = parent_metadata.child_metadata(source_id);

        let depth = child.cascade_depth();
        if depth > self.max_cascade_depth {
            return Err(crate::Error::cascade_depth_exceeded(
                depth,
                self.max_cascade_depth,
            ));
        }

        Ok(child)
    }
}

impl<S: EventStore + 'static> std::fmt::Debug for PolicyContext<S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let pending_count = self.pending_commits.lock().unwrap().len();
        f.debug_struct("PolicyContext")
            .field("source_event_id", &self.source_event.id)
            .field("max_cascade_depth", &self.max_cascade_depth)
            .field("cascade_depth", &self.cascade_depth())
            .field("pending_commits", &pending_count)
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use super::super::test_support::test_envelope;
    use super::*;
    use crate::test_fixtures::{MockEventStore, SimpleTestEntity, SimpleTestEvent};
    use crate::{AggregateRoot, AggregateVersion};
    use uuid::Uuid;

    #[test]
    fn test_policy_context_new() {
        let store = Arc::new(MockEventStore::new());
        let envelope = test_envelope("TestEvent", "TestAggregate");
        let ctx = PolicyContext::new(store, envelope.clone(), 10);

        assert_eq!(ctx.source_event().id, envelope.id);
        assert_eq!(ctx.cascade_depth(), 0);
    }

    #[test]
    fn test_policy_context_cascade_depth_from_metadata() {
        let store = Arc::new(MockEventStore::new());
        let chain = vec![Uuid::new_v4(), Uuid::new_v4()];
        let metadata = EventMetadata::new().with_causation_chain(chain);
        let envelope = test_envelope("TestEvent", "TestAggregate").with_metadata(metadata);
        let ctx = PolicyContext::new(store, envelope, 10);

        assert_eq!(ctx.cascade_depth(), 2);
    }

    #[test]
    fn test_policy_context_build_causation_metadata() {
        let store = Arc::new(MockEventStore::new());
        let correlation_id = Uuid::new_v4();
        let metadata = EventMetadata::new().with_correlation_id(correlation_id);
        let envelope = test_envelope("TestEvent", "TestAggregate").with_metadata(metadata);
        let source_id = envelope.id;
        let ctx = PolicyContext::new(store, envelope, 10);

        let child = ctx.build_causation_metadata().unwrap();

        assert_eq!(child.correlation_id, Some(correlation_id));
        assert_eq!(child.causation_id, Some(source_id));
        assert_eq!(child.causation_chain.len(), 1);
        assert_eq!(child.causation_chain[0], source_id);
    }

    #[test]
    fn test_policy_context_build_causation_metadata_no_parent_metadata() {
        let store = Arc::new(MockEventStore::new());
        let envelope = test_envelope("TestEvent", "TestAggregate");
        let source_id = envelope.id;
        let ctx = PolicyContext::new(store, envelope, 10);

        let child = ctx.build_causation_metadata().unwrap();

        assert_eq!(child.correlation_id, Some(source_id));
        assert_eq!(child.causation_id, Some(source_id));
        assert_eq!(child.causation_chain, vec![source_id]);
    }

    #[test]
    fn test_policy_context_build_causation_metadata_extends_chain() {
        let store = Arc::new(MockEventStore::new());
        let root_id = Uuid::new_v4();
        let parent_id = Uuid::new_v4();
        let correlation_id = Uuid::new_v4();

        let metadata = EventMetadata::new()
            .with_correlation_id(correlation_id)
            .with_causation_id(parent_id)
            .with_causation_chain(vec![root_id, parent_id]);

        let envelope = test_envelope("TestEvent", "TestAggregate").with_metadata(metadata);
        let source_id = envelope.id;
        let ctx = PolicyContext::new(store, envelope, 10);

        let child = ctx.build_causation_metadata().unwrap();

        assert_eq!(child.correlation_id, Some(correlation_id));
        assert_eq!(child.causation_id, Some(source_id));
        assert_eq!(child.causation_chain.len(), 3);
        assert_eq!(child.causation_chain[0], root_id);
        assert_eq!(child.causation_chain[1], parent_id);
        assert_eq!(child.causation_chain[2], source_id);
    }

    #[test]
    fn test_policy_context_cascade_depth_exceeded() {
        let store = Arc::new(MockEventStore::new());
        let chain = vec![Uuid::new_v4(); 5];
        let metadata = EventMetadata::new().with_causation_chain(chain);
        let envelope = test_envelope("TestEvent", "TestAggregate").with_metadata(metadata);
        let ctx = PolicyContext::new(store, envelope, 5);

        let result = ctx.build_causation_metadata();
        assert!(result.is_err());
        assert!(result.unwrap_err().is_cascade_depth_exceeded());
    }

    #[test]
    fn test_policy_context_cascade_depth_at_limit_succeeds() {
        let store = Arc::new(MockEventStore::new());
        let chain = vec![Uuid::new_v4(); 4];
        let metadata = EventMetadata::new().with_causation_chain(chain);
        let envelope = test_envelope("TestEvent", "TestAggregate").with_metadata(metadata);
        let ctx = PolicyContext::new(store, envelope, 5);

        let result = ctx.build_causation_metadata();
        assert!(result.is_ok());
        assert_eq!(result.unwrap().cascade_depth(), 5);
    }

    /// Buffering needs this clear to happen eagerly (unlike the direct
    /// `commit`/`commit_deleted` paths, which only clear after the flush
    /// actually persists): the runner discards this context's aggregates on
    /// failure anyway, so a handler inspecting `pending_events()` after
    /// `ctx.commit()` must see the buffered commit reflected immediately.
    #[tokio::test]
    async fn test_policy_context_commit_clears_pending_before_flush() {
        let store = Arc::new(MockEventStore::new());
        let envelope = test_envelope("TestEvent", "TestAggregate");
        let ctx = PolicyContext::new(store, envelope, 10);

        let id = crate::EntityId::new();
        let mut agg = AggregateRoot::<SimpleTestEntity>::new(id);
        agg.apply(SimpleTestEvent::Created { value: 42 }).unwrap();

        ctx.commit(&mut agg).await.unwrap();

        assert!(agg.pending_events().is_empty());
    }

    #[tokio::test]
    async fn test_policy_context_commit_injects_metadata() {
        let store = Arc::new(MockEventStore::new());
        let envelope = test_envelope("TestEvent", "TestAggregate");
        let source_id = envelope.id;
        let ctx = PolicyContext::new(Arc::clone(&store), envelope, 10);

        let id = crate::EntityId::new();
        let mut agg = AggregateRoot::<SimpleTestEntity>::new(id);
        agg.apply(SimpleTestEvent::Created { value: 42 }).unwrap();

        ctx.commit(&mut agg).await.unwrap();
        ctx.flush().await.unwrap();

        let loaded = crate::event_store::load::<_, SimpleTestEntity>(&*store, id)
            .await
            .unwrap();
        assert_eq!(loaded.value, 42);

        let stream_id = crate::StreamId::new("SimpleTestEntity", id.as_uuid());
        let version = store.get_version(stream_id).await.unwrap();
        assert_eq!(version, AggregateVersion::new(1));

        assert!(agg.pending_events().is_empty());

        let stored = store.get_events();
        let last_envelope = stored.last().unwrap();
        let meta = last_envelope
            .metadata
            .as_ref()
            .expect("Should have metadata");
        assert_eq!(meta.causation_id, Some(source_id));
        assert_eq!(meta.correlation_id, Some(source_id));
        assert_eq!(meta.causation_chain, vec![source_id]);
    }

    #[tokio::test]
    async fn test_policy_context_commit_cascade_depth_exceeded() {
        let store = Arc::new(MockEventStore::new());
        let chain = vec![Uuid::new_v4(); 3];
        let metadata = EventMetadata::new().with_causation_chain(chain);
        let envelope = test_envelope("TestEvent", "TestAggregate").with_metadata(metadata);
        let ctx = PolicyContext::new(Arc::clone(&store), envelope, 3);

        let id = crate::EntityId::new();
        let mut agg = AggregateRoot::<SimpleTestEntity>::new(id);
        agg.apply(SimpleTestEvent::Created { value: 1 }).unwrap();

        let result = ctx.commit(&mut agg).await;
        assert!(result.is_err());
        assert!(result.unwrap_err().is_cascade_depth_exceeded());
    }

    #[tokio::test]
    async fn test_policy_context_load_as() {
        let store = Arc::new(MockEventStore::new());

        let id = crate::EntityId::new();
        let mut agg = AggregateRoot::<SimpleTestEntity>::new(id);
        agg.apply(SimpleTestEvent::Created { value: 99 }).unwrap();
        store.commit(&mut agg).await.unwrap();

        let envelope = test_envelope("TestEvent", "TestAggregate");
        let ctx = PolicyContext::new(Arc::clone(&store), envelope, 10);

        let loaded = ctx.load_as::<SimpleTestEntity>(id).await.unwrap();
        assert_eq!(loaded.value, 99);
    }

    #[test]
    fn test_policy_context_debug() {
        let store = Arc::new(MockEventStore::new());
        let envelope = test_envelope("TestEvent", "TestAggregate");
        let ctx = PolicyContext::new(store, envelope, 10);

        let debug = format!("{ctx:?}");
        assert!(debug.contains("PolicyContext"));
        assert!(debug.contains("max_cascade_depth: 10"));
        assert!(debug.contains("cascade_depth: 0"));
        assert!(debug.contains("pending_commits: 0"));
    }

    #[tokio::test]
    async fn test_policy_context_load_any_as() {
        let store = Arc::new(MockEventStore::new());

        let id = crate::EntityId::new();
        let mut agg = AggregateRoot::<SimpleTestEntity>::new(id);
        agg.apply(SimpleTestEvent::Created { value: 55 }).unwrap();
        store.commit(&mut agg).await.unwrap();

        let envelope = test_envelope("TestEvent", "TestAggregate");
        let ctx = PolicyContext::new(Arc::clone(&store), envelope, 10);

        let loaded = ctx.load_any_as::<SimpleTestEntity>(id).await.unwrap();
        assert!(loaded.is_active());
    }

    #[tokio::test]
    async fn test_policy_context_commit_preserves_existing_metadata() {
        let store = Arc::new(MockEventStore::new());
        let correlation_id = Uuid::new_v4();
        let parent_metadata = EventMetadata::new().with_correlation_id(correlation_id);
        let envelope = test_envelope("TestEvent", "TestAggregate").with_metadata(parent_metadata);
        let source_id = envelope.id;
        let ctx = PolicyContext::new(Arc::clone(&store), envelope, 10);

        let id = crate::EntityId::new();
        let mut agg = AggregateRoot::<SimpleTestEntity>::new(id);

        let existing = EventMetadata::new().with_causation_id(Uuid::new_v4());
        agg.apply_with_metadata(SimpleTestEvent::Created { value: 1 }, existing.clone())
            .unwrap();
        agg.apply(SimpleTestEvent::Updated { value: 2 }).unwrap();

        ctx.commit(&mut agg).await.unwrap();
        ctx.flush().await.unwrap();

        let stored = store.get_events();
        assert_eq!(stored.len(), 2);

        let meta0 = stored[0].metadata.as_ref().unwrap();
        assert_eq!(meta0.causation_id, existing.causation_id);

        let meta1 = stored[1].metadata.as_ref().unwrap();
        assert_eq!(meta1.causation_id, Some(source_id));
        assert_eq!(meta1.correlation_id, Some(correlation_id));
        assert_eq!(meta1.causation_chain, vec![source_id]);
    }

    #[tokio::test]
    async fn test_policy_context_commit_without_flush_does_not_persist() {
        let store = Arc::new(MockEventStore::new());
        let envelope = test_envelope("TestEvent", "TestAggregate");
        let ctx = PolicyContext::new(Arc::clone(&store), envelope, 10);

        let id = crate::EntityId::new();
        let mut agg = AggregateRoot::<SimpleTestEntity>::new(id);
        agg.apply(SimpleTestEvent::Created { value: 42 }).unwrap();

        ctx.commit(&mut agg).await.unwrap();

        let stored = store.get_events();
        assert!(
            stored.is_empty(),
            "Events should be buffered, not persisted"
        );

        assert!(agg.pending_events().is_empty());
    }

    #[tokio::test]
    async fn test_policy_context_commit_then_flush_persists() {
        let store = Arc::new(MockEventStore::new());
        let envelope = test_envelope("TestEvent", "TestAggregate");
        let ctx = PolicyContext::new(Arc::clone(&store), envelope, 10);

        let id = crate::EntityId::new();
        let mut agg = AggregateRoot::<SimpleTestEntity>::new(id);
        agg.apply(SimpleTestEvent::Created { value: 42 }).unwrap();

        ctx.commit(&mut agg).await.unwrap();
        ctx.flush().await.unwrap();

        let stored = store.get_events();
        assert_eq!(stored.len(), 1, "Events should be persisted after flush");
    }

    #[tokio::test]
    async fn test_policy_context_multiple_commits_then_flush() {
        let store = Arc::new(MockEventStore::new());
        let envelope = test_envelope("TestEvent", "TestAggregate");
        let ctx = PolicyContext::new(Arc::clone(&store), envelope, 10);

        let id1 = crate::EntityId::new();
        let mut agg1 = AggregateRoot::<SimpleTestEntity>::new(id1);
        agg1.apply(SimpleTestEvent::Created { value: 1 }).unwrap();
        ctx.commit(&mut agg1).await.unwrap();

        let id2 = crate::EntityId::new();
        let mut agg2 = AggregateRoot::<SimpleTestEntity>::new(id2);
        agg2.apply(SimpleTestEvent::Created { value: 2 }).unwrap();
        ctx.commit(&mut agg2).await.unwrap();

        assert!(store.get_events().is_empty());

        ctx.flush().await.unwrap();

        let stored = store.get_events();
        assert_eq!(stored.len(), 2, "Both commits should be persisted");
    }

    #[tokio::test]
    async fn test_policy_context_multiple_commits_dropped_on_error() {
        let store = Arc::new(MockEventStore::new());
        let envelope = test_envelope("TestEvent", "TestAggregate");

        {
            let ctx = PolicyContext::new(Arc::clone(&store), envelope, 10);

            let id = crate::EntityId::new();
            let mut agg = AggregateRoot::<SimpleTestEntity>::new(id);
            agg.apply(SimpleTestEvent::Created { value: 1 }).unwrap();
            ctx.commit(&mut agg).await.unwrap();

            // Simulate handler error — ctx is dropped without flush
        }

        let stored = store.get_events();
        assert!(
            stored.is_empty(),
            "Events should not be persisted when ctx is dropped without flush"
        );
    }

    #[tokio::test]
    async fn test_policy_context_flush_without_commits_is_noop() {
        let store = Arc::new(MockEventStore::new());
        let envelope = test_envelope("TestEvent", "TestAggregate");
        let ctx = PolicyContext::new(Arc::clone(&store), envelope, 10);

        ctx.flush().await.unwrap();

        assert!(store.get_events().is_empty());
    }

    #[tokio::test]
    async fn test_policy_context_debug_shows_pending_count() {
        let store = Arc::new(MockEventStore::new());
        let envelope = test_envelope("TestEvent", "TestAggregate");
        let ctx = PolicyContext::new(Arc::clone(&store), envelope, 10);

        let id = crate::EntityId::new();
        let mut agg = AggregateRoot::<SimpleTestEntity>::new(id);
        agg.apply(SimpleTestEvent::Created { value: 1 }).unwrap();
        ctx.commit(&mut agg).await.unwrap();

        let debug = format!("{ctx:?}");
        assert!(
            debug.contains("pending_commits: 1"),
            "Debug should show 1 pending commit, got: {debug}"
        );
    }
}
