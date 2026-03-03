//! Reactor system for cross-aggregate event reactions.
//!
//! Provides the [`Reactor`] trait for defining event handlers that react to events
//! by issuing commands on other aggregates, and [`ReactorContext`] for safe event
//! store access with automatic causation tracking.
//!
//! # Overview
//!
//! In an event-sourced system, a single command may produce events that should
//! trigger effects on other aggregates. For example, kicking a user may need to
//! update a group aggregate. Reactors provide this cross-aggregate orchestration.
//!
//! # Design Principles
//!
//! - **Async/eventually consistent**: Reactors process events outside the original
//!   transaction.
//! - **Forced causation tracking**: All interactions go through [`ReactorContext`],
//!   which guarantees causation metadata is always set.
//! - **Cascade depth limits**: Reactions can trigger further reactions; depth is
//!   bounded to prevent infinite loops.
//!
//! # Examples
//!
//! ```ignore
//! use event_sauce_core::reactor::{Reactor, ReactorContext, ReactorRunner};
//!
//! struct MyReactor;
//!
//! #[async_trait::async_trait]
//! impl<S: EventStore + 'static> Reactor<S> for MyReactor {
//!     fn name(&self) -> &str { "MyReactor" }
//!     fn event_filter(&self) -> EventFilter { EventFilter::by_event_type("User.Kicked") }
//!     async fn handle(&self, event: &EventEnvelope, ctx: &ReactorContext<S>) -> Result<()> {
//!         // Load, modify, and commit another aggregate
//!         Ok(())
//!     }
//! }
//! ```

use std::sync::Arc;

use async_trait::async_trait;

use crate::{
    Aggregate, AggregateId, AggregateRoot, DeletedAggregateRoot, EntityIdFor, EventEnvelope,
    EventFilter, EventMetadata, EventStore, Loaded, Result,
};

/// A reactor handles events by issuing commands on other aggregates.
///
/// Reactors are the mechanism for cross-aggregate event orchestration.
/// Each reactor declares which events it handles via [`event_filter()`](Reactor::event_filter)
/// and processes matching events in [`handle()`](Reactor::handle).
///
/// The type parameter `S` is the event store type, allowing the reactor to
/// work with any `EventStore` implementation while remaining dyn-compatible.
///
/// # Implementing a Reactor
///
/// ```ignore
/// struct NotifyOnKick;
///
/// #[async_trait]
/// impl<S: EventStore + 'static> Reactor<S> for NotifyOnKick {
///     fn name(&self) -> &str { "NotifyOnKick" }
///
///     fn event_filter(&self) -> EventFilter {
///         EventFilter::by_event_type("User.Kicked")
///     }
///
///     async fn handle(&self, event: &EventEnvelope, ctx: &ReactorContext<S>) -> Result<()> {
///         // React to the event by loading and modifying other aggregates
///         Ok(())
///     }
/// }
/// ```
#[async_trait]
pub trait Reactor<S: EventStore + 'static>: Send + Sync {
    /// Unique name for this reactor.
    ///
    /// Used for subscription checkpoint tracking, so each reactor resumes
    /// from where it left off after restarts.
    fn name(&self) -> &str;

    /// Which events this reactor handles.
    ///
    /// Only events matching this filter will be passed to [`handle()`](Self::handle).
    fn event_filter(&self) -> EventFilter;

    /// Handle a single event.
    ///
    /// Use `ctx` to load aggregates and commit changes. The context
    /// automatically injects causation metadata into all produced events.
    ///
    /// # Errors
    ///
    /// Returns an error if loading, applying, or committing fails.
    async fn handle(&self, event: &EventEnvelope, ctx: &ReactorContext<S>) -> Result<()>;
}

/// Provides event store access with automatic causation tracking.
///
/// `ReactorContext` wraps an event store and a source event. All load operations
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
/// async fn handle(&self, event: &EventEnvelope, ctx: &ReactorContext<impl EventStore>) -> Result<()> {
///     let mut group = ctx.load_as::<Group>(some_entity_id).await?;
///     group.apply(SomeEvent { ... })?;
///     ctx.commit(&mut group).await?;
///     Ok(())
/// }
/// ```
pub struct ReactorContext<S: EventStore> {
    store: Arc<S>,
    source_event: EventEnvelope,
    max_cascade_depth: usize,
}

impl<S: EventStore + 'static> ReactorContext<S> {
    /// Creates a new reactor context.
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
    /// Injects causation metadata into all pending events before delegating
    /// to the event store. Returns `Error::CascadeDepthExceeded` if the
    /// chain is too deep.
    ///
    /// # Errors
    ///
    /// Returns `Error::CascadeDepthExceeded` if the cascade depth exceeds the limit.
    /// Returns errors from the underlying event store commit.
    pub async fn commit<A>(&self, aggregate: &mut AggregateRoot<A>) -> Result<()>
    where
        A: Aggregate + serde::Serialize,
        A::Event: serde::Serialize,
    {
        let metadata = self.build_causation_metadata()?;
        aggregate.set_pending_metadata(&metadata);
        self.store.commit(aggregate).await
    }

    /// Commits a deleted aggregate with automatic causation tracking.
    ///
    /// # Errors
    ///
    /// Returns `Error::CascadeDepthExceeded` if the cascade depth exceeds the limit.
    /// Returns errors from the underlying event store commit.
    pub async fn commit_deleted<A>(&self, aggregate: &mut DeletedAggregateRoot<A>) -> Result<()>
    where
        A: Aggregate + serde::Serialize,
        A::DeletedState: serde::Serialize,
        A::Event: serde::Serialize,
    {
        let metadata = self.build_causation_metadata()?;
        aggregate.set_pending_metadata(&metadata);
        self.store.commit_deleted(aggregate).await
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

        // Get or create parent metadata
        let parent_metadata = self.source_event.metadata.clone().unwrap_or_default();

        let child = parent_metadata.child_metadata(source_id);

        // Check depth limit
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

impl<S: EventStore + 'static> std::fmt::Debug for ReactorContext<S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ReactorContext")
            .field("source_event_id", &self.source_event.id)
            .field("max_cascade_depth", &self.max_cascade_depth)
            .field("cascade_depth", &self.cascade_depth())
            .finish_non_exhaustive()
    }
}

/// Runs reactors by routing events to matching handlers.
///
/// `ReactorRunner` manages a set of reactors and provides methods for
/// processing events either continuously or in one-shot mode
/// (`process_pending()`).
///
/// # Builder Pattern
///
/// ```ignore
/// let runner = ReactorRunner::new(store)
///     .with_max_cascade_depth(5)
///     .register(Arc::new(MyReactor));
/// ```
pub struct ReactorRunner<S: EventStore> {
    store: Arc<S>,
    reactors: Vec<Arc<dyn Reactor<S>>>,
    max_cascade_depth: usize,
}

impl<S: EventStore + 'static> ReactorRunner<S> {
    /// Creates a new reactor runner.
    #[must_use]
    pub fn new(store: Arc<S>) -> Self {
        Self {
            store,
            reactors: Vec::new(),
            max_cascade_depth: 10,
        }
    }

    /// Sets the maximum cascade depth (default: 10).
    #[must_use]
    pub fn with_max_cascade_depth(mut self, depth: usize) -> Self {
        self.max_cascade_depth = depth;
        self
    }

    /// Registers a reactor.
    #[must_use]
    pub fn register(mut self, reactor: Arc<dyn Reactor<S>>) -> Self {
        self.reactors.push(reactor);
        self
    }

    /// Returns the registered reactors.
    #[must_use]
    pub fn reactors(&self) -> &[Arc<dyn Reactor<S>>] {
        &self.reactors
    }

    /// Processes all pending events in one-shot mode.
    ///
    /// Loads all events from the store, routes them to matching reactors,
    /// and processes cascading reactions until no new events are produced
    /// or the max depth is reached.
    ///
    /// Returns the total number of events processed.
    ///
    /// # Errors
    ///
    /// Returns an error if event loading, reactor handling, or committing fails.
    pub async fn process_pending(&self) -> Result<usize> {
        use futures::StreamExt;

        let mut total_processed = 0;
        let mut from_position = crate::Position::start();

        // Process events in rounds to handle cascading
        loop {
            let event_stream = self.store.stream_all(from_position).await?;
            futures::pin_mut!(event_stream);

            let mut round_processed = 0;
            let mut last_position = from_position;

            while let Some(envelope_result) = event_stream.next().await {
                let envelope = envelope_result?;

                // Track position for next round
                last_position = crate::Position::new(last_position.as_i64() + 1);

                // Route to matching reactors
                for reactor in &self.reactors {
                    if reactor.event_filter().matches(&envelope) {
                        let ctx = ReactorContext::new(
                            Arc::clone(&self.store),
                            envelope.clone(),
                            self.max_cascade_depth,
                        );
                        reactor.handle(&envelope, &ctx).await?;
                        round_processed += 1;
                    }
                }
            }

            total_processed += round_processed;

            // If no events were processed this round, we're done
            if round_processed == 0 {
                break;
            }

            // Move position forward for next round (to pick up cascaded events)
            from_position = last_position;
        }

        Ok(total_processed)
    }

    /// Processes a single event envelope through matching reactors.
    ///
    /// Returns the number of reactors that handled the event.
    ///
    /// # Errors
    ///
    /// Returns an error if reactor handling fails.
    pub async fn process_event(&self, envelope: &EventEnvelope) -> Result<usize> {
        let mut handled = 0;

        for reactor in &self.reactors {
            if reactor.event_filter().matches(envelope) {
                let ctx = ReactorContext::new(
                    Arc::clone(&self.store),
                    envelope.clone(),
                    self.max_cascade_depth,
                );
                reactor.handle(envelope, &ctx).await?;
                handled += 1;
            }
        }

        Ok(handled)
    }
}

impl<S: EventStore + 'static> std::fmt::Debug for ReactorRunner<S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let reactor_names: Vec<&str> = self.reactors.iter().map(|r| r.name()).collect();
        f.debug_struct("ReactorRunner")
            .field("reactors", &reactor_names)
            .field("max_cascade_depth", &self.max_cascade_depth)
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_fixtures::{MockEventStore, SimpleTestEntity, SimpleTestEvent};
    use crate::{AggregateRoot, AggregateVersion, EventVersion};
    use serde_json::json;
    use uuid::Uuid;

    // Helper to create a test envelope
    fn test_envelope(event_type: &str, aggregate_type: &str) -> EventEnvelope {
        EventEnvelope::new(
            Uuid::new_v4(),
            Uuid::new_v4(),
            aggregate_type.to_string(),
            event_type.to_string(),
            EventVersion::new(1),
            json!({}),
        )
    }

    // === ReactorContext tests ===

    #[test]
    fn test_reactor_context_new() {
        let store = Arc::new(MockEventStore::new());
        let envelope = test_envelope("TestEvent", "TestAggregate");
        let ctx = ReactorContext::new(store, envelope.clone(), 10);

        assert_eq!(ctx.source_event().id, envelope.id);
        assert_eq!(ctx.cascade_depth(), 0);
    }

    #[test]
    fn test_reactor_context_cascade_depth_from_metadata() {
        let store = Arc::new(MockEventStore::new());
        let chain = vec![Uuid::new_v4(), Uuid::new_v4()];
        let metadata = EventMetadata::new().with_causation_chain(chain);
        let envelope = test_envelope("TestEvent", "TestAggregate").with_metadata(metadata);
        let ctx = ReactorContext::new(store, envelope, 10);

        assert_eq!(ctx.cascade_depth(), 2);
    }

    #[test]
    fn test_reactor_context_build_causation_metadata() {
        let store = Arc::new(MockEventStore::new());
        let correlation_id = Uuid::new_v4();
        let metadata = EventMetadata::new().with_correlation_id(correlation_id);
        let envelope = test_envelope("TestEvent", "TestAggregate").with_metadata(metadata);
        let source_id = envelope.id;
        let ctx = ReactorContext::new(store, envelope, 10);

        let child = ctx.build_causation_metadata().unwrap();

        assert_eq!(child.correlation_id, Some(correlation_id));
        assert_eq!(child.causation_id, Some(source_id));
        assert_eq!(child.causation_chain.len(), 1);
        assert_eq!(child.causation_chain[0], source_id);
    }

    #[test]
    fn test_reactor_context_build_causation_metadata_no_parent_metadata() {
        let store = Arc::new(MockEventStore::new());
        let envelope = test_envelope("TestEvent", "TestAggregate");
        let source_id = envelope.id;
        let ctx = ReactorContext::new(store, envelope, 10);

        let child = ctx.build_causation_metadata().unwrap();

        // Without parent correlation_id, uses parent event ID
        assert_eq!(child.correlation_id, Some(source_id));
        assert_eq!(child.causation_id, Some(source_id));
        assert_eq!(child.causation_chain, vec![source_id]);
    }

    #[test]
    fn test_reactor_context_build_causation_metadata_extends_chain() {
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
        let ctx = ReactorContext::new(store, envelope, 10);

        let child = ctx.build_causation_metadata().unwrap();

        assert_eq!(child.correlation_id, Some(correlation_id));
        assert_eq!(child.causation_id, Some(source_id));
        assert_eq!(child.causation_chain.len(), 3);
        assert_eq!(child.causation_chain[0], root_id);
        assert_eq!(child.causation_chain[1], parent_id);
        assert_eq!(child.causation_chain[2], source_id);
    }

    #[test]
    fn test_reactor_context_cascade_depth_exceeded() {
        let store = Arc::new(MockEventStore::new());
        let chain = vec![Uuid::new_v4(); 5];
        let metadata = EventMetadata::new().with_causation_chain(chain);
        let envelope = test_envelope("TestEvent", "TestAggregate").with_metadata(metadata);
        let ctx = ReactorContext::new(store, envelope, 5);

        // Chain has 5 elements, adding 1 more = 6, which exceeds max of 5
        let result = ctx.build_causation_metadata();
        assert!(result.is_err());
        assert!(result.unwrap_err().is_cascade_depth_exceeded());
    }

    #[test]
    fn test_reactor_context_cascade_depth_at_limit_succeeds() {
        let store = Arc::new(MockEventStore::new());
        let chain = vec![Uuid::new_v4(); 4];
        let metadata = EventMetadata::new().with_causation_chain(chain);
        let envelope = test_envelope("TestEvent", "TestAggregate").with_metadata(metadata);
        let ctx = ReactorContext::new(store, envelope, 5);

        // Chain has 4 elements, adding 1 more = 5, which equals max of 5
        let result = ctx.build_causation_metadata();
        assert!(result.is_ok());
        assert_eq!(result.unwrap().cascade_depth(), 5);
    }

    #[tokio::test]
    async fn test_reactor_context_commit_injects_metadata() {
        let store = Arc::new(MockEventStore::new());
        let envelope = test_envelope("TestEvent", "TestAggregate");
        let source_id = envelope.id;
        let ctx = ReactorContext::new(Arc::clone(&store), envelope, 10);

        let id = crate::EntityId::new();
        let mut agg = AggregateRoot::<SimpleTestEntity>::new(id);
        agg.apply(SimpleTestEvent::Created { value: 42 }).unwrap();

        ctx.commit(&mut agg).await.unwrap();

        // Load and verify the committed event has causation metadata
        let loaded = crate::event_store::load::<_, SimpleTestEntity>(&*store, id)
            .await
            .unwrap();
        assert_eq!(loaded.value, 42);

        // Verify events were committed (version should be 1)
        let stream_id = crate::StreamId::new("SimpleTestEntity", id.as_uuid());
        let version = store.get_version(stream_id).await.unwrap();
        assert_eq!(version, AggregateVersion::new(1));

        // Verify pending events were cleared
        assert!(agg.pending_events().is_empty());

        // Verify the stored envelope has metadata
        let stored = store.get_events();
        let last_envelope = stored.last().unwrap();
        let meta = last_envelope
            .metadata
            .as_ref()
            .expect("Should have metadata");
        assert_eq!(meta.causation_id, Some(source_id));
        assert_eq!(meta.correlation_id, Some(source_id)); // No parent correlation, uses source id
        assert_eq!(meta.causation_chain, vec![source_id]);
    }

    #[tokio::test]
    async fn test_reactor_context_commit_cascade_depth_exceeded() {
        let store = Arc::new(MockEventStore::new());
        let chain = vec![Uuid::new_v4(); 3];
        let metadata = EventMetadata::new().with_causation_chain(chain);
        let envelope = test_envelope("TestEvent", "TestAggregate").with_metadata(metadata);
        let ctx = ReactorContext::new(Arc::clone(&store), envelope, 3);

        let id = crate::EntityId::new();
        let mut agg = AggregateRoot::<SimpleTestEntity>::new(id);
        agg.apply(SimpleTestEvent::Created { value: 1 }).unwrap();

        let result = ctx.commit(&mut agg).await;
        assert!(result.is_err());
        assert!(result.unwrap_err().is_cascade_depth_exceeded());
    }

    #[tokio::test]
    async fn test_reactor_context_load_as() {
        let store = Arc::new(MockEventStore::new());

        // Commit an aggregate first
        let id = crate::EntityId::new();
        let mut agg = AggregateRoot::<SimpleTestEntity>::new(id);
        agg.apply(SimpleTestEvent::Created { value: 99 }).unwrap();
        store.commit(&mut agg).await.unwrap();

        // Load via reactor context
        let envelope = test_envelope("TestEvent", "TestAggregate");
        let ctx = ReactorContext::new(Arc::clone(&store), envelope, 10);

        let loaded = ctx.load_as::<SimpleTestEntity>(id).await.unwrap();
        assert_eq!(loaded.value, 99);
    }

    #[test]
    fn test_reactor_context_debug() {
        let store = Arc::new(MockEventStore::new());
        let envelope = test_envelope("TestEvent", "TestAggregate");
        let ctx = ReactorContext::new(store, envelope, 10);

        let debug = format!("{ctx:?}");
        assert!(debug.contains("ReactorContext"));
        assert!(debug.contains("max_cascade_depth: 10"));
        assert!(debug.contains("cascade_depth: 0"));
    }

    // === ReactorRunner tests ===

    #[test]
    fn test_reactor_runner_new() {
        let store = Arc::new(MockEventStore::new());
        let runner = ReactorRunner::new(store);

        assert!(runner.reactors().is_empty());
    }

    #[test]
    fn test_reactor_runner_with_max_cascade_depth() {
        let store = Arc::new(MockEventStore::new());
        let runner = ReactorRunner::new(store).with_max_cascade_depth(5);

        assert_eq!(runner.max_cascade_depth, 5);
    }

    struct CountingReactor {
        name: String,
        filter: EventFilter,
        count: Arc<std::sync::Mutex<usize>>,
    }

    #[async_trait]
    impl<S: EventStore + 'static> Reactor<S> for CountingReactor {
        fn name(&self) -> &str {
            &self.name
        }

        fn event_filter(&self) -> EventFilter {
            self.filter.clone()
        }

        async fn handle(&self, _event: &EventEnvelope, _ctx: &ReactorContext<S>) -> Result<()> {
            *self.count.lock().unwrap() += 1;
            Ok(())
        }
    }

    #[test]
    fn test_reactor_runner_register() {
        let store = Arc::new(MockEventStore::new());
        let count = Arc::new(std::sync::Mutex::new(0));
        let reactor = Arc::new(CountingReactor {
            name: "test".to_string(),
            filter: EventFilter::all(),
            count,
        });

        let runner = ReactorRunner::new(store).register(reactor);
        assert_eq!(runner.reactors().len(), 1);
    }

    #[tokio::test]
    async fn test_reactor_runner_process_event() {
        let store = Arc::new(MockEventStore::new());
        let count = Arc::new(std::sync::Mutex::new(0));
        let reactor = Arc::new(CountingReactor {
            name: "test".to_string(),
            filter: EventFilter::by_event_type("MatchMe"),
            count: Arc::clone(&count),
        });

        let runner = ReactorRunner::new(store).register(reactor);

        // Matching event
        let envelope = test_envelope("MatchMe", "TestAggregate");
        let handled = runner.process_event(&envelope).await.unwrap();
        assert_eq!(handled, 1);
        assert_eq!(*count.lock().unwrap(), 1);

        // Non-matching event
        let envelope = test_envelope("DontMatchMe", "TestAggregate");
        let handled = runner.process_event(&envelope).await.unwrap();
        assert_eq!(handled, 0);
        assert_eq!(*count.lock().unwrap(), 1);
    }

    #[tokio::test]
    async fn test_reactor_runner_process_event_multiple_reactors() {
        let store = Arc::new(MockEventStore::new());
        let count1 = Arc::new(std::sync::Mutex::new(0));
        let count2 = Arc::new(std::sync::Mutex::new(0));

        let reactor1 = Arc::new(CountingReactor {
            name: "reactor1".to_string(),
            filter: EventFilter::by_event_type("TestEvent"),
            count: Arc::clone(&count1),
        });
        let reactor2 = Arc::new(CountingReactor {
            name: "reactor2".to_string(),
            filter: EventFilter::by_event_type("TestEvent"),
            count: Arc::clone(&count2),
        });

        let runner = ReactorRunner::new(store)
            .register(reactor1)
            .register(reactor2);

        let envelope = test_envelope("TestEvent", "TestAggregate");
        let handled = runner.process_event(&envelope).await.unwrap();

        assert_eq!(handled, 2);
        assert_eq!(*count1.lock().unwrap(), 1);
        assert_eq!(*count2.lock().unwrap(), 1);
    }

    #[tokio::test]
    async fn test_reactor_runner_process_pending_no_events() {
        let store = Arc::new(MockEventStore::new());
        let runner: ReactorRunner<MockEventStore> = ReactorRunner::new(store);

        let processed = runner.process_pending().await.unwrap();
        assert_eq!(processed, 0);
    }

    #[tokio::test]
    async fn test_reactor_runner_process_pending_with_events() {
        let store = Arc::new(MockEventStore::new());

        // Add some events to the store
        store.add_event(test_envelope("TestEvent", "TestAggregate"));
        store.add_event(test_envelope("OtherEvent", "TestAggregate"));
        store.add_event(test_envelope("TestEvent", "TestAggregate"));

        let count = Arc::new(std::sync::Mutex::new(0));
        let reactor = Arc::new(CountingReactor {
            name: "test".to_string(),
            filter: EventFilter::by_event_type("TestEvent"),
            count: Arc::clone(&count),
        });

        let runner = ReactorRunner::new(Arc::clone(&store)).register(reactor);
        let processed = runner.process_pending().await.unwrap();

        // 2 matching events out of 3 total
        assert_eq!(processed, 2);
        assert_eq!(*count.lock().unwrap(), 2);
    }

    #[test]
    fn test_reactor_runner_debug() {
        let store = Arc::new(MockEventStore::new());
        let count = Arc::new(std::sync::Mutex::new(0));
        let reactor = Arc::new(CountingReactor {
            name: "my_reactor".to_string(),
            filter: EventFilter::all(),
            count,
        });

        let runner = ReactorRunner::new(store).register(reactor);
        let debug = format!("{runner:?}");
        assert!(debug.contains("ReactorRunner"));
        assert!(debug.contains("my_reactor"));
        assert!(debug.contains("max_cascade_depth: 10"));
    }

    #[tokio::test]
    async fn test_reactor_context_load_any_as() {
        let store = Arc::new(MockEventStore::new());

        let id = crate::EntityId::new();
        let mut agg = AggregateRoot::<SimpleTestEntity>::new(id);
        agg.apply(SimpleTestEvent::Created { value: 55 }).unwrap();
        store.commit(&mut agg).await.unwrap();

        let envelope = test_envelope("TestEvent", "TestAggregate");
        let ctx = ReactorContext::new(Arc::clone(&store), envelope, 10);

        let loaded = ctx.load_any_as::<SimpleTestEntity>(id).await.unwrap();
        assert!(loaded.is_active());
    }

    #[tokio::test]
    async fn test_reactor_context_commit_preserves_existing_metadata() {
        let store = Arc::new(MockEventStore::new());
        let correlation_id = Uuid::new_v4();
        let parent_metadata = EventMetadata::new().with_correlation_id(correlation_id);
        let envelope = test_envelope("TestEvent", "TestAggregate").with_metadata(parent_metadata);
        let source_id = envelope.id;
        let ctx = ReactorContext::new(Arc::clone(&store), envelope, 10);

        let id = crate::EntityId::new();
        let mut agg = AggregateRoot::<SimpleTestEntity>::new(id);

        // Apply with existing metadata
        let existing = EventMetadata::new().with_causation_id(Uuid::new_v4());
        agg.apply_with_metadata(SimpleTestEvent::Created { value: 1 }, existing.clone())
            .unwrap();
        // Apply without metadata
        agg.apply(SimpleTestEvent::Updated { value: 2 }).unwrap();

        ctx.commit(&mut agg).await.unwrap();

        let stored = store.get_events();
        assert_eq!(stored.len(), 2);

        // First event keeps its existing metadata (set_pending_metadata doesn't overwrite)
        let meta0 = stored[0].metadata.as_ref().unwrap();
        assert_eq!(meta0.causation_id, existing.causation_id);

        // Second event gets reactor causation metadata
        let meta1 = stored[1].metadata.as_ref().unwrap();
        assert_eq!(meta1.causation_id, Some(source_id));
        assert_eq!(meta1.correlation_id, Some(correlation_id));
        assert_eq!(meta1.causation_chain, vec![source_id]);
    }

    // === reactor! macro tests ===

    // Test event structs implementing EventType + Deserialize
    #[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
    struct FooCreatedEvent {
        pub name: String,
    }

    impl crate::EventType for FooCreatedEvent {
        const EVENT_TYPE: &'static str = "Foo.Created";
    }

    #[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
    struct FooUpdatedEvent {
        pub value: i32,
    }

    impl crate::EventType for FooUpdatedEvent {
        const EVENT_TYPE: &'static str = "Foo.Updated";
    }

    #[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
    struct BarDeletedEvent {
        pub reason: String,
    }

    impl crate::EventType for BarDeletedEvent {
        const EVENT_TYPE: &'static str = "Bar.Deleted";
    }

    // Test: reactor! generates a struct
    crate::reactor! {
        TestReactor {
            on FooCreatedEvent |_event, _ctx| {
                Ok(())
            },
        }
    }

    #[test]
    fn test_reactor_macro_generates_struct() {
        // Verify the reactor! macro generates a usable struct
        let reactor: &dyn Reactor<MockEventStore> = &TestReactor;
        assert_eq!(reactor.name(), "TestReactor");
    }

    #[test]
    fn test_reactor_macro_name() {
        let store = Arc::new(MockEventStore::new());
        let reactor: &dyn Reactor<MockEventStore> = &TestReactor;
        let _ = store; // suppress unused
        assert_eq!(reactor.name(), "TestReactor");
    }

    #[test]
    fn test_reactor_macro_event_filter_single() {
        let reactor: &dyn Reactor<MockEventStore> = &TestReactor;
        let filter = reactor.event_filter();

        let matching = test_envelope("Foo.Created", "Foo");
        let non_matching = test_envelope("Foo.Updated", "Foo");

        assert!(filter.matches(&matching));
        assert!(!filter.matches(&non_matching));
    }

    // Test: reactor! with multiple event handlers
    crate::reactor! {
        MultiEventReactor {
            on FooCreatedEvent |_event, _ctx| {
                Ok(())
            },
            on FooUpdatedEvent |_event, _ctx| {
                Ok(())
            },
            on BarDeletedEvent |_event, _ctx| {
                Ok(())
            },
        }
    }

    #[test]
    fn test_reactor_macro_event_filter_multiple() {
        let reactor: &dyn Reactor<MockEventStore> = &MultiEventReactor;
        let filter = reactor.event_filter();

        assert!(filter.matches(&test_envelope("Foo.Created", "Foo")));
        assert!(filter.matches(&test_envelope("Foo.Updated", "Foo")));
        assert!(filter.matches(&test_envelope("Bar.Deleted", "Bar")));
        assert!(!filter.matches(&test_envelope("Other.Event", "Other")));
    }

    // A reactor that captures deserialized event data for testing
    struct CapturingReactor(Arc<std::sync::Mutex<Option<String>>>);

    #[async_trait]
    impl<S: EventStore + 'static> Reactor<S> for CapturingReactor {
        fn name(&self) -> &'static str {
            "CapturingReactor"
        }
        fn event_filter(&self) -> EventFilter {
            EventFilter::by_event_type("Foo.Created")
        }
        async fn handle(&self, event: &EventEnvelope, _ctx: &ReactorContext<S>) -> Result<()> {
            let foo: FooCreatedEvent = serde_json::from_value(event.event_data.clone())?;
            *self.0.lock().unwrap() = Some(foo.name);
            Ok(())
        }
    }

    #[tokio::test]
    async fn test_reactor_macro_handle_deserializes_event() {
        let store = Arc::new(MockEventStore::new());
        let received = Arc::new(std::sync::Mutex::new(None::<String>));

        let reactor = CapturingReactor(Arc::clone(&received));
        let mut envelope = test_envelope("Foo.Created", "Foo");
        envelope.event_data = serde_json::json!({"name": "hello"});

        let ctx = ReactorContext::new(Arc::clone(&store), envelope.clone(), 10);
        reactor.handle(&envelope, &ctx).await.unwrap();

        assert_eq!(*received.lock().unwrap(), Some("hello".to_string()));
    }

    #[tokio::test]
    async fn test_reactor_macro_handle_routes_to_correct_handler() {
        // Verify the generated handle() method deserializes and routes correctly
        let store = Arc::new(MockEventStore::new());

        // FooCreatedEvent envelope
        let mut envelope = test_envelope("Foo.Created", "Foo");
        envelope.event_data = serde_json::json!({"name": "test_name"});

        let ctx = ReactorContext::new(Arc::clone(&store), envelope.clone(), 10);

        // The generated handle should succeed (just returns Ok)
        let result =
            <TestReactor as Reactor<MockEventStore>>::handle(&TestReactor, &envelope, &ctx).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_reactor_macro_handle_deserialization_error() {
        let store = Arc::new(MockEventStore::new());

        // Envelope with wrong data shape for FooCreatedEvent (missing "name" field)
        let mut envelope = test_envelope("Foo.Created", "Foo");
        envelope.event_data = serde_json::json!({"wrong_field": 42});

        let ctx = ReactorContext::new(Arc::clone(&store), envelope.clone(), 10);

        let result =
            <TestReactor as Reactor<MockEventStore>>::handle(&TestReactor, &envelope, &ctx).await;
        assert!(result.is_err());
        assert!(result.unwrap_err().is_serialization());
    }

    #[tokio::test]
    async fn test_reactor_macro_handle_unmatched_event() {
        let store = Arc::new(MockEventStore::new());

        // Envelope with event type that doesn't match any handler
        let envelope = test_envelope("Unknown.Event", "Unknown");
        let ctx = ReactorContext::new(Arc::clone(&store), envelope.clone(), 10);

        // Should succeed (no-op for unmatched events)
        let result =
            <TestReactor as Reactor<MockEventStore>>::handle(&TestReactor, &envelope, &ctx).await;
        assert!(result.is_ok());
    }

    // Test: reactor! with doc comment
    crate::reactor! {
        /// A documented reactor for testing.
        DocReactor {
            on FooCreatedEvent |_event, _ctx| {
                Ok(())
            },
        }
    }

    #[test]
    fn test_reactor_macro_with_doc_comment() {
        let reactor: &dyn Reactor<MockEventStore> = &DocReactor;
        assert_eq!(reactor.name(), "DocReactor");
    }

    // Test: reactor! with pub visibility
    crate::reactor! {
        pub PubReactor {
            on FooCreatedEvent |_event, _ctx| {
                Ok(())
            },
        }
    }

    #[test]
    fn test_reactor_macro_with_visibility() {
        let reactor: &dyn Reactor<MockEventStore> = &PubReactor;
        assert_eq!(reactor.name(), "PubReactor");
    }

    // Test: reactor! registers with ReactorRunner
    #[test]
    fn test_reactor_macro_with_runner() {
        let store = Arc::new(MockEventStore::new());
        let runner = ReactorRunner::new(store).register(Arc::new(TestReactor));
        assert_eq!(runner.reactors().len(), 1);
        assert_eq!(runner.reactors()[0].name(), "TestReactor");
    }

    #[tokio::test]
    async fn test_reactor_macro_process_event_via_runner() {
        let store = Arc::new(MockEventStore::new());
        let runner = ReactorRunner::new(Arc::clone(&store)).register(Arc::new(TestReactor));

        let mut envelope = test_envelope("Foo.Created", "Foo");
        envelope.event_data = serde_json::json!({"name": "via_runner"});

        let handled = runner.process_event(&envelope).await.unwrap();
        assert_eq!(handled, 1);
    }

    // Test: reactor! with handler that accesses event fields
    crate::reactor! {
        FieldAccessReactor {
            on FooCreatedEvent |event, _ctx| {
                assert_eq!(event.name, "expected_name");
                Ok(())
            },
        }
    }

    #[tokio::test]
    async fn test_reactor_macro_handler_accesses_event_fields() {
        let store = Arc::new(MockEventStore::new());

        let mut envelope = test_envelope("Foo.Created", "Foo");
        envelope.event_data = serde_json::json!({"name": "expected_name"});

        let ctx = ReactorContext::new(Arc::clone(&store), envelope.clone(), 10);

        let result = <FieldAccessReactor as Reactor<MockEventStore>>::handle(
            &FieldAccessReactor,
            &envelope,
            &ctx,
        )
        .await;
        assert!(result.is_ok());
    }

    // Test: reactor! with handler that accesses ctx
    crate::reactor! {
        CtxAccessReactor {
            on FooCreatedEvent |_event, ctx| {
                assert_eq!(ctx.cascade_depth(), 0);
                Ok(())
            },
        }
    }

    #[tokio::test]
    async fn test_reactor_macro_handler_accesses_ctx() {
        let store = Arc::new(MockEventStore::new());

        let mut envelope = test_envelope("Foo.Created", "Foo");
        envelope.event_data = serde_json::json!({"name": "test"});

        let ctx = ReactorContext::new(Arc::clone(&store), envelope.clone(), 10);

        let result = <CtxAccessReactor as Reactor<MockEventStore>>::handle(
            &CtxAccessReactor,
            &envelope,
            &ctx,
        )
        .await;
        assert!(result.is_ok());
    }

    // Test: reactor! with async handler that loads and commits
    crate::reactor! {
        AsyncReactor {
            on FooCreatedEvent |_event, ctx| {
                // Create a new aggregate reacting to the event
                let id = crate::EntityId::new();
                let mut agg = AggregateRoot::<SimpleTestEntity>::new(id);
                agg.apply(SimpleTestEvent::Created { value: 42 })
                    .map_err(|e| crate::Error::invalid_state(format!("{e:?}")))?;
                ctx.commit(&mut agg).await?;
                Ok(())
            },
        }
    }

    #[tokio::test]
    async fn test_reactor_macro_async_handler() {
        let store = Arc::new(MockEventStore::new());

        let mut envelope = test_envelope("Foo.Created", "Foo");
        envelope.event_data = serde_json::json!({"name": "hello"});

        let ctx = ReactorContext::new(Arc::clone(&store), envelope.clone(), 10);

        let result =
            <AsyncReactor as Reactor<MockEventStore>>::handle(&AsyncReactor, &envelope, &ctx).await;
        assert!(result.is_ok());

        // Verify the aggregate was committed
        let stored = store.get_events();
        assert_eq!(stored.len(), 1);

        // Verify causation metadata was injected
        let meta = stored[0].metadata.as_ref().expect("Should have metadata");
        assert_eq!(meta.causation_id, Some(envelope.id));
    }

    // Test: reactor! multiple handlers route correctly
    crate::reactor! {
        RoutingReactor {
            on FooCreatedEvent |_event, ctx| {
                let id = crate::EntityId::new();
                let mut agg = AggregateRoot::<SimpleTestEntity>::new(id);
                agg.apply(SimpleTestEvent::Created { value: 99 })
                    .map_err(|e| crate::Error::invalid_state(format!("{e:?}")))?;
                ctx.commit(&mut agg).await?;
                Ok(())
            },
            on FooUpdatedEvent |event, ctx| {
                let id = crate::EntityId::new();
                let mut agg = AggregateRoot::<SimpleTestEntity>::new(id);
                let val = event.value * 10;
                agg.apply(SimpleTestEvent::Created { value: val })
                    .map_err(|e| crate::Error::invalid_state(format!("{e:?}")))?;
                ctx.commit(&mut agg).await?;
                Ok(())
            },
        }
    }

    #[tokio::test]
    async fn test_reactor_macro_routes_to_correct_async_handler() {
        let store = Arc::new(MockEventStore::new());

        // Send FooUpdatedEvent — should go to the second handler
        let mut envelope = test_envelope("Foo.Updated", "Foo");
        envelope.event_data = serde_json::json!({"value": 7});

        let ctx = ReactorContext::new(Arc::clone(&store), envelope.clone(), 10);

        let result =
            <RoutingReactor as Reactor<MockEventStore>>::handle(&RoutingReactor, &envelope, &ctx)
                .await;
        assert!(result.is_ok());

        let stored = store.get_events();
        assert_eq!(stored.len(), 1);

        // The second handler creates SimpleTestEntity with value = event.value * 10 = 70
        let data: serde_json::Value = stored[0].event_data.clone();
        assert_eq!(data["Created"]["value"], 70);
    }
}
