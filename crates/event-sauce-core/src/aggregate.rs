//! Aggregate trait for event-sourced entities.
//!
//! Defines the `Aggregate` trait that marks an entity as event-sourced,
//! specifying its event and error types. Infrastructure concerns (version,
//! pending events) are handled by `AggregateRoot<A>`.

use std::fmt::Debug;

use crate::{AggregateError, AggregateType, DomainEvent, Entity, EventApplicator};

/// Trait for event-sourced aggregates.
///
/// An aggregate is an entity whose state changes are captured as domain events.
/// This trait specifies only the domain-level concerns: what events the aggregate
/// produces and what errors it can return. All infrastructure (version tracking,
/// pending events, event application) is handled by [`AggregateRoot<A>`](crate::AggregateRoot).
///
/// # Relationship to Entity
///
/// `Aggregate` extends `Entity`, adding event sourcing capabilities:
/// - `Entity`: identity + construction
/// - `Aggregate`: entity + event types + error types
///
/// # Examples
///
/// ```
/// use event_sauce_core::{Aggregate, AggregateError, Entity, EntityId, DomainEvent, EventApplicator, EventVersion};
/// use serde::{Serialize, Deserialize};
/// use thiserror::Error;
/// use chrono::Utc;
///
/// #[derive(Serialize, Deserialize)]
/// struct Counter {
///     id: EntityId,
///     value: i32,
/// }
///
/// impl Entity for Counter {
///     fn new(id: EntityId) -> Self {
///         Self { id, value: 0 }
///     }
///     fn entity_id(&self) -> EntityId { self.id }
/// }
///
/// #[derive(Debug, Clone, Serialize, Deserialize)]
/// enum CounterEvent {
///     Incremented { amount: i32, timestamp: chrono::DateTime<Utc> },
/// }
///
/// impl DomainEvent for CounterEvent {
///     type Aggregate = Counter;
///     fn event_type(&self) -> &'static str { "Incremented" }
///     fn event_version(&self) -> EventVersion { EventVersion::new(1) }
///     fn occurred_at(&self) -> chrono::DateTime<Utc> {
///         match self {
///             CounterEvent::Incremented { timestamp, .. } => *timestamp,
///         }
///     }
/// }
///
/// impl EventApplicator<Counter> for CounterEvent {
///     fn dispatch(&self, counter: &mut Counter) -> Result<(), CounterError> {
///         match self {
///             CounterEvent::Incremented { amount, .. } => counter.value += amount,
///         }
///         Ok(())
///     }
///     fn dispatch_unchecked(&self, counter: &mut Counter) {
///         match self {
///             CounterEvent::Incremented { amount, .. } => counter.value += amount,
///         }
///     }
/// }
///
/// #[derive(Debug, Error)]
/// #[error("Counter error")]
/// struct CounterError;
/// impl AggregateError for CounterError {}
///
/// impl Aggregate for Counter {
///     type Event = CounterEvent;
///     type Error = CounterError;
///     type DeletedState = Self;
/// }
/// ```
pub trait Aggregate: Entity + Into<Self::DeletedState> {
    /// The type of events this aggregate produces.
    ///
    /// Must implement `DomainEvent` (for serialization) and `EventApplicator<Self>`
    /// (for dispatching events to the entity).
    type Event: DomainEvent<Aggregate = Self> + EventApplicator<Self> + Clone;

    /// The type of errors that can occur during event application.
    type Error: AggregateError;

    /// The state type after deletion.
    ///
    /// Defaults to `Self` (via `#[aggregate]` macro), meaning delete events
    /// simply return the aggregate in a terminal state. Override with
    /// `#[aggregate(deleted_state = "DeletedUser")]` to transform into a
    /// different type (e.g., for PII stripping).
    ///
    /// The `Into<Self::DeletedState>` supertrait on `Aggregate` enables the
    /// default `DeleteEvent::delete()` implementation. When `DeletedState = Self`,
    /// this is the identity conversion (always available). When custom, implement
    /// `From<Self> for DeletedState`.
    type DeletedState: Debug + Send + Sync;

    /// Returns whether this aggregate's data should be encrypted at rest.
    ///
    /// Encrypted aggregates have their event data and snapshot data encrypted
    /// at rest using per-aggregate encryption keys. Deleting the key
    /// renders the aggregate's history permanently unreadable
    /// (GDPR right-to-be-forgotten / crypto-shredding).
    ///
    /// Defaults to `false`. Override by using `#[aggregate(..., encrypted)]`
    /// or by implementing manually.
    #[must_use]
    fn is_encrypted() -> bool {
        false
    }

    /// Returns the aggregate type name.
    ///
    /// Defaults to the short type name (last segment of the full path),
    /// wrapped in an [`AggregateType`]. The `#[aggregate]` proc macro
    /// overrides this with a `stringify!`-based implementation for
    /// compiler-version stability.
    ///
    /// # Coverage Note
    ///
    /// The `unwrap_or("Unknown")` fallback is defensive programming.
    /// Rust's `type_name()` always returns a non-empty string, so `.last()`
    /// will always return `Some(_)`. The fallback cannot be reached in practice.
    #[must_use]
    fn aggregate_type() -> AggregateType {
        AggregateType::from_owned(
            std::any::type_name::<Self>()
                .split("::")
                .last()
                .unwrap_or("Unknown")
                .to_string(),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_fixtures::SimpleTestEvent;
    use crate::{AggregateVersion, EntityId};

    // Import SimpleTestEntity — it has DeletedState = Self in test_fixtures
    use crate::test_fixtures::SimpleTestEntity;

    #[test]
    fn test_aggregate_type_name() {
        let type_name = SimpleTestEntity::aggregate_type();
        assert_eq!(type_name, "SimpleTestEntity");
    }

    #[test]
    fn test_aggregate_is_encrypted_defaults_to_false() {
        assert!(!SimpleTestEntity::is_encrypted());
    }

    #[test]
    fn test_aggregate_has_entity_identity() {
        let id = EntityId::new();
        let aggregate = SimpleTestEntity::new(id);
        assert_eq!(aggregate.entity_id(), id);
    }

    #[test]
    fn test_aggregate_event_applicator_works() {
        let mut aggregate = SimpleTestEntity::new(EntityId::new());
        let event = SimpleTestEvent::Created { value: 42 };

        EventApplicator::dispatch(&event, &mut aggregate).unwrap();
        assert_eq!(aggregate.value, 42);
    }

    #[test]
    fn test_aggregate_root_integration() {
        let id = EntityId::new();
        let mut root = crate::AggregateRoot::<SimpleTestEntity>::new(id);

        assert_eq!(root.entity_id(), id);
        assert_eq!(root.version(), AggregateVersion::initial());
        assert_eq!(root.pending_events().len(), 0);

        root.apply(SimpleTestEvent::Created { value: 42 }).unwrap();

        assert_eq!(root.value, 42);
        assert_eq!(root.version(), AggregateVersion::new(1));
        assert_eq!(root.pending_events().len(), 1);
    }
}
