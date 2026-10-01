//! Event applicator trait for dispatching events to entities.
//!
//! The `EventApplicator` trait formalizes the dispatch from an event enum
//! to individual `ApplyEvent` implementations, enabling `AggregateRoot<A>`
//! to provide event application methods.

use crate::Aggregate;

/// Trait for dispatching events to entities.
///
/// This trait is implemented on event enum types and dispatches each variant
/// to its corresponding `ApplyEvent` implementation. It bridges the gap between
/// the event enum (which the aggregate knows about) and individual event structs
/// (which implement `ApplyEvent`).
///
/// Events dispatch directly to `&mut A` (the entity), not to `AggregateRoot<A>`.
///
/// # Provided by Macros
///
/// This trait is automatically implemented by the `define_events!` macro and
/// the `#[derive(Event)]` proc macro. Manual implementation is only needed
/// when not using these macros.
///
/// # Examples
///
/// ```ignore
/// use event_sauce_core::{Aggregate, EventApplicator};
///
/// impl EventApplicator<MyEntity> for MyEvent {
///     fn dispatch(&self, entity: &mut MyEntity) -> Result<(), MyError> {
///         match self {
///             MyEvent::Created(e) => {
///                 e.validate(entity)?;
///                 e.apply(entity);
///                 e.post_validate(entity)?;
///             }
///         }
///         Ok(())
///     }
///
///     fn dispatch_unchecked(&self, entity: &mut MyEntity) {
///         match self {
///             MyEvent::Created(e) => e.apply(entity),
///         }
///     }
/// }
/// ```
pub trait EventApplicator<A: Aggregate> {
    /// Dispatches the event to the entity with full validation.
    ///
    /// Runs the complete event application lifecycle:
    /// 1. Pre-validation via `ApplyEvent::validate()`
    /// 2. State changes via `ApplyEvent::apply()`
    /// 3. Post-validation via `ApplyEvent::post_validate()`
    ///
    /// # Errors
    ///
    /// Returns an error if pre-validation or post-validation fails.
    fn dispatch(&self, aggregate: &mut A) -> Result<(), A::Error>;

    /// Runs ONLY the pre-validation of the event, mutating nothing.
    ///
    /// [`AggregateRoot::apply`](crate::AggregateRoot::apply) calls this first so a
    /// REFUSAL can be told apart from a failed apply. A refusal leaves the aggregate
    /// exactly as it was, so it must not poison it — `dispatch` alone cannot say
    /// which of the two happened, because both come back as one `A::Error`.
    ///
    /// The default answers `Ok(())`, which is the conservative reading for a
    /// fully hand-written impl: every failure then looks like a failed apply
    /// and poisons, as it did before this existed. `define_events!` and
    /// `#[derive(Event)]` both override it, forwarding to each variant's
    /// `ApplyEvent::validate` — they generate `dispatch` too, so they know
    /// exactly where the pre-validation step sits in it.
    ///
    /// # Errors
    ///
    /// Returns the aggregate's error if pre-validation refuses the event.
    fn validate_only(&self, aggregate: &A) -> Result<(), A::Error> {
        let _ = aggregate;
        Ok(())
    }

    /// Dispatches the event to the entity without validation.
    ///
    /// Only applies state changes, skipping validation. Used for event replay
    /// from the event store, where events are historical facts.
    fn dispatch_unchecked(&self, aggregate: &mut A);

    /// Returns whether this event variant is an init event.
    ///
    /// Only returns true for events that construct the aggregate from scratch.
    /// The default returns false (regular events).
    fn is_init(&self) -> bool {
        false
    }

    /// Dispatches an init event with full validation, constructing a new entity.
    ///
    /// Called by the framework after checking `is_init()`. The default panics
    /// because regular events should never reach this path.
    ///
    /// # Errors
    ///
    /// Returns an error if validation fails.
    ///
    /// # Panics
    ///
    /// The default implementation panics. Only init event variants override this.
    fn dispatch_init(&self, _id: crate::EntityId) -> Result<A, A::Error> {
        unreachable!("dispatch_init called on non-init event")
    }

    /// Dispatches an init event without validation (for replay).
    ///
    /// Called by the framework after checking `is_init()`. The default panics
    /// because regular events should never reach this path.
    ///
    /// # Panics
    ///
    /// The default implementation panics. Only init event variants override this.
    fn dispatch_init_unchecked(&self, _id: crate::EntityId) -> A {
        unreachable!("dispatch_init_unchecked called on non-init event")
    }

    /// Returns whether this event variant is a delete event.
    ///
    /// Only returns true for events that terminate the aggregate.
    /// The default returns false (regular events).
    fn is_delete(&self) -> bool {
        false
    }

    /// Dispatches a delete event with full validation, consuming the entity.
    ///
    /// Called by the framework after checking `is_delete()`. The default panics
    /// because regular events should never reach this path.
    ///
    /// # Errors
    ///
    /// Returns an error if validation fails.
    ///
    /// # Panics
    ///
    /// The default implementation panics. Only delete event variants override this.
    fn dispatch_delete(&self, _aggregate: A) -> Result<A::DeletedState, A::Error> {
        unreachable!("dispatch_delete called on non-delete event")
    }

    /// Dispatches a delete event without validation (for replay).
    ///
    /// Called by the framework after checking `is_delete()`. The default panics
    /// because regular events should never reach this path.
    ///
    /// # Panics
    ///
    /// The default implementation panics. Only delete event variants override this.
    fn dispatch_delete_unchecked(&self, _aggregate: A) -> A::DeletedState {
        unreachable!("dispatch_delete_unchecked called on non-delete event")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{AggregateError, AggregateVersion, DomainEvent, Entity, EntityId, EventVersion};
    use chrono::Utc;
    use thiserror::Error;

    #[derive(Debug, Error)]
    #[error("Test error: {0}")]
    struct TestError(String);

    impl AggregateError for TestError {}

    #[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
    enum TestEvent {
        Added { amount: i32 },
        Validated { amount: i32 },
    }

    #[derive(Debug, serde::Serialize, serde::Deserialize)]
    struct TestEntity {
        id: EntityId,
        value: i32,
    }

    impl crate::Entity for TestEntity {
        fn new(id: EntityId) -> Self {
            Self { id, value: 0 }
        }

        fn entity_id(&self) -> EntityId {
            self.id
        }
    }

    impl crate::DefaultEntity for TestEntity {}

    impl EventApplicator<TestEntity> for TestEvent {
        fn dispatch(&self, entity: &mut TestEntity) -> Result<(), TestError> {
            match self {
                TestEvent::Added { amount } => {
                    entity.value += amount;
                }
                TestEvent::Validated { amount } => {
                    if *amount < 0 {
                        return Err(TestError("Amount cannot be negative".to_string()));
                    }
                    entity.value += amount;
                }
            }
            Ok(())
        }

        fn dispatch_unchecked(&self, entity: &mut TestEntity) {
            match self {
                TestEvent::Added { amount } | TestEvent::Validated { amount } => {
                    entity.value += amount;
                }
            }
        }
    }

    impl DomainEvent for TestEvent {
        type Aggregate = TestEntity;

        fn event_type(&self) -> &'static str {
            match self {
                TestEvent::Added { .. } => "Added",
                TestEvent::Validated { .. } => "Validated",
            }
        }

        fn event_version(&self) -> EventVersion {
            EventVersion::new(1)
        }

        fn occurred_at(&self) -> chrono::DateTime<Utc> {
            Utc::now()
        }
    }

    impl Aggregate for TestEntity {
        type Event = TestEvent;
        type Error = TestError;
        type DeletedState = Self;
    }

    #[test]
    fn test_dispatch_applies_state() {
        let mut entity = TestEntity::new(EntityId::new());
        let event = TestEvent::Added { amount: 5 };

        EventApplicator::dispatch(&event, &mut entity).unwrap();

        assert_eq!(entity.value, 5);
    }

    #[test]
    fn test_dispatch_with_validation_succeeds() {
        let mut entity = TestEntity::new(EntityId::new());
        let event = TestEvent::Validated { amount: 10 };

        let result = EventApplicator::dispatch(&event, &mut entity);

        assert!(result.is_ok());
        assert_eq!(entity.value, 10);
    }

    #[test]
    fn test_dispatch_with_validation_fails() {
        let mut entity = TestEntity::new(EntityId::new());
        let event = TestEvent::Validated { amount: -5 };

        let result = EventApplicator::dispatch(&event, &mut entity);

        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err().to_string(),
            "Test error: Amount cannot be negative"
        );
    }

    #[test]
    fn test_dispatch_unchecked_skips_validation() {
        let mut entity = TestEntity::new(EntityId::new());
        let event = TestEvent::Validated { amount: -5 };

        EventApplicator::dispatch_unchecked(&event, &mut entity);

        assert_eq!(entity.value, -5);
    }

    #[test]
    fn test_dispatch_multiple_events() {
        let mut entity = TestEntity::new(EntityId::new());

        EventApplicator::dispatch(&TestEvent::Added { amount: 5 }, &mut entity).unwrap();
        EventApplicator::dispatch(&TestEvent::Added { amount: 10 }, &mut entity).unwrap();
        EventApplicator::dispatch(&TestEvent::Validated { amount: 3 }, &mut entity).unwrap();

        assert_eq!(entity.value, 18);
    }

    #[test]
    fn test_aggregate_root_uses_event_applicator() {
        let mut root = crate::AggregateRoot::<TestEntity>::new(EntityId::new());

        root.apply(TestEvent::Added { amount: 5 }).unwrap();

        assert_eq!(root.value, 5);
        assert_eq!(root.version(), AggregateVersion::new(1));
        assert_eq!(root.pending_events().len(), 1);
    }

    #[test]
    fn test_aggregate_root_validates_via_event_applicator() {
        let mut root = crate::AggregateRoot::<TestEntity>::new(EntityId::new());

        let result = root.apply(TestEvent::Validated { amount: -5 });

        assert!(result.is_err());
        assert_eq!(root.value, 0); // State unchanged
        assert_eq!(root.version(), AggregateVersion::initial()); // Version unchanged
        assert_eq!(root.pending_events().len(), 0); // No pending events
    }
}
