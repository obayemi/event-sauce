//! Apply event trait for event application logic.
//!
//! Provides the `ApplyEvent` trait that allows events to define their own
//! application logic, making events self-contained and reducing boilerplate.

use crate::Aggregate;

/// Trait for applying events to aggregates.
///
/// This trait allows each event type to define how it should be applied
/// to an entity, promoting encapsulation and reducing the need for
/// large match statements in aggregate code.
///
/// Events operate directly on the entity (`&mut A`), not on `AggregateRoot<A>`.
/// The `AggregateRoot` handles infrastructure (version, pending events) while
/// `ApplyEvent` handles pure domain state changes.
///
/// # Type Parameters
///
/// - `A`: The aggregate type this event applies to (must implement `Aggregate`)
///
/// # Pattern
///
/// Events should implement this trait to define:
/// 1. How to validate the event can be applied (optional, via `validate`)
/// 2. How to apply the event to update entity state (via `apply`)
/// 3. How to validate post-conditions (optional, via `post_validate`)
///
/// # Examples
///
/// ## Simple Event Without Validation
///
/// ```
/// use event_sauce_core::{Aggregate, ApplyEvent, AggregateError, Entity, EntityId, DomainEvent, EventApplicator, EventVersion};
/// use thiserror::Error;
/// use chrono::Utc;
/// use serde::{Serialize, Deserialize};
///
/// #[derive(Debug, Serialize, Deserialize)]
/// struct Counter { id: EntityId, value: i32 }
///
/// impl Entity for Counter {
///     fn new(id: EntityId) -> Self { Self { id, value: 0 } }
///     fn entity_id(&self) -> EntityId { self.id }
/// }
///
/// #[derive(Debug, Error)]
/// #[error("Counter error")]
/// enum CounterError {}
/// impl AggregateError for CounterError {}
///
/// #[derive(Debug, Clone, Serialize, Deserialize)]
/// enum CounterEvent { Incremented { amount: i32 } }
///
/// impl DomainEvent for CounterEvent {
///     type Aggregate = Counter;
///     fn event_type(&self) -> &'static str { "Incremented" }
///     fn event_version(&self) -> EventVersion { EventVersion::new(1) }
///     fn occurred_at(&self) -> chrono::DateTime<Utc> { Utc::now() }
/// }
///
/// impl EventApplicator<Counter> for CounterEvent {
///     fn dispatch(&self, c: &mut Counter) -> Result<(), CounterError> {
///         match self { CounterEvent::Incremented { amount } => c.value += amount }
///         Ok(())
///     }
///     fn dispatch_unchecked(&self, c: &mut Counter) {
///         match self { CounterEvent::Incremented { amount } => c.value += amount }
///     }
/// }
///
/// impl Aggregate for Counter {
///     type Event = CounterEvent;
///     type Error = CounterError;
///     type DeletedState = Self;
/// }
///
/// struct Incremented { amount: i32 }
///
/// impl ApplyEvent<Counter> for Incremented {
///     fn apply(&self, counter: &mut Counter) {
///         counter.value += self.amount;
///     }
/// }
/// ```
pub trait ApplyEvent<A: Aggregate> {
    /// Validates that the event can be applied to the entity.
    ///
    /// Checks business rules and invariants without modifying state.
    /// The default implementation always succeeds.
    ///
    /// # Errors
    ///
    /// Returns an error of type `A::Error` if the event fails validation.
    fn validate(&self, _aggregate: &A) -> Result<(), A::Error> {
        Ok(())
    }

    /// Applies the event to the entity, updating its state.
    ///
    /// This method performs the actual state transformation. It assumes
    /// that validation has already been performed (or is not needed).
    /// Should be pure and deterministic.
    fn apply(&self, aggregate: &mut A);

    /// Validates the entity state after the event has been applied.
    ///
    /// Checks post-conditions and invariants after state changes.
    /// The default implementation always succeeds.
    ///
    /// # Errors
    ///
    /// Returns an error of type `A::Error` if post-conditions are violated.
    fn post_validate(&self, _aggregate: &A) -> Result<(), A::Error> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{AggregateError, DomainEvent, Entity, EntityId, EventApplicator};
    use chrono::Utc;
    use thiserror::Error;

    #[derive(Debug, Error)]
    #[error("Test error: {0}")]
    struct TestError(String);

    impl AggregateError for TestError {}

    #[derive(Debug, PartialEq, Clone, serde::Serialize, serde::Deserialize)]
    enum Status {
        Active,
        Inactive,
    }

    #[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
    enum TestEvent {
        Simple { amount: i32 },
        Validated { amount: i32 },
        PostValidated { amount: i32, max_value: i32 },
    }

    impl DomainEvent for TestEvent {
        type Aggregate = TestEntity;

        fn event_type(&self) -> &'static str {
            match self {
                TestEvent::Simple { .. } => "Simple",
                TestEvent::Validated { .. } => "Validated",
                TestEvent::PostValidated { .. } => "PostValidated",
            }
        }

        fn event_version(&self) -> crate::EventVersion {
            crate::EventVersion::new(1)
        }

        fn occurred_at(&self) -> chrono::DateTime<Utc> {
            Utc::now()
        }
    }

    #[derive(Debug, serde::Serialize, serde::Deserialize)]
    struct TestEntity {
        id: EntityId,
        value: i32,
        status: Status,
    }

    impl crate::Entity for TestEntity {
        fn new(id: EntityId) -> Self {
            Self {
                id,
                value: 0,
                status: Status::Active,
            }
        }

        fn entity_id(&self) -> EntityId {
            self.id
        }
    }

    impl crate::DefaultEntity for TestEntity {}

    impl EventApplicator<TestEntity> for TestEvent {
        fn dispatch(&self, entity: &mut TestEntity) -> Result<(), TestError> {
            match self {
                TestEvent::Simple { amount }
                | TestEvent::Validated { amount }
                | TestEvent::PostValidated { amount, .. } => {
                    entity.value += amount;
                }
            }
            Ok(())
        }

        fn dispatch_unchecked(&self, entity: &mut TestEntity) {
            match self {
                TestEvent::Simple { amount }
                | TestEvent::Validated { amount }
                | TestEvent::PostValidated { amount, .. } => {
                    entity.value += amount;
                }
            }
        }
    }

    impl Aggregate for TestEntity {
        type Event = TestEvent;
        type Error = TestError;
        type DeletedState = Self;
    }

    struct SimpleEvent {
        amount: i32,
    }

    impl ApplyEvent<TestEntity> for SimpleEvent {
        fn apply(&self, entity: &mut TestEntity) {
            entity.value += self.amount;
        }
    }

    struct ValidatedEvent {
        amount: i32,
    }

    impl ApplyEvent<TestEntity> for ValidatedEvent {
        fn validate(&self, entity: &TestEntity) -> Result<(), <TestEntity as Aggregate>::Error> {
            if entity.status == Status::Inactive {
                return Err(TestError("Entity is inactive".to_string()));
            }
            if self.amount < 0 {
                return Err(TestError("Amount cannot be negative".to_string()));
            }
            Ok(())
        }

        fn apply(&self, entity: &mut TestEntity) {
            entity.value += self.amount;
        }
    }

    #[test]
    fn test_apply_event_without_validation() {
        let mut entity = TestEntity::new(EntityId::new());
        entity.value = 10;

        let event = SimpleEvent { amount: 5 };
        event.apply(&mut entity);

        assert_eq!(entity.value, 15);
    }

    #[test]
    fn test_apply_event_default_validation_succeeds() {
        let entity = TestEntity::new(EntityId::new());
        let event = SimpleEvent { amount: 5 };

        let result = event.validate(&entity);
        assert!(result.is_ok());
    }

    #[test]
    fn test_apply_event_with_successful_validation() {
        let mut entity = TestEntity::new(EntityId::new());
        entity.value = 10;

        let event = ValidatedEvent { amount: 5 };

        assert!(event.validate(&entity).is_ok());
        event.apply(&mut entity);

        assert_eq!(entity.value, 15);
    }

    #[test]
    fn test_apply_event_validation_fails_on_inactive_status() {
        let mut entity = TestEntity::new(EntityId::new());
        entity.status = Status::Inactive;

        let event = ValidatedEvent { amount: 5 };
        let result = event.validate(&entity);

        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err().to_string(),
            "Test error: Entity is inactive"
        );
    }

    #[test]
    fn test_apply_event_validation_fails_on_negative_amount() {
        let entity = TestEntity::new(EntityId::new());

        let event = ValidatedEvent { amount: -5 };
        let result = event.validate(&entity);

        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err().to_string(),
            "Test error: Amount cannot be negative"
        );
    }

    #[test]
    fn test_apply_event_apply_without_validation() {
        let mut entity = TestEntity::new(EntityId::new());
        entity.value = 10;
        entity.status = Status::Inactive;

        let event = ValidatedEvent { amount: 5 };

        // Validation would fail
        assert!(event.validate(&entity).is_err());

        // But apply still works (for replay)
        event.apply(&mut entity);
        assert_eq!(entity.value, 15);
    }

    #[test]
    fn test_apply_event_multiple_applications() {
        let mut entity = TestEntity::new(EntityId::new());

        let event1 = SimpleEvent { amount: 5 };
        let event2 = SimpleEvent { amount: 10 };
        let event3 = SimpleEvent { amount: 3 };

        event1.apply(&mut entity);
        event2.apply(&mut entity);
        event3.apply(&mut entity);

        assert_eq!(entity.value, 18);
    }

    #[test]
    fn test_apply_event_is_deterministic() {
        let event = SimpleEvent { amount: 5 };

        let mut entity1 = TestEntity::new(EntityId::new());
        entity1.value = 10;
        event.apply(&mut entity1);

        let mut entity2 = TestEntity::new(EntityId::new());
        entity2.value = 10;
        event.apply(&mut entity2);

        assert_eq!(entity1.value, entity2.value);
    }

    // Tests for post_validate
    struct PostValidatedEvent {
        amount: i32,
        max_value: i32,
    }

    impl ApplyEvent<TestEntity> for PostValidatedEvent {
        fn apply(&self, entity: &mut TestEntity) {
            entity.value += self.amount;
        }

        fn post_validate(
            &self,
            entity: &TestEntity,
        ) -> Result<(), <TestEntity as Aggregate>::Error> {
            if entity.value > self.max_value {
                return Err(TestError(format!(
                    "Value {} exceeds maximum {}",
                    entity.value, self.max_value
                )));
            }
            Ok(())
        }
    }

    #[test]
    fn test_post_validate_default_succeeds() {
        let entity = TestEntity::new(EntityId::new());
        let event = SimpleEvent { amount: 5 };

        let result = event.post_validate(&entity);
        assert!(result.is_ok());
    }

    #[test]
    fn test_post_validate_succeeds() {
        let mut entity = TestEntity::new(EntityId::new());
        entity.value = 10;

        let event = PostValidatedEvent {
            amount: 5,
            max_value: 20,
        };

        event.apply(&mut entity);
        let result = event.post_validate(&entity);

        assert!(result.is_ok());
        assert_eq!(entity.value, 15);
    }

    #[test]
    fn test_post_validate_fails() {
        let mut entity = TestEntity::new(EntityId::new());
        entity.value = 10;

        let event = PostValidatedEvent {
            amount: 15,
            max_value: 20,
        };

        event.apply(&mut entity);
        let result = event.post_validate(&entity);

        assert!(result.is_err());
        assert_eq!(entity.value, 25);
        assert_eq!(
            result.unwrap_err().to_string(),
            "Test error: Value 25 exceeds maximum 20"
        );
    }

    #[test]
    fn test_post_validate_checks_invariants() {
        let mut entity = TestEntity::new(EntityId::new());
        entity.value = 18;

        let event = PostValidatedEvent {
            amount: 1,
            max_value: 20,
        };

        event.apply(&mut entity);
        assert!(event.post_validate(&entity).is_ok());

        let event2 = PostValidatedEvent {
            amount: 2,
            max_value: 20,
        };
        event2.apply(&mut entity);
        assert!(event2.post_validate(&entity).is_err());
    }
}
