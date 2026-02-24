//! Event applicator trait for dispatching events to aggregates.
//!
//! The `EventApplicator` trait formalizes the dispatch from an event enum
//! to individual `ApplyEvent` implementations, enabling the `Aggregate` trait
//! to provide default implementations for `apply`, `apply_internal`, and
//! `apply_unchecked`.

use crate::Aggregate;

/// Trait for dispatching events to aggregates.
///
/// This trait is implemented on event enum types and dispatches each variant
/// to its corresponding `ApplyEvent` implementation. It bridges the gap between
/// the event enum (which the aggregate knows about) and individual event structs
/// (which implement `ApplyEvent`).
///
/// # Type Parameters
///
/// - `A`: The aggregate type this event enum applies to
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
/// impl EventApplicator<MyAggregate> for MyEvent {
///     fn dispatch(&self, aggregate: &mut MyAggregate) -> Result<(), MyError> {
///         match self {
///             MyEvent::Created { .. } => { /* validate + apply + post_validate */ }
///             MyEvent::Updated { .. } => { /* validate + apply + post_validate */ }
///         }
///         Ok(())
///     }
///
///     fn dispatch_unchecked(&self, aggregate: &mut MyAggregate) {
///         match self {
///             MyEvent::Created { .. } => { /* apply only */ }
///             MyEvent::Updated { .. } => { /* apply only */ }
///         }
///     }
/// }
/// ```
pub trait EventApplicator<A: Aggregate> {
    /// Dispatches the event to the aggregate with full validation.
    ///
    /// This method runs the complete event application lifecycle:
    /// 1. Pre-validation via `ApplyEvent::validate()`
    /// 2. State changes via `ApplyEvent::apply()`
    /// 3. Post-validation via `ApplyEvent::post_validate()`
    ///
    /// # Errors
    ///
    /// Returns an error if pre-validation or post-validation fails.
    fn dispatch(&self, aggregate: &mut A) -> Result<(), A::Error>;

    /// Dispatches the event to the aggregate without validation.
    ///
    /// This method only applies state changes, skipping both pre-validation
    /// and post-validation. It is used for event replay from the event store,
    /// where events are historical facts that should not be re-validated.
    fn dispatch_unchecked(&self, aggregate: &mut A);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{AggregateError, AggregateId, DomainEvent, Version};
    use chrono::Utc;
    use thiserror::Error;

    #[derive(Debug, Error)]
    #[error("Test error: {0}")]
    struct TestError(String);

    impl AggregateError for TestError {}

    #[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
    struct TestState {
        value: i32,
    }

    #[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
    enum TestEvent {
        Added { amount: i32 },
        Validated { amount: i32 },
    }

    struct TestAggregate {
        id: crate::DefaultAggregateId,
        state: TestState,
        version: Version,
        pending_events: Vec<TestEvent>,
    }

    impl EventApplicator<TestAggregate> for TestEvent {
        fn dispatch(&self, aggregate: &mut TestAggregate) -> Result<(), TestError> {
            match self {
                TestEvent::Added { amount } => {
                    aggregate.state.value += amount;
                }
                TestEvent::Validated { amount } => {
                    if *amount < 0 {
                        return Err(TestError("Amount cannot be negative".to_string()));
                    }
                    aggregate.state.value += amount;
                }
            }
            Ok(())
        }

        fn dispatch_unchecked(&self, aggregate: &mut TestAggregate) {
            match self {
                TestEvent::Added { amount } | TestEvent::Validated { amount } => {
                    aggregate.state.value += amount;
                }
            }
        }
    }

    impl DomainEvent for TestEvent {
        type Aggregate = TestAggregate;

        fn event_type(&self) -> &'static str {
            match self {
                TestEvent::Added { .. } => "Added",
                TestEvent::Validated { .. } => "Validated",
            }
        }

        fn event_version(&self) -> u64 {
            1
        }

        fn occurred_at(&self) -> chrono::DateTime<Utc> {
            Utc::now()
        }
    }

    impl crate::Aggregate for TestAggregate {
        type Id = crate::DefaultAggregateId;
        type Event = TestEvent;
        type Error = TestError;
        type State = TestState;

        fn new(id: Self::Id) -> Self {
            Self {
                id,
                state: TestState { value: 0 },
                version: Version::initial(),
                pending_events: Vec::new(),
            }
        }

        fn aggregate_id(&self) -> &Self::Id {
            &self.id
        }

        fn version(&self) -> Version {
            self.version
        }

        fn pending_events(&self) -> &[Self::Event] {
            &self.pending_events
        }

        fn clear_pending_events(&mut self) {
            self.pending_events.clear();
        }

        fn push_pending_event(&mut self, event: Self::Event) {
            self.pending_events.push(event);
        }

        fn increment_version(&mut self) {
            self.version = self.version.next();
        }

        fn state(&self) -> &Self::State {
            &self.state
        }

        fn from_snapshot(id: Self::Id, version: Version, state: Self::State) -> Self {
            Self {
                id,
                state,
                version,
                pending_events: Vec::new(),
            }
        }
    }

    #[test]
    fn test_dispatch_applies_state() {
        let mut aggregate = TestAggregate::new(crate::DefaultAggregateId::new());
        let event = TestEvent::Added { amount: 5 };

        EventApplicator::dispatch(&event, &mut aggregate).unwrap();

        assert_eq!(aggregate.state.value, 5);
    }

    #[test]
    fn test_dispatch_with_validation_succeeds() {
        let mut aggregate = TestAggregate::new(crate::DefaultAggregateId::new());
        let event = TestEvent::Validated { amount: 10 };

        let result = EventApplicator::dispatch(&event, &mut aggregate);

        assert!(result.is_ok());
        assert_eq!(aggregate.state.value, 10);
    }

    #[test]
    fn test_dispatch_with_validation_fails() {
        let mut aggregate = TestAggregate::new(crate::DefaultAggregateId::new());
        let event = TestEvent::Validated { amount: -5 };

        let result = EventApplicator::dispatch(&event, &mut aggregate);

        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err().to_string(),
            "Test error: Amount cannot be negative"
        );
    }

    #[test]
    fn test_dispatch_unchecked_skips_validation() {
        let mut aggregate = TestAggregate::new(crate::DefaultAggregateId::new());
        let event = TestEvent::Validated { amount: -5 };

        // dispatch would fail, but dispatch_unchecked succeeds
        EventApplicator::dispatch_unchecked(&event, &mut aggregate);

        assert_eq!(aggregate.state.value, -5);
    }

    #[test]
    fn test_dispatch_multiple_events() {
        let mut aggregate = TestAggregate::new(crate::DefaultAggregateId::new());

        EventApplicator::dispatch(&TestEvent::Added { amount: 5 }, &mut aggregate).unwrap();
        EventApplicator::dispatch(&TestEvent::Added { amount: 10 }, &mut aggregate).unwrap();
        EventApplicator::dispatch(&TestEvent::Validated { amount: 3 }, &mut aggregate).unwrap();

        assert_eq!(aggregate.state.value, 18);
    }

    #[test]
    fn test_aggregate_uses_event_applicator_in_apply() {
        let mut aggregate = TestAggregate::new(crate::DefaultAggregateId::new());

        aggregate.apply(TestEvent::Added { amount: 5 }).unwrap();

        assert_eq!(aggregate.state.value, 5);
        assert_eq!(aggregate.version(), Version::new(1));
        assert_eq!(aggregate.pending_events().len(), 1);
    }

    #[test]
    fn test_aggregate_apply_validates_via_event_applicator() {
        let mut aggregate = TestAggregate::new(crate::DefaultAggregateId::new());

        let result = aggregate.apply(TestEvent::Validated { amount: -5 });

        assert!(result.is_err());
        assert_eq!(aggregate.state.value, 0); // State unchanged
        assert_eq!(aggregate.version(), Version::initial()); // Version unchanged
        assert_eq!(aggregate.pending_events().len(), 0); // No pending events
    }
}
