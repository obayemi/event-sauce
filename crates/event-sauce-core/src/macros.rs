//! Helper macros for reducing boilerplate in event-sourced aggregates
//!
//! This module provides declarative macros that simplify common patterns in event sourcing:
//! - `command_handler!` - Automatic command method generation

/// Generate command handler methods for an aggregate.
///
/// This macro reduces boilerplate by automatically generating command methods
/// that create events, apply timestamps, and apply the events to the aggregate.
///
/// # Syntax
///
/// ```ignore
/// command_handler! {
///     impl AggregateType {
///         fn command_name(param1: Type1, param2: Type2)
///             -> EventStruct { field1, field2 };
///
///         fn another_command()
///             -> AnotherEventStruct { };
///     }
/// }
/// ```
///
/// # Generated Code
///
/// For each command, the macro generates a public method that:
/// 1. Creates the event struct with provided parameters
/// 2. Adds `timestamp: chrono::Utc::now()` automatically
/// 3. Applies the event using `self.apply(event)?`
/// 4. Returns `Result<(), <Self as Aggregate>::Error>`
///
/// # Examples
///
/// ```ignore
/// use event_sauce_core::command_handler;
///
/// command_handler! {
///     impl Counter {
///         fn increment(amount: i32) -> CounterIncrementedEvent { amount };
///         fn decrement(amount: i32) -> CounterDecrementedEvent { amount };
///         fn reset() -> CounterResetEvent { };
///     }
/// }
/// ```
///
/// This expands to:
///
/// ```ignore
/// impl Counter {
///     pub fn increment(&mut self, amount: i32)
///         -> Result<(), <Self as Aggregate>::Error>
///     {
///         let event = CounterIncrementedEvent {
///             amount,
///             timestamp: ::chrono::Utc::now(),
///         };
///         self.apply(event)?;
///         Ok(())
///     }
///
///     pub fn decrement(&mut self, amount: i32)
///         -> Result<(), <Self as Aggregate>::Error>
///     {
///         let event = CounterDecrementedEvent {
///             amount,
///             timestamp: ::chrono::Utc::now(),
///         };
///         self.apply(event)?;
///         Ok(())
///     }
///
///     pub fn reset(&mut self)
///         -> Result<(), <Self as Aggregate>::Error>
///     {
///         let event = CounterResetEvent {
///             timestamp: ::chrono::Utc::now(),
///         };
///         self.apply(event)?;
///         Ok(())
///     }
/// }
/// ```
///
/// # Benefits
///
/// - Reduces boilerplate by ~70%
/// - Ensures consistent command pattern
/// - Automatic timestamp handling
/// - Type-safe command parameters
/// - Clear, declarative syntax
#[macro_export]
macro_rules! command_handler {
    (
        impl $aggregate:ty {
            $(
                $(#[$attr:meta])*
                fn $command:ident($($param:ident: $param_ty:ty),* $(,)?)
                    -> $event_struct:ident { $($field:ident),* $(,)? }
            );* $(;)?
        }
    ) => {
        impl $aggregate {
            $(
                $(#[$attr])*
                #[allow(missing_docs)]
                pub fn $command(&mut self, $($param: $param_ty),*)
                    -> ::std::result::Result<(), <Self as $crate::Aggregate>::Error>
                {
                    let event = $event_struct {
                        $($field: $param,)*
                        timestamp: ::chrono::Utc::now(),
                    };
                    self.apply(event)?;
                    ::std::result::Result::Ok(())
                }
            )*
        }
    };
}

#[cfg(test)]
mod tests {
    use crate::{
        Aggregate, AggregateError, AggregateId, ApplyEvent, DefaultAggregateId, DomainEvent,
        Version,
    };
    use chrono::{DateTime, Utc};
    use serde::{Deserialize, Serialize};

    // Test Events
    #[derive(Debug, Clone, Serialize, Deserialize)]
    struct IncrementedEvent {
        amount: i32,
        timestamp: DateTime<Utc>,
    }

    #[derive(Debug, Clone, Serialize, Deserialize)]
    struct DecrementedEvent {
        amount: i32,
        timestamp: DateTime<Utc>,
    }

    #[derive(Debug, Clone, Serialize, Deserialize)]
    struct ResetEvent {
        timestamp: DateTime<Utc>,
    }

    #[derive(Debug, Clone, Serialize, Deserialize)]
    enum TestEvent {
        Incremented(IncrementedEvent),
        Decremented(DecrementedEvent),
        Reset(ResetEvent),
    }

    impl DomainEvent for TestEvent {
        type Aggregate = TestAggregate;

        fn event_type(&self) -> &'static str {
            match self {
                TestEvent::Incremented(_) => "Test.Incremented",
                TestEvent::Decremented(_) => "Test.Decremented",
                TestEvent::Reset(_) => "Test.Reset",
            }
        }

        fn event_version(&self) -> i32 {
            1
        }

        fn occurred_at(&self) -> DateTime<Utc> {
            match self {
                TestEvent::Incremented(e) => e.timestamp,
                TestEvent::Decremented(e) => e.timestamp,
                TestEvent::Reset(e) => e.timestamp,
            }
        }
    }

    // Into implementations for events
    impl From<IncrementedEvent> for TestEvent {
        fn from(e: IncrementedEvent) -> Self {
            TestEvent::Incremented(e)
        }
    }

    impl From<DecrementedEvent> for TestEvent {
        fn from(e: DecrementedEvent) -> Self {
            TestEvent::Decremented(e)
        }
    }

    impl From<ResetEvent> for TestEvent {
        fn from(e: ResetEvent) -> Self {
            TestEvent::Reset(e)
        }
    }

    // Test Error
    #[derive(Debug, thiserror::Error)]
    enum TestError {
        #[error("Invalid amount: {0}")]
        InvalidAmount(i32),
    }

    impl AggregateError for TestError {}

    // Test State
    #[derive(Debug, Clone, Default, Serialize, Deserialize)]
    struct TestAggregateState {
        value: i32,
    }

    // Test Aggregate
    #[derive(Debug, Serialize, Deserialize)]
    struct TestAggregate {
        id: DefaultAggregateId,
        state: TestAggregateState,
        version: Version,
        pending_events: Vec<TestEvent>,
    }

    impl Aggregate for TestAggregate {
        type Id = DefaultAggregateId;
        type Event = TestEvent;
        type Error = TestError;
        type State = TestAggregateState;

        fn new(id: Self::Id) -> Self {
            Self {
                id,
                state: TestAggregateState::default(),
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

        fn apply<E: Into<Self::Event>>(&mut self, event: E) -> Result<(), Self::Error> {
            let event = event.into();
            self.apply_internal(&event)?;
            self.pending_events.push(event);
            Ok(())
        }

        fn apply_internal(&mut self, event: &Self::Event) -> Result<(), Self::Error> {
            self.apply_event(event)?;
            self.version = self.version.next();
            Ok(())
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

    impl TestAggregate {
        fn apply_event(&mut self, event: &TestEvent) -> Result<(), TestError> {
            match event {
                TestEvent::Incremented(e) => {
                    e.validate(self)?;
                    e.apply(self);
                    e.post_validate(self)?;
                }
                TestEvent::Decremented(e) => {
                    e.validate(self)?;
                    e.apply(self);
                    e.post_validate(self)?;
                }
                TestEvent::Reset(e) => {
                    e.validate(self)?;
                    e.apply(self);
                    e.post_validate(self)?;
                }
            }
            Ok(())
        }

        fn value(&self) -> i32 {
            self.state.value
        }
    }

    // ApplyEvent implementations
    impl ApplyEvent<TestAggregate> for IncrementedEvent {
        fn validate(&self, _aggregate: &TestAggregate) -> Result<(), TestError> {
            if self.amount <= 0 {
                return Err(TestError::InvalidAmount(self.amount));
            }
            Ok(())
        }

        fn apply(&self, aggregate: &mut TestAggregate) {
            aggregate.state.value += self.amount;
        }
    }

    impl ApplyEvent<TestAggregate> for DecrementedEvent {
        fn validate(&self, _aggregate: &TestAggregate) -> Result<(), TestError> {
            if self.amount <= 0 {
                return Err(TestError::InvalidAmount(self.amount));
            }
            Ok(())
        }

        fn apply(&self, aggregate: &mut TestAggregate) {
            aggregate.state.value -= self.amount;
        }
    }

    impl ApplyEvent<TestAggregate> for ResetEvent {
        fn apply(&self, aggregate: &mut TestAggregate) {
            aggregate.state.value = 0;
        }
    }

    // This is the test for the command_handler! macro
    // This will FAIL in RED phase until we implement the macro
    command_handler! {
        impl TestAggregate {
            fn increment(amount: i32) -> IncrementedEvent { amount };
            fn decrement(amount: i32) -> DecrementedEvent { amount };
            fn reset() -> ResetEvent { };
        }
    }

    #[test]
    fn test_command_handler_increment() {
        let mut aggregate = TestAggregate::new(DefaultAggregateId::new());

        // Test that generated method exists and works
        let result = aggregate.increment(5);

        assert!(result.is_ok());
        assert_eq!(aggregate.value(), 5);
        assert_eq!(aggregate.pending_events().len(), 1);
        assert_eq!(aggregate.version(), Version::from(1));
    }

    #[test]
    fn test_command_handler_decrement() {
        let mut aggregate = TestAggregate::new(DefaultAggregateId::new());
        aggregate.increment(10).unwrap();

        // Test that generated decrement method works
        let result = aggregate.decrement(3);

        assert!(result.is_ok());
        assert_eq!(aggregate.value(), 7);
        assert_eq!(aggregate.pending_events().len(), 2);
    }

    #[test]
    fn test_command_handler_reset() {
        let mut aggregate = TestAggregate::new(DefaultAggregateId::new());
        aggregate.increment(10).unwrap();

        // Test that generated reset method works
        let result = aggregate.reset();

        assert!(result.is_ok());
        assert_eq!(aggregate.value(), 0);
        assert_eq!(aggregate.pending_events().len(), 2);
    }

    #[test]
    fn test_command_handler_validation() {
        let mut aggregate = TestAggregate::new(DefaultAggregateId::new());

        // Test that validation is applied through the generated method
        let result = aggregate.increment(0);

        assert!(result.is_err());
        assert_eq!(aggregate.value(), 0); // Value should be unchanged
        assert_eq!(aggregate.pending_events().len(), 0); // No event added
    }

    #[test]
    fn test_command_handler_timestamp() {
        let mut aggregate = TestAggregate::new(DefaultAggregateId::new());
        let before = Utc::now();

        aggregate.increment(5).unwrap();

        let after = Utc::now();

        // Check that the event has a timestamp
        let event = &aggregate.pending_events()[0];
        let event_time = event.occurred_at();

        assert!(event_time >= before);
        assert!(event_time <= after);
    }

    #[test]
    fn test_command_handler_multiple_commands() {
        let mut aggregate = TestAggregate::new(DefaultAggregateId::new());

        // Test multiple commands in sequence
        aggregate.increment(5).unwrap();
        aggregate.increment(3).unwrap();
        aggregate.decrement(2).unwrap();
        aggregate.reset().unwrap();
        aggregate.increment(10).unwrap();

        assert_eq!(aggregate.value(), 10);
        assert_eq!(aggregate.pending_events().len(), 5);
        assert_eq!(aggregate.version(), Version::from(5));
    }

    #[test]
    fn test_command_handler_return_type() {
        let mut aggregate = TestAggregate::new(DefaultAggregateId::new());

        // Verify the return type is Result<(), TestError>
        let result: Result<(), TestError> = aggregate.increment(5);

        assert!(result.is_ok());
    }
}
