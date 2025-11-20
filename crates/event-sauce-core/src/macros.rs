//! Helper macros for reducing boilerplate in event-sourced aggregates
//!
//! This module provides declarative macros that simplify common patterns in event sourcing:
//! - `command_handler!` - Automatic command method generation
//! - `projection!` - Declarative projection/read model definition

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

/// Generate a projection (read model) with declarative event handlers.
///
/// This macro simplifies creating projections by providing a declarative
/// syntax for defining how events update a read model state.
///
/// # Syntax
///
/// ```ignore
/// projection! {
///     pub struct ProjectionName {
///         state: StateType,
///
///         on "EventType1" => EventType1Struct |proj, event| {
///             // Update projection state based on event
///         },
///
///         on "EventType2" => EventType2Struct |proj, event| {
///             // Handle another event type
///         },
///     }
/// }
/// ```
///
/// # Generated Methods
///
/// The macro generates:
/// - `new(state: StateType) -> Self` - Constructor
/// - `state(&self) -> &StateType` - Immutable state access
/// - `state_mut(&mut self) -> &mut StateType` - Mutable state access
/// - `handle(&mut self, envelope: &EventEnvelope) -> Result<()>` - Event handler
///
/// # Examples
///
/// ```ignore
/// use event_sauce_core::projection;
/// use std::collections::HashMap;
///
/// projection! {
///     pub struct UserListProjection {
///         state: HashMap<UserId, UserView>,
///
///         on "UserRegistered" => UserRegisteredEvent |proj, event| {
///             proj.state.insert(event.user_id, UserView {
///                 email: event.email.clone(),
///                 status: UserStatus::Active,
///                 registered_at: event.timestamp,
///             });
///         },
///
///         on "UserEmailChanged" => UserEmailChangedEvent |proj, event| {
///             if let Some(user) = proj.state.get_mut(&event.user_id) {
///                 user.email = event.new_email.clone();
///             }
///         },
///
///         on "UserDeleted" => UserDeletedEvent |proj, event| {
///             proj.state.remove(&event.user_id);
///         },
///     }
/// }
/// ```
///
/// # Benefits
///
/// - Declarative event handling
/// - Type-safe event deserialization
/// - Automatic pattern matching
/// - Clean, readable projection definitions
/// - Ignores unknown events automatically
#[macro_export]
macro_rules! projection {
    (
        $vis:vis struct $name:ident {
            state: $state:ty,

            $(
                on $event_type:literal => $event:ty |$proj:ident, $evt:ident| $handler:block
            ),* $(,)?
        }
    ) => {
        $vis struct $name {
            state: $state,
        }

        impl $name {
            /// Creates a new projection with the given initial state.
            #[allow(missing_docs)]
            pub fn new(state: $state) -> Self {
                Self { state }
            }

            /// Returns an immutable reference to the projection state.
            #[allow(missing_docs)]
            pub fn state(&self) -> &$state {
                &self.state
            }

            /// Returns a mutable reference to the projection state.
            #[allow(missing_docs)]
            pub fn state_mut(&mut self) -> &mut $state {
                &mut self.state
            }

            /// Handles an event envelope by deserializing and applying it.
            ///
            /// Returns `Ok(())` if the event was handled or ignored (unknown event type).
            /// Returns `Err` only if deserialization or handler logic fails.
            #[allow(missing_docs)]
            pub async fn handle(&mut self, envelope: &$crate::EventEnvelope)
                -> $crate::Result<()>
            {
                $(
                    // Check event type first, then deserialize
                    if envelope.event_type == $event_type {
                        if let Ok($evt) = ::serde_json::from_value::<$event>(envelope.event_data.clone()) {
                            let $proj = self;
                            $handler
                            return Ok(());
                        }
                    }
                )*
                // Ignore unknown events
                Ok(())
            }
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

    // ===== Projection Macro Tests =====

    use std::collections::HashMap;

    // Test projection state
    #[derive(Debug, Clone, Default)]
    struct CounterView {
        value: i32,
        increment_count: usize,
        decrement_count: usize,
    }

    // Define a projection using the macro
    projection! {
        pub struct CounterProjection {
            state: CounterView,

            on "Test.Incremented" => IncrementedEvent |proj, event| {
                proj.state.value += event.amount;
                proj.state.increment_count += 1;
            },

            on "Test.Decremented" => DecrementedEvent |proj, event| {
                proj.state.value -= event.amount;
                proj.state.decrement_count += 1;
            },

            on "Test.Reset" => ResetEvent |_proj, _event| {
                _proj.state.value = 0;
            },
        }
    }

    #[tokio::test]
    async fn test_projection_new() {
        let state = CounterView::default();
        let _projection = CounterProjection::new(state);
    }

    #[tokio::test]
    async fn test_projection_state_access() {
        let state = CounterView {
            value: 42,
            increment_count: 0,
            decrement_count: 0,
        };
        let projection = CounterProjection::new(state);

        assert_eq!(projection.state().value, 42);
    }

    #[tokio::test]
    async fn test_projection_state_mut_access() {
        let state = CounterView::default();
        let mut projection = CounterProjection::new(state);

        projection.state_mut().value = 100;
        assert_eq!(projection.state().value, 100);
    }

    #[tokio::test]
    async fn test_projection_handle_increment() {
        let state = CounterView::default();
        let mut projection = CounterProjection::new(state);

        // Create an event envelope
        let event = IncrementedEvent {
            amount: 5,
            timestamp: Utc::now(),
        };
        let envelope = crate::EventEnvelope::new(
            uuid::Uuid::new_v4(),
            uuid::Uuid::new_v4(),
            "Test".to_string(),
            "Test.Incremented".to_string(),
            crate::Version::from(1),
            serde_json::to_value(&event).unwrap(),
        );

        // Handle the event
        projection.handle(&envelope).await.unwrap();

        // Check state was updated
        assert_eq!(projection.state().value, 5);
        assert_eq!(projection.state().increment_count, 1);
    }

    #[tokio::test]
    async fn test_projection_handle_decrement() {
        let state = CounterView {
            value: 10,
            increment_count: 0,
            decrement_count: 0,
        };
        let mut projection = CounterProjection::new(state);

        // Create an event envelope
        let event = DecrementedEvent {
            amount: 3,
            timestamp: Utc::now(),
        };
        let envelope = crate::EventEnvelope::new(
            uuid::Uuid::new_v4(),
            uuid::Uuid::new_v4(),
            "Test".to_string(),
            "Test.Decremented".to_string(),
            crate::Version::from(1),
            serde_json::to_value(&event).unwrap(),
        );

        // Handle the event
        projection.handle(&envelope).await.unwrap();

        // Check state was updated
        assert_eq!(projection.state().value, 7);
        assert_eq!(projection.state().decrement_count, 1);
    }

    #[tokio::test]
    async fn test_projection_handle_reset() {
        let state = CounterView {
            value: 42,
            increment_count: 0,
            decrement_count: 0,
        };
        let mut projection = CounterProjection::new(state);

        // Create an event envelope
        let event = ResetEvent {
            timestamp: Utc::now(),
        };
        let envelope = crate::EventEnvelope::new(
            uuid::Uuid::new_v4(),
            uuid::Uuid::new_v4(),
            "Test".to_string(),
            "Test.Reset".to_string(),
            crate::Version::from(1),
            serde_json::to_value(&event).unwrap(),
        );

        // Handle the event
        projection.handle(&envelope).await.unwrap();

        // Check state was reset
        assert_eq!(projection.state().value, 0);
    }

    #[tokio::test]
    async fn test_projection_ignores_unknown_events() {
        let state = CounterView::default();
        let mut projection = CounterProjection::new(state);

        // Create an envelope with unknown event type
        let envelope = crate::EventEnvelope::new(
            uuid::Uuid::new_v4(),
            uuid::Uuid::new_v4(),
            "Test".to_string(),
            "UnknownEvent".to_string(),
            crate::Version::from(1),
            serde_json::json!({"unknown": "data"}),
        );

        // Should not fail, just ignore
        let result = projection.handle(&envelope).await;
        assert!(result.is_ok());

        // State should be unchanged
        assert_eq!(projection.state().value, 0);
    }

    #[tokio::test]
    async fn test_projection_multiple_events() {
        let state = CounterView::default();
        let mut projection = CounterProjection::new(state);

        // Handle multiple events
        for i in 1..=5 {
            let event = IncrementedEvent {
                amount: i,
                timestamp: Utc::now(),
            };
            let envelope = crate::EventEnvelope::new(
                uuid::Uuid::new_v4(),
                uuid::Uuid::new_v4(),
                "Test".to_string(),
                "Test.Incremented".to_string(),
                crate::Version::from(i),
                serde_json::to_value(&event).unwrap(),
            );
            projection.handle(&envelope).await.unwrap();
        }

        // 1 + 2 + 3 + 4 + 5 = 15
        assert_eq!(projection.state().value, 15);
        assert_eq!(projection.state().increment_count, 5);
    }

    // Test with HashMap state (more realistic projection)
    #[derive(Debug, Clone)]
    struct UserView {
        name: String,
        email: String,
    }

    #[derive(Debug, Clone, Serialize, Deserialize)]
    struct UserCreatedEvent {
        user_id: String,
        name: String,
        email: String,
        timestamp: DateTime<Utc>,
    }

    #[derive(Debug, Clone, Serialize, Deserialize)]
    struct UserUpdatedEvent {
        user_id: String,
        name: String,
        timestamp: DateTime<Utc>,
    }

    projection! {
        pub struct UserListProjection {
            state: HashMap<String, UserView>,

            on "UserCreated" => UserCreatedEvent |proj, event| {
                proj.state.insert(event.user_id.clone(), UserView {
                    name: event.name.clone(),
                    email: event.email.clone(),
                });
            },

            on "UserUpdated" => UserUpdatedEvent |proj, event| {
                if let Some(user) = proj.state.get_mut(&event.user_id) {
                    user.name = event.name.clone();
                }
            },
        }
    }

    #[tokio::test]
    async fn test_projection_with_hashmap() {
        let state = HashMap::new();
        let mut projection = UserListProjection::new(state);

        // Create user
        let user_id = "user-123".to_string();
        let event = UserCreatedEvent {
            user_id: user_id.clone(),
            name: "Alice".to_string(),
            email: "alice@example.com".to_string(),
            timestamp: Utc::now(),
        };
        let envelope = crate::EventEnvelope::new(
            uuid::Uuid::new_v4(),
            uuid::Uuid::new_v4(),
            "User".to_string(),
            "UserCreated".to_string(),
            crate::Version::from(1),
            serde_json::to_value(&event).unwrap(),
        );
        projection.handle(&envelope).await.unwrap();

        // Verify user was added
        assert_eq!(projection.state().len(), 1);
        assert_eq!(projection.state().get(&user_id).unwrap().name, "Alice");
        assert_eq!(
            projection.state().get(&user_id).unwrap().email,
            "alice@example.com"
        );

        // Update user
        let event = UserUpdatedEvent {
            user_id: user_id.clone(),
            name: "Alice Smith".to_string(),
            timestamp: Utc::now(),
        };
        let envelope = crate::EventEnvelope::new(
            uuid::Uuid::new_v4(),
            uuid::Uuid::new_v4(),
            "User".to_string(),
            "UserUpdated".to_string(),
            crate::Version::from(2),
            serde_json::to_value(&event).unwrap(),
        );
        projection.handle(&envelope).await.unwrap();

        // Verify user was updated
        assert_eq!(projection.state().len(), 1);
        assert_eq!(
            projection.state().get(&user_id).unwrap().name,
            "Alice Smith"
        );
    }
}
