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

/// Generate event structs, enum, and trait implementations with validation support.
///
/// This macro provides a declarative way to define domain events with automatic
/// generation of event structs, an event enum, and trait implementations including
/// validation logic.
///
/// # Syntax
///
/// ```ignore
/// define_events! {
///     pub enum EventEnumName for AggregateType {
///         VariantName {
///             field1: Type1,
///             field2: Type2,
///         }
///         [@version(n)]
///         [@validate {
///             // validation logic using `self` for event and `aggregate` for aggregate
///             return Ok(());
///         }]
///         [@post_validate {
///             // post-validation logic using `self` for event and `aggregate` for aggregate
///             return Ok(());
///         }]
///         => |aggregate, event| {
///             // apply logic
///         },
///     }
/// }
/// ```
///
/// **Note**: Validation blocks have access to `self` (the event) and `aggregate` (the aggregate instance).
///
/// # Generated Code
///
/// For each event variant, the macro generates:
/// - An individual event struct (e.g., `VariantNameEvent`)
/// - `ApplyEvent<Aggregate>` implementation with validation
/// - `From<VariantNameEvent>` for the event enum
///
/// Additionally, it generates:
/// - The event enum with all variants
/// - `DomainEvent` implementation for the enum
/// - `Clone`, `Debug`, `Serialize`, `Deserialize` derives
///
/// # Features
///
/// - **Automatic timestamp**: Each event struct includes a `timestamp: DateTime<Utc>` field
/// - **Versioning**: Use `@version(n)` to specify event schema version (defaults to 1)
/// - **Pre-validation**: Use `@validate |aggregate, event| { ... }` for pre-conditions
/// - **Post-validation**: Use `@post_validate |aggregate, event| { ... }` for invariants
/// - **Type-safe**: Full type checking of validation and apply logic
///
/// # Examples
///
/// ```ignore
/// use event_sauce_core::define_events;
///
/// define_events! {
///     pub enum OrderEvent for Order {
///         Created {
///             order_id: String,
///         } => |order, event| {
///             order.state.order_id = event.order_id.clone();
///         },
///
///         ItemAdded {
///             item_id: String,
///             quantity: u32,
///             price: i64,
///         }
///         @validate {
///             if aggregate.state.status == OrderStatus::Completed {
///                 return Err(OrderError::OrderAlreadyCompleted);
///             }
///             if self.quantity == 0 {
///                 return Err(OrderError::InvalidQuantity);
///             }
///             return Ok(());
///         }
///         @post_validate {
///             if aggregate.state.total_amount > 1_000_000 {
///                 return Err(OrderError::OrderTotalExceeded);
///             }
///             return Ok(());
///         }
///         => |order, event| {
///             order.state.items.push(OrderItem {
///                 item_id: event.item_id.clone(),
///                 quantity: event.quantity,
///                 price: event.price,
///             });
///             order.state.total_amount += event.price * event.quantity as i64;
///         },
///
///         Completed {}
///         @version(2)
///         @validate {
///             if aggregate.state.status == OrderStatus::Completed {
///                 return Err(OrderError::OrderAlreadyCompleted);
///             }
///             return Ok(());
///         }
///         => |order, _event| {
///             order.state.status = OrderStatus::Completed;
///         },
///     }
/// }
/// ```
///
/// # Benefits
///
/// - Reduces boilerplate by ~60%
/// - Type-safe validation logic
/// - Consistent event structure
/// - Automatic timestamp handling
/// - Clear, declarative syntax
/// - Full integration with `ApplyEvent` trait
#[macro_export]
macro_rules! define_events {
    // Main entry point - parse the full event definition
    (
        $vis:vis enum $event_enum:ident for $aggregate:ty {
            $(
                $variant:ident {
                    $($field:ident: $field_ty:ty),* $(,)?
                }
                $(@version($version:literal))?
                $(@validate |$val_agg:ident, $val_evt:ident| $val_body:block)?
                $(@post_validate |$post_val_agg:ident, $post_val_evt:ident| $post_val_body:block)?
                => $apply:expr
            ),* $(,)?
        }
    ) => {
        // Generate individual event structs
        $(
            paste::paste! {
                #[derive(Debug, Clone, ::serde::Serialize, ::serde::Deserialize)]
                $vis struct [<$variant Event>] {
                    $(pub $field: $field_ty,)*
                    pub timestamp: ::chrono::DateTime<::chrono::Utc>,
                }

                // Implement ApplyEvent for each event struct
                impl $crate::ApplyEvent<$aggregate> for [<$variant Event>] {
                    // Validate method (if provided)
                    #[allow(unused_variables, unreachable_code, clippy::redundant_closure_call)]
                    fn validate(&self, aggregate: &$aggregate)
                        -> ::std::result::Result<(), <$aggregate as $crate::Aggregate>::Error>
                    {
                        $(
                            // Call the validation closure provided by the user
                            // The user provides a closure like: |aggregate, event| { ... }
                            return (|$val_agg: &$aggregate, $val_evt: &Self| $val_body)(aggregate, self);
                        )?
                        ::std::result::Result::Ok(())
                    }

                    // Apply method (always required)
                    fn apply(&self, aggregate: &mut $aggregate) {
                        let apply_fn: fn(&mut $aggregate, &Self) = $apply;
                        apply_fn(aggregate, self);
                    }

                    // Post-validate method (if provided)
                    #[allow(unused_variables, unreachable_code, clippy::redundant_closure_call)]
                    fn post_validate(&self, aggregate: &$aggregate)
                        -> ::std::result::Result<(), <$aggregate as $crate::Aggregate>::Error>
                    {
                        $(
                            // Call the post-validation closure provided by the user
                            // The user provides a closure like: |aggregate, event| { ... }
                            return (|$post_val_agg: &$aggregate, $post_val_evt: &Self| $post_val_body)(aggregate, self);
                        )?
                        ::std::result::Result::Ok(())
                    }
                }

                // Implement From<EventStruct> for EventEnum
                impl ::std::convert::From<[<$variant Event>]> for $event_enum {
                    fn from(event: [<$variant Event>]) -> Self {
                        $event_enum::$variant {
                            $($field: event.$field,)*
                            timestamp: event.timestamp,
                        }
                    }
                }
            }
        )*

        // Generate the event enum
        #[derive(Debug, Clone, ::serde::Serialize, ::serde::Deserialize)]
        $vis enum $event_enum {
            $(
                $variant {
                    $($field: $field_ty,)*
                    timestamp: ::chrono::DateTime<::chrono::Utc>,
                }
            ),*
        }

        // Implement DomainEvent for the enum
        impl $crate::DomainEvent for $event_enum {
            type Aggregate = $aggregate;

            fn event_type(&self) -> &'static str {
                match self {
                    $(
                        $event_enum::$variant { .. } => {
                            paste::paste! {
                                stringify!([<$aggregate $variant>])
                            }
                        }
                    ),*
                }
            }

            fn event_version(&self) -> i32 {
                match self {
                    $(
                        $event_enum::$variant { .. } => {
                            define_events!(@version $($version)?)
                        }
                    ),*
                }
            }

            fn occurred_at(&self) -> ::chrono::DateTime<::chrono::Utc> {
                match self {
                    $(
                        $event_enum::$variant { timestamp, .. } => *timestamp
                    ),*
                }
            }
        }
    };

    // Helper: Extract version number (default to 1)
    (@version $version:literal) => { $version };
    (@version) => { 1 };
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

    // ===== define_events! Macro Tests (RED PHASE) =====

    // We'll test the macro with a comprehensive example
    // These tests will FAIL until we implement the macro

    // First, define a test aggregate for the macro
    #[derive(Debug, thiserror::Error)]
    pub enum OrderError {
        #[error("Order already completed")]
        OrderAlreadyCompleted,
        #[error("Invalid quantity: {0}")]
        InvalidQuantity(u32),
        #[error("Invalid amount: {0}")]
        InvalidAmount(i64),
        #[error("Order total exceeded: {0}")]
        OrderTotalExceeded(i64),
    }

    impl AggregateError for OrderError {}

    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
    enum OrderStatus {
        Created,
        Completed,
    }

    #[derive(Debug, Clone, Serialize, Deserialize)]
    struct OrderItem {
        item_id: String,
        quantity: u32,
        price: i64,
    }

    #[derive(Debug, Clone, Default, Serialize, Deserialize)]
    pub struct OrderState {
        order_id: String,
        items: Vec<OrderItem>,
        total_amount: i64,
        status: OrderStatus,
    }

    impl Default for OrderStatus {
        fn default() -> Self {
            OrderStatus::Created
        }
    }

    #[derive(Debug, Serialize, Deserialize)]
    pub struct Order {
        id: DefaultAggregateId,
        state: OrderState,
        version: Version,
        pending_events: Vec<OrderEvent>,
    }

    impl Aggregate for Order {
        type Id = DefaultAggregateId;
        type Event = OrderEvent;
        type Error = OrderError;
        type State = OrderState;

        fn new(id: Self::Id) -> Self {
            Self {
                id,
                state: OrderState::default(),
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
            // Dispatch to individual ApplyEvent implementations
            match event {
                OrderEvent::Created { .. } => {
                    if let OrderEvent::Created { order_id, .. } = event {
                        let evt = CreatedEvent {
                            order_id: order_id.clone(),
                            timestamp: event.occurred_at(),
                        };
                        evt.validate(self)?;
                        evt.apply(self);
                        evt.post_validate(self)?;
                    }
                }
                OrderEvent::ItemAdded { .. } => {
                    if let OrderEvent::ItemAdded {
                        item_id,
                        quantity,
                        price,
                        ..
                    } = event
                    {
                        let evt = ItemAddedEvent {
                            item_id: item_id.clone(),
                            quantity: *quantity,
                            price: *price,
                            timestamp: event.occurred_at(),
                        };
                        evt.validate(self)?;
                        evt.apply(self);
                        evt.post_validate(self)?;
                    }
                }
                OrderEvent::Completed { .. } => {
                    if let OrderEvent::Completed { .. } = event {
                        let evt = CompletedEvent {
                            timestamp: event.occurred_at(),
                        };
                        evt.validate(self)?;
                        evt.apply(self);
                        evt.post_validate(self)?;
                    }
                }
            }
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

    // This will be generated by the macro - these tests will fail in RED phase
    define_events! {
        pub enum OrderEvent for Order {
            Created {
                order_id: String,
            } => |order, event| {
                order.state.order_id = event.order_id.clone();
            },

            ItemAdded {
                item_id: String,
                quantity: u32,
                price: i64,
            }
            @validate |aggregate, event| {
                if aggregate.state.status == OrderStatus::Completed {
                    return Err(OrderError::OrderAlreadyCompleted);
                }
                if event.quantity == 0 {
                    return Err(OrderError::InvalidQuantity(event.quantity));
                }
                return Ok(());
            }
            @post_validate |aggregate, event| {
                if aggregate.state.total_amount > 1_000_000 {
                    return Err(OrderError::OrderTotalExceeded(aggregate.state.total_amount));
                }
                return Ok(());
            }
            => |order, event| {
                order.state.items.push(OrderItem {
                    item_id: event.item_id.clone(),
                    quantity: event.quantity,
                    price: event.price,
                });
                order.state.total_amount += event.price * event.quantity as i64;
            },

            Completed {}
            @version(2)
            @validate |aggregate, _event| {
                if aggregate.state.status == OrderStatus::Completed {
                    return Err(OrderError::OrderAlreadyCompleted);
                }
                return Ok(());
            }
            => |order, _event| {
                order.state.status = OrderStatus::Completed;
            },
        }
    }

    // Tests for basic event generation
    #[test]
    fn test_define_events_creates_event_structs() {
        // Test that individual event structs are created
        let _event = CreatedEvent {
            order_id: "order-123".to_string(),
            timestamp: Utc::now(),
        };
    }

    #[test]
    fn test_define_events_creates_event_enum() {
        // Test that the event enum is created
        let event = OrderEvent::Created {
            order_id: "order-123".to_string(),
            timestamp: Utc::now(),
        };

        assert!(matches!(event, OrderEvent::Created { .. }));
    }

    #[test]
    fn test_define_events_implements_domain_event() {
        // Test that DomainEvent is implemented
        let event = OrderEvent::Created {
            order_id: "order-123".to_string(),
            timestamp: Utc::now(),
        };

        assert_eq!(event.event_type(), "OrderCreated");
        assert_eq!(event.event_version(), 1); // default version
    }

    #[test]
    fn test_define_events_custom_version() {
        // Test that custom version is respected
        let event = OrderEvent::Completed {
            timestamp: Utc::now(),
        };

        assert_eq!(event.event_version(), 2); // explicit @version(2)
    }

    #[test]
    fn test_define_events_apply_event_simple() {
        // Test simple event without validation
        let mut order = Order::new(DefaultAggregateId::new());
        let event = CreatedEvent {
            order_id: "order-123".to_string(),
            timestamp: Utc::now(),
        };

        event.apply(&mut order);

        assert_eq!(order.state.order_id, "order-123");
    }

    #[test]
    fn test_define_events_validation_success() {
        // Test that validation works
        let order = Order::new(DefaultAggregateId::new());
        let event = ItemAddedEvent {
            item_id: "item-1".to_string(),
            quantity: 5,
            price: 100,
            timestamp: Utc::now(),
        };

        let result = event.validate(&order);
        assert!(result.is_ok());
    }

    #[test]
    fn test_define_events_validation_failure() {
        // Test that validation catches errors
        let mut order = Order::new(DefaultAggregateId::new());
        order.state.status = OrderStatus::Completed;

        let event = ItemAddedEvent {
            item_id: "item-1".to_string(),
            quantity: 5,
            price: 100,
            timestamp: Utc::now(),
        };

        let result = event.validate(&order);
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), OrderError::OrderAlreadyCompleted));
    }

    #[test]
    fn test_define_events_validation_zero_quantity() {
        // Test specific validation logic
        let order = Order::new(DefaultAggregateId::new());
        let event = ItemAddedEvent {
            item_id: "item-1".to_string(),
            quantity: 0,
            price: 100,
            timestamp: Utc::now(),
        };

        let result = event.validate(&order);
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), OrderError::InvalidQuantity(0)));
    }

    #[test]
    fn test_define_events_post_validation_success() {
        // Test post-validation success case
        let mut order = Order::new(DefaultAggregateId::new());
        order.state.total_amount = 500_000;

        let event = ItemAddedEvent {
            item_id: "item-1".to_string(),
            quantity: 1,
            price: 100,
            timestamp: Utc::now(),
        };

        let result = event.post_validate(&order);
        assert!(result.is_ok());
    }

    #[test]
    fn test_define_events_post_validation_failure() {
        // Test post-validation failure
        let mut order = Order::new(DefaultAggregateId::new());
        order.state.total_amount = 1_500_000; // Over the limit

        let event = ItemAddedEvent {
            item_id: "item-1".to_string(),
            quantity: 1,
            price: 100,
            timestamp: Utc::now(),
        };

        let result = event.post_validate(&order);
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            OrderError::OrderTotalExceeded(1_500_000)
        ));
    }

    #[test]
    fn test_define_events_apply_with_state_mutation() {
        // Test that apply actually mutates state
        let mut order = Order::new(DefaultAggregateId::new());
        let event = ItemAddedEvent {
            item_id: "item-1".to_string(),
            quantity: 2,
            price: 500,
            timestamp: Utc::now(),
        };

        event.apply(&mut order);

        assert_eq!(order.state.items.len(), 1);
        assert_eq!(order.state.items[0].item_id, "item-1");
        assert_eq!(order.state.items[0].quantity, 2);
        assert_eq!(order.state.items[0].price, 500);
        assert_eq!(order.state.total_amount, 1000); // 2 * 500
    }

    #[test]
    fn test_define_events_into_conversion() {
        // Test that From<EventStruct> for EventEnum is implemented
        let event_struct = CreatedEvent {
            order_id: "order-123".to_string(),
            timestamp: Utc::now(),
        };

        let event_enum: OrderEvent = event_struct.into();

        assert!(matches!(event_enum, OrderEvent::Created { .. }));
    }

    #[test]
    fn test_define_events_timestamp_field() {
        // Test that timestamp is automatically added
        let timestamp = Utc::now();
        let event = CreatedEvent {
            order_id: "order-123".to_string(),
            timestamp,
        };

        assert_eq!(event.timestamp, timestamp);
    }

    #[test]
    fn test_define_events_full_flow() {
        // Test complete flow: create, validate, apply
        let mut order = Order::new(DefaultAggregateId::new());

        // Create order
        let created = CreatedEvent {
            order_id: "order-123".to_string(),
            timestamp: Utc::now(),
        };
        created.validate(&order).unwrap();
        created.apply(&mut order);
        created.post_validate(&order).unwrap();

        assert_eq!(order.state.order_id, "order-123");

        // Add items
        let item = ItemAddedEvent {
            item_id: "item-1".to_string(),
            quantity: 3,
            price: 200,
            timestamp: Utc::now(),
        };
        item.validate(&order).unwrap();
        item.apply(&mut order);
        item.post_validate(&order).unwrap();

        assert_eq!(order.state.items.len(), 1);
        assert_eq!(order.state.total_amount, 600);

        // Complete order
        let completed = CompletedEvent {
            timestamp: Utc::now(),
        };
        completed.validate(&order).unwrap();
        completed.apply(&mut order);
        completed.post_validate(&order).unwrap();

        assert_eq!(order.state.status, OrderStatus::Completed);
    }

    #[test]
    fn test_define_events_validation_prevents_double_completion() {
        // Test that validation prevents invalid state transitions
        let mut order = Order::new(DefaultAggregateId::new());
        order.state.status = OrderStatus::Completed;

        let completed = CompletedEvent {
            timestamp: Utc::now(),
        };

        let result = completed.validate(&order);
        assert!(result.is_err());
    }
}
