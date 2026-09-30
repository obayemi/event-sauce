use crate::{
    Aggregate, AggregateError, AggregateRoot, AggregateVersion, ApplyEvent, DomainEvent, Entity,
    EntityId,
};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

// Test Events
#[derive(Debug, Clone, Serialize, Deserialize)]
struct IncrementedEvent {
    amount: i32,
    timestamp: DateTime<Utc>,
}

impl crate::EventType for IncrementedEvent {
    const EVENT_TYPE: &'static str = "Test.Incremented";
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct DecrementedEvent {
    amount: i32,
    timestamp: DateTime<Utc>,
}

impl crate::EventType for DecrementedEvent {
    const EVENT_TYPE: &'static str = "Test.Decremented";
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ResetEvent {
    timestamp: DateTime<Utc>,
}

impl crate::EventType for ResetEvent {
    const EVENT_TYPE: &'static str = "Test.Reset";
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct CreatedEvent {
    value: i32,
    timestamp: DateTime<Utc>,
}

impl crate::EventType for CreatedEvent {
    const EVENT_TYPE: &'static str = "Test.Created";
}

impl crate::InitEvent<TestAggregate> for CreatedEvent {
    fn init(&self, id: EntityId) -> TestAggregate {
        TestAggregate {
            id,
            value: self.value,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ClosedEvent {
    timestamp: DateTime<Utc>,
}

impl crate::EventType for ClosedEvent {
    const EVENT_TYPE: &'static str = "Test.Closed";
}

impl crate::DeleteEvent<TestAggregate> for ClosedEvent {}

#[derive(Debug, Clone, Serialize, Deserialize)]
enum TestEvent {
    Incremented(IncrementedEvent),
    Decremented(DecrementedEvent),
    Reset(ResetEvent),
    Created(CreatedEvent),
    Closed(ClosedEvent),
}

impl DomainEvent for TestEvent {
    type Aggregate = TestAggregate;

    fn event_type(&self) -> &'static str {
        match self {
            TestEvent::Incremented(_) => "Test.Incremented",
            TestEvent::Decremented(_) => "Test.Decremented",
            TestEvent::Reset(_) => "Test.Reset",
            TestEvent::Created(_) => "Test.Created",
            TestEvent::Closed(_) => "Test.Closed",
        }
    }

    fn event_version(&self) -> crate::EventVersion {
        crate::EventVersion::new(1)
    }

    fn occurred_at(&self) -> DateTime<Utc> {
        match self {
            TestEvent::Incremented(e) => e.timestamp,
            TestEvent::Decremented(e) => e.timestamp,
            TestEvent::Reset(e) => e.timestamp,
            TestEvent::Created(e) => e.timestamp,
            TestEvent::Closed(e) => e.timestamp,
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

impl From<CreatedEvent> for TestEvent {
    fn from(e: CreatedEvent) -> Self {
        TestEvent::Created(e)
    }
}

impl From<ClosedEvent> for TestEvent {
    fn from(e: ClosedEvent) -> Self {
        TestEvent::Closed(e)
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

// Test Aggregate (entity with embedded state)
#[derive(Debug, Serialize, Deserialize)]
struct TestAggregate {
    id: EntityId,
    value: i32,
}

impl Entity for TestAggregate {
    fn new(id: EntityId) -> Self {
        Self { id, value: 0 }
    }

    fn entity_id(&self) -> EntityId {
        self.id
    }
}

impl crate::DefaultEntity for TestAggregate {}

impl crate::EventApplicator<TestAggregate> for TestEvent {
    fn dispatch(&self, aggregate: &mut TestAggregate) -> Result<(), TestError> {
        match self {
            TestEvent::Incremented(e) => {
                e.validate(aggregate)?;
                e.apply(aggregate);
                e.post_validate(aggregate)?;
            }
            TestEvent::Decremented(e) => {
                e.validate(aggregate)?;
                e.apply(aggregate);
                e.post_validate(aggregate)?;
            }
            TestEvent::Reset(e) => {
                e.validate(aggregate)?;
                e.apply(aggregate);
                e.post_validate(aggregate)?;
            }
            TestEvent::Created(_) | TestEvent::Closed(_) => {
                unreachable!("dispatch() called on an init/delete event")
            }
        }
        Ok(())
    }

    fn dispatch_unchecked(&self, aggregate: &mut TestAggregate) {
        match self {
            TestEvent::Incremented(e) => {
                e.apply(aggregate);
            }
            TestEvent::Decremented(e) => {
                e.apply(aggregate);
            }
            TestEvent::Reset(e) => {
                e.apply(aggregate);
            }
            TestEvent::Created(_) | TestEvent::Closed(_) => {
                unreachable!("dispatch_unchecked() called on an init/delete event")
            }
        }
    }
}

impl Aggregate for TestAggregate {
    type Event = TestEvent;
    type Error = TestError;
    type DeletedState = Self;
}

impl TestAggregate {
    fn value(&self) -> i32 {
        self.value
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
        aggregate.value += self.amount;
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
        aggregate.value -= self.amount;
    }
}

impl ApplyEvent<TestAggregate> for ResetEvent {
    fn apply(&self, aggregate: &mut TestAggregate) {
        aggregate.value = 0;
    }
}

// This is the test for the command_handler! macro
command_handler! {
    impl TestAggregate {
        @clock fn increment(amount: i32) -> IncrementedEvent { amount };
        @clock fn decrement(amount: i32) -> DecrementedEvent { amount };
        @clock fn reset() -> ResetEvent { };
        @clock @init fn create(value: i32) -> CreatedEvent { value };
        @clock @delete fn close() -> ClosedEvent { };
    }
}

#[test]
fn test_command_handler_increment() {
    let mut root = AggregateRoot::<TestAggregate>::new(EntityId::new());

    // Test that generated command method works through AggregateRoot
    let result = root.increment(5);

    assert!(result.is_ok());
    assert_eq!(root.value(), 5);
    assert_eq!(root.pending_events().len(), 1);
    assert_eq!(root.version(), AggregateVersion::from(1));
}

#[test]
fn test_command_handler_decrement() {
    let mut root = AggregateRoot::<TestAggregate>::new(EntityId::new());
    root.increment(10).unwrap();

    // Test that generated decrement method works
    let result = root.decrement(3);

    assert!(result.is_ok());
    assert_eq!(root.value(), 7);
    assert_eq!(root.pending_events().len(), 2);
}

#[test]
fn test_command_handler_reset() {
    let mut root = AggregateRoot::<TestAggregate>::new(EntityId::new());
    root.increment(10).unwrap();

    // Test that generated reset method works
    let result = root.reset();

    assert!(result.is_ok());
    assert_eq!(root.value(), 0);
    assert_eq!(root.pending_events().len(), 2);
}

#[test]
fn test_command_handler_validation() {
    let mut root = AggregateRoot::<TestAggregate>::new(EntityId::new());

    // Test that validation is applied through the generated command
    let result = root.increment(0);

    assert!(result.is_err());
    assert_eq!(root.value(), 0); // Value should be unchanged
    assert_eq!(root.pending_events().len(), 0); // No event added
}

#[test]
fn test_command_handler_timestamp() {
    let mut root = AggregateRoot::<TestAggregate>::new(EntityId::new());
    let before = Utc::now();

    root.increment(5).unwrap();

    let after = Utc::now();

    // Check that the event has a timestamp
    let event = &root.pending_events()[0];
    let event_time = event.occurred_at();

    assert!(event_time >= before);
    assert!(event_time <= after);
}

#[test]
fn test_command_handler_multiple_commands() {
    let mut root = AggregateRoot::<TestAggregate>::new(EntityId::new());

    // Test multiple commands in sequence
    root.increment(5).unwrap();
    root.increment(3).unwrap();
    root.decrement(2).unwrap();
    root.reset().unwrap();
    root.increment(10).unwrap();

    assert_eq!(root.value(), 10);
    assert_eq!(root.pending_events().len(), 5);
    assert_eq!(root.version(), AggregateVersion::from(5));
}

#[test]
fn test_command_handler_return_type() {
    let mut root = AggregateRoot::<TestAggregate>::new(EntityId::new());

    // Verify the return type is Result<(), TestError>
    let result: Result<(), TestError> = root.increment(5);

    assert!(result.is_ok());
}

#[test]
fn test_command_handler_event_helper() {
    let root = AggregateRoot::<TestAggregate>::new(EntityId::new());

    // Event helpers are available on the entity via Deref
    let event = root.increment_event(42);
    assert_eq!(event.amount, 42);
}

// ===== command_handler! Event Helper Function Tests =====

#[test]
fn test_command_handler_generates_event_helper_functions() {
    // Test that <command>_event helper functions are generated
    let aggregate = TestAggregate::new(EntityId::new());

    // These should create events without applying them
    let event = aggregate.increment_event(5);
    assert_eq!(event.amount, 5);

    let event = aggregate.decrement_event(3);
    assert_eq!(event.amount, 3);

    let event = aggregate.reset_event();
    // Reset has no fields besides timestamp
    let _ = event.timestamp;
}

#[test]
fn test_event_helper_creates_event_without_applying() {
    let mut root = AggregateRoot::<TestAggregate>::new(EntityId::new());

    // Create event using helper - should NOT apply it
    let event = root.increment_event(10);

    // Aggregate state should be unchanged
    assert_eq!(root.value(), 0);
    assert_eq!(root.pending_events().len(), 0);

    // Now apply the event through AggregateRoot
    root.apply(event).unwrap();

    // Now state should be updated
    assert_eq!(root.value(), 10);
    assert_eq!(root.pending_events().len(), 1);
}

#[test]
fn test_event_helper_adds_timestamp() {
    let aggregate = TestAggregate::new(EntityId::new());
    let before = Utc::now();

    let event = aggregate.increment_event(5);

    let after = Utc::now();

    // Check that the event has a timestamp
    assert!(event.timestamp >= before);
    assert!(event.timestamp <= after);
}

#[test]
fn test_command_handler_init_creates_aggregate() {
    let root = TestAggregate::create(9).unwrap();

    assert_eq!(root.value(), 9);
    assert_eq!(root.pending_events().len(), 1);
}

#[test]
fn test_command_handler_init_creates_aggregate_with_explicit_id() {
    let id = EntityId::new();
    let root = TestAggregate::create_with_id(id, 3).unwrap();

    assert_eq!(root.entity_id(), id);
    assert_eq!(root.value(), 3);
}

#[test]
fn test_command_handler_init_commands_trait_on_uninit_root() {
    let id = EntityId::new();
    let root = crate::UninitAggregateRoot::<TestAggregate>::new(id)
        .create(11)
        .unwrap();

    assert_eq!(root.entity_id(), id);
    assert_eq!(root.value(), 11);
}

#[test]
fn test_command_handler_delete_closes_aggregate() {
    let root = AggregateRoot::<TestAggregate>::new(EntityId::new());
    let deleted = root.close().unwrap();

    assert_eq!(deleted.value(), 0);
}

#[test]
fn test_command_method_uses_event_helper() {
    let mut root = AggregateRoot::<TestAggregate>::new(EntityId::new());

    // The command method should internally use the event helper
    // We verify this by checking that the behavior is consistent
    let before = Utc::now();
    root.apply(root.increment_event(7)).unwrap();
    let after = Utc::now();

    assert_eq!(root.value(), 7);
    let event = &root.pending_events()[0];
    let event_time = event.occurred_at();
    assert!(event_time >= before);
    assert!(event_time <= after);
}

#[test]
fn test_event_helper_can_be_used_for_conditional_application() {
    let mut root = AggregateRoot::<TestAggregate>::new(EntityId::new());

    // Create multiple events using helpers
    let event1 = root.increment_event(5);
    let _event2 = root.increment_event(10);
    let event3 = root.increment_event(15);

    // Conditionally apply only some events
    root.apply(event1).unwrap();
    // Skip _event2
    root.apply(event3).unwrap();

    // Should only have applied event1 and event3
    assert_eq!(root.value(), 20); // 5 + 15
    assert_eq!(root.pending_events().len(), 2);
}
