use super::*;
use crate::{AggregateError, ApplyEvent, DomainEvent, EntityId};
use chrono::Utc;

// Test error type
#[derive(Debug, thiserror::Error)]
#[error("Test error")]
struct TestError;

impl AggregateError for TestError {}

// Test event structs
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct IncrementedEvent {
    amount: i32,
    timestamp: chrono::DateTime<Utc>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct ResetEvent {
    timestamp: chrono::DateTime<Utc>,
}

// An event whose apply mutates value first, then post_validate rejects a
// value above the cap — used to exercise the poison path (L9).
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct CappedEvent {
    amount: i32,
    max_value: i32,
    timestamp: chrono::DateTime<Utc>,
}

// Test event enum
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
enum CounterEvent {
    Incremented(IncrementedEvent),
    Reset(ResetEvent),
    Capped(CappedEvent),
}

impl DomainEvent for CounterEvent {
    type Aggregate = CounterEntity;

    fn event_type(&self) -> &'static str {
        match self {
            CounterEvent::Incremented(_) => "Counter.Incremented",
            CounterEvent::Reset(_) => "Counter.Reset",
            CounterEvent::Capped(_) => "Counter.Capped",
        }
    }

    fn event_version(&self) -> crate::EventVersion {
        crate::EventVersion::new(1)
    }

    fn occurred_at(&self) -> chrono::DateTime<Utc> {
        match self {
            CounterEvent::Incremented(e) => e.timestamp,
            CounterEvent::Reset(e) => e.timestamp,
            CounterEvent::Capped(e) => e.timestamp,
        }
    }
}

impl From<IncrementedEvent> for CounterEvent {
    fn from(e: IncrementedEvent) -> Self {
        CounterEvent::Incremented(e)
    }
}

impl From<ResetEvent> for CounterEvent {
    fn from(e: ResetEvent) -> Self {
        CounterEvent::Reset(e)
    }
}

impl From<CappedEvent> for CounterEvent {
    fn from(e: CappedEvent) -> Self {
        CounterEvent::Capped(e)
    }
}

impl ApplyEvent<CounterEntity> for IncrementedEvent {
    fn validate(&self, _entity: &CounterEntity) -> Result<(), TestError> {
        if self.amount < 0 {
            return Err(TestError);
        }
        Ok(())
    }

    fn apply(&self, entity: &mut CounterEntity) {
        entity.value += self.amount;
    }
}

impl ApplyEvent<CounterEntity> for ResetEvent {
    fn apply(&self, entity: &mut CounterEntity) {
        entity.value = 0;
    }
}

impl ApplyEvent<CounterEntity> for CappedEvent {
    fn apply(&self, entity: &mut CounterEntity) {
        entity.value += self.amount;
    }

    fn post_validate(&self, entity: &CounterEntity) -> Result<(), TestError> {
        if entity.value > self.max_value {
            return Err(TestError);
        }
        Ok(())
    }
}

impl EventApplicator<CounterEntity> for CounterEvent {
    fn dispatch(&self, entity: &mut CounterEntity) -> Result<(), TestError> {
        match self {
            CounterEvent::Incremented(e) => {
                e.validate(entity)?;
                e.apply(entity);
                e.post_validate(entity)?;
            }
            CounterEvent::Reset(e) => {
                e.validate(entity)?;
                e.apply(entity);
                e.post_validate(entity)?;
            }
            CounterEvent::Capped(e) => {
                e.validate(entity)?;
                e.apply(entity);
                e.post_validate(entity)?;
            }
        }
        Ok(())
    }

    fn validate_only(&self, entity: &CounterEntity) -> Result<(), TestError> {
        match self {
            CounterEvent::Incremented(e) => e.validate(entity),
            CounterEvent::Reset(e) => e.validate(entity),
            CounterEvent::Capped(e) => e.validate(entity),
        }
    }

    fn dispatch_unchecked(&self, entity: &mut CounterEntity) {
        match self {
            CounterEvent::Incremented(e) => e.apply(entity),
            CounterEvent::Reset(e) => e.apply(entity),
            CounterEvent::Capped(e) => e.apply(entity),
        }
    }
}

// Test entity
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct CounterEntity {
    id: EntityId,
    value: i32,
}

impl crate::Entity for CounterEntity {
    fn new(id: EntityId) -> Self {
        Self { id, value: 0 }
    }

    fn entity_id(&self) -> EntityId {
        self.id
    }
}

impl crate::DefaultEntity for CounterEntity {}

impl Aggregate for CounterEntity {
    type Event = CounterEvent;
    type Error = TestError;
    type DeletedState = Self;
}

// Custom methods on the aggregate root
impl AggregateRoot<CounterEntity> {
    fn increment(&mut self, amount: i32) -> Result<(), TestError> {
        self.apply(IncrementedEvent {
            amount,
            timestamp: Utc::now(),
        })
    }

    fn reset(&mut self) -> Result<(), TestError> {
        self.apply(ResetEvent {
            timestamp: Utc::now(),
        })
    }
}

#[test]
fn test_aggregate_root_new() {
    let id = EntityId::new();
    let counter = AggregateRoot::<CounterEntity>::new(id);

    assert_eq!(counter.entity_id(), id);
    assert_eq!(counter.version(), AggregateVersion::initial());
    assert_eq!(counter.pending_events().len(), 0);
    assert_eq!(counter.value, 0);
}

#[test]
fn test_aggregate_root_deref() {
    let counter = AggregateRoot::<CounterEntity>::new(EntityId::new());

    // Deref gives read-only access to entity fields
    assert_eq!(counter.value, 0);
}

// NOTE: DerefMut is intentionally not implemented for AggregateRoot.
// This is a compile-time guarantee enforced by the type system.
// See trybuild tests for compile-fail verification if needed.

#[test]
fn test_aggregate_root_apply() {
    let mut counter = AggregateRoot::<CounterEntity>::new(EntityId::new());

    counter.increment(5).unwrap();

    assert_eq!(counter.value, 5);
    assert_eq!(counter.version(), AggregateVersion::new(1));
    assert_eq!(counter.pending_events().len(), 1);
}

#[test]
fn test_aggregate_root_multiple_events() {
    let mut counter = AggregateRoot::<CounterEntity>::new(EntityId::new());

    counter.increment(5).unwrap();
    counter.increment(3).unwrap();
    counter.increment(2).unwrap();

    assert_eq!(counter.value, 10);
    assert_eq!(counter.version(), AggregateVersion::new(3));
    assert_eq!(counter.pending_events().len(), 3);
}

#[test]
fn test_aggregate_root_reset() {
    let mut counter = AggregateRoot::<CounterEntity>::new(EntityId::new());

    counter.increment(10).unwrap();
    counter.reset().unwrap();

    assert_eq!(counter.value, 0);
    assert_eq!(counter.version(), AggregateVersion::new(2));
}

#[test]
fn test_aggregate_root_replay() {
    let id = EntityId::new();
    let mut counter = AggregateRoot::<CounterEntity>::new(id);

    let events = vec![
        CounterEvent::Incremented(IncrementedEvent {
            amount: 10,
            timestamp: Utc::now(),
        }),
        CounterEvent::Incremented(IncrementedEvent {
            amount: 5,
            timestamp: Utc::now(),
        }),
        CounterEvent::Reset(ResetEvent {
            timestamp: Utc::now(),
        }),
        CounterEvent::Incremented(IncrementedEvent {
            amount: 3,
            timestamp: Utc::now(),
        }),
    ];

    for event in &events {
        counter.apply_unchecked(event);
    }

    assert_eq!(counter.value, 3);
    assert_eq!(counter.version(), AggregateVersion::new(4));
    assert_eq!(counter.pending_events().len(), 0);
}

#[test]
fn test_aggregate_root_restore() {
    let entity = CounterEntity {
        id: EntityId::new(),
        value: 42,
    };

    let counter = AggregateRoot::<CounterEntity>::restore(AggregateVersion::new(5), entity);

    assert_eq!(counter.value, 42);
    assert_eq!(counter.version(), AggregateVersion::new(5));
}

#[test]
fn test_aggregate_root_clear_pending() {
    let mut counter = AggregateRoot::<CounterEntity>::new(EntityId::new());

    counter.increment(5).unwrap();
    counter.increment(3).unwrap();
    assert_eq!(counter.pending_events().len(), 2);

    counter.clear_pending_events();
    assert_eq!(counter.pending_events().len(), 0);
}

#[test]
fn test_aggregate_root_type_name() {
    let type_name = AggregateRoot::<CounterEntity>::aggregate_type();
    assert_eq!(type_name, "CounterEntity");
}

#[test]
fn test_aggregate_root_entity_method() {
    let mut counter = AggregateRoot::<CounterEntity>::new(EntityId::new());
    counter.increment(42).unwrap();

    let entity = counter.entity();
    assert_eq!(entity.value, 42);
}

#[test]
fn test_aggregate_root_serialize() {
    let mut counter = AggregateRoot::<CounterEntity>::new(EntityId::new());
    counter.increment(5).unwrap();
    counter.increment(3).unwrap();

    let json = serde_json::to_value(&counter).unwrap();

    assert!(
        json.get("entity").is_some(),
        "JSON should have 'entity' field"
    );
    assert!(
        json.get("version").is_some(),
        "JSON should have 'version' field"
    );
    assert!(
        json.get("pending_events").is_some(),
        "JSON should have 'pending_events' field"
    );

    assert_eq!(json["entity"]["value"], 8);
    assert_eq!(json["version"], 2);
    assert_eq!(json["pending_events"].as_array().unwrap().len(), 2);
}

#[test]
fn test_aggregate_root_apply_with_actor() {
    let mut counter = AggregateRoot::<CounterEntity>::new(EntityId::new());
    let actor_id = EntityId::new();

    counter
        .apply_with_actor(
            IncrementedEvent {
                amount: 7,
                timestamp: Utc::now(),
            },
            actor_id,
        )
        .unwrap();

    assert_eq!(counter.value, 7);
    assert_eq!(counter.version(), AggregateVersion::new(1));
    assert_eq!(counter.pending_events().len(), 1);

    // Verify actor_id is stored
    let pending = counter.pending_events_with_actors();
    assert_eq!(pending[0].actor_id, Some(actor_id));
}

#[test]
fn test_apply_without_actor_has_none_actor_id() {
    let mut counter = AggregateRoot::<CounterEntity>::new(EntityId::new());

    counter.increment(5).unwrap();

    let pending = counter.pending_events_with_actors();
    assert_eq!(pending[0].actor_id, None);
}

#[test]
fn test_mixed_actor_and_non_actor_events() {
    let mut counter = AggregateRoot::<CounterEntity>::new(EntityId::new());
    let actor_id = EntityId::new();

    counter.increment(5).unwrap();
    counter
        .apply_with_actor(
            IncrementedEvent {
                amount: 3,
                timestamp: Utc::now(),
            },
            actor_id,
        )
        .unwrap();
    counter.increment(2).unwrap();

    assert_eq!(counter.value, 10);
    assert_eq!(counter.pending_events().len(), 3);

    let pending = counter.pending_events_with_actors();
    assert_eq!(pending[0].actor_id, None);
    assert_eq!(pending[1].actor_id, Some(actor_id));
    assert_eq!(pending[2].actor_id, None);
}

#[test]
fn test_aggregate_root_apply_with_metadata() {
    let mut counter = AggregateRoot::<CounterEntity>::new(EntityId::new());
    let metadata = crate::EventMetadata::new()
        .with_correlation_id(uuid::Uuid::new_v4())
        .with_causation_id(uuid::Uuid::new_v4());

    counter
        .apply_with_metadata(
            IncrementedEvent {
                amount: 5,
                timestamp: Utc::now(),
            },
            metadata.clone(),
        )
        .unwrap();

    assert_eq!(counter.value, 5);
    assert_eq!(counter.version(), AggregateVersion::new(1));

    let pending = counter.pending_events_with_actors();
    assert_eq!(pending[0].metadata, Some(metadata));
}

#[test]
fn test_set_pending_metadata() {
    let mut counter = AggregateRoot::<CounterEntity>::new(EntityId::new());

    // Apply without metadata
    counter.increment(5).unwrap();
    counter.increment(3).unwrap();

    let metadata = crate::EventMetadata::new().with_correlation_id(uuid::Uuid::new_v4());

    counter.set_pending_metadata(&metadata);

    let pending = counter.pending_events_with_actors();
    assert_eq!(pending[0].metadata, Some(metadata.clone()));
    assert_eq!(pending[1].metadata, Some(metadata));
}

#[test]
fn test_set_pending_metadata_does_not_overwrite_existing() {
    let mut counter = AggregateRoot::<CounterEntity>::new(EntityId::new());
    let existing_metadata = crate::EventMetadata::new().with_causation_id(uuid::Uuid::new_v4());

    counter
        .apply_with_metadata(
            IncrementedEvent {
                amount: 5,
                timestamp: Utc::now(),
            },
            existing_metadata.clone(),
        )
        .unwrap();
    counter.increment(3).unwrap();

    let new_metadata = crate::EventMetadata::new().with_correlation_id(uuid::Uuid::new_v4());

    counter.set_pending_metadata(&new_metadata);

    let pending = counter.pending_events_with_actors();
    // First event keeps its existing metadata
    assert_eq!(pending[0].metadata, Some(existing_metadata));
    // Second event gets the new metadata
    assert_eq!(pending[1].metadata, Some(new_metadata));
}

#[test]
fn test_aggregate_root_clone_preserves_all_fields() {
    let mut counter = AggregateRoot::<CounterEntity>::new(EntityId::new());
    counter.increment(7).unwrap();
    counter.increment(3).unwrap();

    let cloned = counter.clone();

    assert_eq!(cloned.value, counter.value);
    assert_eq!(cloned.entity_id(), counter.entity_id());
    assert_eq!(cloned.version(), counter.version());
    assert_eq!(
        cloned.pending_events().len(),
        counter.pending_events().len()
    );
}

// === Poison tests (L9) ===

#[test]
fn test_aggregate_root_new_is_not_poisoned() {
    let counter = AggregateRoot::<CounterEntity>::new(EntityId::new());
    assert!(!counter.is_poisoned());
}

#[test]
fn test_successful_apply_does_not_poison() {
    let mut counter = AggregateRoot::<CounterEntity>::new(EntityId::new());
    counter.increment(5).unwrap();
    assert!(!counter.is_poisoned());
}

#[test]
fn test_capped_apply_within_limit_succeeds() {
    // A CappedEvent that stays within the cap passes post_validate and
    // leaves the aggregate clean (covers the accepting branch).
    let mut counter = AggregateRoot::<CounterEntity>::new(EntityId::new());
    counter
        .apply(CappedEvent {
            amount: 5,
            max_value: 10,
            timestamp: Utc::now(),
        })
        .unwrap();
    assert!(!counter.is_poisoned());
    assert_eq!(counter.value, 5);

    // Replaying the same event unchecked reproduces the state.
    let mut replay = AggregateRoot::<CounterEntity>::new(EntityId::new());
    replay.apply_unchecked(&CounterEvent::Capped(CappedEvent {
        amount: 5,
        max_value: 10,
        timestamp: Utc::now(),
    }));
    assert_eq!(replay.value, 5);
}

#[test]
fn test_failed_apply_poisons_aggregate() {
    let mut counter = AggregateRoot::<CounterEntity>::new(EntityId::new());

    // CappedEvent's apply mutates value first, then post_validate rejects a
    // value above the cap — so the event is rejected after the entity is
    // already mutated through the real apply() path.
    let result = counter.apply(CappedEvent {
        amount: 25,
        max_value: 20,
        timestamp: Utc::now(),
    });

    assert!(result.is_err());
    assert!(
        counter.is_poisoned(),
        "a rejected apply must poison the aggregate"
    );
    // The version was NOT bumped and no event was recorded, yet the entity
    // was already mutated by the apply closure: this is the inconsistency
    // the poison flag guards against.
    assert_eq!(counter.version(), AggregateVersion::initial());
    assert_eq!(counter.pending_events().len(), 0);
    assert_eq!(counter.value, 25);
}

#[test]
fn test_failed_apply_with_actor_poisons_aggregate() {
    let mut counter = AggregateRoot::<CounterEntity>::new(EntityId::new());

    let result = counter.apply_with_actor(
        CappedEvent {
            amount: 25,
            max_value: 20,
            timestamp: Utc::now(),
        },
        EntityId::new(),
    );

    assert!(result.is_err());
    assert!(counter.is_poisoned());
}

#[test]
fn test_failed_apply_with_metadata_poisons_aggregate() {
    let mut counter = AggregateRoot::<CounterEntity>::new(EntityId::new());

    let result = counter.apply_with_metadata(
        CappedEvent {
            amount: 25,
            max_value: 20,
            timestamp: Utc::now(),
        },
        crate::EventMetadata::new(),
    );

    assert!(result.is_err());
    assert!(counter.is_poisoned());
}

/// A pre-validation refusal through `apply_with_actor` leaves the
/// aggregate untouched and unpoisoned.
#[test]
fn test_refused_apply_with_actor_does_not_poison() {
    let mut counter = AggregateRoot::<CounterEntity>::new(EntityId::new());

    let result = counter.apply_with_actor(
        IncrementedEvent {
            amount: -5,
            timestamp: Utc::now(),
        },
        EntityId::new(),
    );

    assert!(result.is_err());
    assert!(!counter.is_poisoned());
    assert_eq!(counter.value, 0);
    assert_eq!(counter.version(), AggregateVersion::initial());
    assert_eq!(counter.pending_events().len(), 0);
}

/// `apply_with_metadata` skips `validate_only`, so even a
/// pre-validation refusal poisons the aggregate.
#[test]
fn test_apply_with_metadata_poisons_on_pre_validation_refusal() {
    let mut counter = AggregateRoot::<CounterEntity>::new(EntityId::new());

    let result = counter.apply_with_metadata(
        IncrementedEvent {
            amount: -5,
            timestamp: Utc::now(),
        },
        crate::EventMetadata::new(),
    );

    assert!(result.is_err());
    assert!(counter.is_poisoned());
}

#[test]
fn test_counter_event_domain_event_accessors() {
    // Exercises the DomainEvent accessors across all CounterEvent variants,
    // including the Capped variant used by the poison tests.
    let ts = Utc::now();
    let events = [
        CounterEvent::Incremented(IncrementedEvent {
            amount: 1,
            timestamp: ts,
        }),
        CounterEvent::Reset(ResetEvent { timestamp: ts }),
        CounterEvent::Capped(CappedEvent {
            amount: 1,
            max_value: 1,
            timestamp: ts,
        }),
    ];
    let expected_types = ["Counter.Incremented", "Counter.Reset", "Counter.Capped"];
    for (event, expected) in events.iter().zip(expected_types) {
        assert_eq!(event.event_type(), expected);
        assert_eq!(event.event_version(), crate::EventVersion::new(1));
        assert_eq!(event.occurred_at(), ts);
    }
}

#[test]
fn test_clone_preserves_poison() {
    let mut counter = AggregateRoot::<CounterEntity>::new(EntityId::new());
    let _ = counter.apply(CappedEvent {
        amount: 25,
        max_value: 20,
        timestamp: Utc::now(),
    });
    assert!(counter.is_poisoned());

    let cloned = counter.clone();
    assert!(cloned.is_poisoned());
}

// === Delete event tests ===

struct ClosedEvent {
    timestamp: chrono::DateTime<Utc>,
}

impl crate::DeleteEvent<CounterEntity> for ClosedEvent {
    fn delete(&self, mut entity: CounterEntity) -> CounterEntity {
        entity.value = -1; // Mark as closed
        entity
    }
}

impl From<ClosedEvent> for CounterEvent {
    fn from(e: ClosedEvent) -> Self {
        CounterEvent::Reset(ResetEvent {
            timestamp: e.timestamp,
        })
    }
}

#[test]
fn test_aggregate_root_apply_delete() {
    let mut counter = AggregateRoot::<CounterEntity>::new(EntityId::new());
    counter.increment(10).unwrap();

    let id = counter.entity_id();
    let deleted = counter
        .apply_delete(ClosedEvent {
            timestamp: Utc::now(),
        })
        .unwrap();

    assert_eq!(deleted.entity_id(), id);
    assert_eq!(deleted.state().value, -1);
    assert_eq!(deleted.version(), AggregateVersion::new(2));
    // Pending events include the increment + the delete event
    assert_eq!(deleted.pending_events().len(), 2);
}

#[test]
fn test_aggregate_root_apply_delete_transfers_pending_events() {
    let mut counter = AggregateRoot::<CounterEntity>::new(EntityId::new());
    counter.increment(5).unwrap();
    counter.increment(3).unwrap();

    let deleted = counter
        .apply_delete(ClosedEvent {
            timestamp: Utc::now(),
        })
        .unwrap();

    // 2 increments + 1 delete = 3 pending events
    assert_eq!(deleted.pending_events().len(), 3);
    assert_eq!(deleted.version(), AggregateVersion::new(3));
}

#[test]
fn test_aggregate_root_apply_delete_with_actor() {
    let mut counter = AggregateRoot::<CounterEntity>::new(EntityId::new());
    counter.increment(10).unwrap();

    let actor_id = EntityId::new();
    let deleted = counter
        .apply_delete_with_actor(
            ClosedEvent {
                timestamp: Utc::now(),
            },
            actor_id,
        )
        .unwrap();

    assert_eq!(deleted.version(), AggregateVersion::new(2));
    let pending = deleted.pending_events_with_actors();
    // First event (increment) has no actor
    assert_eq!(pending[0].actor_id, None);
    // Delete event has actor
    assert_eq!(pending[1].actor_id, Some(actor_id));
}

// Delete event with validation
struct ValidatedCloseEvent {
    timestamp: chrono::DateTime<Utc>,
}

impl crate::DeleteEvent<CounterEntity> for ValidatedCloseEvent {
    fn validate_delete(&self, entity: &CounterEntity) -> Result<(), TestError> {
        if entity.value > 0 {
            return Err(TestError);
        }
        Ok(())
    }
}

impl From<ValidatedCloseEvent> for CounterEvent {
    fn from(e: ValidatedCloseEvent) -> Self {
        CounterEvent::Reset(ResetEvent {
            timestamp: e.timestamp,
        })
    }
}

#[test]
fn test_aggregate_root_apply_delete_validation_fails() {
    let mut counter = AggregateRoot::<CounterEntity>::new(EntityId::new());
    counter.increment(10).unwrap();

    let result = counter.apply_delete(ValidatedCloseEvent {
        timestamp: Utc::now(),
    });
    assert!(result.is_err());
}

#[test]
fn test_aggregate_root_apply_delete_validation_succeeds() {
    let counter = AggregateRoot::<CounterEntity>::new(EntityId::new());

    let deleted = counter
        .apply_delete(ValidatedCloseEvent {
            timestamp: Utc::now(),
        })
        .unwrap();

    assert_eq!(deleted.version(), AggregateVersion::new(1));
}
