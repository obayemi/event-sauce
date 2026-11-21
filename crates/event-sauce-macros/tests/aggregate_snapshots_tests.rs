//! Tests for `#[aggregate(snapshots(...))]`, the per-aggregate snapshot strategy.

use chrono::{DateTime, Utc};
use event_sauce_core::{
    Aggregate, AggregateError, AggregateVersion, DomainEvent, Entity, EntityId, EventApplicator,
    EventVersion, SnapshotStrategy,
};
use serde::{Deserialize, Serialize};

#[derive(Debug, thiserror::Error)]
#[error("snapshot fixture error")]
struct FixtureError;

impl AggregateError for FixtureError {}

macro_rules! fixture {
    ($name:ident, $event:ident, $event_lit:literal, $($attr:tt)*) => {
        #[derive(Debug, Clone, Serialize, Deserialize)]
        struct $event;

        impl DomainEvent for $event {
            type Aggregate = $name;
            fn event_type(&self) -> &'static str {
                stringify!($event)
            }
            fn event_version(&self) -> EventVersion {
                EventVersion::new(1)
            }
            fn occurred_at(&self) -> DateTime<Utc> {
                Utc::now()
            }
        }

        impl EventApplicator<$name> for $event {
            fn dispatch(&self, _entity: &mut $name) -> Result<(), FixtureError> {
                Ok(())
            }
            fn dispatch_unchecked(&self, _entity: &mut $name) {}
        }

        #[event_sauce_macros::aggregate(
            event = $event_lit,
            error = "FixtureError",
            $($attr)*
        )]
        #[derive(Debug, Clone, Serialize, Deserialize)]
        struct $name {
            #[id]
            id: EntityId,
        }
    };
}

fn at(version: i64) -> AggregateVersion {
    AggregateVersion::new(version)
}

fixture!(
    EveryThree,
    EveryThreeEvent,
    "EveryThreeEvent",
    snapshots(every = 3)
);
fixture!(
    AlwaysAgg,
    AlwaysAggEvent,
    "AlwaysAggEvent",
    snapshots(always)
);
fixture!(NeverAgg, NeverAggEvent, "NeverAggEvent", snapshots(never));
fixture!(
    CustomAgg,
    CustomAggEvent,
    "CustomAggEvent",
    snapshots(with = "self::EVERY_FIVE")
);
fixture!(PlainAgg, PlainAggEvent, "PlainAggEvent",);

static EVERY_FIVE: EveryFive = EveryFive;

struct EveryFive;

impl SnapshotStrategy for EveryFive {
    fn should_snapshot(&self, version: AggregateVersion) -> bool {
        version.as_i64() % 5 == 0
    }
}

#[test]
fn test_every_declares_an_interval_strategy() {
    let strategy = EveryThree::snapshot_strategy().unwrap();
    assert!(!strategy.should_snapshot(at(2)));
    assert!(strategy.should_snapshot(at(3)));
    assert!(strategy.should_snapshot(at(6)));
}

#[test]
fn test_always_declares_always_snapshot() {
    let strategy = AlwaysAgg::snapshot_strategy().unwrap();
    assert!(strategy.should_snapshot(at(1)));
}

#[test]
fn test_never_declares_never_snapshot() {
    let strategy = NeverAgg::snapshot_strategy().unwrap();
    assert!(!strategy.should_snapshot(at(100)));
}

#[test]
fn test_with_declares_the_user_defined_static() {
    let strategy = CustomAgg::snapshot_strategy().unwrap();
    assert!(!strategy.should_snapshot(at(4)));
    assert!(strategy.should_snapshot(at(5)));
}

#[test]
fn test_without_snapshots_declares_nothing() {
    assert!(PlainAgg::snapshot_strategy().is_none());
}

#[test]
fn test_entities_are_still_constructible() {
    let id = EntityId::new();
    assert_eq!(EveryThree::new(id).entity_id(), id);
}
