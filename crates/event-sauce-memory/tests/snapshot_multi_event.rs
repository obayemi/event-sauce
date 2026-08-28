//! Regression test for snapshot strategy behavior on multi-event commits (M5).
//!
//! `EveryNEvents(n)` is documented as "snapshot every N events". A single commit
//! can append several events at once, advancing the aggregate version *past* a
//! multiple of `n` without ever landing exactly on it (e.g. version 1 -> 6 with
//! `EveryNEvents(5)`). The strategy must fire when a commit *crosses* a boundary,
//! not only when the post-commit version is an exact multiple. Since the default
//! configuration is `EveryNEvents(100)`, aggregates that grow via multi-event
//! commits would otherwise never snapshot -> unbounded replay growth.

use event_sauce_core::{
    command_handler, define_events, Aggregate, AggregateError, AggregateRoot, Entity, EntityId,
    EventStore, EveryNEvents, SnapshotConfig, StreamId,
};
use event_sauce_memory::InMemoryEventStore;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

#[derive(Debug, thiserror::Error)]
enum CounterError {}

impl AggregateError for CounterError {}

#[derive(Debug, Serialize, Deserialize)]
struct Counter {
    id: EntityId,
    name: String,
    ticks: u64,
}

impl Entity for Counter {
    fn entity_id(&self) -> EntityId {
        self.id
    }
}

impl Aggregate for Counter {
    type Event = CounterEvent;
    type Error = CounterError;
    type DeletedState = Self;
}

define_events! {
    enum CounterEvent for Counter {
        Created {
            name: String,
        }
        @init
        => |id, event| {
            Counter {
                id,
                name: event.name.clone(),
                ticks: 0,
            }
        },

        Ticked {
        } => |counter, _event| {
            counter.ticks += 1;
        },
    }
}

command_handler! {
    impl Counter {
        @clock @init fn create(name: String) -> CreatedEvent { name };
        @clock fn tick() -> TickedEvent { };
    }
}

fn store_with_every_n(n: u32) -> Arc<InMemoryEventStore> {
    Arc::new(
        InMemoryEventStore::builder()
            .snapshot_config(
                SnapshotConfig::builder()
                    .default_strategy(EveryNEvents(n))
                    .build(),
            )
            .build(),
    )
}

/// A single multi-event commit that *crosses* the `EveryNEvents` boundary
/// (version 1 -> 6 with `EveryNEvents(5)`) must produce a snapshot.
///
/// Today this fails: `should_snapshot` only fires when the post-commit version
/// is an exact multiple of N, so a commit that jumps over the boundary never
/// snapshots.
#[tokio::test]
async fn test_every_n_events_snapshots_when_multi_event_commit_crosses_boundary() {
    let store = store_with_every_n(5);

    // Init event -> version 1.
    let mut counter = Counter::create("crossing".to_string()).unwrap();
    // Apply 5 tick events without committing in between, so the single commit
    // takes version 1 -> 6, crossing the boundary at 5 without landing on it.
    for _ in 0..5 {
        counter.tick().unwrap();
    }
    assert_eq!(counter.version().as_i64(), 6);

    store.commit(&mut counter).await.unwrap();

    // A snapshot must exist for this aggregate because the commit crossed the
    // `EveryNEvents(5)` boundary.
    let stream_id = StreamId::new(
        AggregateRoot::<Counter>::aggregate_type(),
        counter.entity_id().as_uuid(),
    );
    let snapshot = store.load_snapshot(stream_id).await.unwrap();

    assert!(
        snapshot.is_some(),
        "EveryNEvents(5) must snapshot when a multi-event commit crosses the boundary \
         (version 1 -> 6), but no snapshot was written"
    );
}

/// Control: an exact-boundary single-event commit (version lands exactly on a
/// multiple of N) already snapshots today. Guards against a false RED in the
/// crossing test by proving the `load_snapshot` detection machinery is sound.
#[tokio::test]
async fn test_every_n_events_snapshots_on_exact_boundary_landing() {
    let store = store_with_every_n(5);

    // Init (v1) committed separately, then commit ticks one at a time so the
    // aggregate lands exactly on version 5.
    let mut counter = Counter::create("exact".to_string()).unwrap();
    store.commit(&mut counter).await.unwrap();
    for _ in 0..4 {
        counter.tick().unwrap();
        store.commit(&mut counter).await.unwrap();
    }
    assert_eq!(counter.version().as_i64(), 5);

    let stream_id = StreamId::new(
        AggregateRoot::<Counter>::aggregate_type(),
        counter.entity_id().as_uuid(),
    );
    let snapshot = store.load_snapshot(stream_id).await.unwrap();

    assert!(
        snapshot.is_some(),
        "EveryNEvents(5) should snapshot when the version lands exactly on 5"
    );
}
