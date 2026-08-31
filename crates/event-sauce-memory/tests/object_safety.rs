//! `Repository<A>` behind a trait object.
//!
//! A service that wires its repositories at a composition root holds them as
//! `Arc<dyn Repository<A>>` — one field, one type, whatever backend was chosen. A
//! trait with a generic id parameter cannot be one, so the methods the trait
//! REQUIRES take an [`EntityId`] —
//! [`RepositoryExt`], which is blanket-implemented for `?Sized` and therefore
//! reaches through the object too.
//!
//! This file is the proof. If `Repository` stops being object-safe it does not
//! compile.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use std::sync::Arc;

use event_sauce_core::{
    define_events, Aggregate, AggregateError, DefaultEntity, Entity, EntityId, EventStore,
    Repository,
};
use event_sauce_memory::InMemoryEventStore;
use serde::{Deserialize, Serialize};

#[derive(Debug, thiserror::Error)]
#[error("the counter refused")]
struct CounterError;

impl AggregateError for CounterError {}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Counter {
    id: EntityId,
    count: i32,
}

impl Entity for Counter {
    fn new(id: EntityId) -> Self {
        Self { id, count: 0 }
    }

    fn entity_id(&self) -> EntityId {
        self.id
    }
}

impl DefaultEntity for Counter {}

define_events! {
    enum CounterEvent for Counter {
        Bumped { by: i32 }
        @occurred_at(at)
        => |counter, event| {
            counter.count += event.by;
        },
    }
}

impl Aggregate for Counter {
    type Event = CounterEvent;
    type Error = CounterError;
    type DeletedState = Self;
}

/// A typed id, so the extension trait has something to be exercised with.
#[derive(Debug, Clone, Copy)]
struct CounterId(EntityId);

impl event_sauce_core::EntityIdFor<Counter> for CounterId {
    fn entity_id(&self) -> EntityId {
        self.0
    }
}

#[tokio::test]
async fn a_repository_can_be_held_as_a_trait_object() {
    let store = Arc::new(InMemoryEventStore::builder().build());

    // The line this file exists for. It does not compile unless `Repository<A>`
    // is object-safe.
    let repo: Arc<dyn Repository<Counter>> = Arc::new(store.repository::<Counter>());

    let mut counter = repo.create();
    counter
        .apply(BumpedEvent {
            by: 3,
            at: chrono::Utc::now(),
        })
        .unwrap();
    repo.save(&mut counter).await.unwrap();

    // Through the object, by raw id — the required half of the trait.
    let loaded = repo.load_by_id(counter.entity_id()).await.unwrap();
    assert_eq!(loaded.count, 3);

    // The typed-id spelling is still there on a CONCRETE repository, which is what
    // nearly every call site holds.
    let concrete = store.repository::<Counter>();
    let typed = CounterId(counter.entity_id());
    assert_eq!(concrete.load(typed).await.unwrap().count, 3);
    assert!(concrete.exists(typed).await.unwrap());
}
