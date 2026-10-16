//! `command_handler!` still expands to `aggregate_command!`, deprecated.

#![allow(clippy::unwrap_used)]
#![expect(
    deprecated,
    reason = "pins that the old name still expands, deprecated"
)]
#![deny(unfulfilled_lint_expectations)]

use chrono::{DateTime, Utc};
use event_sauce_core::{
    command_handler, define_events, Aggregate, AggregateError, AggregateRoot, DefaultEntity,
    Entity, EntityId,
};
use serde::{Deserialize, Serialize};

#[derive(Debug, thiserror::Error)]
#[error("tally error")]
struct TallyError;

impl AggregateError for TallyError {}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Tally {
    id: EntityId,
    count: u32,
}

impl Entity for Tally {
    fn new(id: EntityId) -> Self {
        Self { id, count: 0 }
    }

    fn entity_id(&self) -> EntityId {
        self.id
    }
}

impl DefaultEntity for Tally {}

impl Aggregate for Tally {
    type Event = TallyEvent;
    type Error = TallyError;
    type DeletedState = Self;
}

define_events! {
    enum TallyEvent for Tally {
        Counted {
            by: u32,
        }
        => |tally, event| {
            tally.count += event.by;
        },
    }
}

command_handler! {
    impl Tally {
        fn count(by: u32, timestamp: DateTime<Utc>) -> CountedEvent { by, timestamp };
    }
}

#[test]
fn the_deprecated_name_generates_the_same_aggregate_commands() {
    let mut tally = AggregateRoot::<Tally>::new(EntityId::new());

    tally.count(2, Utc::now()).unwrap();

    assert_eq!(tally.count, 2);
}
