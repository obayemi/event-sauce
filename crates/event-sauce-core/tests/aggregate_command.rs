//! `aggregate_command!` names what it generates: aggregate commands.
//!
//! The macro used to be called `command_handler!`, a name that reads like the
//! service command handler loading, deciding and saving around an aggregate.
//! The old name still expands to the same code, deprecated, so existing
//! callers keep compiling while the compiler points them at the new one.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use chrono::{DateTime, Utc};
use event_sauce_core::{
    aggregate_command, define_events, Aggregate, AggregateError, AggregateRoot, DefaultEntity,
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

aggregate_command! {
    impl Tally {
        fn count(by: u32, timestamp: DateTime<Utc>) -> CountedEvent { by, timestamp };
    }
}

#[expect(
    deprecated,
    reason = "pins that the old name still expands, deprecated"
)]
mod legacy {
    use super::{CountedEvent, Tally};
    use chrono::{DateTime, Utc};
    use event_sauce_core::command_handler;

    command_handler! {
        impl Tally {
            fn count_legacy(by: u32, timestamp: DateTime<Utc>) -> CountedEvent { by, timestamp };
        }
    }
}

#[test]
fn an_aggregate_command_applies_its_event() {
    let mut tally = AggregateRoot::<Tally>::new(EntityId::new());

    TallyCommands::count(&mut tally, 3, Utc::now()).unwrap();

    assert_eq!(tally.count, 3);
}

#[test]
fn the_deprecated_name_generates_the_same_aggregate_commands() {
    let mut tally = AggregateRoot::<Tally>::new(EntityId::new());

    legacy::TallyCommands::count_legacy(&mut tally, 2, Utc::now()).unwrap();

    assert_eq!(tally.count, 2);
}
