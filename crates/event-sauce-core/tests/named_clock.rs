//! `@occurred_at(field)`: the generated instant, named by the domain.
//!
//! By default `define_events!` adds a field called `timestamp` to every event it
//! generates. That is one instant with a name that says nothing about which clock it
//! came from, and a producer whose facts carry TWO — the time the world did
//! something, and the time this service found out — cannot use it: both would be
//! called `timestamp` somewhere, and a field that sometimes holds one and sometimes
//! the other answers neither question.
//!
//! `@occurred_at` names it instead. The variant says which of its instants is the one
//! [`DomainEvent::occurred_at`] answers with, that field is generated under that name,
//! and every other clock the fact carries is an ordinary declared field.
//!
//! The default is unchanged: a variant with no marker gets `timestamp`, which is what
//! every existing caller already has.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use chrono::{DateTime, TimeZone as _, Utc};
use event_sauce_core::{
    command_handler, define_events, Aggregate, AggregateError, ApplyEvent, DomainEvent, Entity,
    EntityId,
};
use serde::{Deserialize, Serialize};

fn at(seconds: i64) -> DateTime<Utc> {
    Utc.timestamp_opt(seconds, 0)
        .single()
        .unwrap_or(DateTime::<Utc>::UNIX_EPOCH)
}

#[derive(Debug, thiserror::Error)]
#[error("the reading is older than what the sensor has already recorded")]
struct StaleReading;

impl AggregateError for StaleReading {}

/// A sensor reading, which happened at one instant and was received at another.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct Sensor {
    id: EntityId,
    /// DATA time: when the world did the thing.
    measured_at: DateTime<Utc>,
    /// DETECTION time: when this service found out.
    received_at: DateTime<Utc>,
    celsius: i32,
}

impl Entity for Sensor {
    fn new(id: EntityId) -> Self {
        Self {
            id,
            measured_at: DateTime::<Utc>::UNIX_EPOCH,
            received_at: DateTime::<Utc>::UNIX_EPOCH,
            celsius: 0,
        }
    }

    fn entity_id(&self) -> EntityId {
        self.id
    }
}

impl event_sauce_core::DefaultEntity for Sensor {}

impl Aggregate for Sensor {
    type Event = SensorEvent;
    type Error = StaleReading;
    type DeletedState = Self;
}

define_events! {
    enum SensorEvent for Sensor {
        // Both clocks, both named. `measured_at` is the one the history orders on,
        // so it is the one the marker names; `received_at` is an ordinary field.
        Measured {
            received_at: DateTime<Utc>,
            celsius: i32,
        }
        @occurred_at(measured_at)
        @validate |sensor, event| {
            if event.measured_at < sensor.measured_at {
                return Err(StaleReading);
            }
            return Ok(());
        }
        => |sensor, event| {
            sensor.measured_at = event.measured_at;
            sensor.received_at = event.received_at;
            sensor.celsius = event.celsius;
        },

        // No marker, so this one still gets `timestamp` — the default is what every
        // existing caller already relies on.
        Recalibrated {
            offset: i32,
        }
        => |sensor, event| {
            sensor.celsius += event.offset;
        },
    }
}

#[test]
fn the_named_instant_is_the_field_the_domain_asked_for() {
    let event = MeasuredEvent {
        measured_at: at(1_000),
        received_at: at(1_030),
        celsius: 21,
    };

    // Both survive as distinct fields, under the names the domain chose. Neither is
    // called `timestamp`, which is the whole point.
    assert_eq!(event.measured_at, at(1_000));
    assert_eq!(event.received_at, at(1_030));
}

#[test]
fn occurred_at_answers_with_the_named_instant_and_not_the_other_clock() {
    let event = SensorEvent::from(MeasuredEvent {
        measured_at: at(1_000),
        received_at: at(1_030),
        celsius: 21,
    });

    // DATA time, not the moment we heard about it. A history orders on when the
    // world did something.
    assert_eq!(event.occurred_at(), at(1_000));
}

#[test]
fn a_variant_with_no_marker_keeps_the_default_name() {
    let event = RecalibratedEvent {
        offset: 2,
        timestamp: at(2_000),
    };

    assert_eq!(SensorEvent::from(event).occurred_at(), at(2_000));
}

#[test]
fn validate_reads_the_named_instant() {
    let mut sensor = Sensor::new(EntityId::new());
    MeasuredEvent {
        measured_at: at(1_000),
        received_at: at(1_030),
        celsius: 21,
    }
    .apply(&mut sensor);

    // A reading from before the one already recorded is a replay, and the guard
    // reads the named field to say so.
    let stale = MeasuredEvent {
        measured_at: at(900),
        received_at: at(1_100),
        celsius: 30,
    };

    assert!(stale.validate(&sensor).is_err());
    assert_eq!(sensor.celsius, 21, "a refused reading changes nothing");
}

// ---------------------------------------------------------------------------
// `@no_clock`: the command takes its instants instead of reading them
// ---------------------------------------------------------------------------

command_handler! {
    impl Sensor {
        // The default: every field of the event is a parameter, instants included,
        // and nothing in the generated body calls `Utc::now()`.
        fn measure(
            measured_at: DateTime<Utc>,
            received_at: DateTime<Utc>,
            celsius: i32,
        ) -> MeasuredEvent { measured_at, received_at, celsius };

        // `@clock` is the opt-in: this one is filled from the wall clock.
        @clock fn recalibrate(offset: i32) -> RecalibratedEvent { offset };
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct Operator {
    id: EntityId,
}

impl Entity for Operator {
    fn new(id: EntityId) -> Self {
        Self { id }
    }

    fn entity_id(&self) -> EntityId {
        self.id
    }
}

impl event_sauce_core::DefaultEntity for Operator {}

#[derive(Debug, thiserror::Error)]
#[error("operator error")]
struct OperatorError;

impl AggregateError for OperatorError {}

define_events! {
    enum OperatorEvent for Operator {
        Registered {} => |_, _| {},
    }
}

impl Aggregate for Operator {
    type Event = OperatorEvent;
    type Error = OperatorError;
    type DeletedState = Self;
}

/// Carries one `@occurred_at` variant per event kind (init, actor init,
/// actor, delete, actor delete).
#[derive(Debug, Serialize, Deserialize)]
struct Machine {
    id: EntityId,
    value: i32,
}

impl Entity for Machine {
    fn entity_id(&self) -> EntityId {
        self.id
    }
}

#[derive(Debug, thiserror::Error)]
#[error("machine error")]
struct MachineError;

impl AggregateError for MachineError {}

define_events! {
    enum MachineEvent for Machine {
        Installed {
            model: String,
        }
        @init
        @occurred_at(installed_at)
        => |id, _event| {
            Machine { id, value: 0 }
        },

        Commissioned {
            model: String,
        }
        @init
        @actor(Operator)
        @occurred_at(commissioned_at)
        => |id, _event| {
            Machine { id, value: 0 }
        },

        Calibrated {
            delta: i32,
        }
        @actor(Operator)
        @occurred_at(calibrated_at)
        => |machine, event| {
            machine.value += event.delta;
        },

        Decommissioned {
            reason: String,
        }
        @delete
        @occurred_at(decommissioned_at)
        => |machine, _event| { machine },

        Scrapped {
            reason: String,
        }
        @delete
        @actor(Operator)
        @occurred_at(scrapped_at)
        => |machine, _event| { machine },
    }
}

impl Aggregate for Machine {
    type Event = MachineEvent;
    type Error = MachineError;
    type DeletedState = Self;
}

#[test]
fn occurred_at_names_the_instant_on_an_init_event() {
    let event = InstalledEvent {
        model: "X1".to_string(),
        installed_at: at(10),
    };
    assert_eq!(MachineEvent::from(event).occurred_at(), at(10));
}

#[test]
fn occurred_at_names_the_instant_on_an_actor_init_event() {
    let event = CommissionedEvent {
        model: "X1".to_string(),
        commissioned_at: at(20),
    };
    assert_eq!(MachineEvent::from(event).occurred_at(), at(20));
}

#[test]
fn occurred_at_names_the_instant_on_an_actor_event() {
    let event = CalibratedEvent {
        delta: 3,
        calibrated_at: at(30),
    };
    assert_eq!(MachineEvent::from(event).occurred_at(), at(30));
}

#[test]
fn occurred_at_names_the_instant_on_a_delete_event() {
    let event = DecommissionedEvent {
        reason: "eol".to_string(),
        decommissioned_at: at(40),
    };
    assert_eq!(MachineEvent::from(event).occurred_at(), at(40));
}

#[test]
fn occurred_at_names_the_instant_on_an_actor_delete_event() {
    let event = ScrappedEvent {
        reason: "damaged".to_string(),
        scrapped_at: at(50),
    };
    assert_eq!(MachineEvent::from(event).occurred_at(), at(50));
}

#[test]
fn a_no_clock_command_builds_the_event_from_its_arguments() {
    let sensor = Sensor::new(EntityId::new());

    let event = sensor.measure_event(at(1_000), at(1_030), 21);

    // The instants are the ones the caller passed, to the second. A clock read would
    // put `now` in one of them, and this assertion is what says it did not.
    assert_eq!(event.measured_at, at(1_000));
    assert_eq!(event.received_at, at(1_030));
    assert_eq!(event.celsius, 21);
}

#[test]
fn the_default_command_still_stamps_itself() {
    let sensor = Sensor::new(EntityId::new());

    let before = Utc::now();
    let event = sensor.recalibrate_event(2);
    let after = Utc::now();

    assert!(event.timestamp >= before && event.timestamp <= after);
}
