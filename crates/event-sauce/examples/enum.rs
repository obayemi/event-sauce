use chrono::Utc;
use event_sauce_core::{AggregateError, AggregateId, ApplyEvent, Version};
use event_sauce_macros::{Aggregate as DeriveAggregate, Event as DeriveEvent};
use serde::{Deserialize, Serialize};
use std::fmt;
use uuid::Uuid;

/// Unique identifier for a counter
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
struct StateMachineId(Uuid);

impl StateMachineId {
    fn new() -> Self {
        Self(Uuid::new_v4())
    }
}

impl fmt::Display for StateMachineId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "StateMachine-{}", self.0)
    }
}

impl AggregateId for StateMachineId {}

/// Domain errors
#[derive(Debug, thiserror::Error)]
enum StateMachineError {}

#[derive(DeriveEvent, Debug, Clone, Serialize, Deserialize)]
#[event(version = 1, type_prefix = "StateMachine")]
enum StateMachineEvent {
    Created(StateMachineCreated),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct StateMachineCreated {
    timestamp: chrono::DateTime<Utc>,
}

impl ApplyEvent<StateMachine, StateMachineError> for StateMachineCreated {
    fn validate(&self, _machine: &StateMachine) -> Result<(), StateMachineError> {
        Ok(())
    }

    fn apply(&self, _machine: &mut StateMachine) {}
}

#[derive(Debug, Clone)]
enum MachineState {
    StateA,
    StateB
}

impl AggregateError for StateMachineError {}

#[derive(DeriveAggregate, Debug, Clone)]
#[aggregate(
    id = "StateMachineId",
    event = "StateMachineEvent",
    error = "StateMachineError"
)]
struct StateMachine {
    #[aggregate_id]
    id: StateMachineId,

    state: MachineState,

    #[aggregate_version]
    version: Version,

    #[aggregate_events]
    pending_events: Vec<StateMachineEvent>,
}

impl StateMachine {
    fn new(id: StateMachineId) -> Self {
        Self {
            id,
            state: MachineState::StateA,
            version: Version::initial(),
            pending_events: Vec::new(),
        }
    }
    fn apply_event(&mut self, _event: &StateMachineEvent) {}
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("hello world");

    let id = StateMachineId::new();
    let _machine = StateMachine::new(id);

    Ok(())
}
