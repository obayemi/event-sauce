// This test verifies that the aggregate attribute macro fails
// when required parameters are missing

use event_sauce_core::{AggregateId, DomainEvent};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
struct TestId(uuid::Uuid);

impl std::fmt::Display for TestId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl AggregateId for TestId {
    fn to_uuid(&self) -> uuid::Uuid {
        self.0
    }
    fn from_uuid(uuid: uuid::Uuid) -> Self {
        Self(uuid)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
enum TestEvent {
    Created,
}

impl DomainEvent for TestEvent {
    type Aggregate = TestAggregate;
    fn event_type(&self) -> &'static str {
        "Created"
    }
    fn event_version(&self) -> u64 {
        1
    }
    fn occurred_at(&self) -> chrono::DateTime<chrono::Utc> {
        chrono::Utc::now()
    }
}

// Missing required parameters - should fail
#[event_sauce_macros::aggregate(id = "TestId")]
struct TestAggregate {
    value: i32,
}

fn main() {}
