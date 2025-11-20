// This test verifies that the aggregate attribute macro fails
// when event parameter is missing

use event_sauce_core::AggregateId;
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

#[derive(Debug, thiserror::Error)]
enum TestError {
    #[error("Test error")]
    Test,
}

impl event_sauce_core::AggregateError for TestError {}

// Missing event parameter - should fail
#[event_sauce_macros::aggregate(id = "TestId", error = "TestError")]
struct TestAggregate {
    value: i32,
}

fn main() {}
