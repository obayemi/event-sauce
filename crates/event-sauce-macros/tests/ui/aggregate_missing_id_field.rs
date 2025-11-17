// This test verifies that the Aggregate derive macro fails
// when the #[aggregate_id] field attribute is missing

use event_sauce_core::{Aggregate, AggregateId, DomainEvent, Version};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct TestId(String);

impl std::fmt::Display for TestId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl AggregateId for TestId {}

#[derive(Debug, Clone)]
enum TestEvent {
    Created,
}

impl DomainEvent for TestEvent {
    fn event_type(&self) -> &'static str {
        "Created"
    }
    fn event_version(&self) -> i32 {
        1
    }
    fn occurred_at(&self) -> chrono::DateTime<chrono::Utc> {
        chrono::Utc::now()
    }
}

// Define a dummy error type
#[derive(Debug)]
struct TestError;

impl std::error::Error for TestError {}
impl std::fmt::Display for TestError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Test error")
    }
}

impl event_sauce_core::AggregateError for TestError {}

// Missing #[aggregate_id] field attribute - should fail
#[derive(event_sauce_macros::Aggregate)]
#[aggregate(id = "TestId", event = "TestEvent", error = "TestError")]
struct TestAggregate {
    id: TestId,  // Missing #[aggregate_id] attribute
    #[aggregate_version]
    version: Version,
    #[aggregate_events]
    events: Vec<TestEvent>,
}

fn main() {}
