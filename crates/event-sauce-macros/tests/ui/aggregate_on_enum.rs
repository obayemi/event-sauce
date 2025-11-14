// This test verifies that the Aggregate derive macro fails
// when used on an enum instead of a struct

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

// Using Aggregate on enum - should fail
#[derive(event_sauce_macros::Aggregate)]
#[aggregate(id = "TestId", event = "TestEvent")]
enum TestAggregate {
    Variant {
        #[aggregate_id]
        id: TestId,
        #[aggregate_version]
        version: Version,
        #[aggregate_events]
        events: Vec<TestEvent>,
    }
}

fn main() {}
