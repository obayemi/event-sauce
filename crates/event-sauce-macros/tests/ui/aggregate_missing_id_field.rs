// This test verifies that the aggregate attribute macro fails
// when no ID field is found

use event_sauce_core::EntityId;

#[derive(Debug, thiserror::Error)]
enum TestError {
    #[error("Test error")]
    Test,
}

impl event_sauce_core::AggregateError for TestError {}

// Missing id field - should fail
#[event_sauce_macros::aggregate(event = "TestEvent", error = "TestError")]
struct TestAggregate {
    value: i32,
}

fn main() {}
