// This test verifies that the aggregate attribute macro fails
// when used on an enum instead of a struct

#[derive(Debug, thiserror::Error)]
enum TestError {
    #[error("Test error")]
    Test,
}

impl event_sauce_core::AggregateError for TestError {}

// Using aggregate on enum - should fail
#[event_sauce_macros::aggregate(event = "TestEvent", error = "TestError")]
enum TestAggregate {
    Variant {
        value: i32,
    }
}

fn main() {}
