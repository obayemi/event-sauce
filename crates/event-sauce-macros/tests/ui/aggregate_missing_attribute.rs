// This test verifies that the aggregate attribute macro fails
// when required parameters are missing

use event_sauce_core::EntityId;

// Missing required event parameter - should fail
#[event_sauce_macros::aggregate()]
struct TestAggregate {
    id: EntityId,
    value: i32,
}

fn main() {}
