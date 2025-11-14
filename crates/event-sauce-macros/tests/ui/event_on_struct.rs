// This test verifies that the Event derive macro fails
// when used on a struct instead of an enum

use event_sauce_core::DomainEvent;
use chrono::{DateTime, Utc};

// Using Event on struct - should fail
#[derive(event_sauce_macros::Event, Debug, Clone)]
#[event(version = 1)]
struct TestEvent {
    timestamp: DateTime<Utc>,
}

fn main() {}
