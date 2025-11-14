// This test verifies that the Event derive macro fails
// when the #[event(...)] attribute is missing

use event_sauce_core::DomainEvent;
use chrono::{DateTime, Utc};

// Missing #[event(...)] attribute - should fail
#[derive(event_sauce_macros::Event, Debug, Clone)]
enum TestEvent {
    Created {
        timestamp: DateTime<Utc>,
    },
}

fn main() {}
