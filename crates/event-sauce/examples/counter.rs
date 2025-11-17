//! Simple Counter Example
//!
//! This example demonstrates the basics of event sourcing with event-sauce:
//! - Defining aggregates and events
//! - Using derive macros
//! - In-memory event store
//! - Command execution
//! - Event replay
//!
//! Run with: cargo run -p event-sauce --example counter --features "memory,macros"

use chrono::Utc;
use event_sauce_core::{Aggregate, AggregateError, AggregateId, ApplyEvent, Version};
use event_sauce_macros::{Aggregate as DeriveAggregate, Event as DeriveEvent};
use serde::{Deserialize, Serialize};
use std::fmt;
use uuid::Uuid;

// ============================================================================
// Domain Model
// ============================================================================

/// Unique identifier for a counter
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
struct CounterId(Uuid);

impl CounterId {
    fn new() -> Self {
        Self(Uuid::new_v4())
    }
}

impl fmt::Display for CounterId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Counter-{}", self.0)
    }
}

impl AggregateId for CounterId {}

// ============================================================================
// Event Structs - Separated event definitions
// ============================================================================

/// Event: Counter was incremented
#[derive(Debug, Clone, Serialize, Deserialize)]
struct CounterIncrementedEvent {
    amount: i32,
    timestamp: chrono::DateTime<Utc>,
}

/// Event: Counter was decremented
#[derive(Debug, Clone, Serialize, Deserialize)]
struct CounterDecrementedEvent {
    amount: i32,
    timestamp: chrono::DateTime<Utc>,
}

/// Event: Counter was reset
#[derive(Debug, Clone, Serialize, Deserialize)]
struct CounterResetEvent {
    timestamp: chrono::DateTime<Utc>,
}

/// Domain events enum wrapping the separated event structs
#[derive(DeriveEvent, Debug, Clone, Serialize, Deserialize)]
#[event(version = 1, type_prefix = "Counter")]
enum CounterEvent {
    Incremented(CounterIncrementedEvent),
    Decremented(CounterDecrementedEvent),
    Reset(CounterResetEvent),
}

/// Domain errors
#[derive(Debug, thiserror::Error)]
enum CounterError {
    #[error("Invalid amount: {0} (must be positive)")]
    InvalidAmount(i32),

    #[error("Would result in negative value: current={current}, requested={requested}")]
    WouldBeNegative { current: i32, requested: i32 },
}

impl AggregateError for CounterError {}

// ============================================================================
// ApplyEvent Implementations
// ============================================================================

impl ApplyEvent<Counter, CounterError> for CounterIncrementedEvent {
    fn validate(&self, _counter: &Counter) -> Result<(), CounterError> {
        if self.amount <= 0 {
            return Err(CounterError::InvalidAmount(self.amount));
        }
        Ok(())
    }

    fn apply(&self, counter: &mut Counter) {
        counter.value += self.amount;
    }
}

impl ApplyEvent<Counter, CounterError> for CounterDecrementedEvent {
    fn validate(&self, counter: &Counter) -> Result<(), CounterError> {
        if self.amount <= 0 {
            return Err(CounterError::InvalidAmount(self.amount));
        }

        if counter.value - self.amount < 0 {
            return Err(CounterError::WouldBeNegative {
                current: counter.value,
                requested: self.amount,
            });
        }
        Ok(())
    }

    fn apply(&self, counter: &mut Counter) {
        counter.value -= self.amount;
    }
}

impl ApplyEvent<Counter, CounterError> for CounterResetEvent {
    fn apply(&self, counter: &mut Counter) {
        counter.value = 0;
    }
}

/// Counter aggregate
#[derive(DeriveAggregate, Debug, Clone)]
#[aggregate(id = "CounterId", event = "CounterEvent", error = "CounterError")]
struct Counter {
    #[aggregate_id]
    id: CounterId,

    value: i32,

    #[aggregate_version]
    version: Version,

    #[aggregate_events]
    pending_events: Vec<CounterEvent>,
}

impl Counter {
    /// Create a new counter
    fn new(id: CounterId) -> Self {
        Self {
            id,
            value: 0,
            version: Version::initial(),
            pending_events: Vec::new(),
        }
    }

    /// Increment the counter
    fn increment(&mut self, amount: i32) -> Result<(), CounterError> {
        let event = CounterIncrementedEvent {
            amount,
            timestamp: Utc::now(),
        };

        // Validate using the event's validation logic
        event.validate(self)?;

        self.apply(event);
        Ok(())
    }

    /// Decrement the counter
    fn decrement(&mut self, amount: i32) -> Result<(), CounterError> {
        let event = CounterDecrementedEvent {
            amount,
            timestamp: Utc::now(),
        };

        // Validate using the event's validation logic
        event.validate(self)?;

        self.apply(event);
        Ok(())
    }

    /// Reset the counter to zero
    fn reset(&mut self) {
        self.apply(CounterResetEvent {
            timestamp: Utc::now(),
        });
    }

    /// Get the current value
    fn value(&self) -> i32 {
        self.value
    }

    /// Apply an event to update state (required by Aggregate trait)
    fn apply_event(&mut self, event: &CounterEvent) {
        match event {
            CounterEvent::Incremented(e) => e.apply(self),
            CounterEvent::Decremented(e) => e.apply(self),
            CounterEvent::Reset(e) => e.apply(self),
        }
    }
}

// ============================================================================
// Main Example
// ============================================================================

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Event Sourcing Counter Example ===\n");

    // Create a new counter
    let id = CounterId::new();
    let mut counter = Counter::new(id);
    println!("✓ Created counter: {}", id);
    println!("  Initial value: {}", counter.value());

    // Execute some commands
    println!("\n--- Executing Commands ---");

    counter.increment(5)?;
    println!("✓ Incremented by 5 → value: {}", counter.value());

    counter.increment(3)?;
    println!("✓ Incremented by 3 → value: {}", counter.value());

    counter.decrement(2)?;
    println!("✓ Decremented by 2 → value: {}", counter.value());

    println!("\nCurrent value: {}", counter.value());
    println!("Version: {}", counter.version());
    println!("Pending events: {}", counter.pending_events().len());

    // Show the events
    println!("\n--- Generated Events ---");
    for (i, event) in counter.pending_events().iter().enumerate() {
        println!("{}. {:?}", i + 1, event);
    }

    // Demonstrate error handling
    println!("\n--- Testing Error Handling ---");

    match counter.increment(0) {
        Ok(_) => println!("❌ Should have failed"),
        Err(e) => println!("✓ Rejected invalid increment: {}", e),
    }

    match counter.decrement(100) {
        Ok(_) => println!("❌ Should have failed"),
        Err(e) => println!("✓ Rejected invalid decrement: {}", e),
    }

    // Demonstrate event replay
    println!("\n--- Event Replay ---");

    let events: Vec<_> = counter.pending_events().iter().cloned().collect();
    let mut replayed = Counter::new(id);

    println!("Replaying {} events...", events.len());
    for event in events {
        replayed.apply_unchecked(&event);
    }

    println!("✓ Replayed counter value: {}", replayed.value());
    println!("✓ Replayed counter version: {}", replayed.version());

    assert_eq!(counter.value(), replayed.value());
    assert_eq!(counter.version(), replayed.version());
    println!("✓ State matches original!");

    // Reset demonstration
    println!("\n--- Reset ---");
    counter.reset();
    println!("✓ Reset counter → value: {}", counter.value());

    // Final summary
    println!("\n--- Summary ---");
    println!("Total events generated: {}", counter.pending_events().len());
    println!("Final value: {}", counter.value());
    println!("Final version: {}", counter.version());

    Ok(())
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_counter_is_zero() {
        let counter = Counter::new(CounterId::new());

        assert_eq!(counter.value(), 0);
        assert_eq!(counter.version(), Version::initial());
        assert_eq!(counter.pending_events().len(), 0);
    }

    #[test]
    fn test_increment() {
        let mut counter = Counter::new(CounterId::new());

        counter.increment(5).unwrap();

        assert_eq!(counter.value(), 5);
        assert_eq!(counter.version(), Version::from(1));
        assert_eq!(counter.pending_events().len(), 1);
    }

    #[test]
    fn test_decrement() {
        let mut counter = Counter::new(CounterId::new());

        counter.increment(10).unwrap();
        counter.decrement(3).unwrap();

        assert_eq!(counter.value(), 7);
        assert_eq!(counter.version(), Version::from(2));
    }

    #[test]
    fn test_reset() {
        let mut counter = Counter::new(CounterId::new());

        counter.increment(10).unwrap();
        counter.reset();

        assert_eq!(counter.value(), 0);
        assert_eq!(counter.version(), Version::from(2));
    }

    #[test]
    fn test_cannot_increment_zero() {
        let mut counter = Counter::new(CounterId::new());

        let result = counter.increment(0);

        assert!(result.is_err());
        assert_eq!(counter.value(), 0); // State unchanged
    }

    #[test]
    fn test_cannot_decrement_below_zero() {
        let mut counter = Counter::new(CounterId::new());

        counter.increment(5).unwrap();
        let result = counter.decrement(10);

        assert!(result.is_err());
        assert_eq!(counter.value(), 5); // State unchanged
    }

    #[test]
    fn test_event_replay() {
        let mut counter = Counter::new(CounterId::new());

        counter.increment(10).unwrap();
        counter.decrement(3).unwrap();
        counter.increment(5).unwrap();

        // Replay events
        let events = counter.pending_events().to_vec();
        let mut replayed = Counter::new(*counter.aggregate_id());

        for event in events {
            replayed.apply_unchecked(&event);
        }

        assert_eq!(replayed.value(), counter.value());
        assert_eq!(replayed.version(), counter.version());
    }

    #[test]
    fn test_multiple_operations() {
        let mut counter = Counter::new(CounterId::new());

        counter.increment(5).unwrap();
        counter.increment(3).unwrap();
        counter.decrement(2).unwrap();
        counter.increment(10).unwrap();
        counter.decrement(1).unwrap();
        counter.reset();
        counter.increment(7).unwrap();

        assert_eq!(counter.value(), 7);
        assert_eq!(counter.pending_events().len(), 7);
    }
}
