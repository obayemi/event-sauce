//! Simple Counter Example
//!
//! This example demonstrates the basics of event sourcing with event-sauce:
//! - Defining aggregates with aggregate (cleaner separation)
//! - Using derive macros
//! - In-memory event store for persistence
//! - Command execution
//! - Saving events to storage
//! - Loading events from storage
//! - Event replay and aggregate reconstruction
//!
//! Run with: cargo run -p event-sauce --example counter --features "memory,macros"

use chrono::Utc;
use event_sauce_core::{load, Aggregate, ApplyEvent, DomainEvent, EventStore};
use event_sauce_macros::{aggregate, AggregateError, AggregateId, Event as DeriveEvent};
use event_sauce_memory::InMemoryEventStore;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

// ============================================================================
// Domain Model
// ============================================================================

/// Unique identifier for a counter
#[derive(AggregateId, Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
struct CounterId(Uuid);

impl CounterId {
    fn new() -> Self {
        Self(Uuid::new_v4())
    }
}

impl Default for CounterId {
    fn default() -> Self {
        Self(Uuid::nil())
    }
}

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
#[event(version = 1, type_prefix = "Counter", aggregate = "Counter")]
enum CounterEvent {
    Incremented(CounterIncrementedEvent),
    Decremented(CounterDecrementedEvent),
    Reset(CounterResetEvent),
}

/// Domain errors
#[derive(AggregateError, Debug, thiserror::Error)]
enum CounterError {
    #[error("Invalid amount: {0} (must be positive)")]
    InvalidAmount(i32),

    #[error("Would result in negative value: current={current}, requested={requested}")]
    WouldBeNegative { current: i32, requested: i32 },
}

// ============================================================================
// ApplyEvent Implementations
// ============================================================================
//
// NOTE: ApplyEvent is implemented for the Counter aggregate type.
// The Event macro auto-generates the apply_event method that dispatches to these.

impl ApplyEvent<Counter> for CounterIncrementedEvent {
    fn validate(&self, _counter: &Counter) -> Result<(), <Counter as Aggregate>::Error> {
        if self.amount <= 0 {
            return Err(CounterError::InvalidAmount(self.amount));
        }
        Ok(())
    }

    fn apply(&self, counter: &mut Counter) {
        counter.value += self.amount;
    }
}

impl ApplyEvent<Counter> for CounterDecrementedEvent {
    fn validate(&self, counter: &Counter) -> Result<(), <Counter as Aggregate>::Error> {
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

    // Post-validation: Ensure counter never goes negative (defense in depth)
    // This catches any bugs in the apply logic or validation
    fn post_validate(&self, counter: &Counter) -> Result<(), <Counter as Aggregate>::Error> {
        if counter.value < 0 {
            return Err(CounterError::WouldBeNegative {
                current: counter.value,
                requested: self.amount,
            });
        }
        Ok(())
    }
}

impl ApplyEvent<Counter> for CounterResetEvent {
    fn apply(&self, counter: &mut Counter) {
        counter.value = 0;
    }
}

// ============================================================================
// Counter - Business Logic Only
// ============================================================================
//
// The aggregate macro generates a CounterState inner struct and transforms
// Counter into a wrapper that includes:
// - state: CounterState
// - version: Version
// - pending_events: Vec<CounterEvent>
//
// This separates infrastructure concerns from business state.

/// Counter aggregate - contains only business data
#[aggregate(id = "CounterId", event = "CounterEvent", error = "CounterError")]
#[derive(Default)]
struct Counter {
    value: i32,
}

impl Counter {
    /// Get the current value
    fn value(&self) -> i32 {
        self.value
    }
}

// apply_event is auto-generated by the Event macro

// ============================================================================
// Command Methods - Implemented on Counter
// ============================================================================

impl Counter {
    /// Create a new counter using the Aggregate trait's new method
    fn create(id: CounterId) -> Self {
        <Self as Aggregate>::new(id)
    }

    /// Increment the counter
    fn increment(&mut self, amount: i32) -> Result<(), CounterError> {
        let event = CounterIncrementedEvent {
            amount,
            timestamp: Utc::now(),
        };

        // apply() automatically runs: validate() → apply() → post_validate()
        self.apply(event)?;
        Ok(())
    }

    /// Decrement the counter
    fn decrement(&mut self, amount: i32) -> Result<(), CounterError> {
        let event = CounterDecrementedEvent {
            amount,
            timestamp: Utc::now(),
        };

        // apply() automatically runs: validate() → apply() → post_validate()
        self.apply(event)?;
        Ok(())
    }

    /// Reset the counter to zero
    fn reset(&mut self) {
        // apply() automatically runs: validate() → apply() → post_validate()
        self.apply(CounterResetEvent {
            timestamp: Utc::now(),
        })
        .expect("Reset should never fail");
    }
}

// ============================================================================
// Main Example
// ============================================================================

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Event Sourcing Counter Example with Storage ===\n");

    // Initialize the event store
    let store = InMemoryEventStore::new();
    println!("✓ Created in-memory event store\n");

    // Create a new counter
    let id = CounterId::new();
    let mut counter = Counter::create(id);
    println!("--- Step 1: Create Counter ---");
    println!("✓ Created counter: {id}");
    println!("  Initial value: {}", counter.value());
    println!("  Initial version: {}", counter.version());

    // Execute some commands
    println!("\n--- Step 2: Execute Commands ---");

    counter.increment(5)?;
    println!("✓ Incremented by 5 → value: {}", counter.value());

    counter.increment(3)?;
    println!("✓ Incremented by 3 → value: {}", counter.value());

    counter.decrement(2)?;
    println!("✓ Decremented by 2 → value: {}", counter.value());

    println!("\n  Current value: {}", counter.value());
    println!("  Current version: {}", counter.version());
    println!("  Pending events: {}", counter.pending_events().len());

    // Show the generated events
    println!("\n--- Step 3: Generated Events ---");
    for (i, event) in counter.pending_events().iter().enumerate() {
        println!("  {}. {:?}", i + 1, event.event_type());
    }

    // Save events to the store using the new commit method
    println!("\n--- Step 4: Save to Event Store ---");
    let num_events = counter.pending_events().len();

    store.commit(&mut counter).await?;
    println!("✓ Saved {num_events} events to store");
    println!(
        "✓ Pending events cleared: {}",
        counter.pending_events().len()
    );

    // Load events from store and rebuild aggregate using the new load function
    println!("\n--- Step 5: Load from Event Store ---");
    println!("Loading counter from event store...");

    let loaded_counter: Counter = load(&store, id).await?;

    println!("✓ Loaded counter from store");
    println!("  Loaded value: {}", loaded_counter.value());
    println!("  Loaded version: {}", loaded_counter.version());

    // Verify state matches
    assert_eq!(counter.value(), loaded_counter.value());
    assert_eq!(counter.version(), loaded_counter.version());
    println!("✓ State matches original!");

    // Continue with more operations on the loaded counter
    println!("\n--- Step 6: Continue Operations ---");
    let mut counter = loaded_counter;
    counter.increment(10)?;
    println!("✓ Incremented by 10 → value: {}", counter.value());

    counter.reset();
    println!("✓ Reset counter → value: {}", counter.value());

    // Save the new events using commit
    println!("\n--- Step 7: Save New Events ---");
    let num_new_events = counter.pending_events().len();

    store.commit(&mut counter).await?;
    println!("✓ Saved {num_new_events} new events to store");
    println!("✓ Final version: {}", counter.version());

    // Load complete history using the load function
    println!("\n--- Step 8: Load Complete History ---");
    let final_counter: Counter = load(&store, id).await?;

    println!("✓ Loaded complete counter history");
    println!("  Final value: {}", final_counter.value());
    println!("  Final version: {}", final_counter.version());
    println!("  Total events: {}", final_counter.version().as_i32());

    // Demonstrate error handling
    println!("\n--- Step 9: Test Business Rules ---");

    let mut test_counter = Counter::create(CounterId::new());
    test_counter.increment(5)?;

    match test_counter.increment(0) {
        Ok(_) => println!("❌ Should have failed"),
        Err(e) => println!("✓ Rejected invalid increment: {e}"),
    }

    match test_counter.decrement(100) {
        Ok(_) => println!("❌ Should have failed"),
        Err(e) => println!("✓ Rejected invalid decrement: {e}"),
    }

    // Final summary
    println!("\n--- Summary ---");
    println!("✓ Created counter and executed commands");
    println!("✓ Saved events to in-memory store");
    println!("✓ Loaded events and reconstructed state");
    println!("✓ Continued with more operations");
    println!("✓ Maintained consistency across save/load cycles");
    println!("✓ Enforced business rules");
    println!("\nFinal state:");
    println!("  Value: {}", final_counter.value());
    println!("  Version: {}", final_counter.version());

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
        let counter = Counter::create(CounterId::new());

        assert_eq!(counter.value(), 0);
        assert_eq!(counter.version(), Version::initial());
        assert_eq!(counter.pending_events().len(), 0);
    }

    #[test]
    fn test_increment() {
        let mut counter = Counter::create(CounterId::new());

        counter.increment(5).unwrap();

        assert_eq!(counter.value(), 5);
        assert_eq!(counter.version(), Version::from(1));
        assert_eq!(counter.pending_events().len(), 1);
    }

    #[test]
    fn test_decrement() {
        let mut counter = Counter::create(CounterId::new());

        counter.increment(10).unwrap();
        counter.decrement(3).unwrap();

        assert_eq!(counter.value(), 7);
        assert_eq!(counter.version(), Version::from(2));
    }

    #[test]
    fn test_reset() {
        let mut counter = Counter::create(CounterId::new());

        counter.increment(10).unwrap();
        counter.reset();

        assert_eq!(counter.value(), 0);
        assert_eq!(counter.version(), Version::from(2));
    }

    #[test]
    fn test_cannot_increment_zero() {
        let mut counter = Counter::create(CounterId::new());

        let result = counter.increment(0);

        assert!(result.is_err());
        assert_eq!(counter.value(), 0); // State unchanged
    }

    #[test]
    fn test_cannot_decrement_below_zero() {
        let mut counter = Counter::create(CounterId::new());

        counter.increment(5).unwrap();
        let result = counter.decrement(10);

        assert!(result.is_err());
        assert_eq!(counter.value(), 5); // State unchanged
    }

    #[test]
    fn test_event_replay() {
        let mut counter = Counter::create(CounterId::new());

        counter.increment(10).unwrap();
        counter.decrement(3).unwrap();
        counter.increment(5).unwrap();

        // Replay events
        let events = counter.pending_events().to_vec();
        let mut replayed = Counter::create(*counter.aggregate_id());

        for event in events {
            replayed.apply_unchecked(&event);
        }

        assert_eq!(replayed.value(), counter.value());
        assert_eq!(replayed.version(), counter.version());
    }

    #[test]
    fn test_multiple_operations() {
        let mut counter = Counter::create(CounterId::new());

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

    #[test]
    fn test_state_wrapper_pattern() {
        // The Counter wrapper provides access to the state via Deref
        let counter = Counter::create(CounterId::new());

        // Can access state via Deref
        assert_eq!(counter.value, 0);

        // Can access via state() method
        assert_eq!(counter.state().value, 0);
    }
}
