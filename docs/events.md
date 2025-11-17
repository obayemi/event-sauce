# Event System Guide

This guide explains how to define and work with events in event-sauce using the new event system with aggregate-specific errors and validation.

## Table of Contents

- [Event Basics](#event-basics)
- [Defining Events](#defining-events)
- [Event Validation](#event-validation)
- [Self-Contained Events](#self-contained-events)
- [Event Replay](#event-replay)
- [Best Practices](#best-practices)

## Event Basics

Events are immutable facts that represent state changes in your domain. In event-sauce, events are the source of truth for your aggregates.

### Key Principles

1. **Events are facts**: They represent things that have happened
2. **Events are immutable**: Once created, they never change
3. **Events are self-describing**: They contain all information needed to understand what happened
4. **Events drive state**: Aggregate state is derived by applying events

## Defining Events

Events are defined using the `#[derive(Event)]` macro on an enum:

```rust
use event_sauce_core::DomainEvent;
use event_sauce_macros::Event as DeriveEvent;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

#[derive(DeriveEvent, Debug, Clone, Serialize, Deserialize)]
#[event(version = 1, type_prefix = "BankAccount")]
enum BankAccountEvent {
    Opened {
        account_id: String,
        owner: String,
        initial_balance: i64,
        timestamp: DateTime<Utc>,
    },
    Deposited {
        amount: i64,
        timestamp: DateTime<Utc>,
    },
    Withdrawn {
        amount: i64,
        timestamp: DateTime<Utc>,
    },
}
```

### Event Attributes

- **`version`**: The schema version for this event type (required)
- **`type_prefix`**: Prefix for generated event type names (optional)

### Event Requirements

1. **Timestamp field**: All event variants must have a `timestamp: DateTime<Utc>` field
2. **Serializable**: Events must implement `Serialize` and `Deserialize`
3. **Cloneable**: Events must implement `Clone`

### Generated Event Types

The derive macro automatically generates event type names:

```rust
BankAccountEvent::Opened { .. }      → "BankAccountOpened"
BankAccountEvent::Deposited { .. }   → "BankAccountDeposited"
BankAccountEvent::Withdrawn { .. }   → "BankAccountWithdrawn"
```

## Event Validation

Events can include validation logic that's enforced when creating new events but skipped during replay.

### Why Validation?

- **Business rule enforcement**: Ensure invariants when creating events
- **Better error messages**: Provide rich, domain-specific errors
- **Replay performance**: Skip validation when replaying historical events

### Validation Pattern

Validation happens in business methods before creating events:

```rust
impl BankAccount {
    fn withdraw(&mut self, amount: i64) -> Result<(), AccountError> {
        // Validate business rules
        if self.status != AccountStatus::Active {
            return Err(AccountError::AccountNotActive(self.status));
        }

        if amount <= 0 {
            return Err(AccountError::InvalidAmount(amount));
        }

        if self.balance < amount {
            return Err(AccountError::InsufficientFunds {
                balance: self.balance,
                requested: amount,
            });
        }

        // Create event (validation passed)
        let event = BankAccountEvent::Withdrawn {
            amount,
            timestamp: Utc::now(),
        };

        // Apply and record
        self.apply(&event);
        self.pending_events.push(event);

        Ok(())
    }
}
```

## Self-Contained Events

The `ApplyEvent` trait allows events to define their own application logic:

```rust
use event_sauce_core::{ApplyEvent, AggregateError};

// Define error type
#[derive(Debug, Error)]
enum BankAccountError {
    #[error("Insufficient funds")]
    InsufficientFunds,
}

impl AggregateError for BankAccountError {}

// Event struct
struct MoneyWithdrawn {
    amount: i64,
    timestamp: DateTime<Utc>,
}

// Self-contained event logic
impl ApplyEvent<BankAccount, BankAccountError> for MoneyWithdrawn {
    fn validate(&self, account: &BankAccount) -> Result<(), BankAccountError> {
        if account.balance < self.amount {
            return Err(BankAccountError::InsufficientFunds);
        }
        Ok(())
    }

    fn apply(&self, account: &mut BankAccount) {
        account.balance -= self.amount;
    }
}
```

### Benefits

- **Encapsulation**: Event logic lives with the event
- **Testability**: Test events independently
- **Maintainability**: No large match statements
- **Flexibility**: Easy to add new events

## Event Replay

Event replay reconstructs aggregate state from historical events. The new system supports optimized replay.

### Replay Methods

#### 1. Regular Apply (with validation)

Used when creating new events:

```rust
let event = BankAccountEvent::Withdrawn { amount: 100, timestamp: Utc::now() };
account.apply(&event)?;  // Validates and applies
```

#### 2. Unchecked Apply (without validation)

Used when replaying historical events:

```rust
// Replay events from event store
for event in historical_events {
    account.apply_unchecked(&event);  // Fast replay, no validation
}
```

### Why Skip Validation on Replay?

1. **Performance**: Avoid redundant validation
2. **Correctness**: Historical events are facts (already validated when created)
3. **Determinism**: Same events always produce same state

### Complete Replay Example

```rust
impl BankAccount {
    /// Reconstruct from event history
    fn from_events(id: AccountId, events: Vec<BankAccountEvent>) -> Self {
        let mut account = Self {
            id,
            owner: String::new(),
            balance: 0,
            status: AccountStatus::Active,
            version: Version::initial(),
            pending_events: Vec::new(),
        };

        // Replay without validation
        for event in events {
            account.apply_unchecked(&event);
        }

        account
    }
}
```

## Best Practices

### 1. Event Naming

Use past tense to indicate events are facts:

```rust
✅ Good:
- AccountOpened
- MoneyDeposited
- OrderShipped

❌ Bad:
- OpenAccount
- DepositMoney
- ShipOrder
```

### 2. Event Granularity

Events should represent single, atomic changes:

```rust
✅ Good:
- UserRegistered
- EmailVerified
- ProfileUpdated

❌ Bad:
- UserRegisteredAndEmailVerifiedAndProfileUpdated
```

### 3. Event Data

Include all data needed to understand what happened:

```rust
✅ Good:
Withdrawn {
    amount: i64,
    account_id: String,
    timestamp: DateTime<Utc>,
}

❌ Bad:
Withdrawn { amount: i64 }  // Missing context
```

### 4. Event Versioning

Plan for schema evolution from the start:

```rust
#[derive(Event)]
#[event(version = 1)]  // Start at v1
enum BankAccountEvent {
    // Events...
}

// Later, when schema changes:
#[derive(Event)]
#[event(version = 2)]  // Increment version
enum BankAccountEvent {
    // Updated events...
}
```

### 5. Validation Placement

Validate in business methods, not in apply:

```rust
✅ Good:
fn withdraw(&mut self, amount: i64) -> Result<(), Error> {
    if amount <= 0 {
        return Err(Error::InvalidAmount);
    }
    let event = ...;
    self.apply(&event);
}

❌ Bad:
fn apply_event(&mut self, event: &Event) {
    match event {
        Event::Withdrawn { amount, .. } => {
            if amount <= 0 {  // Don't validate in apply!
                panic!("Invalid amount");
            }
            self.balance -= amount;
        }
    }
}
```

### 6. Timestamp Handling

Always use `Utc::now()` for consistency:

```rust
✅ Good:
let event = AccountOpened {
    timestamp: Utc::now(),
    ...
};

❌ Bad:
let event = AccountOpened {
    timestamp: Local::now().into(),  // Timezone issues!
    ...
};
```

### 7. Testing Events

Test events independently:

```rust
#[test]
fn test_withdrawn_event_reduces_balance() {
    let mut account = BankAccount { balance: 1000, .. };
    let event = BankAccountEvent::Withdrawn {
        amount: 300,
        timestamp: Utc::now(),
    };

    account.apply_event(&event);

    assert_eq!(account.balance, 700);
}
```

## Advanced Topics

### Event Upcasting

Handle schema evolution by converting old events to new format:

```rust
impl BankAccountEventV2 {
    fn from_v1(v1: BankAccountEventV1) -> Self {
        match v1 {
            BankAccountEventV1::Withdrawn { amount, timestamp } => {
                // Add new fields with defaults
                BankAccountEventV2::Withdrawn {
                    amount,
                    timestamp,
                    fee: 0,  // New field with default
                }
            }
        }
    }
}
```

### Event Correlation

Link related events using correlation IDs:

```rust
struct OrderPlaced {
    order_id: OrderId,
    correlation_id: Uuid,  // Links to parent process
    timestamp: DateTime<Utc>,
}
```

### Event Enrichment

Add contextual data to events:

```rust
struct UserRegistered {
    user_id: UserId,
    email: String,
    // Enrichment data
    ip_address: String,
    user_agent: String,
    timestamp: DateTime<Utc>,
}
```

## Summary

The event-sauce event system provides:

- **Type-safe events** with derive macros
- **Rich validation** with aggregate-specific errors
- **Optimized replay** with `apply_unchecked`
- **Self-contained logic** with `ApplyEvent` trait
- **Schema evolution** with versioning

Next: [Aggregates Guide](aggregates.md) | [Validation Guide](validation.md)
