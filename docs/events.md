# Event System Guide

This guide explains how to define and work with events in event-sauce using the new event system with aggregate-specific errors and validation.

## Table of Contents

- [Event Basics](#event-basics)
- [Defining Events](#defining-events)
  - [Using define_events! Macro (Recommended)](#using-define_events-macro-recommended)
  - [Manual Event Definition](#manual-event-definition)
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

### Using define_events! Macro (Recommended)

The **`define_events!` macro** is the recommended way to define events in event-sauce. It provides a declarative syntax that automatically generates:
- Individual event structs with timestamp fields
- The event enum with all variants
- `DomainEvent` trait implementation
- `ApplyEvent` trait implementations with validation and apply logic
- `From` conversions between event structs and the enum

**Benefits:**
- ✅ **60% less boilerplate** - No manual struct definitions or trait implementations
- ✅ **Declarative syntax** - Clear event definitions with inline validation and apply logic
- ✅ **Type-safe validation** - Business rules co-located with events
- ✅ **Automatic versioning** - Support for schema evolution with `@version`
- ✅ **Pre and post-validation** - `@validate` and `@post_validate` hooks

```rust
use event_sauce_core::{define_events, Aggregate};
use event_sauce_macros::{AggregateError, AggregateId};
use thiserror::Error;

// Define your aggregate error
#[derive(AggregateError, Debug, Error)]
enum OrderError {
    #[error("Order already completed")]
    OrderAlreadyCompleted,
    #[error("Invalid quantity: {0}")]
    InvalidQuantity(u32),
    #[error("Order total exceeded: {0}")]
    OrderTotalExceeded(i64),
}

// Define events with the macro
define_events! {
    pub enum OrderEvent for Order {
        Created {
            order_id: String,
        } => |order, event| {
            order.state.order_id = event.order_id.clone();
        },

        ItemAdded {
            item_id: String,
            quantity: u32,
            price: i64,
        }
        @validate {
            if aggregate.state.status == OrderStatus::Completed {
                return Err(OrderError::OrderAlreadyCompleted);
            }
            if self.quantity == 0 {
                return Err(OrderError::InvalidQuantity(self.quantity));
            }
            return Ok(());
        }
        @post_validate {
            if aggregate.state.total_amount > 1_000_000 {
                return Err(OrderError::OrderTotalExceeded(aggregate.state.total_amount));
            }
            return Ok(());
        }
        => |order, event| {
            order.state.items.push(OrderItem {
                item_id: event.item_id.clone(),
                quantity: event.quantity,
                price: event.price,
            });
            order.state.total_amount += event.price * event.quantity as i64;
        },

        Completed {}
        @version(2)  // Custom version for schema evolution
        @validate {
            if aggregate.state.status == OrderStatus::Completed {
                return Err(OrderError::OrderAlreadyCompleted);
            }
            return Ok(());
        }
        => |order, _event| {
            order.state.status = OrderStatus::Completed;
        },
    }
}
```

**What gets generated:**

```rust
// Individual event structs (automatically generated)
pub struct CreatedEvent {
    pub order_id: String,
    pub timestamp: DateTime<Utc>,
}

pub struct ItemAddedEvent {
    pub item_id: String,
    pub quantity: u32,
    pub price: i64,
    pub timestamp: DateTime<Utc>,
}

pub struct CompletedEvent {
    pub timestamp: DateTime<Utc>,
}

// Event enum (automatically generated)
pub enum OrderEvent {
    Created { order_id: String, timestamp: DateTime<Utc> },
    ItemAdded { item_id: String, quantity: u32, price: i64, timestamp: DateTime<Utc> },
    Completed { timestamp: DateTime<Utc> },
}

// ApplyEvent implementations (automatically generated)
impl ApplyEvent<Order> for CreatedEvent { /* ... */ }
impl ApplyEvent<Order> for ItemAddedEvent { /* ... */ }
impl ApplyEvent<Order> for CompletedEvent { /* ... */ }

// DomainEvent implementation (automatically generated)
impl DomainEvent for OrderEvent { /* ... */ }

// From conversions (automatically generated)
impl From<CreatedEvent> for OrderEvent { /* ... */ }
impl From<ItemAddedEvent> for OrderEvent { /* ... */ }
impl From<CompletedEvent> for OrderEvent { /* ... */ }
```

**Usage in aggregate with `command_handler!` macro (Recommended):**

```rust
use event_sauce::command_handler;

impl Order {
    fn create(id: OrderId, order_id: String) -> Self {
        let mut order = Self::new(id);
        let event = CreatedEvent { order_id, timestamp: Utc::now() };
        order.apply(event).expect("Creation should never fail");
        order
    }
}

// Auto-generate command methods - reduces boilerplate by ~70%!
command_handler! {
    impl Order {
        fn add_item(item_id: String, quantity: u32, price: i64) -> ItemAddedEvent {
            item_id, quantity, price
        };
        fn complete() -> CompletedEvent { };
    }
}
```

**What `command_handler!` generates:**

```rust
impl Order {
    pub fn add_item(&mut self, item_id: String, quantity: u32, price: i64)
        -> Result<(), OrderError>
    {
        let event = ItemAddedEvent {
            item_id,
            quantity,
            price,
            timestamp: Utc::now(),  // Automatic timestamp
        };
        self.apply(event)?;  // Validation happens automatically via define_events!
        Ok(())
    }

    pub fn complete(&mut self) -> Result<(), OrderError> {
        let event = CompletedEvent {
            timestamp: Utc::now(),
        };
        self.apply(event)?;
        Ok(())
    }
}
```

**Benefits of using both macros together:**
- ✅ **Combined 80%+ reduction in boilerplate**
- ✅ **Consistent patterns** - All commands follow the same structure
- ✅ **Automatic timestamp handling** - No need to manually add `Utc::now()`
- ✅ **Type-safe** - Compile-time validation of parameters
- ✅ **Self-documenting** - Clear command structure

### Manual Event Definition

For cases where you need more control or complex event logic, you can define events manually using the separated event pattern

The **recommended approach** is to define events as **separate structs** wrapped in an enum. This allows each event to have its own validation and application logic via the `ApplyEvent` trait:

```rust
use event_sauce_core::{ApplyEvent, DomainEvent};
use event_sauce_macros::{AggregateError, Event as DeriveEvent};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

// Step 1: Define individual event structs
#[derive(Debug, Clone, Serialize, Deserialize)]
struct AccountOpenedEvent {
    account_id: String,
    owner: String,
    initial_balance: i64,
    timestamp: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct AccountDepositedEvent {
    amount: i64,
    timestamp: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct AccountWithdrawnEvent {
    amount: i64,
    timestamp: DateTime<Utc>,
}

// Step 2: Wrap them in an enum for the domain event
// The aggregate attribute auto-generates the EventApplicator impl!
#[derive(DeriveEvent, Debug, Clone, Serialize, Deserialize)]
#[event(version = 1, type_prefix = "Account", aggregate = "BankAccountAggregate")]
enum AccountEvent {
    Opened(AccountOpenedEvent),
    Deposited(AccountDepositedEvent),
    Withdrawn(AccountWithdrawnEvent),
}
```

### Benefits of Separated Events

1. **Self-contained logic**: Each event struct implements its own `ApplyEvent` trait
2. **Better organization**: Event logic stays with the event definition
3. **Easier testing**: Test individual events independently
4. **Type safety**: Each event is a distinct type
5. **Cleaner code**: No large match statements in aggregate

### Event Attributes

- **`version`**: The schema version for this event type (required)
- **`type_prefix`**: Prefix for generated event type names (optional)
- **`aggregate`**: The aggregate type name for auto-generating `EventApplicator` impl (optional but recommended)

### Event Requirements

1. **Timestamp field**: All event variants must have a `timestamp: DateTime<Utc>` field
2. **Serializable**: Events must implement `Serialize` and `Deserialize`
3. **Cloneable**: Events must implement `Clone`

### Generated Event Types

The derive macro automatically generates event type names from the enum variants:

```rust
AccountEvent::Opened(..)      → "AccountOpened"
AccountEvent::Deposited(..)   → "AccountDeposited"
AccountEvent::Withdrawn(..)   → "AccountWithdrawn"
```

With type_prefix and aggregate:
```rust
#[event(version = 1, type_prefix = "Account", aggregate = "BankAccountAggregate")]
//                     ^^^^^^^^^ prefix       ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^ auto-generates EventApplicator

AccountEvent::Opened(..)      → "AccountOpened"
AccountEvent::Deposited(..)   → "AccountDeposited"
```

**Note**: When you specify the `aggregate` attribute, the macro automatically generates an `EventApplicator` trait impl that dispatches to each event's `ApplyEvent::apply()` implementation. The `Aggregate` trait's default methods use `EventApplicator` for event dispatching. You don't need to write this manually!

## ApplyEvent Trait - Self-Contained Events (Recommended)

The **`ApplyEvent` trait** is the recommended way to implement event logic. Each event struct implements both validation and application logic:

```rust
use event_sauce_core::ApplyEvent;
use event_sauce_macros::AggregateError;
use thiserror::Error;

// Define error type (auto-implements AggregateError)
#[derive(AggregateError, Debug, Error)]
enum AccountError {
    #[error("Insufficient funds: balance={balance}, requested={requested}")]
    InsufficientFunds { balance: i64, requested: i64 },

    #[error("Invalid amount: {0} (must be positive)")]
    InvalidAmount(i64),

    #[error("Account is {0:?}")]
    AccountNotActive(AccountStatus),
}

// Step 3: Implement ApplyEvent for each event struct
// Note: Implement for the GENERATED aggregate (e.g., BankAccountAggregate)
impl ApplyEvent<BankAccountAggregate, AccountError> for AccountWithdrawnEvent {
    /// Validate business rules before applying
    fn validate(&self, account: &BankAccountAggregate) -> Result<(), AccountError> {
        if account.status != AccountStatus::Active {
            return Err(AccountError::AccountNotActive(account.status));
        }

        if self.amount <= 0 {
            return Err(AccountError::InvalidAmount(self.amount));
        }

        if account.balance < self.amount {
            return Err(AccountError::InsufficientFunds {
                balance: account.balance,
                requested: self.amount,
            });
        }

        Ok(())
    }

    /// Apply the event to update state
    fn apply(&self, account: &mut BankAccountAggregate) {
        // Thanks to Deref, can access state fields directly
        account.balance -= self.amount;
    }
}

impl ApplyEvent<BankAccountAggregate, AccountError> for AccountDepositedEvent {
    fn validate(&self, account: &BankAccountAggregate) -> Result<(), AccountError> {
        if account.status != AccountStatus::Active {
            return Err(AccountError::AccountNotActive(account.status));
        }

        if self.amount <= 0 {
            return Err(AccountError::InvalidAmount(self.amount));
        }

        Ok(())
    }

    fn apply(&self, account: &mut BankAccountAggregate) {
        account.balance += self.amount;
    }
}
```

### Benefits of ApplyEvent Trait

1. **Encapsulation**: Event logic lives with the event struct
2. **Type safety**: Each event has its own validation and application
3. **Testability**: Test events independently from aggregates
4. **Maintainability**: No large match statements in aggregate code
5. **Flexibility**: Easy to add new events without touching aggregate
6. **Clear validation**: Business rules are co-located with events

### How It Works

When you call `aggregate.apply(event)` in a command:

1. The framework calls `event.validate(&aggregate)?` first
2. If validation passes, it calls `event.apply(&mut aggregate)`
3. The event is added to `pending_events`
4. The version is incremented

During replay with `apply_unchecked`, validation is skipped for performance.

## Event Validation

With the `ApplyEvent` trait, validation is **part of the event** itself:

```rust
// In your command methods on the aggregate
impl BankAccountAggregate {
    fn withdraw(&mut self, amount: i64) -> Result<(), AccountError> {
        // Create event
        let event = AccountWithdrawnEvent {
            amount,
            timestamp: Utc::now(),
        };

        // Validation happens automatically via ApplyEvent trait
        event.validate(self)?;

        // Apply the event (updates state and tracks event)
        self.apply(event);

        Ok(())
    }
}
```

### Why This Pattern?

- **Business rule enforcement**: Validation is co-located with the event
- **Better error messages**: Rich, domain-specific errors from the event
- **Replay performance**: Skip validation when replaying with `apply_unchecked`
- **Clean commands**: Command methods just create events and call `apply()`

## Event Replay

Event replay reconstructs aggregate state from historical events. The new system supports optimized replay.

### Replay Methods

#### 1. Regular Apply (with validation)

Used when creating new events in command methods:

```rust
let event = AccountWithdrawnEvent {
    amount: 100,
    timestamp: Utc::now(),
};

// Validates via ApplyEvent::validate() then applies
event.validate(&account)?;
account.apply(event);
```

#### 2. Unchecked Apply (without validation)

Used when replaying historical events from the event store:

```rust
// Replay events from event store
for event in historical_events {
    account.apply_unchecked(&event);  // Fast replay, skips validation
}
```

### Why Skip Validation on Replay?

1. **Performance**: Avoid redundant validation checks
2. **Correctness**: Historical events are facts (already validated when created)
3. **Determinism**: Same events always produce same state
4. **Simplicity**: No error handling needed during replay

### Complete Replay Example

```rust
impl BankAccountAggregate {
    /// Reconstruct aggregate from event history
    fn from_events(id: AccountId, events: Vec<AccountEvent>) -> Self {
        let mut account = Self::from_state(BankAccountState::new(id));

        // Replay all events without validation
        for event in events {
            account.apply_unchecked(&event);
        }

        account
    }
}
```

**Key Point**: `apply_unchecked` still calls the `apply()` method from the `ApplyEvent` implementation, it just skips the `validate()` call. State updates happen the same way.

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

With the ApplyEvent pattern, **validation lives in the event's `validate()` method**, NOT in `apply()`:

```rust
✅ Good:
impl ApplyEvent<BankAccountAggregate, AccountError> for AccountWithdrawnEvent {
    fn validate(&self, account: &BankAccountAggregate) -> Result<(), AccountError> {
        // Validation here!
        if self.amount <= 0 {
            return Err(AccountError::InvalidAmount(self.amount));
        }
        if account.balance < self.amount {
            return Err(AccountError::InsufficientFunds {
                balance: account.balance,
                requested: self.amount,
            });
        }
        Ok(())
    }

    fn apply(&self, account: &mut BankAccountAggregate) {
        // Pure state transformation - NO validation!
        account.balance -= self.amount;
    }
}

❌ Bad:
impl ApplyEvent<BankAccountAggregate, AccountError> for AccountWithdrawnEvent {
    fn apply(&self, account: &mut BankAccountAggregate) {
        // Don't validate in apply!
        if self.amount <= 0 {
            panic!("Invalid amount");  // ❌ Never do this
        }
        account.balance -= self.amount;
    }
}
```

**Rule**: The `apply()` method should ONLY update state. All validation goes in `validate()`.

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

With the ApplyEvent pattern, test event logic independently from aggregates:

```rust
#[test]
fn test_withdrawn_event_reduces_balance() {
    let mut account = BankAccountAggregate::open(
        AccountId::new(),
        "Test".to_string(),
        1000,
    ).unwrap();

    let event = AccountWithdrawnEvent {
        amount: 300,
        timestamp: Utc::now(),
    };

    // Test validation
    assert!(event.validate(&account).is_ok());

    // Test application
    event.apply(&mut account);
    assert_eq!(account.balance, 700);
}

#[test]
fn test_withdrawn_event_validates_insufficient_funds() {
    let mut account = BankAccountAggregate::open(
        AccountId::new(),
        "Test".to_string(),
        100,
    ).unwrap();

    let event = AccountWithdrawnEvent {
        amount: 300,
        timestamp: Utc::now(),
    };

    // Test validation fails
    let result = event.validate(&account);
    assert!(matches!(
        result,
        Err(AccountError::InsufficientFunds { .. })
    ));
}
```

**Benefits**: You can test validation and application separately, making tests more focused and easier to debug.

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

- **Declarative events** with `define_events!` macro (recommended - 60% less code)
- **Type-safe events** with automatic struct and trait generation
- **Self-contained logic** with integrated validation and apply logic
- **Rich validation** with `@validate` and `@post_validate` hooks
- **Optimized replay** with `apply_unchecked`
- **Clean separation** between validation and application
- **Schema evolution** with `@version` attribute
- **Better testing** with independent event tests

### Quick Reference: Event Pattern (Recommended)

| Step | What | Example |
|------|------|---------|
| 1 | Use define_events! macro | `define_events! { pub enum OrderEvent for Order { ... } }` |
| 2 | Define event variants | `Created { order_id: String }` |
| 3 | Add validation (optional) | `@validate { ... } @post_validate { ... }` |
| 4 | Define apply logic | `=> \|order, event\| { order.state.value = ...; }` |
| 5 | Use in commands | `self.apply(CreatedEvent { ... })?;` |

### Quick Reference: Manual Event Pattern (Advanced)

For cases requiring more control, use the manual approach:

| Step | What | Example |
|------|------|---------|
| 1 | Define event structs | `struct AccountWithdrawnEvent { amount: i64, ... }` |
| 2 | Wrap in enum | `enum AccountEvent { Withdrawn(AccountWithdrawnEvent), ... }` |
| 3 | Derive DomainEvent | `#[derive(Event)]` on enum |
| 4 | Implement ApplyEvent | `impl ApplyEvent<Aggregate> for Event { ... }` |
| 5 | Use in commands | `event.validate(self)?; self.apply(event);` |

### Key Patterns

1. **Use `define_events!` macro** for declarative event definitions (recommended)
2. **Inline validation** with `@validate` for pre-conditions and `@post_validate` for invariants
3. **Automatic struct generation** with timestamp fields
4. **Commands create event structs**, then call `apply()`
5. **Replay uses `apply_unchecked()`** to skip validation for performance
6. **Manual approach available** for complex scenarios requiring fine-grained control

### Working with AggregateState

When using `#[derive(AggregateState)]`:
- Implement `ApplyEvent` for the **generated aggregate** (e.g., `BankAccountAggregate`)
- Use `Deref` to access state fields directly in `validate()` and `apply()`
- Implement commands on the generated aggregate

Next: [Aggregates Guide](aggregates.md) | [Event Stores Guide](event-stores.md)
