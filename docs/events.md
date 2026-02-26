# Event System Guide

This guide explains how to define and work with events in event-sauce using the event system with aggregate-specific errors and validation.

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
- 60% less boilerplate - No manual struct definitions or trait implementations
- Declarative syntax - Clear event definitions with inline validation and apply logic
- Type-safe validation - Business rules co-located with events
- Automatic versioning - Support for schema evolution with `@version`
- Pre and post-validation - `@validate` and `@post_validate` hooks

```rust
use event_sauce_core::{define_events, Aggregate};
use event_sauce_macros::AggregateError;
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
// Events apply directly to &mut Order (the entity), not AggregateRoot
define_events! {
    pub enum OrderEvent for Order {
        Created {
            order_id: String,
        } => |order, event| {
            order.order_id = event.order_id.clone();
        },

        ItemAdded {
            item_id: String,
            quantity: u32,
            price: i64,
        }
        @validate {
            if aggregate.status == OrderStatus::Completed {
                return Err(OrderError::OrderAlreadyCompleted);
            }
            if self.quantity == 0 {
                return Err(OrderError::InvalidQuantity(self.quantity));
            }
            return Ok(());
        }
        @post_validate {
            if aggregate.total_amount > 1_000_000 {
                return Err(OrderError::OrderTotalExceeded(aggregate.total_amount));
            }
            return Ok(());
        }
        => |order, event| {
            order.items.push(OrderItem {
                item_id: event.item_id.clone(),
                quantity: event.quantity,
                price: event.price,
            });
            order.total_amount += event.price * event.quantity as i64;
        },

        Completed {}
        @version(2)  // Custom version for schema evolution
        @validate {
            if aggregate.status == OrderStatus::Completed {
                return Err(OrderError::OrderAlreadyCompleted);
            }
            return Ok(());
        }
        => |order, _event| {
            order.status = OrderStatus::Completed;
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
// Events apply directly to &mut Order (the entity)
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

// Auto-generate command methods — supports both regular and @init commands
command_handler! {
    impl Order {
        // @init commands generate creation functions + UninitAggregateRoot methods
        @init fn create_order(order_id: String) -> CreatedEvent { order_id };

        // Regular commands generate methods on AggregateRoot<Order>
        fn add_item(item_id: String, quantity: u32, price: i64) -> ItemAddedEvent {
            item_id, quantity, price
        };
        fn complete() -> CompletedEvent { };
    }
}

// Usage with creation functions (for @init commands):
let mut order = Order::create_order("order-123".to_string())?;  // Random ID
let mut order = Order::create_order_with_id(id, "order-123".to_string())?;  // Explicit ID

// Usage with regular commands:
order.add_item("item-1".to_string(), 2, 500)?;
order.complete()?;
```

**What `command_handler!` generates for `@init` commands:**

```rust
// Event helper associated function (no &self — aggregate doesn't exist yet)
impl Order {
    pub fn create_order_event(order_id: String) -> CreatedEvent { /* ... */ }
}

// Init commands trait on UninitAggregateRoot
pub trait OrderInitCommands {
    fn create_order(self, order_id: String) -> Result<AggregateRoot<Order>, OrderError>;
}
impl OrderInitCommands for UninitAggregateRoot<Order> { /* ... */ }

// Creation functions as associated functions on the aggregate
impl Order {
    pub fn create_order(order_id: String) -> Result<AggregateRoot<Order>, OrderError> { /* ... */ }
    pub fn create_order_with_id(id: EntityId, order_id: String) -> Result<AggregateRoot<Order>, OrderError> { /* ... */ }
}
```

**What `command_handler!` generates for regular commands:**

```rust
// Event helper functions on Order (the entity)
impl Order {
    pub fn add_item_event(&self, item_id: String, quantity: u32, price: i64)
        -> ItemAddedEvent { /* ... */ }
    pub fn complete_event(&self) -> CompletedEvent { /* ... */ }
}

// Commands trait on AggregateRoot<Order>
pub trait OrderCommands {
    fn add_item(&mut self, item_id: String, quantity: u32, price: i64) -> Result<(), OrderError>;
    fn complete(&mut self) -> Result<(), OrderError>;
}
impl OrderCommands for AggregateRoot<Order> { /* ... */ }
```

**Benefits of using both macros together:**
- Combined 80%+ reduction in boilerplate
- Consistent patterns - All commands follow the same structure
- Automatic timestamp handling - No need to manually add `Utc::now()`
- Type-safe - Compile-time validation of parameters
- Self-documenting - Clear command structure
- Init creation functions for ergonomic aggregate construction

### Init Events with define_events!

For aggregates that shouldn't have a default/empty state, mark creation events with `@init`. Init events construct the aggregate instead of mutating it:

```rust
define_events! {
    pub enum OrderEvent for Order {
        // @init event — constructs the aggregate
        Created {
            order_id: String,
        }
        @init
        @validate |evt| {
            if evt.order_id.is_empty() {
                return Err(OrderError::EmptyOrderId);
            }
            Ok(())
        }
        => |id, event| {
            // Return a new aggregate — not &mut, but construction
            Order {
                id,
                order_id: event.order_id.clone(),
                status: OrderStatus::Pending,
            }
        },

        // Regular events — mutate existing aggregate
        Completed {} => |order, _event| {
            order.status = OrderStatus::Completed;
        },
    }
}
```

Key differences for `@init` events:
- `@validate` takes one argument (the event) — no aggregate exists yet
- Apply closure uses `|id, event|` and returns the aggregate
- Generates `InitEvent<A>` instead of `ApplyEvent<A>`
- `@post_validate` still takes `|aggregate, event|` (runs after construction)

### Manual Event Creation

For complex scenarios requiring fine-grained control (conditional events, saga orchestration, multi-event transactions), you can bypass `command_handler!` and create events manually:

```rust
// Advanced escape hatch — use when command_handler! isn't flexible enough
impl AggregateRoot<Order> {
    fn complex_operation(&mut self, input: ComplexInput) -> Result<(), OrderError> {
        // Conditional event creation
        if input.needs_discount {
            self.apply(DiscountAppliedEvent { ... })?;
        }
        // Multiple events in one operation
        for item in input.items {
            self.apply(ItemAddedEvent { ... })?;
        }
        Ok(())
    }
}
```

Prefer `command_handler!` for standard patterns; use manual event creation for complex cases.

### Manual Event Definition

For cases where you need more control or complex event logic, you can define events manually using the separated event pattern.

The **recommended manual approach** is to define events as **separate structs** wrapped in an enum. This allows each event to have its own validation and application logic via the `ApplyEvent` trait:

```rust
use event_sauce_core::{ApplyEvent, DomainEvent};
use event_sauce_macros::{AggregateError, Event as DeriveEvent};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

// Step 1: Define individual event structs
#[derive(Debug, Clone, Serialize, Deserialize)]
struct AccountOpenedEvent {
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
#[event(version = 1, type_prefix = "Account", aggregate = "BankAccount")]
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
AccountEvent::Opened(..)      -> "Account.Opened"
AccountEvent::Deposited(..)   -> "Account.Deposited"
AccountEvent::Withdrawn(..)   -> "Account.Withdrawn"
```

**Note**: When you specify the `aggregate` attribute, the macro automatically generates an `EventApplicator` trait impl that dispatches to each event's `ApplyEvent::apply()` implementation. You don't need to write this manually!

## ApplyEvent Trait - Self-Contained Events (Recommended)

The **`ApplyEvent` trait** is the recommended way to implement event logic. Each event struct implements both validation and application logic. Events operate directly on `&mut Entity`, not on `AggregateRoot`:

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
// Note: Events apply to &mut BankAccount (the entity), not AggregateRoot
impl ApplyEvent<BankAccount> for AccountWithdrawnEvent {
    /// Validate business rules before applying
    fn validate(&self, account: &BankAccount) -> Result<(), AccountError> {
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

    /// Apply the event to update entity state directly
    fn apply(&self, account: &mut BankAccount) {
        account.balance -= self.amount;
    }
}

impl ApplyEvent<BankAccount> for AccountDepositedEvent {
    fn validate(&self, account: &BankAccount) -> Result<(), AccountError> {
        if account.status != AccountStatus::Active {
            return Err(AccountError::AccountNotActive(account.status));
        }

        if self.amount <= 0 {
            return Err(AccountError::InvalidAmount(self.amount));
        }

        Ok(())
    }

    fn apply(&self, account: &mut BankAccount) {
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

When you call `aggregate_root.apply(event)` on an `AggregateRoot`:

1. The event is converted into the aggregate's event type via `Into`
2. `EventApplicator::dispatch()` is called, which runs `validate()`, `apply()`, and `post_validate()`
3. The version is incremented
4. The event is added to `pending_events`

During replay with `apply_unchecked`, validation is skipped for performance.

## Event Validation

With the `ApplyEvent` trait, validation is **part of the event** itself:

```rust
// In your command methods on AggregateRoot<BankAccount>
impl AggregateRoot<BankAccount> {
    fn withdraw(&mut self, amount: i64) -> Result<(), AccountError> {
        // Create event
        let event = AccountWithdrawnEvent {
            amount,
            timestamp: Utc::now(),
        };

        // apply() handles the full lifecycle:
        // validate -> apply state -> post_validate -> increment version -> record event
        self.apply(event)
    }
}
```

### Why This Pattern?

- **Business rule enforcement**: Validation is co-located with the event
- **Better error messages**: Rich, domain-specific errors from the event
- **Replay performance**: Skip validation when replaying with `apply_unchecked`
- **Clean commands**: Command methods just create events and call `apply()`

## Event Replay

Event replay reconstructs aggregate state from historical events. The system supports optimized replay.

### Replay Methods

#### 1. Regular Apply (with validation)

Used when creating new events in command methods:

```rust
let event = AccountWithdrawnEvent {
    amount: 100,
    timestamp: Utc::now(),
};

// Validates via ApplyEvent::validate() then applies
account_root.apply(event)?;
```

#### 2. Unchecked Apply (without validation)

Used when replaying historical events from the event store:

```rust
// Replay events from event store
for event in &historical_events {
    account_root.apply_unchecked(event);  // Fast replay, skips validation
}
```

### Why Skip Validation on Replay?

1. **Performance**: Avoid redundant validation checks
2. **Correctness**: Historical events are facts (already validated when created)
3. **Determinism**: Same events always produce same state
4. **Simplicity**: No error handling needed during replay

### Complete Replay Example

```rust
// Reconstruct aggregate from event history
let mut account = AggregateRoot::<BankAccount>::new(id);

// Replay all events without validation
for event in &events {
    account.apply_unchecked(event);
}
```

**Key Point**: `apply_unchecked` still calls the `apply()` method from the `ApplyEvent` implementation, it just skips the `validate()` call. State updates happen the same way.

## Best Practices

### 1. Event Naming

Use past tense to indicate events are facts:

```rust
// Good:
- AccountOpened
- MoneyDeposited
- OrderShipped

// Bad:
- OpenAccount
- DepositMoney
- ShipOrder
```

### 2. Event Granularity

Events should represent single, atomic changes:

```rust
// Good:
- UserRegistered
- EmailVerified
- ProfileUpdated

// Bad:
- UserRegisteredAndEmailVerifiedAndProfileUpdated
```

### 3. Event Data

Include all data needed to understand what happened:

```rust
// Good:
Withdrawn {
    amount: i64,
    timestamp: DateTime<Utc>,
}

// Bad:
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
// Good:
impl ApplyEvent<BankAccount> for AccountWithdrawnEvent {
    fn validate(&self, account: &BankAccount) -> Result<(), AccountError> {
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

    fn apply(&self, account: &mut BankAccount) {
        // Pure state transformation - NO validation!
        account.balance -= self.amount;
    }
}

// Bad:
impl ApplyEvent<BankAccount> for AccountWithdrawnEvent {
    fn apply(&self, account: &mut BankAccount) {
        // Don't validate in apply!
        if self.amount <= 0 {
            panic!("Invalid amount");  // Never do this
        }
        account.balance -= self.amount;
    }
}
```

**Rule**: The `apply()` method should ONLY update state. All validation goes in `validate()`.

### 6. Timestamp Handling

Always use `Utc::now()` for consistency:

```rust
// Good:
let event = AccountOpened {
    timestamp: Utc::now(),
    ...
};

// Bad:
let event = AccountOpened {
    timestamp: Local::now().into(),  // Timezone issues!
    ...
};
```

### 7. Testing Events

With the ApplyEvent pattern, test event logic independently:

```rust
#[test]
fn test_withdrawn_event_reduces_balance() {
    // Create entity directly for testing
    let mut account = BankAccount {
        id: EntityId::new(),
        owner: "Test".to_string(),
        balance: 1000,
        status: AccountStatus::Active,
    };

    let event = AccountWithdrawnEvent {
        amount: 300,
        timestamp: Utc::now(),
    };

    // Test validation
    assert!(event.validate(&account).is_ok());

    // Test application (directly on entity)
    event.apply(&mut account);
    assert_eq!(account.balance, 700);
}

#[test]
fn test_withdrawn_event_validates_insufficient_funds() {
    let account = BankAccount {
        id: EntityId::new(),
        owner: "Test".to_string(),
        balance: 100,
        status: AccountStatus::Active,
    };

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
    correlation_id: Uuid,  // Links to parent process
    timestamp: DateTime<Utc>,
}
```

### Event Enrichment

Add contextual data to events:

```rust
struct UserRegistered {
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
| 4 | Define apply logic | `=> \|order, event\| { order.value = ...; }` |
| 5 | Use in commands | `self.apply(CreatedEvent { ... })?;` |

### Quick Reference: Manual Event Pattern (Advanced)

For cases requiring more control, use the manual approach:

| Step | What | Example |
|------|------|---------|
| 1 | Define event structs | `struct AccountWithdrawnEvent { amount: i64, ... }` |
| 2 | Wrap in enum | `enum AccountEvent { Withdrawn(AccountWithdrawnEvent), ... }` |
| 3 | Derive DomainEvent | `#[derive(Event)]` on enum |
| 4 | Implement ApplyEvent | `impl ApplyEvent<Entity> for Event { ... }` |
| 5 | Use in commands | `self.apply(event)?;` on `AggregateRoot` |

### Key Patterns

1. **Use `define_events!` macro** for declarative event definitions (recommended)
2. **Inline validation** with `@validate` for pre-conditions and `@post_validate` for invariants
3. **Automatic struct generation** with timestamp fields
4. **Events apply to `&mut Entity` directly** (not through `AggregateRoot`)
5. **Commands operate on `AggregateRoot<Entity>`** which orchestrates the lifecycle
6. **Replay uses `apply_unchecked()`** to skip validation for performance
7. **Manual approach available** for complex scenarios requiring fine-grained control

Next: [Aggregates Guide](aggregates.md) | [Event Stores Guide](event-store.md)
