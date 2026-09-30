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
- Automatic versioning - Support for schema evolution with `@version` and on-load migration with `@upcast`
- Rename safety - Keep historical events loadable after a rename with `@aliases`
- Pre and post-validation - `@validate` and `@post_validate` hooks
- Field-level encryption - Encrypt sensitive fields with `@encrypted_fields` (see [Privacy](privacy.md))

```rust
use event_sauce::{define_events, Aggregate, AggregateError};
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
        @validate |aggregate, event| {
            if aggregate.status == OrderStatus::Completed {
                return Err(OrderError::OrderAlreadyCompleted);
            }
            if event.quantity == 0 {
                return Err(OrderError::InvalidQuantity(event.quantity));
            }
            Ok(())
        }
        @post_validate |aggregate, _event| {
            if aggregate.total_amount > 1_000_000 {
                return Err(OrderError::OrderTotalExceeded(aggregate.total_amount));
            }
            Ok(())
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
        @validate |aggregate, _event| {
            if aggregate.status == OrderStatus::Completed {
                return Err(OrderError::OrderAlreadyCompleted);
            }
            Ok(())
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
        // @clock stamps `timestamp` from the wall clock; without it a command
        // takes its instant as an explicit parameter instead
        @clock @init fn create_order(order_id: String) -> CreatedEvent { order_id };

        // Regular commands generate methods on AggregateRoot<Order>
        @clock fn add_item(item_id: String, quantity: u32, price: i64) -> ItemAddedEvent {
            item_id, quantity, price
        };
        @clock fn complete() -> CompletedEvent { };
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
- Timestamps stamped automatically for `@clock` commands - no need to manually add `Utc::now()`
- Type-safe - Compile-time parameter validation
- Self-documenting - Clear command structure
- Init creation functions for ergonomic aggregate construction

### Commands without `@clock`, and naming the instant with `@occurred_at`

`@clock` is an opt-in, not the default. Without it, a command takes every
field of its event as a parameter, instants included, and stamps no clock of
its own — the form that cannot be wrong, since an event is usually *about* an
instant, and stamping it with the moment the command happened to run is only
right when those two coincide.

It is also what a producer needs when its facts carry two clocks: the time
the world did something, and the time this service found out. A single
injected `timestamp` could only be one of those, and would read as either.
`define_events!`'s `@occurred_at(field)` names the instant that
[`DomainEvent::occurred_at`](https://docs.rs/event-sauce/latest/event_sauce/trait.DomainEvent.html#tymethod.occurred_at)
should answer with, while the event's other clocks stay ordinary fields:

```rust
define_events! {
    enum SensorEvent for Sensor {
        Measured {
            received_at: DateTime<Utc>,   // detection time — an ordinary field
            celsius: i32,
        }
        @occurred_at(measured_at)         // data time — names the instant
        => |sensor, event| {
            sensor.celsius = event.celsius;
        },
    }
}
```

Pair an `@occurred_at` variant with a `command_handler!` command that has no
`@clock` (an `@clock` command would set a `timestamp` field the variant no
longer has), so the caller supplies both instants directly. A variant with
no marker keeps its `timestamp` field and works with either form:

```rust
command_handler! {
    impl Sensor {
        fn measure(
            measured_at: DateTime<Utc>,
            received_at: DateTime<Utc>,
            celsius: i32,
        ) -> MeasuredEvent { measured_at, received_at, celsius };
    }
}

sensor.measure(measured_at, Utc::now(), 21)?;
```

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

### Multiple Init Events

Aggregates can have **multiple `@init` events** for different creation paths. This is common when an entity can be created in different ways, each producing different initial state:

```rust
define_events! {
    pub enum MemberEvent for Member {
        // Admin creation — immediate full access
        AdminCreated {
            email: String,
            name: String,
        }
        @init
        @validate |evt| {
            if evt.email.is_empty() {
                return Err(MemberError::EmptyEmail);
            }
            Ok(())
        }
        => |id, event| {
            Member {
                id,
                email: event.email.clone(),
                name: event.name.clone(),
                role: MemberRole::Admin,
                verified: true,
            }
        },

        // Invite creation — limited access until verified
        CreatedByInvite {
            email: String,
            name: String,
            invite_code: String,
        }
        @init
        @validate |evt| {
            if evt.invite_code.is_empty() {
                return Err(MemberError::InvalidInviteCode);
            }
            Ok(())
        }
        => |id, event| {
            Member {
                id,
                email: event.email.clone(),
                name: event.name.clone(),
                role: MemberRole::Regular,
                verified: false,
            }
        },

        // Regular event — available after either init path
        Verified {} => |member, _event| {
            member.verified = true;
        },
    }
}
```

Each `@init` variant generates its own `InitEvent<A>` implementation with independent validation and construction logic. The `command_handler!` macro also supports multiple `@init` commands:

```rust
command_handler! {
    impl Member {
        @clock @init fn create_admin(email: String, name: String)
            -> AdminCreatedEvent { email, name };
        @clock @init fn create_by_invite(email: String, name: String, invite_code: String)
            -> CreatedByInviteEvent { email, name, invite_code };
        @clock fn verify() -> VerifiedEvent { };
    }
}

// Two distinct creation paths:
let admin = Member::create_admin("admin@co.com".into(), "Alice".into())?;
let invited = Member::create_by_invite("user@co.com".into(), "Bob".into(), "INV123".into())?;
```

This pattern is useful for:
- **Different user roles** (admin vs regular vs guest)
- **Different registration flows** (self-signup vs invitation vs SSO)
- **Different creation contexts** (manual vs imported vs migrated)

### Manual Event Creation

For complex scenarios requiring fine-grained control (conditional events, saga orchestration, multi-event transactions), you can bypass `command_handler!` and create events manually:

```rust
// Advanced escape hatch — use when command_handler! isn't flexible enough.
// AggregateRoot is defined in event-sauce-core, so reach it through an
// extension trait rather than a plain (orphan-rule-violating) inherent impl.
trait OrderCommands {
    fn complex_operation(&mut self, input: ComplexInput) -> Result<(), OrderError>;
}

impl OrderCommands for AggregateRoot<Order> {
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
use event_sauce::{AggregateError, ApplyEvent, DomainEvent, Event as DeriveEvent};
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

- **`version`**: The default schema version for the enum's variants (required at the container level)
- **`type_prefix`**: Prefix for generated event type names (optional)
- **`aggregate`**: The aggregate type name for auto-generating `EventApplicator` impl (optional but recommended)

#### Per-variant version override

The container-level `#[event(version = N)]` sets the default version for every
variant. An individual variant may override it with its own `#[event(version =
N)]` attribute — the mirror of `define_events!`'s per-variant `@version(n)` —
so variants can evolve their schemas independently. This matters for upcasting,
which keys on the stored `from_version`.

```rust
#[derive(event_sauce::Event, Debug, Clone, Serialize, Deserialize)]
#[event(version = 1, aggregate = "Account")]
enum AccountEvent {
    // No per-variant attr -> falls back to the container version (1).
    Opened(AccountOpenedEvent),

    // Independently versioned at 2.
    #[event(version = 2)]
    Migrated(AccountMigratedEvent),
}
```

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
use event_sauce::{AggregateError, ApplyEvent};
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
// In your command methods, reached through an extension trait implemented
// for AggregateRoot<BankAccount> (a plain inherent impl violates the orphan
// rule — see the Aggregates Guide)
trait BankAccountCommands {
    fn withdraw(&mut self, amount: i64) -> Result<(), AccountError>;
}

impl BankAccountCommands for AggregateRoot<BankAccount> {
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

Plan for schema evolution from the start. Each event carries an
`event_version` (set with `@version` in `define_events!`); it is written into
the stored envelope and read back on load to drive [upcasting](#event-upcasting).

```rust
define_events! {
    pub enum BankAccountEvent for BankAccount {
        Withdrawn { amount: i64 }
        @version(1)  // Start at v1
        => |account, event| { account.balance -= event.amount; },
    }
}

// Later, when the event's shape changes, bump the version and add an
// `@upcast` clause that migrates older payloads on load:
define_events! {
    pub enum BankAccountEvent for BankAccount {
        @upcast |event_type, from_version, data| { /* migrate v1 -> v2 */ }

        Withdrawn { amount: i64, fee: i64 }
        @version(2)  // Increment version
        => |account, event| { account.balance -= event.amount + event.fee; },
    }
}
```

Bumping `@version` is what tells `upcast` which payloads are historical. See
[Event Upcasting](#event-upcasting) for the full migration mechanism.

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

Keep instants in UTC (`DateTime<Utc>`). When an event's instant really is
"when this ran", let `@clock` stamp it or use `Utc::now()`, never
`Local::now()`. Otherwise take the instant as a parameter.

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

When you change the **shape** of an event (add a required field, rename a key,
reshape a value), historical payloads stored under an older `event_version` no
longer match the current struct and fail to deserialize. Upcasting migrates
those old payloads to the current shape **on load**, before deserialization.

The seam is `DomainEvent::upcast`. It is called automatically by
`from_envelope` with the envelope's **stored** `event_version` (the version the
payload was written with) and a mutable JSON value. The default implementation
is a no-op, so existing events keep loading unchanged; override it to add a
migration path. Because the event's current `event_version` always reports the
latest schema, you branch on `from_version` so that old payloads are migrated
while current-version payloads pass through untouched.

#### With `define_events!` (recommended)

Add an enum-level `@upcast |event_type, from_version, data| { ... }` clause as
the first item in the enum body, bump `@version`, and add the new field. Because
`define_events!` serialises each variant as a flat object, `data` is that
variant's object directly:

```rust
use event_sauce::{define_events, EventVersion};
use serde_json::json;

define_events! {
    pub enum BankAccountEvent for BankAccount {
        // Migrate payloads written before `fee` existed (v1 -> v2).
        @upcast |event_type, from_version, data| {
            if event_type == "BankAccount.Withdrawn"
                && from_version == EventVersion::new(1)
            {
                if let Some(obj) = data.as_object_mut() {
                    obj.entry("fee").or_insert_with(|| json!(0));
                }
            }
        }

        Withdrawn {
            amount: i64,
            fee: i64, // Added in v2 — historical v1 payloads lack it.
        }
        @version(2)
        => |account, event| {
            account.balance -= event.amount + event.fee;
        },
    }
}
```

When the `@upcast` clause is omitted, the trait default (no migration) applies.

#### Manual `DomainEvent` impl

If you implement `DomainEvent` by hand, override `upcast` directly. With a
serde-tagged enum the payload is keyed by the variant name, so reach into that
key first:

```rust
impl DomainEvent for BankAccountEvent {
    type Aggregate = BankAccount;

    // ... event_type / event_version (returns v2) / occurred_at ...

    fn upcast(event_type: &str, from_version: EventVersion, data: &mut serde_json::Value) {
        if event_type == "BankAccount.Withdrawn" && from_version == EventVersion::new(1) {
            if let Some(obj) = data
                .get_mut("Withdrawn")
                .and_then(serde_json::Value::as_object_mut)
            {
                obj.entry("fee").or_insert_with(|| serde_json::json!(0));
            }
        }
    }
}
```

The migration runs transparently inside `Repository::load` / replay — no
separate conversion step is needed at the call site.

### Renaming Events Safely (Aliases)

The persisted `event_type` key is derived from the **aggregate and variant
identifiers**: `define_events!` writes `concat!(stringify!(Aggregate), ".",
stringify!(Variant))` and `#[derive(Event)]` writes `"{type_prefix}.{Variant}"`.
This means the aggregate type name and each variant name are part of the
**on-disk contract**. Renaming the aggregate or a variant changes the key, so
`from_envelope` no longer matches the old string and every historical event
orphans with `Error: Unknown event type` — a fail-fast default that is
deliberately preserved (an event store must never silently skip an unknown
event, which would corrupt the reconstructed state).

Upcasting (above) handles a change of event **shape**; a rename is a change of
event **name**, so it needs a different tool: a compile-time **alias**. Declare
the old wire string with a per-variant `@aliases("Old.Name", ...)` clause so the
renamed variant keeps loading historical events written under the old name. The
canonical `EVENT_TYPE` (the current name) is unchanged — aliases are only
*additional* names accepted on read, and new events are still written under the
current name.

```rust
use event_sauce::define_events;

// The variant was renamed from `OldCreated` to `Created`. Historical events
// were persisted as `Account.OldCreated`; the alias keeps them loadable into
// the renamed `Created` variant. New events write the canonical `Account.Created`.
define_events! {
    pub enum AccountEvent for Account {
        Created {
            owner: String,
        }
        @aliases("Account.OldCreated")
        => |account, event| {
            account.owner = event.owner.clone();
        },
    }
}
```

`@aliases(...)` sits alongside the other per-variant clauses (after
`@encrypted_fields`, before the `=>` apply closure) and works on every variant
kind (`@init`, `@actor`, `@delete`, ...). Multiple old names are allowed:
`@aliases("Old.Name", "Older.Name")`. A rename is therefore a code-only change —
no data migration is required, and the old key never has to be rewritten on
disk.

> A rename and a shape change are independent and can be combined: add an
> `@aliases(...)` for the old name *and* an `@upcast` clause keyed on the
> stored `event_version` if the fields also changed.

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
- **Schema evolution** with `@version` plus `@upcast` for on-load migration
- **Rename safety** with `@aliases` keeping historical events loadable after a rename
- **Field-level encryption** with `@encrypted_fields` for selective privacy
- **Better testing** with independent event tests

### Quick Reference: Event Pattern (Recommended)

| Step | What | Example |
|------|------|---------|
| 1 | Use define_events! macro | `define_events! { pub enum OrderEvent for Order { ... } }` |
| 2 | Define event variants | `Created { order_id: String }` |
| 3 | Add validation (optional) | `@validate \|agg, evt\| { ... } @post_validate \|agg, evt\| { ... }` |
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
