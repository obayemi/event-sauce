# Aggregates Guide

This guide explains how to define and work with aggregates in event-sauce, including the new aggregate-specific error pattern.

## Table of Contents

- [Aggregate Basics](#aggregate-basics)
- [Defining Aggregates](#defining-aggregates)
- [Aggregate Errors](#aggregate-errors)
- [Business Logic](#business-logic)
- [State Management](#state-management)
- [Best Practices](#best-practices)

## Aggregate Basics

An aggregate is the fundamental building block of event sourcing and Domain-Driven Design. It represents a cluster of domain objects that can be treated as a single unit for data changes.

### Key Principles

1. **Consistency boundary**: Aggregates enforce invariants
2. **Transactional boundary**: Changes to an aggregate are atomic
3. **Event-sourced**: State is derived from events
4. **Identified**: Each aggregate has a unique ID
5. **Versioned**: Optimistic concurrency control

## Defining Aggregates

Aggregates are defined using the `#[derive(Aggregate)]` macro:

```rust
use event_sauce_core::{Aggregate, AggregateId, AggregateError, Version};
use event_sauce_macros::Aggregate as DeriveAggregate;
use uuid::Uuid;

// Define the aggregate ID
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct AccountId(Uuid);

impl AggregateId for AccountId {}

impl fmt::Display for AccountId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Account-{}", self.0)
    }
}

// Define aggregate error (see Aggregate Errors section)
#[derive(Debug, Error)]
enum AccountError {
    #[error("Insufficient funds")]
    InsufficientFunds,
}

impl AggregateError for AccountError {}

// Define the aggregate
#[derive(DeriveAggregate, Debug, Clone)]
#[aggregate(
    id = "AccountId",
    event = "AccountEvent",
    error = "AccountError"
)]
struct BankAccount {
    #[aggregate_id]
    id: AccountId,

    // Domain state
    owner: String,
    balance: i64,
    status: AccountStatus,

    // Event sourcing fields
    #[aggregate_version]
    version: Version,

    #[aggregate_events]
    pending_events: Vec<AccountEvent>,
}
```

### Aggregate Attributes

- **`id`**: The type used for aggregate identification (required)
- **`event`**: The event type for this aggregate (required)
- **`error`**: The error type for this aggregate (required)

### Field Attributes

- **`#[aggregate_id]`**: Marks the ID field
- **`#[aggregate_version]`**: Marks the version field
- **`#[aggregate_events]`**: Marks the pending events field

### Generated Trait Implementation

The macro generates the `Aggregate` trait implementation:

```rust
impl Aggregate for BankAccount {
    type Event = AccountEvent;
    type Id = AccountId;
    type Error = AccountError;

    fn aggregate_id(&self) -> &Self::Id {
        &self.id
    }

    fn version(&self) -> Version {
        self.version
    }

    fn pending_events(&self) -> &[Self::Event] {
        &self.pending_events
    }

    fn clear_pending_events(&mut self) {
        self.pending_events.clear();
    }

    fn apply(&mut self, event: &Self::Event) {
        self.apply_event(event);
        self.version = self.version.next();
    }
}
```

## Aggregate Errors

Aggregate-specific errors provide rich, domain-specific error handling.

### Defining Aggregate Errors

Use `thiserror` for clean error definitions:

```rust
use thiserror::Error;
use event_sauce_core::AggregateError;

#[derive(Debug, Error)]
enum BankAccountError {
    #[error("Insufficient funds: balance={balance}, requested={requested}")]
    InsufficientFunds {
        balance: i64,
        requested: i64,
    },

    #[error("Invalid amount: {0} (must be positive)")]
    InvalidAmount(i64),

    #[error("Account is {0:?}")]
    AccountNotActive(AccountStatus),

    #[error("Withdrawal limit exceeded: limit={limit}, requested={requested}")]
    WithdrawalLimitExceeded {
        limit: i64,
        requested: i64,
    },
}

// Mark as aggregate error
impl AggregateError for BankAccountError {}
```

### Error Benefits

1. **Type safety**: Each aggregate has its own error type
2. **Rich context**: Include relevant data in errors
3. **Clear messages**: Descriptive error messages for debugging
4. **Domain language**: Errors use ubiquitous language

### Using Aggregate Errors

Business methods return `Result<T, Self::Error>`:

```rust
impl BankAccount {
    fn withdraw(&mut self, amount: i64) -> Result<(), BankAccountError> {
        if amount <= 0 {
            return Err(BankAccountError::InvalidAmount(amount));
        }

        if self.balance < amount {
            return Err(BankAccountError::InsufficientFunds {
                balance: self.balance,
                requested: amount,
            });
        }

        // Create and apply event
        let event = AccountEvent::Withdrawn { amount, timestamp: Utc::now() };
        self.apply(&event);
        self.pending_events.push(event);

        Ok(())
    }
}
```

## Business Logic

Business logic methods enforce invariants and create events.

### Command Pattern

Commands are methods that change aggregate state:

```rust
impl BankAccount {
    /// Open a new account (factory method)
    fn open(id: AccountId, owner: String, initial_balance: i64)
        -> Result<Self, BankAccountError>
    {
        // Validate
        if initial_balance < 0 {
            return Err(BankAccountError::InvalidAmount(initial_balance));
        }

        // Create aggregate
        let mut account = Self {
            id,
            owner: String::new(),
            balance: 0,
            status: AccountStatus::Active,
            version: Version::initial(),
            pending_events: Vec::new(),
        };

        // Create and apply event
        let event = AccountEvent::Opened {
            owner,
            initial_balance,
            timestamp: Utc::now(),
        };
        account.apply(&event);
        account.pending_events.push(event);

        Ok(account)
    }

    /// Deposit money (command)
    fn deposit(&mut self, amount: i64) -> Result<(), BankAccountError> {
        // Validate business rules
        if self.status != AccountStatus::Active {
            return Err(BankAccountError::AccountNotActive(self.status));
        }

        if amount <= 0 {
            return Err(BankAccountError::InvalidAmount(amount));
        }

        // Create and apply event
        let event = AccountEvent::Deposited {
            amount,
            timestamp: Utc::now(),
        };
        self.apply(&event);
        self.pending_events.push(event);

        Ok(())
    }

    /// Freeze account (command)
    fn freeze(&mut self) -> Result<(), BankAccountError> {
        if self.status == AccountStatus::Frozen {
            return Err(BankAccountError::AccountAlreadyFrozen);
        }

        let event = AccountEvent::Frozen { timestamp: Utc::now() };
        self.apply(&event);
        self.pending_events.push(event);

        Ok(())
    }
}
```

### Query Pattern

Query methods return aggregate state without modification:

```rust
impl BankAccount {
    /// Get current balance (query)
    fn balance(&self) -> i64 {
        self.balance
    }

    /// Get account status (query)
    fn status(&self) -> AccountStatus {
        self.status
    }

    /// Check if account can accept deposits (query)
    fn can_deposit(&self) -> bool {
        self.status == AccountStatus::Active
    }

    /// Get available balance considering holds (query)
    fn available_balance(&self) -> i64 {
        self.balance - self.holds
    }
}
```

## State Management

Aggregates manage state through event application.

### Apply Event Method

The `apply_event` method updates aggregate state:

```rust
impl BankAccount {
    fn apply_event(&mut self, event: &AccountEvent) {
        match event {
            AccountEvent::Opened {
                owner,
                initial_balance,
                ..
            } => {
                self.owner = owner.clone();
                self.balance = *initial_balance;
                self.status = AccountStatus::Active;
            }
            AccountEvent::Deposited { amount, .. } => {
                self.balance += amount;
            }
            AccountEvent::Withdrawn { amount, .. } => {
                self.balance -= amount;
            }
            AccountEvent::Frozen { .. } => {
                self.status = AccountStatus::Frozen;
            }
        }
    }
}
```

### Apply Rules

1. **Pure state transformation**: No side effects
2. **Deterministic**: Same event = same result
3. **No validation**: Trust that events are valid
4. **Idempotent**: Can be called multiple times safely

### Event Replay

Reconstruct aggregate state from events:

```rust
impl BankAccount {
    /// Reconstruct from event history
    fn from_events(id: AccountId, events: Vec<AccountEvent>) -> Self {
        let mut account = Self {
            id,
            owner: String::new(),
            balance: 0,
            status: AccountStatus::Active,
            version: Version::initial(),
            pending_events: Vec::new(),
        };

        // Replay all events
        for event in events {
            account.apply_unchecked(&event);
        }

        account
    }
}
```

## Best Practices

### 1. Single Responsibility

Each aggregate should have a clear, focused purpose:

```rust
✅ Good: BankAccount handles account operations
✅ Good: Order handles order lifecycle
✅ Good: User handles user profile

❌ Bad: UserAccountOrder handles everything
```

### 2. Protect Invariants

Enforce business rules before creating events:

```rust
✅ Good:
fn withdraw(&mut self, amount: i64) -> Result<(), Error> {
    if self.balance < amount {
        return Err(Error::InsufficientFunds);
    }
    // Create event...
}

❌ Bad:
fn withdraw(&mut self, amount: i64) {
    // No validation, just create event
    let event = Withdrawn { amount };
    self.apply(&event);
}
```

### 3. Small Aggregates

Keep aggregates focused and small:

```rust
✅ Good:
struct Order {
    id: OrderId,
    items: Vec<OrderItem>,  // Small collection
    status: OrderStatus,
}

❌ Bad:
struct Order {
    id: OrderId,
    items: Vec<OrderItem>,  // Could be huge!
    customer: Customer,     // Should be a reference
    payment_history: Vec<Payment>,  // Separate aggregate
    shipments: Vec<Shipment>,  // Separate aggregate
}
```

### 4. Async Operations

Don't perform I/O in aggregates:

```rust
✅ Good:
fn place_order(&mut self, items: Vec<OrderItem>) -> Result<(), OrderError> {
    // Pure business logic only
    let event = OrderPlaced { items, ..};
    self.apply(&event);
    Ok(())
}

❌ Bad:
async fn place_order(&mut self, items: Vec<OrderItem>) -> Result<(), OrderError> {
    // Don't do this!
    let inventory = fetch_inventory().await?;
    let price = calculate_price(&items, &inventory).await?;
    // ...
}
```

### 5. Constructor Pattern

Use factory methods for aggregate creation:

```rust
✅ Good:
impl BankAccount {
    fn open(id: AccountId, owner: String, initial_balance: i64)
        -> Result<Self, BankAccountError>
    {
        // Validation and event creation
    }
}

❌ Bad:
let mut account = BankAccount {
    id,
    owner: String::new(),
    // Manually setting fields
};
```

### 6. Version Management

Let the framework handle versioning:

```rust
✅ Good:
fn deposit(&mut self, amount: i64) -> Result<(), Error> {
    let event = Deposited { amount, .. };
    self.apply(&event);  // Version incremented automatically
    self.pending_events.push(event);
    Ok(())
}

❌ Bad:
fn deposit(&mut self, amount: i64) -> Result<(), Error> {
    let event = Deposited { amount, .. };
    self.balance += amount;  // Don't bypass apply!
    self.version = self.version.next();  // Don't manage version manually!
    Ok(())
}
```

### 7. Testing Aggregates

Test behavior, not implementation:

```rust
#[test]
fn test_withdraw_reduces_balance() {
    let mut account = BankAccount::open(
        AccountId::new(),
        "Alice".to_string(),
        1000,
    ).unwrap();

    account.withdraw(300).unwrap();

    assert_eq!(account.balance(), 700);
    assert_eq!(account.pending_events().len(), 2); // Open + Withdraw
}

#[test]
fn test_withdraw_insufficient_funds() {
    let mut account = BankAccount::open(
        AccountId::new(),
        "Alice".to_string(),
        100,
    ).unwrap();

    let result = account.withdraw(200);

    assert!(matches!(
        result,
        Err(BankAccountError::InsufficientFunds { .. })
    ));
}
```

## Advanced Topics

### Aggregate References

Reference other aggregates by ID only:

```rust
struct Order {
    id: OrderId,
    customer_id: CustomerId,  // Reference, not embedded
    items: Vec<OrderItem>,
}
```

### Eventual Consistency

Coordinate between aggregates using events:

```rust
// In Order aggregate
fn place_order(&mut self) -> Result<(), OrderError> {
    let event = OrderPlaced { order_id: self.id, .. };
    self.apply(&event);
    self.pending_events.push(event);
    Ok(())
}

// Separate event handler updates Inventory aggregate
async fn handle_order_placed(event: OrderPlaced, inventory: &mut Inventory) {
    inventory.reserve_items(event.items).await;
}
```

### Aggregate Lifecycle

```rust
impl Order {
    // Creation
    fn create(id: OrderId, customer_id: CustomerId) -> Result<Self, OrderError> {
        // ...
    }

    // State transitions
    fn confirm(&mut self) -> Result<(), OrderError> { ... }
    fn ship(&mut self) -> Result<(), OrderError> { ... }
    fn complete(&mut self) -> Result<(), OrderError> { ... }

    // Termination
    fn cancel(&mut self) -> Result<(), OrderError> { ... }
}
```

## Summary

Aggregates in event-sauce provide:

- **Type-safe domain models** with derive macros
- **Rich error handling** with aggregate-specific errors
- **Clear boundaries** for consistency and transactions
- **Event-driven state** with replay support
- **Business rule enforcement** with validation

Next: [Events Guide](events.md) | [Validation Guide](validation.md)
