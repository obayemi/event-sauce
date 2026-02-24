# Validation Guide

This guide explains validation strategies in event-sauce, including when to validate, how to validate, and best practices for maintaining data integrity.

## Table of Contents

- [Validation Basics](#validation-basics)
- [Validation Strategies](#validation-strategies)
- [Business Method Validation](#business-method-validation)
- [Event Validation](#event-validation)
- [Replay Without Validation](#replay-without-validation)
- [Testing Validation](#testing-validation)
- [Common Patterns](#common-patterns)
- [Specification Pattern](#specification-pattern)
- [Best Practices](#best-practices)

## Validation Basics

Validation ensures business rules and invariants are enforced before state changes occur. In event-sauce, validation happens at **command time**, not during event application.

### Key Principles

1. **Validate before creating events**: Business methods enforce rules
2. **Events are facts**: Once created, events represent immutable truths
3. **Skip validation on replay**: Historical events don't need re-validation
4. **Rich errors**: Use aggregate-specific errors for clear feedback
5. **Fail fast**: Reject invalid commands immediately

## Validation Strategies

Event-sauce supports multiple validation strategies:

### 1. Business Method Validation (Recommended)

Validate in aggregate methods before creating events:

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

        // Create and apply event (validation passed)
        let event = AccountEvent::Withdrawn {
            amount,
            timestamp: Utc::now(),
        };

        self.apply(&event);
        self.pending_events.push(event);

        Ok(())
    }
}
```

**Benefits**:
- ✅ All validation in one place
- ✅ Clear error messages
- ✅ Simple to understand
- ✅ Easy to test

### 2. Event Validation (Optional)

Implement validation on events using `ApplyEvent` trait:

```rust
impl ApplyEvent<BankAccount, AccountError> for WithdrawnEvent {
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

    fn apply(&self, account: &mut BankAccount) {
        account.balance -= self.amount;
    }
}
```

**Benefits**:
- ✅ Validation coupled with event
- ✅ Reusable validation logic
- ✅ Self-documenting events
- ❌ More complex for simple cases

### 3. Hybrid Approach (Best of Both)

Combine business method and event validation:

```rust
impl BankAccount {
    fn withdraw(&mut self, amount: i64) -> Result<(), AccountError> {
        // Quick pre-checks
        if amount <= 0 {
            return Err(AccountError::InvalidAmount(amount));
        }

        // Create event
        let event = AccountEvent::Withdrawn {
            amount,
            timestamp: Utc::now(),
        };

        // Event validates itself
        event.validate(self)?;

        // Apply and record
        self.apply(&event);
        self.pending_events.push(event);

        Ok(())
    }
}
```

**Benefits**:
- ✅ Reusable validation
- ✅ Clear separation of concerns
- ✅ Flexible and composable

## Business Method Validation

Business methods are the primary place for validation.

### Validation Categories

#### 1. Input Validation

Validate command parameters:

```rust
fn transfer(&mut self, to: AccountId, amount: i64) -> Result<(), AccountError> {
    // Validate amount
    if amount <= 0 {
        return Err(AccountError::InvalidAmount(amount));
    }

    // Validate transfer limit
    if amount > self.daily_transfer_limit {
        return Err(AccountError::TransferLimitExceeded {
            limit: self.daily_transfer_limit,
            requested: amount,
        });
    }

    // Create event...
}
```

#### 2. State Validation

Check aggregate state before operations:

```rust
fn deposit(&mut self, amount: i64) -> Result<(), AccountError> {
    // Check account status
    if self.status != AccountStatus::Active {
        return Err(AccountError::AccountNotActive(self.status));
    }

    // Check account limits
    if self.balance + amount > self.max_balance {
        return Err(AccountError::MaxBalanceExceeded);
    }

    // Create event...
}
```

#### 3. Invariant Protection

Enforce business invariants:

```rust
fn close(&mut self) -> Result<(), AccountError> {
    // Invariant: can't close account with balance
    if self.balance != 0 {
        return Err(AccountError::CannotCloseWithBalance {
            balance: self.balance,
        });
    }

    // Invariant: can't close already closed account
    if self.status == AccountStatus::Closed {
        return Err(AccountError::AlreadyClosed);
    }

    // Create event...
}
```

### Validation Order

Order validations from cheapest to most expensive:

```rust
✅ Good:
fn withdraw(&mut self, amount: i64) -> Result<(), AccountError> {
    // 1. Cheap: parameter validation
    if amount <= 0 {
        return Err(AccountError::InvalidAmount(amount));
    }

    // 2. Medium: state checks
    if self.status != AccountStatus::Active {
        return Err(AccountError::AccountNotActive(self.status));
    }

    // 3. Expensive: balance check with calculation
    if self.available_balance() < amount {
        return Err(AccountError::InsufficientFunds { .. });
    }

    // Create event...
}

❌ Bad:
fn withdraw(&mut self, amount: i64) -> Result<(), AccountError> {
    // Expensive check first!
    if self.available_balance() < amount {
        return Err(AccountError::InsufficientFunds { .. });
    }

    // Cheap check last
    if amount <= 0 {
        return Err(AccountError::InvalidAmount(amount));
    }

    // ...
}
```

## Event Validation

Events can optionally implement validation via `ApplyEvent` trait.

### Default Implementation

By default, events have no validation:

```rust
impl<A, E: AggregateError> ApplyEvent<A, E> for MyEvent {
    // Default: no validation
    fn validate(&self, _aggregate: &A) -> Result<(), E> {
        Ok(())
    }

    fn apply(&self, aggregate: &mut A) {
        // State changes...
    }
}
```

### Custom Validation

Override `validate()` for event-specific rules:

```rust
impl ApplyEvent<Order, OrderError> for ItemAddedEvent {
    fn validate(&self, order: &Order) -> Result<(), OrderError> {
        // Check order is in correct state
        if order.status != OrderStatus::Draft {
            return Err(OrderError::CannotModifySubmittedOrder);
        }

        // Check item limit
        if order.items.len() >= order.max_items {
            return Err(OrderError::TooManyItems {
                max: order.max_items,
            });
        }

        // Check quantity
        if self.quantity <= 0 {
            return Err(OrderError::InvalidQuantity(self.quantity));
        }

        Ok(())
    }

    fn apply(&self, order: &mut Order) {
        order.items.push(OrderItem {
            product_id: self.product_id,
            quantity: self.quantity,
        });
    }
}
```

### When to Use Event Validation

Use event validation when:

- ✅ Validation logic is complex and reusable
- ✅ Events might be created from multiple places
- ✅ You want self-documenting events
- ✅ Testing validation independently is valuable

Don't use event validation when:

- ❌ Validation is simple and only used once
- ❌ Business logic is tightly coupled to aggregate
- ❌ Adding complexity without clear benefit

## Replay Without Validation

Historical events skip validation during replay for performance.

### Apply vs Apply Unchecked

```rust
// Creating new events: validate
let event = AccountEvent::Withdrawn { amount: 100, .. };
account.apply(&event)?;  // May validate

// Replaying historical events: no validation
for event in historical_events {
    account.apply_unchecked(&event);  // No validation
}
```

### Implementing Apply Unchecked

The `Aggregate` trait provides a default implementation:

```rust
impl Aggregate for BankAccount {
    // ...

    // apply_unchecked is now a default method on the Aggregate trait.
    // It uses EventApplicator::dispatch_unchecked() for direct state change
    // followed by version increment.
}
```

### Why Skip Validation on Replay?

1. **Performance**: Avoid redundant validation
2. **Correctness**: Events were validated when created
3. **Determinism**: Same events always produce same state
4. **Historical accuracy**: Past events are facts

### Example: Reconstructing from History

```rust
impl BankAccount {
    /// Reconstruct account from event history
    fn from_events(id: AccountId, events: Vec<AccountEvent>) -> Self {
        let mut account = Self {
            id,
            owner: String::new(),
            balance: 0,
            status: AccountStatus::Active,
            version: Version::initial(),
            pending_events: Vec::new(),
        };

        // Fast replay without validation
        for event in events {
            account.apply_unchecked(&event);
        }

        account
    }
}
```

## Testing Validation

Comprehensive testing ensures validation works correctly.

### Test Valid Cases

Test that valid operations succeed:

```rust
#[test]
fn test_withdraw_with_sufficient_funds() {
    let mut account = BankAccount::open(
        AccountId::new(),
        "Alice".to_string(),
        1000,
    ).unwrap();

    let result = account.withdraw(300);

    assert!(result.is_ok());
    assert_eq!(account.balance(), 700);
}
```

### Test Invalid Cases

Test that invalid operations fail with correct errors:

```rust
#[test]
fn test_withdraw_insufficient_funds() {
    let mut account = BankAccount::open(
        AccountId::new(),
        "Alice".to_string(),
        100,
    ).unwrap();

    let result = account.withdraw(200);

    assert!(result.is_err());
    match result.unwrap_err() {
        AccountError::InsufficientFunds { balance, requested } => {
            assert_eq!(balance, 100);
            assert_eq!(requested, 200);
        }
        _ => panic!("Wrong error type"),
    }
}

#[test]
fn test_withdraw_negative_amount() {
    let mut account = BankAccount::open(
        AccountId::new(),
        "Alice".to_string(),
        1000,
    ).unwrap();

    let result = account.withdraw(-50);

    assert!(result.is_err());
    match result.unwrap_err() {
        AccountError::InvalidAmount(amount) => {
            assert_eq!(amount, -50);
        }
        _ => panic!("Wrong error type"),
    }
}
```

### Test Boundary Cases

Test edge cases and boundaries:

```rust
#[test]
fn test_withdraw_exact_balance() {
    let mut account = BankAccount::open(
        AccountId::new(),
        "Alice".to_string(),
        1000,
    ).unwrap();

    let result = account.withdraw(1000);

    assert!(result.is_ok());
    assert_eq!(account.balance(), 0);
}

#[test]
fn test_withdraw_zero_amount() {
    let mut account = BankAccount::open(
        AccountId::new(),
        "Alice".to_string(),
        1000,
    ).unwrap();

    let result = account.withdraw(0);

    assert!(result.is_err());
}
```

### Test State Transitions

Test validation across state changes:

```rust
#[test]
fn test_cannot_withdraw_from_frozen_account() {
    let mut account = BankAccount::open(
        AccountId::new(),
        "Alice".to_string(),
        1000,
    ).unwrap();

    // Freeze account
    account.freeze().unwrap();
    assert_eq!(account.status(), AccountStatus::Frozen);

    // Try to withdraw
    let result = account.withdraw(100);

    assert!(result.is_err());
    match result.unwrap_err() {
        AccountError::AccountNotActive(status) => {
            assert_eq!(status, AccountStatus::Frozen);
        }
        _ => panic!("Wrong error type"),
    }
}
```

## Common Patterns

### Pattern 1: Range Validation

```rust
fn set_age(&mut self, age: u8) -> Result<(), UserError> {
    if age < 18 || age > 120 {
        return Err(UserError::InvalidAge { age });
    }

    let event = UserEvent::AgeUpdated { age, .. };
    self.apply(&event);
    self.pending_events.push(event);

    Ok(())
}
```

### Pattern 2: Format Validation

```rust
fn set_email(&mut self, email: String) -> Result<(), UserError> {
    // Simple email validation
    if !email.contains('@') || !email.contains('.') {
        return Err(UserError::InvalidEmail { email });
    }

    let event = UserEvent::EmailUpdated { email, .. };
    self.apply(&event);
    self.pending_events.push(event);

    Ok(())
}
```

### Pattern 3: State Machine Validation

```rust
fn ship(&mut self) -> Result<(), OrderError> {
    // Validate state transition
    match self.status {
        OrderStatus::Confirmed => {
            // Valid transition
        }
        OrderStatus::Draft => {
            return Err(OrderError::CannotShipDraftOrder);
        }
        OrderStatus::Shipped => {
            return Err(OrderError::AlreadyShipped);
        }
        OrderStatus::Cancelled => {
            return Err(OrderError::CannotShipCancelledOrder);
        }
    }

    let event = OrderEvent::Shipped { .. };
    self.apply(&event);
    self.pending_events.push(event);

    Ok(())
}
```

### Pattern 4: Collection Validation

```rust
fn add_item(&mut self, item: OrderItem) -> Result<(), OrderError> {
    // Check collection limits
    if self.items.len() >= 100 {
        return Err(OrderError::TooManyItems);
    }

    // Check for duplicates
    if self.items.iter().any(|i| i.product_id == item.product_id) {
        return Err(OrderError::DuplicateItem {
            product_id: item.product_id,
        });
    }

    let event = OrderEvent::ItemAdded { item, .. };
    self.apply(&event);
    self.pending_events.push(event);

    Ok(())
}
```

### Pattern 5: Cross-Field Validation

```rust
fn schedule_delivery(&mut self, date: DateTime<Utc>) -> Result<(), OrderError> {
    // Validate delivery date is after order date
    if date <= self.ordered_at {
        return Err(OrderError::InvalidDeliveryDate {
            delivery: date,
            ordered: self.ordered_at,
        });
    }

    // Validate delivery date is not too far in future
    let max_date = self.ordered_at + Duration::days(30);
    if date > max_date {
        return Err(OrderError::DeliveryDateTooFar {
            date,
            max: max_date,
        });
    }

    let event = OrderEvent::DeliveryScheduled { date, .. };
    self.apply(&event);
    self.pending_events.push(event);

    Ok(())
}
```

## Specification Pattern

The Specification Pattern provides reusable, composable business rules as named objects. Specifications can be combined with AND/OR/NOT combinators and integrated directly into the validation lifecycle.

### Core Trait

```rust
use event_sauce::Specification;

// Specifications implement is_satisfied_by + error_message
// and provide check(), validate_or(), and(), or(), not() out of the box.
```

### Defining Specifications

#### With `spec!` Macro

```rust
use event_sauce::{spec, Specification};

// Simple specification with static message
spec!(IsActive for Account, "Account must be active", |account| {
    account.status == AccountStatus::Active
});

// Parameterized specification with context fields
spec!(HasSufficientFunds for Account, "Insufficient funds",
    balance = account.balance, requested = amount,
    |account, amount: i64| {
        account.balance >= *amount
    }
);
// Error message: "Insufficient funds: balance=100, requested=200"
```

#### With `#[specification]` Attribute Macro

```rust
use event_sauce_macros::specification;

#[specification("Account must be active")]
fn is_active(account: &Account) -> bool {
    account.status == AccountStatus::Active
}
// Generates: struct IsActive; impl Specification<Account> for IsActive { ... }

#[specification("Insufficient funds", requested = amount)]
fn has_sufficient_funds(account: &Account, amount: i64) -> bool {
    account.balance >= *amount
}
// Generates: struct HasSufficientFunds { pub amount: i64 }
```

### Composing Specifications

```rust
// AND: both must be satisfied
let spec = IsActive.and(HasPositiveBalance);

// OR: at least one must be satisfied
let spec = IsActive.or(HasPositiveBalance);

// NOT: inverts the specification
let spec = IsActive.not();

// Complex composition
let spec = IsActive.and(HasSufficientFunds { amount: 100 }).or(IsAdmin);
```

### Integration with Error Types

Use `#[aggregate_error]` to auto-inject a `SpecificationFailed` variant with `From` conversion:

```rust
use event_sauce_macros::aggregate_error;

#[aggregate_error(aggregate = "Order")]
#[derive(Debug, thiserror::Error)]
enum OrderError {
    #[error("Invalid amount: {0}")]
    InvalidAmount(i64),
}
// Auto-generates: SpecificationFailed(#[from] SpecificationError<Order>)
// Now `spec.check(order)?` works automatically via the `?` operator.
```

### Integration with `define_events!`

Use `@validate_spec` and `@post_validate_spec` for declarative spec-based validation:

```rust
spec!(OrderIsPending for Order, "Order must be pending", |o| {
    o.status == OrderStatus::Pending
});

define_events! {
    enum OrderEvent for Order {
        Completed {}
        @validate_spec(OrderIsPending)
        => |order, _event| {
            order.status = OrderStatus::Completed;
        },
    }
}
```

You can also use specifications inside `@validate` closures for more control:

```rust
@validate |agg, evt| {
    HasSufficientFunds { amount: evt.amount }
        .check(agg)?; // SpecificationError -> OrderError via From
    Ok(())
}
```

### Custom Error Mapping

For cases where you don't use `#[aggregate_error]`, use `validate_or`:

```rust
IsActive.validate_or(&account, |msg| AccountError::ValidationFailed(msg))?;
```

## Best Practices

### 1. Validate Early

Fail fast with early validation:

```rust
✅ Good:
fn process(&mut self, data: Data) -> Result<(), Error> {
    // Validate first
    if data.amount <= 0 {
        return Err(Error::InvalidAmount);
    }

    // Then process
    let event = ...;
    self.apply(&event);
}

❌ Bad:
fn process(&mut self, data: Data) -> Result<(), Error> {
    // Process first
    let event = ...;
    self.apply(&event);

    // Validate later (too late!)
    if data.amount <= 0 {
        return Err(Error::InvalidAmount);
    }
}
```

### 2. Rich Error Context

Include relevant context in errors:

```rust
✅ Good:
#[derive(Debug, Error)]
enum AccountError {
    #[error("Insufficient funds: balance={balance}, requested={requested}")]
    InsufficientFunds { balance: i64, requested: i64 },
}

❌ Bad:
#[derive(Debug, Error)]
enum AccountError {
    #[error("Insufficient funds")]
    InsufficientFunds,  // No context!
}
```

### 3. Clear Error Messages

Write descriptive error messages:

```rust
✅ Good:
#[error("Cannot ship order in {0:?} status (must be Confirmed)")]
CannotShipOrder(OrderStatus),

❌ Bad:
#[error("Invalid state")]
InvalidState,
```

### 4. No Validation in Apply

Keep your `ApplyEvent::apply()` implementations pure:

**Note**: With the modern pattern, you implement `ApplyEvent` trait for each event struct. The dispatching is handled by the `EventApplicator` trait, auto-generated by the `#[event(aggregate = "...")]` attribute or `define_events!` macro.

```rust
✅ Good:
impl ApplyEvent<BankAccountAggregate, AccountError> for AccountWithdrawnEvent {
    fn validate(&self, account: &BankAccountAggregate) -> Result<(), AccountError> {
        // Validation happens HERE
        if account.balance < self.amount {
            return Err(AccountError::InsufficientFunds { .. });
        }
        Ok(())
    }

    fn apply(&self, account: &mut BankAccountAggregate) {
        // Pure state change only - NO validation!
        account.balance -= self.amount;
    }
}

❌ Bad:
impl ApplyEvent<BankAccountAggregate, AccountError> for AccountWithdrawnEvent {
    fn apply(&self, account: &mut BankAccountAggregate) {
        // Don't validate in apply!
        if account.balance < self.amount {
            panic!("Insufficient funds");  // ❌ Wrong!
        }
        account.balance -= self.amount;
    }
}
```

### 5. Test All Error Paths

Ensure complete test coverage:

```rust
#[test]
fn test_all_withdrawal_errors() {
    // Test insufficient funds
    test_insufficient_funds();

    // Test invalid amount
    test_negative_amount();
    test_zero_amount();

    // Test invalid status
    test_frozen_account();
    test_closed_account();
}
```

### 6. Document Validation Rules

Make business rules explicit:

```rust
impl BankAccount {
    /// Withdraw money from the account.
    ///
    /// # Business Rules
    /// - Account must be Active
    /// - Amount must be positive
    /// - Balance must be sufficient
    ///
    /// # Errors
    /// - `AccountNotActive`: Account is frozen or closed
    /// - `InvalidAmount`: Amount is zero or negative
    /// - `InsufficientFunds`: Balance is less than amount
    fn withdraw(&mut self, amount: i64) -> Result<(), AccountError> {
        // Implementation...
    }
}
```

### 7. Use Type System

Leverage types for validation:

```rust
✅ Good:
// Amount can't be negative
#[derive(Debug, Clone, Copy)]
struct PositiveAmount(u64);

impl PositiveAmount {
    fn new(value: u64) -> Result<Self, Error> {
        if value == 0 {
            return Err(Error::AmountMustBePositive);
        }
        Ok(Self(value))
    }
}

fn withdraw(&mut self, amount: PositiveAmount) -> Result<(), Error> {
    // Amount is guaranteed positive
}

❌ Bad:
fn withdraw(&mut self, amount: i64) -> Result<(), Error> {
    // Must validate every time
    if amount <= 0 {
        return Err(Error::InvalidAmount);
    }
}
```

## Summary

Validation in event-sauce follows these principles:

- **Validate at command time**: Business methods enforce rules before creating events
- **Events are facts**: Once created, events don't need re-validation
- **Skip validation on replay**: Use `apply_unchecked()` for performance
- **Rich errors**: Use aggregate-specific error types with context
- **Test thoroughly**: Cover valid cases, invalid cases, and boundaries
- **Keep apply pure**: No validation in `ApplyEvent::apply()` methods

Next: [Aggregates Guide](aggregates.md) | [Events Guide](events.md)
