# Post-Validate Feature Example

## Overview

The EventSauce event system now supports optional post-validation of aggregate state after event application. This allows you to enforce business invariants that depend on the aggregate's state after an event has been applied.

## Flow

The new event application flow is:
1. **validate()** - Pre-condition validation (check if event can be applied)
2. **apply()** - State transformation (apply the event to update state)
3. **post_validate()** - Post-condition validation (check aggregate invariants)
4. **Add to pending** - Only if all steps succeed

## Example

```rust
use event_sauce_core::{ApplyEvent, AggregateError};
use thiserror::Error;

#[derive(Debug, Error)]
enum BankAccountError {
    #[error("Insufficient funds: balance={balance}, requested={requested}")]
    InsufficientFunds { balance: i64, requested: i64 },

    #[error("Balance cannot be negative: {0}")]
    NegativeBalance(i64),

    #[error("Account is frozen")]
    AccountFrozen,
}

impl AggregateError for BankAccountError {}

struct BankAccount {
    balance: i64,
    is_frozen: bool,
}

struct MoneyWithdrawn {
    amount: i64,
}

impl ApplyEvent<BankAccount, BankAccountError> for MoneyWithdrawn {
    // Pre-validation: Check if withdrawal can happen
    fn validate(&self, account: &BankAccount) -> Result<(), BankAccountError> {
        if account.is_frozen {
            return Err(BankAccountError::AccountFrozen);
        }
        if account.balance < self.amount {
            return Err(BankAccountError::InsufficientFunds {
                balance: account.balance,
                requested: self.amount,
            });
        }
        Ok(())
    }

    // Apply state changes
    fn apply(&self, account: &mut BankAccount) {
        account.balance -= self.amount;
    }

    // Post-validation: Ensure invariants hold after state change
    fn post_validate(&self, account: &BankAccount) -> Result<(), BankAccountError> {
        // This catches any bugs in the apply logic
        if account.balance < 0 {
            return Err(BankAccountError::NegativeBalance(account.balance));
        }
        Ok(())
    }
}
```

## Benefits

1. **Defense in Depth**: Even if `validate()` has a bug, `post_validate()` catches invalid states
2. **Invariant Enforcement**: Ensure aggregate invariants are never violated
3. **Explicit Contracts**: Make post-conditions explicit in code
4. **Optional**: Default implementation returns `Ok(())`, only implement when needed

## When to Use

Use `post_validate()` when:
- You have critical business invariants that must never be violated
- The invariant depends on the aggregate state after applying an event
- You want defense against bugs in event application logic
- You need to enforce complex state constraints

## Backward Compatibility

- Existing events without `post_validate()` work unchanged (default returns `Ok(())`)
- The `ApplyEvent` trait provides a default `post_validate()` implementation
- All `apply()` methods now return `Result<(), Self::Error>` for consistency
