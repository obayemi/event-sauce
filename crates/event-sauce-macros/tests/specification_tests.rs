//! Integration tests for #[specification] attribute macro.

use event_sauce_core::Specification;

// ============================================================================
// Test Types
// ============================================================================

#[derive(Debug)]
struct Account {
    balance: i64,
    status: AccountStatus,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AccountStatus {
    Active,
    Frozen,
}

// ============================================================================
// Simple static message (no extra params)
// ============================================================================

#[event_sauce_macros::specification("Account must be active")]
fn is_active(account: &Account) -> bool {
    account.status == AccountStatus::Active
}

#[test]
fn test_specification_simple_struct_created() {
    // Verify IsActive struct exists and is a unit struct
    let _spec = IsActive;
}

#[test]
fn test_specification_simple_satisfied() {
    let account = Account {
        balance: 100,
        status: AccountStatus::Active,
    };
    assert!(IsActive.is_satisfied_by(&account));
}

#[test]
fn test_specification_simple_unsatisfied() {
    let account = Account {
        balance: 100,
        status: AccountStatus::Frozen,
    };
    assert!(!IsActive.is_satisfied_by(&account));
}

#[test]
fn test_specification_simple_error_message() {
    let account = Account {
        balance: 100,
        status: AccountStatus::Frozen,
    };
    assert_eq!(IsActive.error_message(&account), "Account must be active");
}

#[test]
fn test_specification_simple_check() {
    let active = Account {
        balance: 100,
        status: AccountStatus::Active,
    };
    let frozen = Account {
        balance: 100,
        status: AccountStatus::Frozen,
    };
    assert!(IsActive.check(&active).is_ok());
    assert!(IsActive.check(&frozen).is_err());
}

// ============================================================================
// Parameterized specification (with extra params -> struct fields)
// ============================================================================

#[event_sauce_macros::specification("Insufficient funds", requested = amount)]
fn has_sufficient_funds(account: &Account, amount: i64) -> bool {
    account.balance >= *amount
}

#[test]
fn test_specification_parameterized_struct_created() {
    let _spec = HasSufficientFunds { amount: 100 };
}

#[test]
fn test_specification_parameterized_satisfied() {
    let spec = HasSufficientFunds { amount: 50 };
    let account = Account {
        balance: 100,
        status: AccountStatus::Active,
    };
    assert!(spec.is_satisfied_by(&account));
}

#[test]
fn test_specification_parameterized_unsatisfied() {
    let spec = HasSufficientFunds { amount: 200 };
    let account = Account {
        balance: 100,
        status: AccountStatus::Active,
    };
    assert!(!spec.is_satisfied_by(&account));
}

#[test]
fn test_specification_parameterized_error_message() {
    let spec = HasSufficientFunds { amount: 200 };
    let account = Account {
        balance: 100,
        status: AccountStatus::Active,
    };
    assert_eq!(
        spec.error_message(&account),
        "Insufficient funds: requested=200"
    );
}

#[test]
fn test_specification_parameterized_check() {
    let spec = HasSufficientFunds { amount: 50 };
    let account = Account {
        balance: 100,
        status: AccountStatus::Active,
    };
    assert!(spec.check(&account).is_ok());

    let spec = HasSufficientFunds { amount: 200 };
    let result = spec.check(&account);
    assert!(result.is_err());
    assert_eq!(
        result.unwrap_err().message,
        "Insufficient funds: requested=200"
    );
}

// ============================================================================
// Context referencing both candidate and spec fields
// ============================================================================

#[event_sauce_macros::specification(
    "Insufficient funds",
    balance = account.balance,
    requested = amount
)]
fn has_sufficient_funds_detailed(account: &Account, amount: i64) -> bool {
    account.balance >= *amount
}

#[test]
fn test_specification_mixed_context_error_message() {
    let spec = HasSufficientFundsDetailed { amount: 200 };
    let account = Account {
        balance: 100,
        status: AccountStatus::Active,
    };
    assert_eq!(
        spec.error_message(&account),
        "Insufficient funds: balance=100, requested=200"
    );
}

#[test]
fn test_specification_mixed_context_satisfied() {
    let spec = HasSufficientFundsDetailed { amount: 50 };
    let account = Account {
        balance: 100,
        status: AccountStatus::Active,
    };
    assert!(spec.check(&account).is_ok());
}

// ============================================================================
// Pascal case conversion
// ============================================================================

#[event_sauce_macros::specification("Balance must be positive")]
fn has_positive_balance(account: &Account) -> bool {
    account.balance > 0
}

#[test]
fn test_specification_pascal_case_multi_word() {
    // Function `has_positive_balance` -> struct `HasPositiveBalance`
    let _spec = HasPositiveBalance;
    let account = Account {
        balance: 100,
        status: AccountStatus::Active,
    };
    assert!(HasPositiveBalance.is_satisfied_by(&account));
}

// ============================================================================
// Composability with combinators
// ============================================================================

#[test]
fn test_specification_composable_with_and() {
    let spec = IsActive.and(HasPositiveBalance);
    let account = Account {
        balance: 100,
        status: AccountStatus::Active,
    };
    assert!(spec.check(&account).is_ok());
}

#[test]
fn test_specification_composable_with_and_fails() {
    let spec = IsActive.and(HasPositiveBalance);
    let account = Account {
        balance: -50,
        status: AccountStatus::Active,
    };
    assert!(spec.check(&account).is_err());
}

#[test]
fn test_specification_composable_with_or() {
    let spec = IsActive.or(HasPositiveBalance);
    let account = Account {
        balance: -50,
        status: AccountStatus::Active,
    };
    // Active but negative balance -> OR succeeds
    assert!(spec.check(&account).is_ok());
}

#[test]
fn test_specification_composable_with_not() {
    let spec = IsActive.not();
    let account = Account {
        balance: 100,
        status: AccountStatus::Frozen,
    };
    assert!(spec.check(&account).is_ok());
}

// ============================================================================
// Parameterized specs composable
// ============================================================================

#[test]
fn test_parameterized_spec_composable() {
    let spec = IsActive.and(HasSufficientFunds { amount: 50 });
    let account = Account {
        balance: 100,
        status: AccountStatus::Active,
    };
    assert!(spec.check(&account).is_ok());

    let frozen_account = Account {
        balance: 100,
        status: AccountStatus::Frozen,
    };
    assert!(spec.check(&frozen_account).is_err());
}

// ============================================================================
// Send + Sync
// ============================================================================

#[test]
fn test_specification_is_send_sync() {
    fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<IsActive>();
    assert_send_sync::<HasPositiveBalance>();
    assert_send_sync::<HasSufficientFunds>();
}

// ============================================================================
// validate_or
// ============================================================================

#[test]
fn test_specification_validate_or() {
    let account = Account {
        balance: 100,
        status: AccountStatus::Frozen,
    };
    let result: Result<(), String> = IsActive.validate_or(&account, |msg| msg);
    assert!(result.is_err());
    assert_eq!(result.unwrap_err(), "Account must be active");
}

// ============================================================================
// Public visibility
// ============================================================================

#[event_sauce_macros::specification("Must be non-negative")]
pub fn is_non_negative(account: &Account) -> bool {
    account.balance >= 0
}

#[test]
fn test_specification_public_visibility() {
    let _spec = IsNonNegative;
    let account = Account {
        balance: 0,
        status: AccountStatus::Active,
    };
    assert!(IsNonNegative.is_satisfied_by(&account));
}
