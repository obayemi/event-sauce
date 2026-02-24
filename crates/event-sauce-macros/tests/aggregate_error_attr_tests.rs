//! Integration tests for #[aggregate_error(for = "Type")] attribute macro.

use event_sauce_core::AggregateError as AggregateErrorTrait;
use event_sauce_core::SpecificationError;

// ============================================================================
// Test Aggregate Type (minimal for testing)
// ============================================================================

struct Account;

// ============================================================================
// Basic Usage
// ============================================================================

#[event_sauce_macros::aggregate_error(aggregate = "Account")]
#[derive(Debug, thiserror::Error)]
enum AccountError {
    #[error("Account is closed")]
    AccountClosed,
}

#[test]
fn test_aggregate_error_attr_implements_trait() {
    fn assert_aggregate_error<T: AggregateErrorTrait>() {}
    assert_aggregate_error::<AccountError>();
}

#[test]
fn test_aggregate_error_attr_has_spec_variant() {
    let spec_err = SpecificationError::<Account>::new("balance too low".to_string());
    let err = AccountError::SpecificationFailed(spec_err);
    assert_eq!(err.to_string(), "balance too low");
}

#[test]
fn test_aggregate_error_attr_from_conversion() {
    let spec_err = SpecificationError::<Account>::new("must be active".to_string());
    let err: AccountError = spec_err.into();
    match err {
        AccountError::SpecificationFailed(e) => {
            assert_eq!(e.message, "must be active");
        }
        _ => panic!("Wrong variant"),
    }
}

#[test]
fn test_aggregate_error_attr_question_mark_operator() {
    fn check_account() -> Result<(), AccountError> {
        let spec_err = SpecificationError::<Account>::new("failed check".to_string());
        Err(spec_err)?;
        Ok(())
    }

    let result = check_account();
    assert!(result.is_err());
    match result.unwrap_err() {
        AccountError::SpecificationFailed(e) => {
            assert_eq!(e.message, "failed check");
        }
        _ => panic!("Wrong variant"),
    }
}

#[test]
fn test_aggregate_error_attr_original_variants_preserved() {
    let err = AccountError::AccountClosed;
    assert_eq!(err.to_string(), "Account is closed");
}

#[test]
fn test_aggregate_error_attr_is_send() {
    fn assert_send<T: Send>() {}
    assert_send::<AccountError>();
}

#[test]
fn test_aggregate_error_attr_is_sync() {
    fn assert_sync<T: Sync>() {}
    assert_sync::<AccountError>();
}

#[test]
fn test_aggregate_error_attr_is_error() {
    fn assert_error<T: std::error::Error>() {}
    assert_error::<AccountError>();
}

// ============================================================================
// With Multiple Variants
// ============================================================================

struct Order;

#[event_sauce_macros::aggregate_error(aggregate = "Order")]
#[derive(Debug, thiserror::Error)]
enum OrderError {
    #[error("Order already completed")]
    OrderAlreadyCompleted,

    #[error("Invalid quantity: {0}")]
    InvalidQuantity(u32),

    #[error("Invalid amount: {amount}")]
    InvalidAmount { amount: i64 },
}

#[test]
fn test_multiple_variants_preserved() {
    let err1 = OrderError::OrderAlreadyCompleted;
    assert_eq!(err1.to_string(), "Order already completed");

    let err2 = OrderError::InvalidQuantity(0);
    assert_eq!(err2.to_string(), "Invalid quantity: 0");

    let err3 = OrderError::InvalidAmount { amount: -100 };
    assert_eq!(err3.to_string(), "Invalid amount: -100");
}

#[test]
fn test_order_error_spec_conversion() {
    let spec_err = SpecificationError::<Order>::new("order must be pending".to_string());
    let err: OrderError = spec_err.into();
    assert_eq!(err.to_string(), "order must be pending");
}

#[test]
fn test_order_error_implements_trait() {
    fn assert_aggregate_error<T: AggregateErrorTrait>() {}
    assert_aggregate_error::<OrderError>();
}

// ============================================================================
// Integration with Specification check()
// ============================================================================

#[test]
fn test_spec_check_with_aggregate_error() {
    use event_sauce_core::Specification;

    struct IsActive;

    impl Specification<Account> for IsActive {
        fn is_satisfied_by(&self, _candidate: &Account) -> bool {
            false // always fails for test
        }

        fn error_message(&self, _candidate: &Account) -> String {
            "Account must be active".to_string()
        }
    }

    fn validate_account(account: &Account) -> Result<(), AccountError> {
        IsActive.check(account)?; // SpecificationError<Account> -> AccountError via From
        Ok(())
    }

    let account = Account;
    let result = validate_account(&account);
    assert!(result.is_err());
    match result.unwrap_err() {
        AccountError::SpecificationFailed(e) => {
            assert_eq!(e.message, "Account must be active");
        }
        _ => panic!("Wrong variant"),
    }
}

// ============================================================================
// Pattern Matching
// ============================================================================

#[test]
fn test_pattern_matching_all_variants() {
    let errors: Vec<AccountError> = vec![
        AccountError::AccountClosed,
        AccountError::SpecificationFailed(SpecificationError::<Account>::new(
            "spec fail".to_string(),
        )),
    ];

    let mut closed = 0;
    let mut spec_failed = 0;

    for error in errors {
        match error {
            AccountError::AccountClosed => closed += 1,
            AccountError::SpecificationFailed(_) => spec_failed += 1,
        }
    }

    assert_eq!(closed, 1);
    assert_eq!(spec_failed, 1);
}

#[test]
fn test_debug_formatting() {
    let err = AccountError::SpecificationFailed(SpecificationError::<Account>::new(
        "debug test".to_string(),
    ));
    let debug = format!("{err:?}");
    assert!(debug.contains("SpecificationFailed"));
}
