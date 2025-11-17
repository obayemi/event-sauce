//! Integration tests for #[derive(AggregateError)] macro.
//!
//! These tests verify that the AggregateError derive macro correctly generates
//! the AggregateError trait implementation.

use event_sauce_core::AggregateError as AggregateErrorTrait;
use event_sauce_macros::AggregateError;
use thiserror::Error;

// ============================================================================
// Basic Tests
// ============================================================================

#[derive(AggregateError, Debug, Error)]
enum SimpleError {
    #[error("Something went wrong")]
    GenericError,
}

#[test]
fn test_aggregate_error_implements_trait() {
    // Compile-time check that AggregateError trait is implemented
    fn assert_aggregate_error<T: AggregateErrorTrait>() {}
    assert_aggregate_error::<SimpleError>();
}

#[test]
fn test_aggregate_error_with_thiserror() {
    let error = SimpleError::GenericError;
    let error_msg = format!("{}", error);
    assert_eq!(error_msg, "Something went wrong");
}

// ============================================================================
// Complex Error Types
// ============================================================================

#[derive(AggregateError, Debug, Error)]
enum BankAccountError {
    #[error("Insufficient funds: balance={balance}, requested={requested}")]
    InsufficientFunds { balance: i64, requested: i64 },

    #[error("Invalid amount: {0} (must be positive)")]
    InvalidAmount(i64),

    #[error("Account not found: {0}")]
    NotFound(String),

    #[error("Account is closed")]
    AccountClosed,
}

#[test]
fn test_complex_error_with_fields() {
    let error = BankAccountError::InsufficientFunds {
        balance: 100,
        requested: 200,
    };

    let error_msg = format!("{}", error);
    assert_eq!(
        error_msg,
        "Insufficient funds: balance=100, requested=200"
    );
}

#[test]
fn test_error_with_tuple_variant() {
    let error = BankAccountError::InvalidAmount(-50);
    let error_msg = format!("{}", error);
    assert_eq!(error_msg, "Invalid amount: -50 (must be positive)");
}

#[test]
fn test_error_with_string() {
    let error = BankAccountError::NotFound("ACC-123".to_string());
    let error_msg = format!("{}", error);
    assert_eq!(error_msg, "Account not found: ACC-123");
}

#[test]
fn test_error_unit_variant() {
    let error = BankAccountError::AccountClosed;
    let error_msg = format!("{}", error);
    assert_eq!(error_msg, "Account is closed");
}

// ============================================================================
// Error Trait Tests
// ============================================================================

#[test]
fn test_aggregate_error_is_error() {
    fn assert_is_error<T: std::error::Error>() {}
    assert_is_error::<BankAccountError>();
}

#[test]
fn test_aggregate_error_is_send() {
    fn assert_send<T: Send>() {}
    assert_send::<BankAccountError>();
}

#[test]
fn test_aggregate_error_is_sync() {
    fn assert_sync<T: Sync>() {}
    assert_sync::<BankAccountError>();
}

#[test]
fn test_aggregate_error_is_static() {
    fn assert_static<T: 'static>() {}
    assert_static::<BankAccountError>();
}

#[test]
fn test_aggregate_error_can_be_boxed() {
    let error: Box<dyn std::error::Error> = Box::new(BankAccountError::AccountClosed);
    let error_msg = format!("{}", error);
    assert_eq!(error_msg, "Account is closed");
}

// ============================================================================
// Multiple Error Types
// ============================================================================

#[derive(AggregateError, Debug, Error)]
enum OrderError {
    #[error("Order not found")]
    NotFound,

    #[error("Invalid quantity: {0}")]
    InvalidQuantity(u32),
}

#[derive(AggregateError, Debug, Error)]
enum ShoppingCartError {
    #[error("Cart is empty")]
    EmptyCart,

    #[error("Item not found: {0}")]
    ItemNotFound(String),
}

#[test]
fn test_multiple_error_types_are_distinct() {
    // Compile-time check that they're different types
    fn process_order(_error: OrderError) {}
    fn process_cart(_error: ShoppingCartError) {}

    let order_error = OrderError::NotFound;
    let cart_error = ShoppingCartError::EmptyCart;

    process_order(order_error);
    process_cart(cart_error);

    // Cannot pass wrong type:
    // process_order(cart_error); // Compilation error!
}

#[test]
fn test_multiple_error_types_implement_trait() {
    fn assert_aggregate_error<T: AggregateErrorTrait>() {}

    assert_aggregate_error::<OrderError>();
    assert_aggregate_error::<ShoppingCartError>();
}

// ============================================================================
// Result Type Usage
// ============================================================================

#[test]
fn test_use_in_result_type() {
    fn withdraw(amount: i64, balance: i64) -> Result<i64, BankAccountError> {
        if amount <= 0 {
            return Err(BankAccountError::InvalidAmount(amount));
        }

        if balance < amount {
            return Err(BankAccountError::InsufficientFunds {
                balance,
                requested: amount,
            });
        }

        Ok(balance - amount)
    }

    // Test success
    let result = withdraw(50, 100);
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), 50);

    // Test invalid amount
    let result = withdraw(-10, 100);
    assert!(result.is_err());
    match result.unwrap_err() {
        BankAccountError::InvalidAmount(amt) => assert_eq!(amt, -10),
        _ => panic!("Wrong error type"),
    }

    // Test insufficient funds
    let result = withdraw(200, 100);
    assert!(result.is_err());
    match result.unwrap_err() {
        BankAccountError::InsufficientFunds { balance, requested } => {
            assert_eq!(balance, 100);
            assert_eq!(requested, 200);
        }
        _ => panic!("Wrong error type"),
    }
}

// ============================================================================
// Pattern Matching
// ============================================================================

#[test]
fn test_pattern_matching() {
    let error = BankAccountError::InsufficientFunds {
        balance: 50,
        requested: 100,
    };

    match error {
        BankAccountError::InsufficientFunds { balance, requested } => {
            assert_eq!(balance, 50);
            assert_eq!(requested, 100);
        }
        _ => panic!("Wrong error variant"),
    }
}

#[test]
fn test_pattern_matching_all_variants() {
    let errors = vec![
        BankAccountError::InsufficientFunds {
            balance: 50,
            requested: 100,
        },
        BankAccountError::InvalidAmount(-10),
        BankAccountError::NotFound("ACC-123".to_string()),
        BankAccountError::AccountClosed,
    ];

    let mut insufficient = 0;
    let mut invalid = 0;
    let mut not_found = 0;
    let mut closed = 0;

    for error in errors {
        match error {
            BankAccountError::InsufficientFunds { .. } => insufficient += 1,
            BankAccountError::InvalidAmount(_) => invalid += 1,
            BankAccountError::NotFound(_) => not_found += 1,
            BankAccountError::AccountClosed => closed += 1,
        }
    }

    assert_eq!(insufficient, 1);
    assert_eq!(invalid, 1);
    assert_eq!(not_found, 1);
    assert_eq!(closed, 1);
}

// ============================================================================
// Debug Formatting
// ============================================================================

#[test]
fn test_debug_formatting() {
    let error = BankAccountError::InsufficientFunds {
        balance: 100,
        requested: 200,
    };

    let debug_str = format!("{:?}", error);
    assert!(debug_str.contains("InsufficientFunds"));
    assert!(debug_str.contains("balance"));
    assert!(debug_str.contains("100"));
}

// ============================================================================
// Struct Error Types
// ============================================================================

#[derive(AggregateError, Debug, Error)]
#[error("Validation failed: {message}")]
struct ValidationError {
    message: String,
}

#[test]
fn test_struct_error_type() {
    fn assert_aggregate_error<T: AggregateErrorTrait>() {}
    assert_aggregate_error::<ValidationError>();

    let error = ValidationError {
        message: "Invalid input".to_string(),
    };

    let error_msg = format!("{}", error);
    assert_eq!(error_msg, "Validation failed: Invalid input");
}

// ============================================================================
// Real-World Usage Pattern
// ============================================================================

#[test]
fn test_realistic_aggregate_usage() {
    // Simulate a realistic aggregate command method
    struct BankAccount {
        balance: i64,
        is_active: bool,
    }

    impl BankAccount {
        fn withdraw(&mut self, amount: i64) -> Result<(), BankAccountError> {
            if !self.is_active {
                return Err(BankAccountError::AccountClosed);
            }

            if amount <= 0 {
                return Err(BankAccountError::InvalidAmount(amount));
            }

            if self.balance < amount {
                return Err(BankAccountError::InsufficientFunds {
                    balance: self.balance,
                    requested: amount,
                });
            }

            self.balance -= amount;
            Ok(())
        }
    }

    let mut account = BankAccount {
        balance: 1000,
        is_active: true,
    };

    // Successful withdrawal
    assert!(account.withdraw(300).is_ok());
    assert_eq!(account.balance, 700);

    // Insufficient funds
    let result = account.withdraw(1000);
    assert!(result.is_err());
    match result.unwrap_err() {
        BankAccountError::InsufficientFunds { balance, requested } => {
            assert_eq!(balance, 700);
            assert_eq!(requested, 1000);
        }
        _ => panic!("Wrong error"),
    }

    // Close account
    account.is_active = false;
    let result = account.withdraw(100);
    assert!(result.is_err());
    match result.unwrap_err() {
        BankAccountError::AccountClosed => {}
        _ => panic!("Wrong error"),
    }
}
