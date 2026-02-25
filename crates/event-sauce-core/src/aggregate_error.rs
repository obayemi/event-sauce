//! Aggregate error trait for event-sourced aggregates.
//!
//! Provides the `AggregateError` marker trait for aggregate-specific errors.

use std::error::Error;

/// Marker trait for aggregate-specific errors.
///
/// All aggregate error types must implement this trait to be used with
/// the event sourcing system. This allows each aggregate to define its
/// own error types while maintaining type safety.
///
/// # Requirements
///
/// - Must implement `std::error::Error`
/// - Must be `Send + Sync` for async usage
/// - Must be `'static` lifetime
///
/// # Examples
///
/// ```
/// use event_sauce_core::AggregateError;
/// use thiserror::Error;
///
/// #[derive(Debug, Error)]
/// pub enum BankAccountError {
///     #[error("Insufficient funds: requested {requested}, available {available}")]
///     InsufficientFunds { requested: i64, available: i64 },
///
///     #[error("Account is closed")]
///     AccountClosed,
///
///     #[error("Invalid amount: {0}")]
///     InvalidAmount(i64),
/// }
///
/// impl AggregateError for BankAccountError {}
/// ```
pub trait AggregateError: Error + Send + Sync + 'static {}

#[cfg(test)]
mod tests {
    use super::*;
    use thiserror::Error;

    #[derive(Debug, Error)]
    #[error("Test error")]
    struct TestError;

    impl AggregateError for TestError {}

    #[test]
    fn test_aggregate_error_is_error() {
        fn assert_error<T: Error>() {}
        assert_error::<TestError>();
    }

    #[test]
    fn test_aggregate_error_is_static() {
        fn assert_static<T: 'static>() {}
        assert_static::<TestError>();
    }

    #[test]
    fn test_aggregate_error_can_be_boxed() {
        let error: Box<dyn AggregateError> = Box::new(TestError);
        assert_eq!(error.to_string(), "Test error");
    }

    #[test]
    fn test_aggregate_error_with_thiserror() {
        #[derive(Debug, Error)]
        enum ComplexError {
            #[error("Validation failed: {0}")]
            Validation(String),

            #[error("Not found: {id}")]
            NotFound { id: String },
        }

        impl AggregateError for ComplexError {}

        let error = ComplexError::Validation("invalid input".to_string());
        assert_eq!(error.to_string(), "Validation failed: invalid input");

        let error = ComplexError::NotFound {
            id: "123".to_string(),
        };
        assert_eq!(error.to_string(), "Not found: 123");
    }
}
