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
/// # Conversion to `event_sauce_core::Error`
///
/// A blanket `From<E> for Error` is provided for any `E: AggregateError`,
/// converting the aggregate error into `Error::InvalidState(error.to_string())`.
/// This enables `?` propagation in policy handlers and other contexts that
/// return `event_sauce_core::Result<T>`:
///
/// ```
/// # use event_sauce_core::{AggregateError, Error};
/// # use thiserror::Error as ThisError;
/// # #[derive(Debug, ThisError)]
/// # #[error("transfer rejected")]
/// # struct TransferRejected;
/// # impl AggregateError for TransferRejected {}
/// fn handler() -> event_sauce_core::Result<()> {
///     fn check() -> Result<(), TransferRejected> { Err(TransferRejected) }
///     check()?; // auto-converts TransferRejected into Error::InvalidState
///     Ok(())
/// }
/// assert!(handler().is_err());
/// ```
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

// Blanket: any aggregate-defined error converts to the framework's `Error`
// as `Error::InvalidState(error.to_string())`. Enables `?` propagation in
// policy handlers and other contexts that return `event_sauce_core::Result<T>`.
//
// This does not conflict with the standard `impl<T> From<T> for T` reflexive
// impl, because `Error` does not — and must not — implement `AggregateError`.
impl<E: AggregateError> From<E> for crate::Error {
    fn from(error: E) -> Self {
        crate::Error::invalid_state(error.to_string())
    }
}

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
    fn test_aggregate_error_converts_to_framework_error() {
        let err: crate::Error = TestError.into();
        assert!(matches!(err, crate::Error::InvalidState(ref msg) if msg == "Test error"));
    }

    #[test]
    fn test_aggregate_error_propagates_via_question_mark() {
        fn fails() -> std::result::Result<(), TestError> {
            Err(TestError)
        }
        fn handler() -> crate::Result<i32> {
            fails()?;
            Ok(0)
        }
        let err = handler().unwrap_err();
        assert!(matches!(err, crate::Error::InvalidState(_)));
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
