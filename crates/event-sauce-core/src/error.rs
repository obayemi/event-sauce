//! Error types for event-sauce-core.
//!
//! Provides comprehensive error types for event sourcing operations.

use crate::AggregateVersion;
use thiserror::Error;

/// Result type for event sourcing operations.
pub type Result<T> = std::result::Result<T, Error>;

/// Core error type for event sourcing operations.
///
/// Covers all error cases that can occur during event sourcing:
/// - Concurrency conflicts (optimistic locking)
/// - Serialization/deserialization errors
/// - Not found errors
/// - Invalid state errors
///
/// # Examples
///
/// ```
/// use event_sauce_core::{Error, AggregateVersion};
///
/// let error = Error::concurrency_conflict(AggregateVersion::new(5), AggregateVersion::new(3));
/// assert!(matches!(error, Error::ConcurrencyConflict { .. }));
///
/// let error = Error::not_found("User", "user-123");
/// assert!(matches!(error, Error::NotFound { .. }));
/// ```
#[derive(Error, Debug)]
pub enum Error {
    /// Optimistic concurrency conflict.
    ///
    /// Occurs when trying to save events with a version that doesn't match
    /// the current version in the event store.
    #[error("Concurrency conflict: expected version {expected}, but current version is {actual}")]
    ConcurrencyConflict {
        /// The expected version.
        expected: AggregateVersion,
        /// The actual current version.
        actual: AggregateVersion,
    },

    /// Aggregate or event not found.
    #[error("Not found: {aggregate_type} with ID {aggregate_id}")]
    NotFound {
        /// The aggregate type that was not found.
        aggregate_type: String,
        /// The aggregate ID that was not found.
        aggregate_id: String,
    },

    /// Serialization error.
    #[error("Serialization error: {0}")]
    Serialization(#[from] serde_json::Error),

    /// Invalid state or operation.
    #[error("Invalid state: {0}")]
    InvalidState(String),

    /// Generic error with custom message.
    #[error("{0}")]
    Custom(String),
}

impl Error {
    /// Creates a concurrency conflict error.
    ///
    /// # Examples
    ///
    /// ```
    /// use event_sauce_core::{Error, AggregateVersion};
    ///
    /// let error = Error::concurrency_conflict(AggregateVersion::new(5), AggregateVersion::new(3));
    /// ```
    #[must_use]
    pub fn concurrency_conflict(expected: AggregateVersion, actual: AggregateVersion) -> Self {
        Self::ConcurrencyConflict { expected, actual }
    }

    /// Creates a not found error.
    ///
    /// # Examples
    ///
    /// ```
    /// use event_sauce_core::Error;
    ///
    /// let error = Error::not_found("User", "user-123");
    /// ```
    #[must_use]
    pub fn not_found(aggregate_type: impl Into<String>, aggregate_id: impl Into<String>) -> Self {
        Self::NotFound {
            aggregate_type: aggregate_type.into(),
            aggregate_id: aggregate_id.into(),
        }
    }

    /// Creates an invalid state error.
    ///
    /// # Examples
    ///
    /// ```
    /// use event_sauce_core::Error;
    ///
    /// let error = Error::invalid_state("Cannot apply event to deleted aggregate");
    /// ```
    #[must_use]
    pub fn invalid_state(message: impl Into<String>) -> Self {
        Self::InvalidState(message.into())
    }

    /// Creates a custom error.
    ///
    /// # Examples
    ///
    /// ```
    /// use event_sauce_core::Error;
    ///
    /// let error = Error::custom("Something went wrong");
    /// ```
    #[must_use]
    pub fn custom(message: impl Into<String>) -> Self {
        Self::Custom(message.into())
    }

    /// Returns true if this is a concurrency conflict error.
    ///
    /// # Examples
    ///
    /// ```
    /// use event_sauce_core::{Error, AggregateVersion};
    ///
    /// let error = Error::concurrency_conflict(AggregateVersion::new(1), AggregateVersion::new(2));
    /// assert!(error.is_concurrency_conflict());
    /// ```
    #[must_use]
    pub fn is_concurrency_conflict(&self) -> bool {
        matches!(self, Self::ConcurrencyConflict { .. })
    }

    /// Returns true if this is a not found error.
    ///
    /// # Examples
    ///
    /// ```
    /// use event_sauce_core::Error;
    ///
    /// let error = Error::not_found("User", "123");
    /// assert!(error.is_not_found());
    /// ```
    #[must_use]
    pub fn is_not_found(&self) -> bool {
        matches!(self, Self::NotFound { .. })
    }

    /// Returns true if this is a serialization error.
    ///
    /// # Examples
    ///
    /// ```
    /// use event_sauce_core::Error;
    /// use serde_json;
    ///
    /// let json_error = serde_json::from_str::<i32>("not a number").unwrap_err();
    /// let error = Error::from(json_error);
    /// assert!(error.is_serialization());
    /// ```
    #[must_use]
    pub fn is_serialization(&self) -> bool {
        matches!(self, Self::Serialization(_))
    }
}

#[cfg(test)]
#[allow(clippy::unnecessary_wraps)]
mod tests {
    use super::*;

    #[test]
    fn test_concurrency_conflict_error() {
        let error = Error::concurrency_conflict(AggregateVersion::new(5), AggregateVersion::new(3));

        assert!(error.is_concurrency_conflict());
        assert!(!error.is_not_found());
        assert!(!error.is_serialization());

        let message = error.to_string();
        assert!(message.contains("Concurrency conflict"));
        assert!(message.contains("expected version v5"));
        assert!(message.contains("current version is v3"));
    }

    #[test]
    fn test_not_found_error() {
        let error = Error::not_found("User", "user-123");

        assert!(error.is_not_found());
        assert!(!error.is_concurrency_conflict());
        assert!(!error.is_serialization());

        let message = error.to_string();
        assert!(message.contains("Not found"));
        assert!(message.contains("User"));
        assert!(message.contains("user-123"));
    }

    #[test]
    fn test_serialization_error() {
        let json_error = serde_json::from_str::<i32>("invalid").unwrap_err();
        let error = Error::from(json_error);

        assert!(error.is_serialization());
        assert!(!error.is_concurrency_conflict());
        assert!(!error.is_not_found());

        let message = error.to_string();
        assert!(message.contains("Serialization error"));
    }

    #[test]
    fn test_invalid_state_error() {
        let error = Error::invalid_state("Cannot delete aggregate");

        assert!(!error.is_concurrency_conflict());
        assert!(!error.is_not_found());
        assert!(!error.is_serialization());

        let message = error.to_string();
        assert!(message.contains("Invalid state"));
        assert!(message.contains("Cannot delete aggregate"));
    }

    #[test]
    fn test_custom_error() {
        let error = Error::custom("Custom error message");

        let message = error.to_string();
        assert_eq!(message, "Custom error message");
    }

    #[test]
    fn test_concurrency_conflict_with_matching_version() {
        let version = AggregateVersion::new(10);
        let error = Error::concurrency_conflict(version, version);

        // Even with matching versions, it should still be a concurrency error
        assert!(error.is_concurrency_conflict());
    }

    #[test]
    fn test_not_found_with_empty_strings() {
        let error = Error::not_found("", "");

        assert!(error.is_not_found());
        let message = error.to_string();
        assert!(message.contains("Not found"));
    }

    #[test]
    fn test_result_type_usage() {
        fn returns_result() -> Result<i32> {
            Ok(42)
        }

        fn returns_error() -> Result<i32> {
            Err(Error::custom("test error"))
        }

        assert_eq!(returns_result().unwrap(), 42);
        assert!(returns_error().is_err());
    }

    #[test]
    fn test_error_from_serde_json() {
        let json_error = serde_json::from_str::<Vec<i32>>("{invalid json}").unwrap_err();
        let error: Error = json_error.into();

        assert!(error.is_serialization());
    }

    #[test]
    fn test_concurrency_conflict_versions() {
        let error = Error::ConcurrencyConflict {
            expected: AggregateVersion::new(5),
            actual: AggregateVersion::new(3),
        };

        if let Error::ConcurrencyConflict { expected, actual } = error {
            assert_eq!(expected, AggregateVersion::new(5));
            assert_eq!(actual, AggregateVersion::new(3));
        } else {
            panic!("Expected ConcurrencyConflict variant");
        }
    }

    #[test]
    fn test_not_found_fields() {
        let error = Error::NotFound {
            aggregate_type: "Order".to_string(),
            aggregate_id: "order-456".to_string(),
        };

        if let Error::NotFound {
            aggregate_type,
            aggregate_id,
        } = error
        {
            assert_eq!(aggregate_type, "Order");
            assert_eq!(aggregate_id, "order-456");
        } else {
            panic!("Expected NotFound variant");
        }
    }

    #[test]
    fn test_invalid_state_message() {
        let message = "Aggregate is in invalid state";
        let error = Error::invalid_state(message);

        if let Error::InvalidState(msg) = error {
            assert_eq!(msg, message);
        } else {
            panic!("Expected InvalidState variant");
        }
    }

    #[test]
    fn test_custom_message() {
        let message = "This is a custom error";
        let error = Error::custom(message);

        if let Error::Custom(msg) = error {
            assert_eq!(msg, message);
        } else {
            panic!("Expected Custom variant");
        }
    }
}
