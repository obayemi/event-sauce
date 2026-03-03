//! Error types for event-sauce-core.
//!
//! Provides comprehensive error types for event sourcing operations.

use crate::AggregateVersion;
use thiserror::Error;
use uuid::Uuid;

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

    /// Encryption key not found for an encrypted aggregate.
    ///
    /// Occurs when trying to load an encrypted aggregate whose encryption key
    /// has been deleted (e.g., GDPR right-to-be-forgotten / crypto-shredding).
    #[error("Encryption key not found for aggregate {aggregate_id}")]
    KeyNotFound {
        /// The aggregate ID whose key was not found.
        aggregate_id: Uuid,
    },

    /// Encryption or decryption error.
    ///
    /// Occurs when encrypting event data before storage or
    /// decrypting event data during aggregate reconstruction.
    #[error("Encryption error: {0}")]
    Encryption(String),

    /// Aggregate has been deleted.
    ///
    /// Occurs when trying to load a deleted aggregate via `load()`.
    /// Use `load_any()` or `load_deleted()` instead.
    #[error("Aggregate deleted: {aggregate_type} with ID {aggregate_id}")]
    AggregateDeleted {
        /// The aggregate type that was deleted.
        aggregate_type: String,
        /// The aggregate ID that was deleted.
        aggregate_id: String,
    },

    /// Policy cascade depth exceeded.
    ///
    /// Occurs when an event reaction chain exceeds the configured maximum
    /// cascade depth, indicating a potential infinite loop.
    #[error("Cascade depth exceeded: {depth} > {max_depth}")]
    CascadeDepthExceeded {
        /// The current cascade depth.
        depth: usize,
        /// The maximum allowed cascade depth.
        max_depth: usize,
    },

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

    /// Creates a key not found error.
    ///
    /// # Examples
    ///
    /// ```
    /// use event_sauce_core::Error;
    /// use uuid::Uuid;
    ///
    /// let error = Error::key_not_found(Uuid::nil());
    /// ```
    #[must_use]
    pub fn key_not_found(aggregate_id: Uuid) -> Self {
        Self::KeyNotFound { aggregate_id }
    }

    /// Creates an encryption error.
    ///
    /// # Examples
    ///
    /// ```
    /// use event_sauce_core::Error;
    ///
    /// let error = Error::encryption("decryption failed");
    /// ```
    #[must_use]
    pub fn encryption(message: impl Into<String>) -> Self {
        Self::Encryption(message.into())
    }

    /// Creates an aggregate deleted error.
    ///
    /// # Examples
    ///
    /// ```
    /// use event_sauce_core::Error;
    ///
    /// let error = Error::aggregate_deleted("User", "user-123");
    /// ```
    #[must_use]
    pub fn aggregate_deleted(
        aggregate_type: impl Into<String>,
        aggregate_id: impl Into<String>,
    ) -> Self {
        Self::AggregateDeleted {
            aggregate_type: aggregate_type.into(),
            aggregate_id: aggregate_id.into(),
        }
    }

    /// Creates a cascade depth exceeded error.
    ///
    /// # Examples
    ///
    /// ```
    /// use event_sauce_core::Error;
    ///
    /// let error = Error::cascade_depth_exceeded(11, 10);
    /// assert!(error.is_cascade_depth_exceeded());
    /// ```
    #[must_use]
    pub fn cascade_depth_exceeded(depth: usize, max_depth: usize) -> Self {
        Self::CascadeDepthExceeded { depth, max_depth }
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

    /// Returns true if this is a key not found error.
    ///
    /// # Examples
    ///
    /// ```
    /// use event_sauce_core::Error;
    /// use uuid::Uuid;
    ///
    /// let error = Error::key_not_found(Uuid::nil());
    /// assert!(error.is_key_not_found());
    /// ```
    #[must_use]
    pub fn is_key_not_found(&self) -> bool {
        matches!(self, Self::KeyNotFound { .. })
    }

    /// Returns true if this is an encryption error.
    ///
    /// # Examples
    ///
    /// ```
    /// use event_sauce_core::Error;
    ///
    /// let error = Error::encryption("failed");
    /// assert!(error.is_encryption());
    /// ```
    #[must_use]
    pub fn is_encryption(&self) -> bool {
        matches!(self, Self::Encryption(_))
    }

    /// Returns true if this is an aggregate deleted error.
    ///
    /// # Examples
    ///
    /// ```
    /// use event_sauce_core::Error;
    ///
    /// let error = Error::aggregate_deleted("User", "123");
    /// assert!(error.is_aggregate_deleted());
    /// ```
    #[must_use]
    pub fn is_aggregate_deleted(&self) -> bool {
        matches!(self, Self::AggregateDeleted { .. })
    }

    /// Returns true if this is a cascade depth exceeded error.
    ///
    /// # Examples
    ///
    /// ```
    /// use event_sauce_core::Error;
    ///
    /// let error = Error::cascade_depth_exceeded(11, 10);
    /// assert!(error.is_cascade_depth_exceeded());
    /// ```
    #[must_use]
    pub fn is_cascade_depth_exceeded(&self) -> bool {
        matches!(self, Self::CascadeDepthExceeded { .. })
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

    #[test]
    fn test_key_not_found_error() {
        let id = uuid::Uuid::new_v4();
        let error = Error::key_not_found(id);

        assert!(error.is_key_not_found());
        assert!(!error.is_concurrency_conflict());
        assert!(!error.is_not_found());
        assert!(!error.is_serialization());
        assert!(!error.is_encryption());

        let message = error.to_string();
        assert!(message.contains("Encryption key not found"));
        assert!(message.contains(&id.to_string()));
    }

    #[test]
    fn test_key_not_found_fields() {
        let id = uuid::Uuid::new_v4();
        let error = Error::KeyNotFound { aggregate_id: id };

        if let Error::KeyNotFound { aggregate_id } = error {
            assert_eq!(aggregate_id, id);
        } else {
            panic!("Expected KeyNotFound variant");
        }
    }

    #[test]
    fn test_encryption_error() {
        let error = Error::encryption("decryption failed: invalid key");

        assert!(error.is_encryption());
        assert!(!error.is_concurrency_conflict());
        assert!(!error.is_not_found());
        assert!(!error.is_serialization());
        assert!(!error.is_key_not_found());

        let message = error.to_string();
        assert!(message.contains("Encryption error"));
        assert!(message.contains("decryption failed: invalid key"));
    }

    #[test]
    fn test_encryption_error_message() {
        let msg = "AES-GCM auth tag mismatch";
        let error = Error::encryption(msg);

        if let Error::Encryption(message) = error {
            assert_eq!(message, msg);
        } else {
            panic!("Expected Encryption variant");
        }
    }

    #[test]
    fn test_aggregate_deleted_error() {
        let error = Error::aggregate_deleted("User", "user-123");

        assert!(error.is_aggregate_deleted());
        assert!(!error.is_concurrency_conflict());
        assert!(!error.is_not_found());
        assert!(!error.is_serialization());
        assert!(!error.is_key_not_found());
        assert!(!error.is_encryption());
        assert!(!error.is_cascade_depth_exceeded());

        let message = error.to_string();
        assert!(message.contains("Aggregate deleted"));
        assert!(message.contains("User"));
        assert!(message.contains("user-123"));
    }

    #[test]
    fn test_cascade_depth_exceeded_error() {
        let error = Error::cascade_depth_exceeded(11, 10);

        assert!(error.is_cascade_depth_exceeded());
        assert!(!error.is_concurrency_conflict());
        assert!(!error.is_not_found());

        let message = error.to_string();
        assert!(message.contains("Cascade depth exceeded"));
        assert!(message.contains("11"));
        assert!(message.contains("10"));
    }

    #[test]
    fn test_cascade_depth_exceeded_fields() {
        let error = Error::CascadeDepthExceeded {
            depth: 15,
            max_depth: 10,
        };

        if let Error::CascadeDepthExceeded { depth, max_depth } = error {
            assert_eq!(depth, 15);
            assert_eq!(max_depth, 10);
        } else {
            panic!("Expected CascadeDepthExceeded variant");
        }
    }

    #[test]
    fn test_aggregate_deleted_fields() {
        let error = Error::AggregateDeleted {
            aggregate_type: "Order".to_string(),
            aggregate_id: "order-456".to_string(),
        };

        if let Error::AggregateDeleted {
            aggregate_type,
            aggregate_id,
        } = error
        {
            assert_eq!(aggregate_type, "Order");
            assert_eq!(aggregate_id, "order-456");
        } else {
            panic!("Expected AggregateDeleted variant");
        }
    }
}
