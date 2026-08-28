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

    /// Uniqueness claim conflict.
    ///
    /// Occurs when an aggregate tries to claim a value that is already
    /// held by another aggregate.
    #[error("Claim conflict: {claim_type} value already claimed{}", held_by.map(|id| format!(" by {id}")).unwrap_or_default())]
    ClaimConflict {
        /// The claim type namespace (e.g., "Profile.email").
        claim_type: String,
        /// The claim key that was attempted (from memory, not from DB).
        claim_key: serde_json::Value,
        /// The aggregate that currently holds the claim, if known.
        held_by: Option<uuid::Uuid>,
    },

    /// Leased worker lost its lease mid-run.
    ///
    /// Occurs when a leased projection or dispatcher worker tries to advance
    /// its checkpoint but no longer holds an active lease for the
    /// subscription — typically because the worker stalled past
    /// `leased_until` and another worker took over. The fenced checkpoint
    /// write is rejected and the in-flight transaction is rolled back rather
    /// than risk double-applying events or regressing the checkpoint.
    #[error("Lease lost for subscription {subscription} (worker {worker_id})")]
    LeaseLost {
        /// The subscription whose lease was lost.
        subscription: String,
        /// The worker that lost the lease.
        worker_id: String,
    },

    /// Backend / database error wrapping the original cause.
    ///
    /// Preserves the source chain so callers can inspect the underlying
    /// error (e.g. downcast to `sqlx::Error` to distinguish constraint
    /// violations from connection failures). Use [`Error::backend`] to
    /// construct.
    #[error("Backend error: {message}")]
    Backend {
        /// Human-readable context describing what operation failed.
        message: String,
        /// The underlying error from the storage backend (e.g. `sqlx::Error`).
        #[source]
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    /// Generic error with custom message.
    #[error("{0}")]
    Custom(String),
}

/// A borrow-view of an [`Error::ClaimConflict`]'s fields.
///
/// Returned by [`Error::as_claim_conflict`] and
/// [`ModifyError::save_claim_conflict`](crate::ModifyError::save_claim_conflict)
/// so a consumer can decode a save-time uniqueness ([claim](crate::AggregateClaim))
/// violation — a duplicate email, slug, share-code, … — into its own
/// user-facing error taxonomy without manually destructuring the [`Error`](enum@Error) enum.
///
/// Borrows from the underlying error, so it carries the claim namespace and the
/// conflicting key without cloning. `held_by` is preserved (it is the aggregate
/// that already holds the claim, when the backend reports it) because it is the
/// one field with real diagnostic value beyond the namespace.
#[derive(Debug, Clone, Copy)]
pub struct ClaimConflict<'a> {
    /// The claim type namespace (e.g. `"Profile.email"`).
    pub claim_type: &'a str,
    /// The value that was attempted but is already claimed.
    pub claim_key: &'a serde_json::Value,
    /// The aggregate that currently holds the claim, if the backend reported it.
    pub held_by: Option<Uuid>,
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

    /// Creates a claim conflict error.
    ///
    /// # Examples
    ///
    /// ```
    /// use event_sauce_core::Error;
    /// use serde_json::json;
    ///
    /// let error = Error::claim_conflict("Profile.email", json!("user@example.com"), None);
    /// assert!(error.is_claim_conflict());
    /// ```
    #[must_use]
    pub fn claim_conflict(
        claim_type: impl Into<String>,
        claim_key: serde_json::Value,
        held_by: Option<Uuid>,
    ) -> Self {
        Self::ClaimConflict {
            claim_type: claim_type.into(),
            claim_key,
            held_by,
        }
    }

    /// Creates a lease lost error.
    ///
    /// # Examples
    ///
    /// ```
    /// use event_sauce_core::Error;
    ///
    /// let error = Error::lease_lost("OrderTotals", "worker-1");
    /// assert!(error.is_lease_lost());
    /// ```
    #[must_use]
    pub fn lease_lost(subscription: impl Into<String>, worker_id: impl Into<String>) -> Self {
        Self::LeaseLost {
            subscription: subscription.into(),
            worker_id: worker_id.into(),
        }
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

    /// Creates a backend error from a context message and underlying cause.
    ///
    /// The `source` is preserved for inspection via [`std::error::Error::source`],
    /// so callers can downcast to e.g. `sqlx::Error` to handle specific
    /// failure modes (constraint violations, deadlocks, connection drops).
    ///
    /// # Examples
    ///
    /// ```
    /// use event_sauce_core::Error;
    /// use std::io;
    ///
    /// let cause = io::Error::other("connection reset");
    /// let error = Error::backend("Failed to fetch events", cause);
    /// assert!(error.is_backend());
    /// assert!(std::error::Error::source(&error).is_some());
    /// ```
    #[must_use]
    pub fn backend<E>(message: impl Into<String>, source: E) -> Self
    where
        E: std::error::Error + Send + Sync + 'static,
    {
        Self::Backend {
            message: message.into(),
            source: Box::new(source),
        }
    }

    /// Returns true if this is a backend error.
    #[must_use]
    pub fn is_backend(&self) -> bool {
        matches!(self, Self::Backend { .. })
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

    /// Returns true if this is an invalid state error.
    ///
    /// Terminal by construction: the stored data breaks an invariant, and it is
    /// the same data on the next pass. A caller that retries on it retries for
    /// ever.
    ///
    /// # Examples
    ///
    /// ```
    /// use event_sauce_core::Error;
    ///
    /// let error = Error::invalid_state("a snapshot with no seed anomaly");
    /// assert!(error.is_invalid_state());
    /// ```
    #[must_use]
    pub fn is_invalid_state(&self) -> bool {
        matches!(self, Self::InvalidState(_))
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

    /// Returns true if this is a claim conflict error.
    ///
    /// # Examples
    ///
    /// ```
    /// use event_sauce_core::Error;
    /// use serde_json::json;
    ///
    /// let error = Error::claim_conflict("Profile.email", json!("user@example.com"), None);
    /// assert!(error.is_claim_conflict());
    /// ```
    #[must_use]
    pub fn is_claim_conflict(&self) -> bool {
        matches!(self, Self::ClaimConflict { .. })
    }

    /// Returns a borrow-view of this error's claim-conflict fields, or `None` if
    /// it is not an [`Error::ClaimConflict`].
    ///
    /// A one-call alternative to destructuring the enum when decoding a
    /// uniqueness violation into a domain-specific error.
    ///
    /// # Examples
    ///
    /// ```
    /// use event_sauce_core::Error;
    /// use serde_json::json;
    ///
    /// let error = Error::claim_conflict("Profile.email", json!("user@example.com"), None);
    /// let conflict = error.as_claim_conflict().expect("claim conflict");
    /// assert_eq!(conflict.claim_type, "Profile.email");
    ///
    /// assert!(Error::not_found("User", "1").as_claim_conflict().is_none());
    /// ```
    #[must_use]
    pub fn as_claim_conflict(&self) -> Option<ClaimConflict<'_>> {
        match self {
            Self::ClaimConflict {
                claim_type,
                claim_key,
                held_by,
            } => Some(ClaimConflict {
                claim_type,
                claim_key,
                held_by: *held_by,
            }),
            _ => None,
        }
    }

    /// Returns true if this is a lease lost error.
    ///
    /// # Examples
    ///
    /// ```
    /// use event_sauce_core::Error;
    ///
    /// let error = Error::lease_lost("OrderTotals", "worker-1");
    /// assert!(error.is_lease_lost());
    /// ```
    #[must_use]
    pub fn is_lease_lost(&self) -> bool {
        matches!(self, Self::LeaseLost { .. })
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

        assert!(error.is_invalid_state());
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
    fn test_claim_conflict_error() {
        let error =
            Error::claim_conflict("Profile.email", serde_json::json!("user@example.com"), None);

        assert!(error.is_claim_conflict());
        assert!(!error.is_concurrency_conflict());
        assert!(!error.is_not_found());

        let message = error.to_string();
        assert!(message.contains("Claim conflict"));
        assert!(message.contains("Profile.email"));
    }

    #[test]
    fn test_claim_conflict_with_held_by() {
        let holder_id = uuid::Uuid::new_v4();
        let error = Error::claim_conflict(
            "Profile.email",
            serde_json::json!("user@example.com"),
            Some(holder_id),
        );

        let message = error.to_string();
        assert!(message.contains(&holder_id.to_string()));
    }

    #[test]
    fn test_claim_conflict_fields() {
        let holder_id = uuid::Uuid::new_v4();
        let error = Error::ClaimConflict {
            claim_type: "Profile.email".to_string(),
            claim_key: serde_json::json!("test@test.com"),
            held_by: Some(holder_id),
        };

        if let Error::ClaimConflict {
            claim_type,
            claim_key,
            held_by,
        } = error
        {
            assert_eq!(claim_type, "Profile.email");
            assert_eq!(claim_key, serde_json::json!("test@test.com"));
            assert_eq!(held_by, Some(holder_id));
        } else {
            panic!("Expected ClaimConflict variant");
        }
    }

    #[test]
    fn test_backend_error_preserves_source() {
        let cause = std::io::Error::other("connection reset");
        let error = Error::backend("Failed to fetch", cause);

        assert!(error.is_backend());
        assert!(!error.is_concurrency_conflict());

        let message = error.to_string();
        assert!(message.contains("Backend error"));
        assert!(message.contains("Failed to fetch"));

        // Source chain is preserved — callers can downcast.
        let source = std::error::Error::source(&error).expect("source preserved");
        assert!(source.to_string().contains("connection reset"));
    }

    #[test]
    fn test_lease_lost_error() {
        let error = Error::lease_lost("OrderTotals", "worker-1");

        assert!(error.is_lease_lost());
        assert!(!error.is_concurrency_conflict());
        assert!(!error.is_not_found());
        assert!(!error.is_backend());

        let message = error.to_string();
        assert!(message.contains("Lease lost"));
        assert!(message.contains("OrderTotals"));
        assert!(message.contains("worker-1"));
    }

    #[test]
    fn test_lease_lost_fields() {
        let error = Error::LeaseLost {
            subscription: "Dispatcher".to_string(),
            worker_id: "worker-7".to_string(),
        };

        if let Error::LeaseLost {
            subscription,
            worker_id,
        } = error
        {
            assert_eq!(subscription, "Dispatcher");
            assert_eq!(worker_id, "worker-7");
        } else {
            panic!("Expected LeaseLost variant");
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

    #[test]
    fn as_claim_conflict_returns_view_for_claim_conflict() {
        let holder = uuid::Uuid::new_v4();
        let error = Error::claim_conflict(
            "Profile.email",
            serde_json::json!("user@example.com"),
            Some(holder),
        );

        let view = error.as_claim_conflict().expect("claim conflict view");
        assert_eq!(view.claim_type, "Profile.email");
        assert_eq!(view.claim_key, &serde_json::json!("user@example.com"));
        assert_eq!(view.held_by, Some(holder));
    }

    #[test]
    fn as_claim_conflict_returns_none_for_other_errors() {
        assert!(Error::not_found("User", "1").as_claim_conflict().is_none());
        assert!(Error::custom("boom").as_claim_conflict().is_none());
    }
}
