//! Typed error for `Repository::modify` and `Repository::modify_deleted`.
//!
//! Distinguishes the three failure modes — loading, domain logic, and saving —
//! so callers can map each to their own error taxonomy without losing the
//! typed aggregate-defined error returned by the closure.

use thiserror::Error;

/// Errors that can occur during a [`Repository::modify`](crate::Repository::modify)
/// (or [`Repository::modify_deleted`](crate::Repository::modify_deleted)) call.
///
/// Distinguishes the three failure modes so callers can map each to their own
/// `ServiceError` taxonomy without losing the typed aggregate-defined error
/// from the closure.
///
/// # Backward compatibility
///
/// A blanket `From<ModifyError<E>> for Error` impl is provided for any
/// `E: AggregateError`. Callers whose functions return `Result<_, Error>` keep
/// compiling unchanged — the error is flattened to `Error::InvalidState` for
/// domain errors and passes through for load/save errors.
///
/// # Examples
///
/// ```
/// # use event_sauce_core::{AggregateError, ModifyError, Error};
/// # use thiserror::Error as ThisError;
/// # #[derive(Debug, ThisError)]
/// # #[error("insufficient funds")]
/// # struct InsufficientFunds;
/// # impl AggregateError for InsufficientFunds {}
/// fn handle(result: Result<(), ModifyError<InsufficientFunds>>) {
///     match result {
///         Ok(()) => {}
///         Err(ModifyError::Load(e)) => eprintln!("load failed: {e}"),
///         Err(ModifyError::Domain(e)) => eprintln!("domain error: {e}"),
///         Err(ModifyError::Save(e)) => eprintln!("save failed: {e}"),
///     }
/// }
/// ```
#[derive(Debug, Error)]
pub enum ModifyError<E> {
    /// Loading the aggregate from the event store failed.
    #[error("load failed: {0}")]
    Load(#[source] crate::Error),
    /// The closure returned an aggregate-defined error.
    #[error("domain error: {0}")]
    Domain(#[source] E),
    /// Saving the aggregate back to the event store failed.
    #[error("save failed: {0}")]
    Save(#[source] crate::Error),
}

impl<E> ModifyError<E> {
    /// Returns a borrow-view of a save-time uniqueness
    /// ([claim](crate::AggregateClaim)) conflict, or `None`.
    ///
    /// Yields `Some` only for the [`Save`](ModifyError::Save) variant carrying an
    /// [`Error::ClaimConflict`](crate::Error::ClaimConflict): claim conflicts are
    /// raised when *appending* events, so a `Load` or `Domain` failure never
    /// produces one. This lets a `From<ModifyError<_>>` bridge decode a duplicate
    /// email/slug/share-code into its own error taxonomy in one call, without
    /// destructuring the variants by hand.
    ///
    /// # Examples
    ///
    /// ```
    /// # use event_sauce_core::{AggregateError, Error, ModifyError};
    /// # use thiserror::Error as ThisError;
    /// # #[derive(Debug, ThisError)]
    /// # #[error("domain")]
    /// # struct DomainErr;
    /// # impl AggregateError for DomainErr {}
    /// use serde_json::json;
    ///
    /// let err: ModifyError<DomainErr> =
    ///     ModifyError::Save(Error::claim_conflict("Group.slug", json!("weddings"), None));
    /// let conflict = err.save_claim_conflict().expect("claim conflict");
    /// assert_eq!(conflict.claim_type, "Group.slug");
    ///
    /// let domain: ModifyError<DomainErr> = ModifyError::Domain(DomainErr);
    /// assert!(domain.save_claim_conflict().is_none());
    /// ```
    #[must_use]
    pub fn save_claim_conflict(&self) -> Option<crate::ClaimConflict<'_>> {
        match self {
            ModifyError::Save(err) => err.as_claim_conflict(),
            ModifyError::Load(_) | ModifyError::Domain(_) => None,
        }
    }
}

impl<E: std::error::Error + Send + Sync + 'static> ModifyError<E> {
    /// Returns `true` if the failure came from the closure (domain logic), not load/save.
    ///
    /// # Examples
    ///
    /// ```
    /// # use event_sauce_core::{AggregateError, ModifyError, Error};
    /// # use thiserror::Error as ThisError;
    /// # #[derive(Debug, ThisError)]
    /// # #[error("boom")]
    /// # struct BoomError;
    /// # impl AggregateError for BoomError {}
    /// let err: ModifyError<BoomError> = ModifyError::Domain(BoomError);
    /// assert!(err.is_domain());
    ///
    /// let err: ModifyError<BoomError> = ModifyError::Load(Error::custom("load failed"));
    /// assert!(!err.is_domain());
    /// ```
    #[must_use]
    pub fn is_domain(&self) -> bool {
        matches!(self, Self::Domain(_))
    }
}

/// Flattens a `ModifyError<E>` into the framework's `Error` type.
///
/// - `Load` and `Save` variants pass through their inner `crate::Error` unchanged.
/// - `Domain` variant is converted via the `AggregateError` blanket
///   (`E` → `Error::InvalidState(message)`).
///
/// This impl enables `?` propagation in functions returning `Result<_, Error>`
/// without any callsite changes when moving from the old `modify` signature.
impl<E: crate::AggregateError> From<ModifyError<E>> for crate::Error {
    fn from(e: ModifyError<E>) -> Self {
        match e {
            ModifyError::Load(err) | ModifyError::Save(err) => err,
            ModifyError::Domain(e) => e.into(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use thiserror::Error;

    #[derive(Debug, Error)]
    #[error("Test error")]
    struct TestError;

    impl crate::AggregateError for TestError {}

    // --- is_domain() ---

    #[test]
    fn is_domain_returns_true_for_domain_variant() {
        let err: ModifyError<TestError> = ModifyError::Domain(TestError);
        assert!(err.is_domain());
    }

    #[test]
    fn is_domain_returns_false_for_load_variant() {
        let err: ModifyError<TestError> = ModifyError::Load(crate::Error::custom("load failed"));
        assert!(!err.is_domain());
    }

    #[test]
    fn is_domain_returns_false_for_save_variant() {
        let err: ModifyError<TestError> = ModifyError::Save(crate::Error::custom("save failed"));
        assert!(!err.is_domain());
    }

    // --- From<ModifyError<E>> for crate::Error ---

    #[test]
    fn load_variant_flattens_to_inner_error() {
        let inner = crate::Error::not_found("User", "u1");
        let err: ModifyError<TestError> = ModifyError::Load(inner);
        let flat: crate::Error = err.into();
        assert!(flat.is_not_found());
    }

    #[test]
    fn save_variant_flattens_to_inner_error() {
        let inner = crate::Error::concurrency_conflict(
            crate::AggregateVersion::new(1),
            crate::AggregateVersion::new(2),
        );
        let err: ModifyError<TestError> = ModifyError::Save(inner);
        let flat: crate::Error = err.into();
        assert!(flat.is_concurrency_conflict());
    }

    #[test]
    fn domain_variant_flattens_via_aggregate_error_blanket() {
        let err: ModifyError<TestError> = ModifyError::Domain(TestError);
        let flat: crate::Error = err.into();
        assert!(matches!(
            flat,
            crate::Error::InvalidState(ref msg) if msg == "Test error"
        ));
    }

    // --- Display messages ---

    #[test]
    fn load_variant_display_contains_inner_message() {
        let err: ModifyError<TestError> =
            ModifyError::Load(crate::Error::custom("store unavailable"));
        assert!(err.to_string().contains("load failed"));
        assert!(err.to_string().contains("store unavailable"));
    }

    #[test]
    fn save_variant_display_contains_inner_message() {
        let err: ModifyError<TestError> = ModifyError::Save(crate::Error::custom("write timeout"));
        assert!(err.to_string().contains("save failed"));
        assert!(err.to_string().contains("write timeout"));
    }

    #[test]
    fn domain_variant_display_contains_error_message() {
        let err: ModifyError<TestError> = ModifyError::Domain(TestError);
        assert!(err.to_string().contains("domain error"));
        assert!(err.to_string().contains("Test error"));
    }

    // --- Backward-compat: ? propagation into Result<_, Error> ---

    #[test]
    fn question_mark_propagation_from_load_error() {
        fn handler(
            result: Result<(), ModifyError<TestError>>,
        ) -> std::result::Result<(), crate::Error> {
            result?;
            Ok(())
        }
        let result = handler(Err(ModifyError::Load(crate::Error::not_found("X", "1"))));
        assert!(result.unwrap_err().is_not_found());
    }

    #[test]
    fn question_mark_propagation_from_domain_error() {
        fn handler(
            result: Result<(), ModifyError<TestError>>,
        ) -> std::result::Result<(), crate::Error> {
            result?;
            Ok(())
        }
        let result = handler(Err(ModifyError::Domain(TestError)));
        assert!(matches!(result.unwrap_err(), crate::Error::InvalidState(_)));
    }

    // --- save_claim_conflict() ---

    #[test]
    fn save_claim_conflict_returns_view_for_save_claim_conflict() {
        let err: ModifyError<TestError> = ModifyError::Save(crate::Error::claim_conflict(
            "Group.slug",
            serde_json::json!("weddings"),
            None,
        ));

        let view = err.save_claim_conflict().expect("claim conflict view");
        assert_eq!(view.claim_type, "Group.slug");
        assert_eq!(view.claim_key, &serde_json::json!("weddings"));
        assert_eq!(view.held_by, None);
    }

    #[test]
    fn save_claim_conflict_none_for_non_claim_save_error() {
        let err: ModifyError<TestError> = ModifyError::Save(crate::Error::custom("disk full"));
        assert!(err.save_claim_conflict().is_none());
    }

    #[test]
    fn save_claim_conflict_none_for_load_and_domain() {
        // Claim conflicts only arise on save; a load/domain failure never yields
        // one even if it somehow carried a ClaimConflict inner error.
        let load: ModifyError<TestError> = ModifyError::Load(crate::Error::claim_conflict(
            "X",
            serde_json::json!(1),
            None,
        ));
        assert!(load.save_claim_conflict().is_none());

        let domain: ModifyError<TestError> = ModifyError::Domain(TestError);
        assert!(domain.save_claim_conflict().is_none());
    }
}
