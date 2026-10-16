use super::*;
use crate::test_fixtures::SimpleTestError;
use crate::{AggregateVersion, CommandError};

fn conflict() -> Error {
    Error::concurrency_conflict(AggregateVersion::new(1), AggregateVersion::new(2))
}

#[test]
fn an_aggregate_error_converts_into_a_domain_error() {
    let err: UnitOfWorkError = SimpleTestError.into();
    assert!(matches!(err, UnitOfWorkError::Domain(_)));
    assert_eq!(err.to_string(), "domain error: test error");
}

#[test]
fn question_mark_propagates_an_aggregate_error() {
    fn fails() -> Result<(), SimpleTestError> {
        Err(SimpleTestError)
    }
    fn handler() -> Result<(), UnitOfWorkError> {
        fails()?;
        Ok(())
    }
    assert!(matches!(handler(), Err(UnitOfWorkError::Domain(_))));
}

#[test]
fn a_store_error_converts_into_a_store_variant() {
    let err: UnitOfWorkError = conflict().into();
    assert!(matches!(err, UnitOfWorkError::Store(_)));
}

#[test]
fn a_command_domain_error_converts_into_a_domain_error() {
    let err: UnitOfWorkError = CommandError::Domain(SimpleTestError).into();
    assert!(matches!(err, UnitOfWorkError::Domain(_)));
}

#[test]
fn a_command_lifecycle_error_keeps_both_lifecycles() {
    let err: UnitOfWorkError = CommandError::<SimpleTestError>::Lifecycle {
        expected: Lifecycle::Active,
        found: Lifecycle::Deleted,
    }
    .into();
    assert!(matches!(
        err,
        UnitOfWorkError::Lifecycle {
            expected: Lifecycle::Active,
            found: Lifecycle::Deleted
        }
    ));
    assert_eq!(
        err.to_string(),
        "expected aggregate to be active, found deleted"
    );
}

#[test]
fn is_concurrency_conflict_only_for_a_conflicting_store_error() {
    assert!(UnitOfWorkError::from(conflict()).is_concurrency_conflict());
    assert!(!UnitOfWorkError::from(Error::custom("disk full")).is_concurrency_conflict());
    assert!(!UnitOfWorkError::LeakedHandle(1).is_concurrency_conflict());
}

#[test]
fn every_bookkeeping_variant_names_what_went_wrong() {
    let id = EntityId::new();
    let aggregate_type = AggregateType::new("Order");
    let cases = [
        (
            UnitOfWorkError::Unregistered {
                aggregate_type: aggregate_type.clone(),
            },
            "not registered",
        ),
        (
            UnitOfWorkError::AlreadyCheckedOut {
                aggregate_type: aggregate_type.clone(),
                id,
            },
            "already checked out",
        ),
        (UnitOfWorkError::LeakedHandle(2), "2 aggregate handle"),
        (
            UnitOfWorkError::DeleteFailed { aggregate_type, id },
            "delete_with",
        ),
    ];
    for (err, needle) in cases {
        assert!(err.to_string().contains(needle), "{err}");
    }
}
