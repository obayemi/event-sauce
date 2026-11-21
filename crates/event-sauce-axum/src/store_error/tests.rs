use std::error::Error as _;

use axum::response::IntoResponse;
use event_sauce_core::{
    AggregateType, AggregateVersion, EntityId, Error, ExecuteError, Lifecycle, ModifyError,
    UnitOfWorkError,
};
use http::StatusCode;
use serde_json::json;

use super::{DeletedStatus, Split, SplitDomain, StoreError, StoreFailure};

#[derive(Debug, PartialEq, Eq, thiserror::Error)]
#[error("rejected")]
struct Rejected;

fn deleted() -> Error {
    Error::AggregateDeleted {
        aggregate_type: "Note".to_owned(),
        aggregate_id: "n-1".to_owned(),
    }
}

fn conflict() -> Error {
    Error::concurrency_conflict(AggregateVersion::new(1), AggregateVersion::new(2))
}

fn claim_conflict() -> Error {
    Error::claim_conflict("Member.email", json!("ada@example.com"), None)
}

fn failure_of(error: impl Into<StoreError>) -> StoreFailure {
    error.into().failure()
}

fn store_of<D: std::fmt::Debug>(split: Split<D>) -> StoreError {
    match split {
        Split::Store(store) => store,
        Split::Domain(domain) => panic!("expected a store failure, got {domain:?}"),
    }
}

async fn body(response: axum::response::Response) -> String {
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    String::from_utf8(bytes.to_vec()).unwrap()
}

#[test]
fn a_missing_aggregate_is_not_found() {
    let error = StoreError::from(Error::not_found("Note", "n-1"));

    assert_eq!(error.failure(), StoreFailure::NotFound);
    assert_eq!(error.status(), StatusCode::NOT_FOUND);
}

#[test]
fn a_deleted_aggregate_is_hidden_as_not_found_by_default() {
    let error = StoreError::from(deleted());

    assert_eq!(error.failure(), StoreFailure::Deleted);
    assert_eq!(error.status(), StatusCode::NOT_FOUND);
}

#[test]
fn a_deleted_aggregate_is_gone_when_the_response_opts_in() {
    let error = StoreError::from(deleted()).deleted_as(DeletedStatus::Gone);

    assert_eq!(error.status(), StatusCode::GONE);
}

#[test]
fn a_deleted_aggregate_can_be_hidden_again() {
    let error = StoreError::from(deleted())
        .deleted_as(DeletedStatus::Gone)
        .deleted_as(DeletedStatus::NotFound);

    assert_eq!(error.status(), StatusCode::NOT_FOUND);
}

#[test]
fn the_deleted_option_leaves_the_other_failures_alone() {
    let missing = StoreError::from(Error::not_found("Note", "n-1")).deleted_as(DeletedStatus::Gone);
    let clash = StoreError::from(conflict()).deleted_as(DeletedStatus::Gone);

    assert_eq!(missing.status(), StatusCode::NOT_FOUND);
    assert_eq!(clash.status(), StatusCode::CONFLICT);
}

#[test]
fn concurrency_and_claim_conflicts_are_conflicts() {
    assert_eq!(failure_of(conflict()), StoreFailure::Conflict);
    assert_eq!(failure_of(claim_conflict()), StoreFailure::Conflict);
    assert_eq!(
        StoreError::from(claim_conflict()).status(),
        StatusCode::CONFLICT
    );
}

#[test]
fn every_other_failure_is_internal() {
    for error in [
        Error::custom("disk full"),
        Error::encryption("no key"),
        Error::InvalidState("broken".to_owned()),
        Error::KeyNotFound {
            aggregate_id: uuid::Uuid::nil(),
        },
    ] {
        let error = StoreError::from(error);

        assert_eq!(error.failure(), StoreFailure::Internal);
        assert_eq!(error.status(), StatusCode::INTERNAL_SERVER_ERROR);
    }
}

#[test]
fn a_store_error_exposes_the_error_it_wraps_as_its_source() {
    let error = StoreError::from(Error::custom("disk full"));

    assert_eq!(error.source().unwrap().to_string(), "disk full");
}

#[test]
fn a_load_or_save_failure_of_a_modify_is_the_failure_of_its_error() {
    let load: ModifyError<Rejected> = ModifyError::Load(Error::not_found("Note", "n-1"));
    let save: ModifyError<Rejected> = ModifyError::Save(conflict());

    assert_eq!(store_of(load.split()).failure(), StoreFailure::NotFound);
    assert_eq!(store_of(save.split()).failure(), StoreFailure::Conflict);
}

#[test]
fn the_domain_error_of_a_modify_goes_back_to_the_application() {
    let error: ModifyError<Rejected> = ModifyError::Domain(Rejected);

    assert!(matches!(error.split(), Split::Domain(Rejected)));
}

#[test]
fn a_load_or_save_failure_of_an_execute_is_the_failure_of_its_error() {
    let load: ExecuteError<Rejected> = ExecuteError::Load(deleted());
    let save: ExecuteError<Rejected> = ExecuteError::Save(claim_conflict());

    assert_eq!(store_of(load.split()).failure(), StoreFailure::Deleted);
    assert_eq!(store_of(save.split()).failure(), StoreFailure::Conflict);
}

#[test]
fn the_domain_error_of_an_execute_goes_back_to_the_application() {
    let error: ExecuteError<Rejected> = ExecuteError::Domain(Rejected);

    assert!(matches!(error.split(), Split::Domain(Rejected)));
}

#[test]
fn an_execute_that_found_the_wrong_lifecycle_answers_by_what_it_found() {
    let found = |found| {
        let error: ExecuteError<Rejected> = ExecuteError::Lifecycle {
            expected: Lifecycle::Missing,
            found,
        };
        store_of(error.split()).failure()
    };

    assert_eq!(found(Lifecycle::Active), StoreFailure::Conflict);
    assert_eq!(found(Lifecycle::Missing), StoreFailure::NotFound);
    assert_eq!(found(Lifecycle::Deleted), StoreFailure::Deleted);
}

#[test]
fn a_unit_of_work_that_found_the_wrong_lifecycle_answers_by_what_it_found() {
    let found = |found| {
        let error = UnitOfWorkError::Lifecycle {
            expected: Lifecycle::Active,
            found,
        };
        store_of(error.split()).failure()
    };

    assert_eq!(found(Lifecycle::Active), StoreFailure::Conflict);
    assert_eq!(found(Lifecycle::Missing), StoreFailure::NotFound);
    assert_eq!(found(Lifecycle::Deleted), StoreFailure::Deleted);
}

#[test]
fn a_unit_of_work_store_failure_is_the_failure_of_its_error() {
    assert_eq!(
        store_of(UnitOfWorkError::Store(conflict()).split()).failure(),
        StoreFailure::Conflict
    );
    assert_eq!(
        store_of(UnitOfWorkError::Store(Error::not_found("Note", "n-1")).split()).failure(),
        StoreFailure::NotFound
    );
}

#[test]
fn the_other_unit_of_work_failures_are_internal() {
    let id = EntityId::new();
    let aggregate_type = AggregateType::new("Note");
    for error in [
        UnitOfWorkError::Unregistered {
            aggregate_type: aggregate_type.clone(),
        },
        UnitOfWorkError::AlreadyCheckedOut {
            aggregate_type: aggregate_type.clone(),
            id,
        },
        UnitOfWorkError::LeakedHandle(2),
        UnitOfWorkError::DeleteFailed { aggregate_type, id },
    ] {
        assert_eq!(store_of(error.split()).failure(), StoreFailure::Internal);
    }
}

#[test]
fn the_domain_error_of_a_unit_of_work_goes_back_to_the_application() {
    let error = UnitOfWorkError::Domain(Box::new(Rejected));

    let Split::Domain(domain) = error.split() else {
        panic!("expected the domain error");
    };

    assert_eq!(domain.downcast_ref::<Rejected>(), Some(&Rejected));
}

#[tokio::test]
async fn the_bodies_are_fixed_texts_per_status() {
    let missing = StoreError::from(Error::not_found("Note", "n-1")).into_response();
    let gone = StoreError::from(deleted())
        .deleted_as(DeletedStatus::Gone)
        .into_response();
    let clash = StoreError::from(conflict()).into_response();
    let broken = StoreError::from(Error::custom("disk full")).into_response();

    assert_eq!(missing.status(), StatusCode::NOT_FOUND);
    assert_eq!(body(missing).await, "not found");
    assert_eq!(body(gone).await, "gone");
    assert_eq!(body(clash).await, "conflict");
    assert_eq!(broken.status(), StatusCode::INTERNAL_SERVER_ERROR);
    assert_eq!(body(broken).await, "internal error");
}

#[tokio::test]
async fn a_body_never_names_the_aggregate_the_claim_or_the_cause() {
    let answers = [
        StoreError::from(Error::not_found("SecretType", "secret-id")).into_response(),
        StoreError::from(deleted()).into_response(),
        StoreError::from(claim_conflict()).into_response(),
        StoreError::from(Error::custom("password=hunter2")).into_response(),
    ];

    for answer in answers {
        let text = body(answer).await;

        for secret in [
            "SecretType",
            "secret-id",
            "Note",
            "n-1",
            "ada@",
            "Member.email",
            "hunter2",
        ] {
            assert!(!text.contains(secret), "{text} leaks {secret}");
        }
    }
}
