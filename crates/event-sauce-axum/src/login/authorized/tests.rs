use axum::response::IntoResponse;
use event_sauce_core::{AggregateRoot, EntityId, Error};
use http::StatusCode;

use super::{permit, Access, AccessRejection, DeniedStatus};
use crate::login::test_support::{registered, Member};
use crate::test_ticket::{ticket_of, Ticket};
use crate::{DeletedStatus, StoreError};

struct Owner;

impl Access<Ticket> for Owner {
    type User = Member;

    fn allows(ticket: &AggregateRoot<Ticket>, member: &AggregateRoot<Member>) -> bool {
        ticket.owner == member.entity_id()
    }
}

struct OwnerOrForbidden;

impl Access<Ticket> for OwnerOrForbidden {
    type User = Member;
    const DENIED: DeniedStatus = DeniedStatus::Forbidden;

    fn allows(ticket: &AggregateRoot<Ticket>, member: &AggregateRoot<Member>) -> bool {
        Owner::allows(ticket, member)
    }
}

#[test]
fn a_policy_hides_the_aggregate_and_does_not_require_registration_by_default() {
    assert_eq!(Owner::DENIED, DeniedStatus::NotFound);
    const { assert!(!Owner::REGISTERED) };
}

#[test]
fn the_owner_is_permitted() {
    let member = registered();
    let ticket = ticket_of(member.entity_id());

    assert!(permit::<Ticket, Owner>(&ticket, &member).is_ok());
}

#[test]
fn somebody_else_is_denied_as_not_found_by_default() {
    let ticket = ticket_of(EntityId::new());

    let refusal = permit::<Ticket, Owner>(&ticket, &registered()).unwrap_err();

    assert!(matches!(
        refusal,
        AccessRejection::Denied(DeniedStatus::NotFound)
    ));
    assert_eq!(refusal.status(), StatusCode::NOT_FOUND);
}

#[test]
fn a_policy_may_deny_as_forbidden() {
    let ticket = ticket_of(EntityId::new());

    let refusal = permit::<Ticket, OwnerOrForbidden>(&ticket, &registered()).unwrap_err();

    assert_eq!(refusal.status(), StatusCode::FORBIDDEN);
}

#[test]
fn a_policy_that_hides_existence_hides_a_deletion_even_when_the_type_says_gone() {
    assert_eq!(
        DeniedStatus::NotFound.deleted_status(DeletedStatus::Gone),
        DeletedStatus::NotFound
    );
}

#[test]
fn a_policy_that_denies_openly_leaves_the_deleted_status_of_the_type() {
    assert_eq!(
        DeniedStatus::Forbidden.deleted_status(DeletedStatus::Gone),
        DeletedStatus::Gone
    );
}

async fn body(response: axum::response::Response) -> String {
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    String::from_utf8(bytes.to_vec()).unwrap()
}

#[tokio::test]
async fn a_hidden_denial_answers_exactly_as_a_missing_aggregate_does() {
    let hidden = AccessRejection::Denied(DeniedStatus::NotFound).into_response();
    let missing = StoreError::from(Error::not_found("Ticket", "t-1")).into_response();

    assert_eq!(hidden.status(), missing.status());
    assert_eq!(body(hidden).await, body(missing).await);
}
