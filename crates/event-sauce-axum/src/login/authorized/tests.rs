use std::sync::Arc;

use event_sauce_core::{AggregateRoot, EntityId};
use http::request::Parts;
use http::{Request, StatusCode};

use super::{admit, deleted_status, permit, Access, AccessRejection, DeniedStatus};
use crate::login::test_support::{anonymous, registered, Member};
use crate::test_ticket::{ticket_of, Ticket};
use crate::{AuthRejection, DeletedStatus, LoginRedirect, ReturnTo};

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

struct RegisteredOwner;

impl Access<Ticket> for RegisteredOwner {
    type User = Member;
    const REGISTERED: bool = true;

    fn allows(ticket: &AggregateRoot<Ticket>, member: &AggregateRoot<Member>) -> bool {
        Owner::allows(ticket, member)
    }
}

fn bare() -> Parts {
    Request::builder()
        .uri("/notes/1")
        .body(())
        .unwrap()
        .into_parts()
        .0
}

fn redirecting() -> Parts {
    let mut parts = bare();
    parts.extensions.insert(
        LoginRedirect::to(ReturnTo::from_static("/login"))
            .register_at(ReturnTo::from_static("/register")),
    );
    parts
}

#[test]
fn a_policy_hides_the_aggregate_and_does_not_require_registration_by_default() {
    assert_eq!(Owner::DENIED, DeniedStatus::NotFound);
    const { assert!(!Owner::REGISTERED) };
}

#[test]
fn nobody_logged_in_is_refused_before_anything_else() {
    let refusal = admit::<Ticket, Owner>(&bare(), None).unwrap_err();

    assert!(matches!(
        refusal,
        AccessRejection::Auth(AuthRejection::Unauthenticated)
    ));
    assert_eq!(refusal.status(), StatusCode::UNAUTHORIZED);
}

#[test]
fn an_anonymous_user_is_admitted_unless_the_policy_wants_a_registered_one() {
    let user = Arc::new(anonymous());

    assert!(admit::<Ticket, Owner>(&bare(), Some(Arc::clone(&user))).is_ok());
    let refusal = admit::<Ticket, RegisteredOwner>(&bare(), Some(user)).unwrap_err();
    assert!(matches!(
        refusal,
        AccessRejection::Auth(AuthRejection::Forbidden)
    ));
    assert_eq!(refusal.status(), StatusCode::FORBIDDEN);
}

#[test]
fn a_registered_user_is_admitted_under_a_registered_policy() {
    assert!(admit::<Ticket, RegisteredOwner>(&bare(), Some(Arc::new(registered()))).is_ok());
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
        deleted_status::<Ticket, Owner>(DeletedStatus::Gone),
        DeletedStatus::NotFound
    );
}

#[test]
fn a_policy_that_denies_openly_leaves_the_deleted_status_of_the_type() {
    assert_eq!(
        deleted_status::<Ticket, OwnerOrForbidden>(DeletedStatus::Gone),
        DeletedStatus::Gone
    );
}

#[test]
fn nobody_logged_in_is_redirected_under_a_login_redirect() {
    let refusal = admit::<Ticket, Owner>(&redirecting(), None).unwrap_err();

    assert_eq!(refusal.status(), StatusCode::SEE_OTHER);
    assert!(matches!(
        refusal,
        AccessRejection::Auth(AuthRejection::Redirect { ref location })
            if location == "/login?return_to=%2Fnotes%2F1"
    ));
}

#[test]
fn an_anonymous_user_is_redirected_to_registration_under_a_login_redirect() {
    let refusal =
        admit::<Ticket, RegisteredOwner>(&redirecting(), Some(Arc::new(anonymous()))).unwrap_err();

    assert!(matches!(
        refusal,
        AccessRejection::Auth(AuthRejection::Redirect { ref location })
            if location == "/register?return_to=%2Fnotes%2F1"
    ));
}
