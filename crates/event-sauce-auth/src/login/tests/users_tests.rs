use chrono::Utc;

use super::*;
use crate::test_support::{credentials, stored_user, DeactivatedEvent, TestUser};

fn registered() -> AggregateRoot<TestUser> {
    TestUser::register(credentials("ada@example.com", "pw")).unwrap()
}

#[test]
fn a_user_built_in_memory_is_new() {
    assert!(is_new(&registered()));
}

#[test]
fn a_user_with_no_event_is_not_new() {
    let user = Arc::into_inner(stored_user(credentials("ada@example.com", "pw"), true)).unwrap();

    assert!(!is_new(&user));
}

#[test]
fn a_loaded_user_with_a_pending_event_is_not_new() {
    let mut user =
        Arc::into_inner(stored_user(credentials("ada@example.com", "pw"), true)).unwrap();
    user.apply(DeactivatedEvent {
        timestamp: Utc::now(),
    })
    .unwrap();

    assert!(!is_new(&user));
}

#[test]
fn a_new_user_with_a_second_pending_event_is_still_new() {
    let mut user = registered();
    user.apply(DeactivatedEvent {
        timestamp: Utc::now(),
    })
    .unwrap();

    assert!(is_new(&user));
}

#[test]
fn an_active_user_is_found_active() {
    let user = stored_user(credentials("ada@example.com", "pw"), true);

    assert!(matches!(Found::from(Some(user)), Found::Active(_)));
}

#[test]
fn an_inactive_user_is_found_inactive() {
    let user = stored_user(credentials("ada@example.com", "pw"), false);

    assert!(matches!(Found::from(Some(user)), Found::Inactive));
}

#[test]
fn no_user_is_found_missing() {
    assert!(matches!(Found::<TestUser>::from(None), Found::Missing));
}

#[test]
fn an_active_user_with_the_same_fingerprint_is_confirmed() {
    let user = stored_user(credentials("ada@example.com", "pw"), true);
    let fingerprint = user.identity().session_fingerprint();

    let confirmed = Found::Active(user.clone()).confirm(fingerprint).unwrap();

    assert_eq!(confirmed.entity_id(), user.entity_id());
}

#[test]
fn an_active_user_with_another_fingerprint_is_stale() {
    let user = stored_user(credentials("ada@example.com", "pw"), true);
    let other = stored_user(credentials("ada@example.com", "other"), true)
        .identity()
        .session_fingerprint();

    assert!(matches!(
        Found::Active(user).confirm(other),
        Err(Stale::CredentialsChanged)
    ));
}

#[test]
fn an_inactive_user_is_stale() {
    assert!(matches!(
        Found::<TestUser>::Inactive.confirm([0; 32]),
        Err(Stale::Inactive)
    ));
}

#[test]
fn a_missing_user_is_stale() {
    assert!(matches!(
        Found::<TestUser>::Missing.confirm([0; 32]),
        Err(Stale::Inactive)
    ));
}
