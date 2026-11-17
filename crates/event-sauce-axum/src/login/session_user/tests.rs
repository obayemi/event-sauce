use std::sync::Arc;

use axum_login::AuthUser;
use event_sauce_auth::UserAggregate;

use super::SessionUser;
use crate::login::test_support::{anonymous, registered, registered_with_another_password};

#[test]
fn the_id_is_the_entity_id_of_the_aggregate() {
    let root = registered();
    let id = root.entity_id();

    assert_eq!(SessionUser::from(root).id(), id);
}

#[test]
fn the_session_auth_hash_is_the_identity_fingerprint() {
    let root = registered();
    let fingerprint = root.identity().session_fingerprint();

    let user = SessionUser::from(root);

    assert_eq!(user.session_auth_hash(), fingerprint.as_slice());
}

#[test]
fn a_credential_change_changes_the_session_auth_hash() {
    let before = SessionUser::from(registered());
    let after = SessionUser::from(registered_with_another_password());

    assert_ne!(before.session_auth_hash(), after.session_auth_hash());
}

#[test]
fn an_anonymous_user_still_has_a_session_auth_hash() {
    let user = SessionUser::from(anonymous());

    assert_eq!(user.session_auth_hash().len(), 32);
}

#[test]
fn clones_share_the_same_aggregate() {
    let user = SessionUser::from(registered());

    let clone = user.clone();

    assert!(Arc::ptr_eq(user.user(), clone.user()));
}

#[test]
fn an_arc_is_wrapped_without_copying_the_aggregate() {
    let root = Arc::new(registered());

    let user = SessionUser::from(root.clone());

    assert!(Arc::ptr_eq(&root, &user.into_user()));
}

#[test]
fn the_session_user_derefs_to_the_aggregate_root() {
    let root = registered();
    let id = root.entity_id();

    let user = SessionUser::from(root);

    assert_eq!(user.entity_id(), id);
}

#[test]
fn debug_names_the_user_without_leaking_the_fingerprint() {
    let user = SessionUser::from(registered());
    let fingerprint = format!("{:?}", user.session_auth_hash());

    let rendered = format!("{user:?}");

    assert!(rendered.contains(&user.entity_id().to_string()));
    assert!(!rendered.contains(&fingerprint));
}
