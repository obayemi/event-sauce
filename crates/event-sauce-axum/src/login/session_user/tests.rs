use std::sync::Arc;

use axum_login::AuthUser;
use event_sauce_auth::UserAggregate;

use super::SessionUser;
use crate::login::test_support::{
    anonymous, authenticated, registered, registered_with_another_password,
};

#[tokio::test]
async fn the_id_is_the_entity_id_of_the_aggregate() {
    let root = registered();
    let id = root.entity_id();

    let user = SessionUser::from(authenticated(root).await);

    assert_eq!(user.id(), id);
}

#[tokio::test]
async fn the_session_auth_hash_is_the_identity_fingerprint() {
    let authenticated = authenticated(registered()).await;
    let fingerprint = authenticated.user().identity().session_fingerprint();

    let user = SessionUser::from(authenticated);

    assert_eq!(user.session_auth_hash(), fingerprint.as_slice());
}

#[tokio::test]
async fn a_credential_change_changes_the_session_auth_hash() {
    let before = SessionUser::from(authenticated(registered()).await);
    let after = SessionUser::from(authenticated(registered_with_another_password()).await);

    assert_ne!(before.session_auth_hash(), after.session_auth_hash());
}

#[tokio::test]
async fn an_anonymous_user_still_has_a_session_auth_hash() {
    let user = SessionUser::from(authenticated(anonymous()).await);

    assert_eq!(user.session_auth_hash().len(), 32);
}

#[tokio::test]
async fn clones_share_the_same_aggregate() {
    let user = SessionUser::from(authenticated(registered()).await);

    let clone = user.clone();

    assert!(Arc::ptr_eq(user.user(), clone.user()));
}

#[tokio::test]
async fn the_authenticated_aggregate_is_wrapped_without_copying_it() {
    let authenticated = authenticated(registered()).await;
    let shared = Arc::clone(authenticated.user());

    let user = SessionUser::from(authenticated);

    assert!(Arc::ptr_eq(&shared, &user.into_user()));
}

#[tokio::test]
async fn a_loaded_aggregate_keeps_its_fingerprint() {
    let root = Arc::new(registered());
    let fingerprint = root.identity().session_fingerprint();

    let user = SessionUser::from_loaded(root.clone());

    assert_eq!(user.session_auth_hash(), fingerprint.as_slice());
    assert!(Arc::ptr_eq(&root, user.user()));
}

#[tokio::test]
async fn the_session_user_derefs_to_the_aggregate_root() {
    let root = registered();
    let id = root.entity_id();

    let user = SessionUser::from(authenticated(root).await);

    assert_eq!(user.entity_id(), id);
}

#[tokio::test]
async fn debug_names_the_user_without_leaking_the_fingerprint() {
    let user = SessionUser::from(authenticated(registered()).await);
    let fingerprint = format!("{:?}", user.session_auth_hash());

    let rendered = format!("{user:?}");

    assert!(rendered.contains(&user.entity_id().to_string()));
    assert!(!rendered.contains(&fingerprint));
}
