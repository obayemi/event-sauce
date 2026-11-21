use std::sync::Arc;

use axum_login::{AuthUser, AuthnBackend};
use event_sauce_auth::{CachedUserLoader, MokaUserCache, UserAggregate};
use event_sauce_core::{EntityId, EventStore, Repository};
use event_sauce_memory::InMemoryEventStore;

use super::Backend;
use crate::login::test_support::{authenticated, registered, Member};

fn backend() -> (Backend<Member>, Arc<InMemoryEventStore>) {
    let store = Arc::new(InMemoryEventStore::new());
    (Backend::new(CachedUserLoader::new(store.clone())), store)
}

#[tokio::test]
async fn a_finished_login_authenticates_into_a_session_user() {
    let (backend, _store) = backend();
    let root = registered();
    let id = root.entity_id();

    let user = backend
        .authenticate(authenticated(root).await)
        .await
        .unwrap()
        .unwrap();

    assert_eq!(user.id(), id);
}

#[tokio::test]
async fn an_unknown_id_resolves_to_no_user() {
    let (backend, _store) = backend();

    assert!(backend.get_user(&EntityId::new()).await.unwrap().is_none());
}

#[tokio::test]
async fn a_stored_user_resolves_though_no_login_ran() {
    let (backend, store) = backend();
    let mut root = registered();
    let id = root.entity_id();
    store.repository::<Member>().save(&mut root).await.unwrap();

    let user = backend.get_user(&id).await.unwrap().unwrap();

    assert_eq!(user.id(), id);
    assert_eq!(
        user.session_auth_hash(),
        root.identity().session_fingerprint().as_slice()
    );
}

#[tokio::test]
async fn a_cache_serves_the_stored_user() {
    let store = Arc::new(InMemoryEventStore::new());
    let loader = CachedUserLoader::new(store.clone()).with_cache(MokaUserCache::new(10));
    let backend = Backend::<Member>::new(loader);
    let mut root = registered();
    let id = root.entity_id();
    store.repository::<Member>().save(&mut root).await.unwrap();

    let first = backend.get_user(&id).await.unwrap().unwrap();
    let second = backend.get_user(&id).await.unwrap().unwrap();

    assert!(Arc::ptr_eq(first.user(), second.user()));
}

#[tokio::test]
async fn clones_resolve_the_same_users() {
    let (backend, store) = backend();
    let mut root = registered();
    let id = root.entity_id();
    store.repository::<Member>().save(&mut root).await.unwrap();

    let user = backend.clone().get_user(&id).await.unwrap().unwrap();

    assert_eq!(user.id(), id);
}

#[test]
fn debug_prints_nothing_of_the_store() {
    let (backend, _store) = backend();

    assert_eq!(format!("{backend:?}"), "Backend { .. }");
}
