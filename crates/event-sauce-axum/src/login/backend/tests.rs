use std::sync::Arc;

use axum_login::{AuthUser, AuthnBackend};
use event_sauce_core::{EntityId, EventStore, Repository};
use event_sauce_memory::InMemoryEventStore;

use super::EventSauceBackend;
use crate::login::test_support::{authenticated, registered, Member};
use crate::SessionUser;

fn backend() -> (
    EventSauceBackend<Member, InMemoryEventStore>,
    Arc<InMemoryEventStore>,
) {
    let store = Arc::new(InMemoryEventStore::new());
    (EventSauceBackend::new(store.clone()), store)
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
}

#[tokio::test]
async fn renewing_reloads_the_user_of_the_session() {
    let (backend, store) = backend();
    let mut root = registered();
    let id = root.entity_id();
    store.repository::<Member>().save(&mut root).await.unwrap();
    let session = backend.get_user(&id).await.unwrap().unwrap();

    let renewed = backend.renew(&session).await.unwrap().unwrap();

    assert_eq!(renewed.id(), id);
    assert_eq!(renewed.session_auth_hash(), session.session_auth_hash());
}

#[tokio::test]
async fn renewing_a_user_that_is_gone_resolves_to_no_user() {
    let (backend, _store) = backend();
    let session = SessionUser::from(authenticated(registered()).await);

    assert!(backend.renew(&session).await.unwrap().is_none());
}

#[tokio::test]
async fn a_shared_cache_serves_the_stored_user() {
    let store = Arc::new(InMemoryEventStore::new());
    let backend = EventSauceBackend::<Member, _>::new(store.clone())
        .with_cache(event_sauce_auth::MokaUserCache::new(10));
    let mut root = registered();
    let id = root.entity_id();
    store.repository::<Member>().save(&mut root).await.unwrap();

    let first = backend.get_user(&id).await.unwrap().unwrap();
    let second = backend.get_user(&id).await.unwrap().unwrap();

    assert_eq!(first.id(), second.id());
}
