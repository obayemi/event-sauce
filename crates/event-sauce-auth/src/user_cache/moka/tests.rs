use event_sauce_core::{AggregateVersion, EntityId};

use super::*;
use crate::user_cache::test_support::{cache_user_at, CacheUser, RenamedEvent};

fn version(n: i64) -> AggregateVersion {
    AggregateVersion::new(n)
}

fn cache() -> MokaUserCache<CacheUser> {
    MokaUserCache::new(100)
}

#[tokio::test]
async fn an_unknown_user_is_a_miss() {
    assert!(cache()
        .get(EntityId::new(), version(1))
        .await
        .unwrap()
        .is_none());
}

#[tokio::test]
async fn a_user_is_a_hit_at_the_version_it_was_cached_at() {
    let cache = cache();
    let id = EntityId::new();
    let user = Arc::new(cache_user_at(id, "ada", 3));

    cache.put(user.clone()).await.unwrap();

    let hit = cache.get(id, version(3)).await.unwrap().unwrap();
    assert!(Arc::ptr_eq(&hit, &user));
}

#[tokio::test]
async fn a_newer_stored_version_is_a_miss() {
    let cache = cache();
    let id = EntityId::new();
    cache
        .put(Arc::new(cache_user_at(id, "ada", 3)))
        .await
        .unwrap();

    assert!(cache.get(id, version(4)).await.unwrap().is_none());
}

#[tokio::test]
async fn an_older_stored_version_is_a_miss() {
    let cache = cache();
    let id = EntityId::new();
    cache
        .put(Arc::new(cache_user_at(id, "ada", 3)))
        .await
        .unwrap();

    assert!(cache.get(id, version(2)).await.unwrap().is_none());
}

#[tokio::test]
async fn a_newer_user_replaces_an_older_one() {
    let cache = cache();
    let id = EntityId::new();
    cache
        .put(Arc::new(cache_user_at(id, "old", 3)))
        .await
        .unwrap();
    cache
        .put(Arc::new(cache_user_at(id, "new", 4)))
        .await
        .unwrap();

    assert_eq!(
        cache.get(id, version(4)).await.unwrap().unwrap().name,
        "new"
    );
    assert!(cache.get(id, version(3)).await.unwrap().is_none());
}

#[tokio::test]
async fn an_older_user_never_replaces_a_newer_one() {
    let cache = cache();
    let id = EntityId::new();
    cache
        .put(Arc::new(cache_user_at(id, "new", 4)))
        .await
        .unwrap();
    cache
        .put(Arc::new(cache_user_at(id, "old", 3)))
        .await
        .unwrap();

    assert_eq!(
        cache.get(id, version(4)).await.unwrap().unwrap().name,
        "new"
    );
}

#[tokio::test]
async fn users_are_cached_apart() {
    let cache = cache();
    let (ada, grace) = (EntityId::new(), EntityId::new());
    cache
        .put(Arc::new(cache_user_at(ada, "ada", 1)))
        .await
        .unwrap();
    cache
        .put(Arc::new(cache_user_at(grace, "grace", 1)))
        .await
        .unwrap();

    assert_eq!(
        cache.get(ada, version(1)).await.unwrap().unwrap().name,
        "ada"
    );
    assert_eq!(
        cache.get(grace, version(1)).await.unwrap().unwrap().name,
        "grace"
    );
}

#[tokio::test]
async fn a_user_with_uncommitted_changes_is_not_cached() {
    let cache = cache();
    let id = EntityId::new();
    let mut user = cache_user_at(id, "ada", 3);
    user.apply(RenamedEvent {
        name: "grace".into(),
        timestamp: chrono::Utc::now(),
    })
    .unwrap();

    cache.put(Arc::new(user)).await.unwrap();

    assert!(cache.get(id, version(3)).await.unwrap().is_none());
    assert!(cache.get(id, version(4)).await.unwrap().is_none());
}
