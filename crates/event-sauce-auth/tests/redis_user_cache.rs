//! `RedisUserCache` against a real Redis container and the in-memory event
//! store, whose crypto key store decides what an encrypted user may be cached.

mod support;
mod user_fixture;

use std::sync::Arc;
use std::time::Duration;

use event_sauce_auth::{CacheError, LoginTtl, RedisUserCache, UserAggregate, UserCache};
use event_sauce_core::{AggregateVersion, EntityId, EventStore};
use event_sauce_memory::InMemoryEventStore;
use redis::aio::ConnectionManager;
use testcontainers::ImageExt;
use testcontainers_modules::{redis::Redis, testcontainers::ContainerAsync};
use user_fixture::{join, load, rename, suspend, Member};

struct World {
    _container: ContainerAsync<Redis>,
    store: Arc<InMemoryEventStore>,
    connection: ConnectionManager,
}

impl World {
    async fn start() -> Self {
        let container = support::start_with_retry(|| Redis::default().with_tag("7-alpine"))
            .await
            .expect("start redis");
        let host = container.get_host().await.expect("host");
        let port = container.get_host_port_ipv4(6379).await.expect("port");
        let client = redis::Client::open(format!("redis://{host}:{port}")).expect("client");
        Self {
            _container: container,
            store: Arc::new(InMemoryEventStore::builder().build()),
            connection: ConnectionManager::new(client).await.expect("connect"),
        }
    }

    fn cache(&self) -> RedisUserCache<Member> {
        RedisUserCache::new(self.connection.clone()).with_crypto_from(self.store.clone())
    }

    async fn raw(&self, id: EntityId) -> Option<Vec<u8>> {
        redis::cmd("GET")
            .arg(format!("event_sauce:user:{id}"))
            .query_async(&mut self.connection.clone())
            .await
            .expect("raw read")
    }

    async fn ttl_millis(&self, id: EntityId) -> i64 {
        redis::cmd("PTTL")
            .arg(format!("event_sauce:user:{id}"))
            .query_async(&mut self.connection.clone())
            .await
            .expect("ttl")
    }
}

fn version(n: i64) -> AggregateVersion {
    AggregateVersion::new(n)
}

#[tokio::test]
async fn a_user_is_a_hit_at_the_version_it_was_cached_at() {
    let world = World::start().await;
    let id = join(&world.store, "ada").await;
    let user = load(&world.store, id).await;
    let cache = world.cache();

    cache.put(user.clone()).await.unwrap();

    let hit = cache.get(id, user.version()).await.unwrap().unwrap();
    assert_eq!(hit.nickname, "ada");
    assert_eq!(hit.version(), user.version());
    assert_eq!(hit.entity_id(), id);
}

#[tokio::test]
async fn an_unknown_user_is_a_miss() {
    let world = World::start().await;

    assert!(world
        .cache()
        .get(EntityId::new(), version(1))
        .await
        .unwrap()
        .is_none());
}

#[tokio::test]
async fn any_other_stored_version_is_a_miss() {
    let world = World::start().await;
    let id = join(&world.store, "ada").await;
    let user = load(&world.store, id).await;
    let cache = world.cache();
    cache.put(user.clone()).await.unwrap();

    assert!(cache
        .get(id, AggregateVersion::new(user.version().as_i64() + 1))
        .await
        .unwrap()
        .is_none());
    assert!(cache
        .get(id, AggregateVersion::new(user.version().as_i64() - 1))
        .await
        .unwrap()
        .is_none());
}

#[tokio::test]
async fn an_edited_user_is_cached_at_its_new_version() {
    let world = World::start().await;
    let id = join(&world.store, "ada").await;
    let cache = world.cache();
    cache.put(load(&world.store, id).await).await.unwrap();

    rename(&world.store, id, "countess").await;
    let edited = load(&world.store, id).await;
    assert!(cache.get(id, edited.version()).await.unwrap().is_none());
    cache.put(edited.clone()).await.unwrap();

    let hit = cache.get(id, edited.version()).await.unwrap().unwrap();
    assert_eq!(hit.nickname, "countess");
}

#[tokio::test]
async fn a_cache_does_not_judge_activity() {
    let world = World::start().await;
    let id = join(&world.store, "ada").await;
    suspend(&world.store, id).await;
    let user = load(&world.store, id).await;
    let cache = world.cache();

    cache.put(user.clone()).await.unwrap();

    let hit = cache.get(id, user.version()).await.unwrap().unwrap();
    assert!(!hit.is_active());
}

#[tokio::test]
async fn an_encrypted_user_is_stored_only_as_ciphertext() {
    let world = World::start().await;
    let id = join(&world.store, "ada").await;
    let user = load(&world.store, id).await;
    let cache = world.cache();

    cache.put(user.clone()).await.unwrap();

    let raw = world.raw(id).await.expect("an entry was written");
    for secret in [&b"ada"[..], b"ada@example.com", b"argon2id"] {
        assert!(
            !raw.windows(secret.len()).any(|window| window == secret),
            "the entry leaks {}",
            String::from_utf8_lossy(secret)
        );
    }
    assert!(cache.get(id, user.version()).await.unwrap().is_some());
}

#[tokio::test]
async fn a_shredded_key_turns_a_hit_into_a_miss() {
    let world = World::start().await;
    let id = join(&world.store, "ada").await;
    let user = load(&world.store, id).await;
    let cache = world.cache();
    cache.put(user.clone()).await.unwrap();

    world
        .store
        .crypto_key_store()
        .unwrap()
        .delete_key(id.as_uuid())
        .await
        .unwrap();

    assert!(cache.get(id, user.version()).await.unwrap().is_none());
}

#[tokio::test]
async fn a_user_without_a_key_is_not_cached() {
    let world = World::start().await;
    let id = join(&world.store, "ada").await;
    let user = load(&world.store, id).await;
    world
        .store
        .crypto_key_store()
        .unwrap()
        .delete_key(id.as_uuid())
        .await
        .unwrap();

    world.cache().put(user).await.unwrap();

    assert!(world.raw(id).await.is_none());
}

#[tokio::test]
async fn an_encrypted_user_cannot_be_cached_without_crypto() {
    let world = World::start().await;
    let id = join(&world.store, "ada").await;
    let user = load(&world.store, id).await;
    let cache = RedisUserCache::<Member>::new(world.connection.clone());

    let error = cache.put(user.clone()).await.unwrap_err();

    assert!(matches!(error, CacheError::Crypto(_)));
    assert!(world.raw(id).await.is_none());
    assert!(cache.get(id, user.version()).await.unwrap().is_none());
}

#[tokio::test]
async fn an_entry_that_is_not_ours_is_an_error() {
    let world = World::start().await;
    let id = EntityId::new();
    redis::cmd("SET")
        .arg(format!("event_sauce:user:{id}"))
        .arg("x")
        .query_async::<()>(&mut world.connection.clone())
        .await
        .unwrap();

    let error = world.cache().get(id, version(1)).await.unwrap_err();

    assert!(matches!(error, CacheError::Malformed));
}

#[tokio::test]
async fn entries_expire() {
    let world = World::start().await;
    let id = join(&world.store, "ada").await;
    let cache = world
        .cache()
        .with_ttl(LoginTtl::new(Duration::from_secs(90)).unwrap());

    cache.put(load(&world.store, id).await).await.unwrap();

    let remaining = world.ttl_millis(id).await;
    assert!((1..=90_000).contains(&remaining), "ttl was {remaining} ms");
}

#[tokio::test]
async fn prefixes_isolate_caches_sharing_a_server() {
    let world = World::start().await;
    let id = join(&world.store, "ada").await;
    let user = load(&world.store, id).await;
    let first = world.cache().with_prefix("a:");
    let second = world.cache().with_prefix("b:");

    first.put(user.clone()).await.unwrap();

    assert!(second.get(id, user.version()).await.unwrap().is_none());
    assert!(first.get(id, user.version()).await.unwrap().is_some());
}
