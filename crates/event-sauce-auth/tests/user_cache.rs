//! `CachedUserLoader` over the in-memory event store, with the in-process
//! cache and with caches that misbehave.

mod user_fixture;

use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};

use async_trait::async_trait;
use event_sauce_auth::{CacheError, CachedUserLoader, MokaUserCache, UserCache};
use event_sauce_core::{AggregateRoot, EntityId, StoredVersion};
use event_sauce_memory::InMemoryEventStore;
use user_fixture::{join, rename, suspend, Member};

fn store() -> Arc<InMemoryEventStore> {
    Arc::new(InMemoryEventStore::new())
}

fn cached_loader(store: &Arc<InMemoryEventStore>) -> CachedUserLoader<Member, InMemoryEventStore> {
    CachedUserLoader::new(store.clone()).with_cache(MokaUserCache::new(100))
}

#[derive(Debug, Default)]
struct CountingCache {
    inner: Option<MokaUserCache<Member>>,
    gets: AtomicUsize,
    puts: AtomicUsize,
    broken: bool,
}

impl CountingCache {
    fn working() -> Self {
        Self {
            inner: Some(MokaUserCache::new(100)),
            ..Self::default()
        }
    }

    fn broken() -> Self {
        Self {
            broken: true,
            ..Self::default()
        }
    }
}

#[async_trait]
impl UserCache<Member> for CountingCache {
    async fn get(
        &self,
        id: EntityId,
        version: StoredVersion,
    ) -> Result<Option<Arc<AggregateRoot<Member>>>, CacheError> {
        self.gets.fetch_add(1, Ordering::SeqCst);
        if self.broken {
            return Err(CacheError::Malformed);
        }
        self.inner.as_ref().unwrap().get(id, version).await
    }

    async fn put(&self, user: Arc<AggregateRoot<Member>>) -> Result<(), CacheError> {
        self.puts.fetch_add(1, Ordering::SeqCst);
        if self.broken {
            return Err(CacheError::Malformed);
        }
        self.inner.as_ref().unwrap().put(user).await
    }
}

#[tokio::test]
async fn a_user_is_loaded_without_a_cache() {
    let store = store();
    let id = join(&store, "ada").await;

    let user = CachedUserLoader::<Member, _>::new(store)
        .load(id)
        .await
        .unwrap()
        .unwrap();

    assert_eq!(user.nickname, "ada");
}

#[tokio::test]
async fn an_unknown_user_is_none() {
    let loader = cached_loader(&store());

    assert!(loader.load(EntityId::new()).await.unwrap().is_none());
}

#[tokio::test]
async fn a_second_load_is_served_by_the_cache() {
    let store = store();
    let id = join(&store, "ada").await;
    let loader = cached_loader(&store);

    let first = loader.load(id).await.unwrap().unwrap();
    let second = loader.load(id).await.unwrap().unwrap();

    assert!(Arc::ptr_eq(&first, &second));
}

#[tokio::test]
async fn an_edited_user_is_never_served_stale() {
    let store = store();
    let id = join(&store, "ada").await;
    let loader = cached_loader(&store);
    loader.load(id).await.unwrap();

    rename(&store, id, "countess").await;

    let user = loader.load(id).await.unwrap().unwrap();
    assert_eq!(user.nickname, "countess");
}

#[tokio::test]
async fn a_suspended_user_is_none_even_when_cached() {
    let store = store();
    let id = join(&store, "ada").await;
    let loader = cached_loader(&store);
    loader.load(id).await.unwrap();

    suspend(&store, id).await;

    assert!(loader.load(id).await.unwrap().is_none());
}

#[tokio::test]
async fn a_miss_fills_the_cache_and_a_hit_does_not() {
    let store = store();
    let id = join(&store, "ada").await;
    let cache = Arc::new(CountingCache::working());
    let loader = CachedUserLoader::<Member, _>::new(store).with_shared_cache(cache.clone());

    loader.load(id).await.unwrap();
    loader.load(id).await.unwrap();

    assert_eq!(cache.gets.load(Ordering::SeqCst), 2);
    assert_eq!(cache.puts.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn an_unknown_user_never_reaches_the_cache() {
    let cache = Arc::new(CountingCache::working());
    let loader = CachedUserLoader::<Member, _>::new(store()).with_shared_cache(cache.clone());

    loader.load(EntityId::new()).await.unwrap();

    assert_eq!(cache.gets.load(Ordering::SeqCst), 0);
    assert_eq!(cache.puts.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn a_failing_cache_falls_back_to_a_full_load() {
    let store = store();
    let id = join(&store, "ada").await;
    let cache = Arc::new(CountingCache::broken());
    let loader = CachedUserLoader::<Member, _>::new(store).with_shared_cache(cache.clone());

    let user = loader.load(id).await.unwrap().unwrap();

    assert_eq!(user.nickname, "ada");
    assert_eq!(cache.puts.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn a_cached_user_is_the_user_a_full_load_gives() {
    let store = store();
    let id = join(&store, "ada").await;
    rename(&store, id, "countess").await;
    let loader = cached_loader(&store);
    loader.load(id).await.unwrap();

    let cached = loader.load(id).await.unwrap().unwrap();

    let loaded = user_fixture::load(&store, id).await;
    assert_eq!(cached.nickname, loaded.nickname);
    assert_eq!(cached.version(), loaded.version());
}
