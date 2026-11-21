use event_sauce_auth::MokaUserCache;

use super::{BuildError, CacheChoice, DEFAULT_CACHE_CAPACITY};
use crate::app::tests::builder;

#[tokio::test]
async fn the_user_cache_defaults_to_moka_with_ten_thousand_entries() {
    assert_eq!(DEFAULT_CACHE_CAPACITY, 10_000);
    assert!(matches!(
        builder().cache,
        CacheChoice::Moka(DEFAULT_CACHE_CAPACITY)
    ));
}

#[tokio::test]
async fn the_cache_capacity_is_overridden() {
    assert!(matches!(
        builder().user_cache_capacity(42).cache,
        CacheChoice::Moka(42)
    ));
}

#[tokio::test]
async fn the_cache_is_removed() {
    assert!(matches!(
        builder().no_user_cache().cache,
        CacheChoice::Disabled
    ));
}

#[tokio::test]
async fn a_custom_cache_replaces_the_default_and_the_last_choice_wins() {
    let custom = builder().user_cache(MokaUserCache::new(5));
    assert!(matches!(custom.cache, CacheChoice::Shared(_)));
    assert!(matches!(
        custom.user_cache_capacity(7).cache,
        CacheChoice::Moka(7)
    ));
}

#[tokio::test]
async fn an_invalid_login_schema_is_refused_before_touching_the_database() {
    let error = builder()
        .login_store_schema("Not A Schema")
        .build()
        .await
        .unwrap_err();

    assert!(matches!(error, BuildError::LoginSchema(_)));
}
