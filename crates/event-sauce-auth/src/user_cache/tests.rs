use event_sauce_core::{AggregateRoot, EntityId};

use super::stored_version;
use super::test_support::{cache_user_at, stored, CacheUser, RenamedEvent};

#[test]
fn a_loaded_user_is_stored_at_its_version() {
    let user = cache_user_at(EntityId::new(), "ada", 3);

    assert_eq!(stored_version(&user), Some(stored(3)));
}

#[test]
fn a_user_the_store_has_never_saved_is_not_stored() {
    let user = AggregateRoot::<CacheUser>::new(EntityId::new());

    assert_eq!(stored_version(&user), None);
}

#[test]
fn a_user_with_uncommitted_changes_is_not_stored() {
    let mut user = cache_user_at(EntityId::new(), "ada", 3);
    user.apply(RenamedEvent {
        name: "grace".into(),
        timestamp: chrono::Utc::now(),
    })
    .unwrap();

    assert_eq!(stored_version(&user), None);
}
