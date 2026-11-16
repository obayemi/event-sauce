use event_sauce_core::EntityId;

use super::*;

#[test]
fn an_entry_round_trips_its_version_and_body() {
    let entry = Entry::new(AggregateVersion::new(42), b"body".to_vec());

    let decoded = Entry::decode(&entry.encode()).unwrap();

    assert_eq!(decoded.version, AggregateVersion::new(42));
    assert_eq!(decoded.body, b"body");
}

#[test]
fn an_entry_may_have_an_empty_body() {
    let decoded = Entry::decode(&Entry::new(AggregateVersion::new(1), Vec::new()).encode());

    assert!(decoded.unwrap().body.is_empty());
}

#[test]
fn an_entry_shorter_than_its_header_is_malformed() {
    assert!(matches!(
        Entry::decode(&[0; VERSION_LEN - 1]),
        Err(CacheError::Malformed)
    ));
}

#[test]
fn the_associated_data_binds_the_user_and_the_version() {
    let (ada, grace) = (EntityId::new(), EntityId::new());
    let at = |id, n| associated_data(id, AggregateVersion::new(n));

    assert_eq!(at(ada, 3), at(ada, 3));
    assert_ne!(at(ada, 3), at(ada, 4));
    assert_ne!(at(ada, 3), at(grace, 3));
}
