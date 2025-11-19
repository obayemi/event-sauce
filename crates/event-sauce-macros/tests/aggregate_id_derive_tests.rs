use event_sauce_core::AggregateId;
use event_sauce_macros::AggregateId as DeriveAggregateId;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, DeriveAggregateId)]
#[repr(transparent)]
struct UserId(AggregateId);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, DeriveAggregateId)]
#[repr(transparent)]
#[display("Account-{}")]
struct AccountId(AggregateId);

#[test]
fn test_derive_generates_new() {
    let id = UserId::new();
    assert_ne!(id.to_uuid(), uuid::Uuid::nil());
}

#[test]
fn test_derive_deref() {
    let id = UserId::new();
    // Via Deref, can call AggregateId methods directly
    let uuid = id.to_uuid();
    assert_eq!(uuid, id.to_uuid());
}

#[test]
fn test_derive_as_ref() {
    let id = UserId::new();
    let aggregate_id: &AggregateId = id.as_ref();
    assert_eq!(aggregate_id.to_uuid(), id.to_uuid());
}

#[test]
fn test_derive_from_aggregate_id() {
    let aggregate_id = AggregateId::new();
    let user_id = UserId::from(aggregate_id);
    assert_eq!(user_id.to_uuid(), aggregate_id.to_uuid());
}

#[test]
fn test_derive_into_aggregate_id() {
    let user_id = UserId::new();
    let aggregate_id: AggregateId = user_id.into();
    assert_eq!(aggregate_id.to_uuid(), user_id.to_uuid());
}

#[test]
fn test_derive_default_display() {
    let aggregate_id = AggregateId::nil();
    let user_id = UserId::from(aggregate_id);
    let display = format!("{}", user_id);
    assert_eq!(display, "00000000-0000-0000-0000-000000000000");
}

#[test]
fn test_derive_custom_display() {
    let aggregate_id = AggregateId::nil();
    let account_id = AccountId::from(aggregate_id);
    let display = format!("{}", account_id);
    assert!(display.starts_with("Account-"));
    assert!(display.contains("00000000-0000-0000-0000-000000000000"));
}

#[test]
fn test_derive_clone_and_copy() {
    let id1 = UserId::new();
    let id2 = id1; // Uses Copy
    assert_eq!(id1, id2);

    let id3 = id1.clone(); // Also supports Clone
    assert_eq!(id1, id3);
}

#[test]
fn test_derive_equality() {
    let aggregate_id = AggregateId::new();
    let id1 = UserId::from(aggregate_id);
    let id2 = UserId::from(aggregate_id);
    let id3 = UserId::new();

    assert_eq!(id1, id2);
    assert_ne!(id1, id3);
}

#[test]
fn test_derive_hash() {
    use std::collections::HashSet;

    let aggregate_id = AggregateId::new();
    let id1 = UserId::from(aggregate_id);
    let id2 = UserId::from(aggregate_id);

    let mut set = HashSet::new();
    set.insert(id1);
    set.insert(id2);

    assert_eq!(set.len(), 1);
    assert!(set.contains(&id1));
}

#[test]
fn test_derive_debug() {
    let id = UserId::new();
    let debug = format!("{:?}", id);
    assert!(debug.contains("UserId"));
}

#[test]
fn test_two_line_usage() {
    // This demonstrates the actual usage - just 2 lines!
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, DeriveAggregateId)]
    #[repr(transparent)]
    struct OrderId(AggregateId);

    let order_id = OrderId::new();
    let uuid = order_id.to_uuid();
    assert_ne!(uuid, uuid::Uuid::nil());
}
