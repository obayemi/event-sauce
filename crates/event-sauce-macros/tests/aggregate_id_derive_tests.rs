#![allow(clippy::uninlined_format_args)]
#![allow(clippy::clone_on_copy)]

use event_sauce_core::AggregateId as _;
use event_sauce_macros::AggregateId as DeriveAggregateId;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, DeriveAggregateId)]
#[repr(transparent)]
struct UserId(Uuid);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, DeriveAggregateId)]
#[repr(transparent)]
#[display("Account-{}")]
struct AccountId(Uuid);

#[test]
fn test_derive_generates_new() {
    let id = UserId::new();
    assert_ne!(id.to_uuid(), uuid::Uuid::nil());
}

#[test]
fn test_derive_to_uuid() {
    let id = UserId::new();
    // Can call to_uuid() to get the inner Uuid
    let uuid = id.to_uuid();
    assert_eq!(uuid, id.to_uuid());
}

#[test]
fn test_derive_construction() {
    // Can construct from Uuid
    let uuid = Uuid::new_v4();
    let user_id = UserId(uuid);
    assert_eq!(user_id.to_uuid(), uuid);
}

#[test]
fn test_derive_default_display() {
    let uuid = Uuid::nil();
    let user_id = UserId(uuid);
    let display = format!("{}", user_id);
    assert_eq!(display, "00000000-0000-0000-0000-000000000000");
}

#[test]
fn test_derive_custom_display() {
    let uuid = Uuid::nil();
    let account_id = AccountId(uuid);
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
    let uuid = Uuid::new_v4();
    let id1 = UserId(uuid);
    let id2 = UserId(uuid);
    let id3 = UserId::new();

    assert_eq!(id1, id2);
    assert_ne!(id1, id3);
}

#[test]
fn test_derive_hash() {
    use std::collections::HashSet;

    let uuid = Uuid::new_v4();
    let id1 = UserId(uuid);
    let id2 = UserId(uuid);

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
    #[derive(
        Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, DeriveAggregateId,
    )]
    #[repr(transparent)]
    struct OrderId(Uuid);

    let order_id = OrderId::new();
    let uuid = order_id.to_uuid();
    assert_ne!(uuid, Uuid::nil());
}
