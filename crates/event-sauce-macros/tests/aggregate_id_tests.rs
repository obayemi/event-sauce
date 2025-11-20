//! Integration tests for #[derive(AggregateId)] macro.
//!
//! These tests verify that the AggregateId derive macro correctly generates
//! implementations for strongly-typed ID wrappers around Uuid.

use event_sauce_core::AggregateId as _;
use event_sauce_macros::AggregateId as DeriveAggregateId;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

// ============================================================================
// Basic Tests
// ============================================================================

#[derive(DeriveAggregateId, Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[repr(transparent)]
struct UserId(Uuid);

#[test]
fn test_aggregate_id_new() {
    let id = UserId::new();
    // Should be able to create a new ID
    let _uuid = id.to_uuid();
}

#[test]
fn test_aggregate_id_to_uuid() {
    let id = UserId::new();
    // Should be able to get the inner Uuid
    let uuid = id.to_uuid();
    assert_eq!(uuid, id.to_uuid());
}

#[test]
fn test_aggregate_id_display() {
    let uuid = Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000").unwrap();
    let id = UserId(uuid);

    // Display should delegate to the inner Uuid's Display
    assert_eq!(id.to_string(), "550e8400-e29b-41d4-a716-446655440000");
}

#[test]
fn test_aggregate_id_clone() {
    let id1 = UserId::new();
    let id2 = id1;

    assert_eq!(id1, id2);
}

#[test]
fn test_aggregate_id_equality() {
    let uuid = Uuid::new_v4();
    let id1 = UserId(uuid);
    let id2 = UserId(uuid);

    assert_eq!(id1, id2);
}

#[test]
fn test_aggregate_id_hash() {
    use std::collections::HashMap;

    let id = UserId::new();
    let mut map = HashMap::new();
    map.insert(id, "test");

    assert_eq!(map.get(&id), Some(&"test"));
}

#[test]
fn test_aggregate_id_is_send_sync() {
    fn assert_send<T: Send>() {}
    fn assert_sync<T: Sync>() {}

    assert_send::<UserId>();
    assert_sync::<UserId>();
}

#[test]
fn test_aggregate_id_construction() {
    let uuid = Uuid::new_v4();
    let user_id = UserId(uuid);

    // Can get the uuid back
    assert_eq!(user_id.to_uuid(), uuid);
}

// ============================================================================
// Multiple ID Types
// ============================================================================

#[derive(DeriveAggregateId, Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[repr(transparent)]
struct OrderId(Uuid);

#[derive(DeriveAggregateId, Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[repr(transparent)]
struct ProductId(Uuid);

#[test]
fn test_multiple_id_types_are_distinct() {
    let uuid = Uuid::new_v4();
    let order_id = OrderId(uuid);
    let product_id = ProductId(uuid);

    // These should have the same underlying Uuid
    assert_eq!(order_id.to_uuid(), product_id.to_uuid());

    // But they're different types, so this won't compile:
    // assert_eq!(order_id, product_id); // Compilation error!
}

#[test]
fn test_multiple_id_types_display() {
    let uuid = Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000").unwrap();

    let order_id = OrderId(uuid);
    let product_id = ProductId(uuid);

    assert_eq!(order_id.to_string(), product_id.to_string());
}

// ============================================================================
// Custom Display Format
// ============================================================================

#[derive(DeriveAggregateId, Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[repr(transparent)]
#[display("User-{}")]
struct CustomDisplayUserId(Uuid);

#[test]
fn test_custom_display_format() {
    let id = CustomDisplayUserId::new();
    let display = format!("{id}");
    assert!(display.starts_with("User-"));
}

// ============================================================================
// Type Safety Tests
// ============================================================================

#[test]
fn test_type_safety() {
    let user_id = UserId::new();
    let order_id = OrderId::new();

    // These are different types, even though they wrap the same underlying type
    // This ensures type safety and prevents mixing different kinds of IDs

    // Can use in separate contexts
    fn process_user(_id: UserId) {}
    fn process_order(_id: OrderId) {}

    process_user(user_id);
    process_order(order_id);

    // Cannot pass wrong type:
    // process_user(order_id); // Compilation error!
    // process_order(user_id); // Compilation error!
}

// ============================================================================
// Debug Formatting
// ============================================================================

#[test]
fn test_debug_formatting() {
    let uuid = Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000").unwrap();
    let id = UserId(uuid);

    let debug_str = format!("{id:?}");
    assert!(debug_str.contains("UserId"));
    assert!(debug_str.contains("550e8400"));
}

// ============================================================================
// Collection Usage
// ============================================================================

#[test]
fn test_use_in_hashmap() {
    use std::collections::HashMap;

    let mut users = HashMap::new();
    let id1 = UserId::new();
    let id2 = UserId::new();

    users.insert(id1, "Alice");
    users.insert(id2, "Bob");

    assert_eq!(users.get(&id1), Some(&"Alice"));
    assert_eq!(users.get(&id2), Some(&"Bob"));
}

#[test]
fn test_use_in_vec() {
    let ids = [UserId::new(), UserId::new(), UserId::new()];

    assert_eq!(ids.len(), 3);
}

#[test]
fn test_use_in_hashset() {
    use std::collections::HashSet;

    let mut set = HashSet::new();
    let id = UserId::new();

    set.insert(id);
    set.insert(id); // Duplicate

    assert_eq!(set.len(), 1);
    assert!(set.contains(&id));
}

// ============================================================================
// Real-World Usage Pattern
// ============================================================================

#[test]
fn test_realistic_usage() {
    // Simulate a realistic scenario
    struct User {
        id: UserId,
        name: String,
    }

    let user_id = UserId::new();
    let user = User {
        id: user_id,
        name: "Alice".to_string(),
    };

    // Can display the ID
    println!("User ID: {}", user.id);

    // Can compare IDs (user_id is Copy, so it can be used after being moved into user)
    assert_eq!(user.id, user_id);

    // Can clone (but Copy types don't need explicit cloning)
    let id_copy = user.id;
    assert_eq!(user.id, id_copy);
}

// ============================================================================
// Format Trait Tests
// ============================================================================

#[test]
fn test_display_trait_is_implemented() {
    fn assert_display<T: std::fmt::Display>() {}
    assert_display::<UserId>();
    assert_display::<OrderId>();
    assert_display::<ProductId>();
}

#[test]
fn test_format_macro() {
    let uuid = Uuid::nil();
    let id = UserId(uuid);

    // Test various format! macro usage
    assert_eq!(format!("{id}"), "00000000-0000-0000-0000-000000000000");
}

#[test]
fn test_aggregate_id_new_and_to_uuid() {
    let uuid = Uuid::new_v4();

    // Test construction from Uuid
    let user_id = UserId(uuid);

    // Test to_uuid()
    assert_eq!(user_id.to_uuid(), uuid);
}

#[test]
fn test_to_uuid_method() {
    let user_id = UserId::new();

    // Can call to_uuid() to get the inner Uuid
    let uuid = user_id.to_uuid();
    let nil_id = UserId(Uuid::nil());

    assert_ne!(user_id.to_uuid(), nil_id.to_uuid());
}
