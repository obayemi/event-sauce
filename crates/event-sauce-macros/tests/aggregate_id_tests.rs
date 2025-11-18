//! Integration tests for #[derive(AggregateId)] macro.
//!
//! These tests verify that the AggregateId derive macro correctly generates
//! the AggregateId trait implementation and Display trait implementation.

use event_sauce_core::AggregateId as AggregateIdTrait;
use event_sauce_macros::AggregateId;
use uuid::Uuid;

// ============================================================================
// Basic Tests
// ============================================================================

#[derive(AggregateId, Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct UserId(Uuid);

#[test]
fn test_aggregate_id_implements_trait() {
    // Compile-time check that AggregateId trait is implemented
    fn assert_aggregate_id<T: AggregateIdTrait>() {}
    assert_aggregate_id::<UserId>();
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
    let id1 = UserId(Uuid::new_v4());
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

    let id = UserId(Uuid::new_v4());
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

// ============================================================================
// Multiple ID Types
// ============================================================================

#[derive(AggregateId, Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct OrderId(Uuid);

#[derive(AggregateId, Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct ProductId(Uuid);

#[test]
fn test_multiple_id_types_are_distinct() {
    let uuid = Uuid::new_v4();
    let order_id = OrderId(uuid);
    let product_id = ProductId(uuid);

    // These should have the same underlying UUID
    assert_eq!(order_id.0, product_id.0);

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
// Integer ID Types
// ============================================================================

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct SequenceId(i64);

impl AggregateIdTrait for SequenceId {
    fn to_uuid(&self) -> Uuid {
        // For testing: create deterministic UUID from i64
        let mut bytes = [0u8; 16];
        bytes[0..8].copy_from_slice(&self.0.to_le_bytes());
        Uuid::from_bytes(bytes)
    }
}

impl std::fmt::Display for SequenceId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(&self.0, f)
    }
}

#[test]
fn test_integer_id_display() {
    let id = SequenceId(12345);
    assert_eq!(id.to_string(), "12345");
}

#[test]
fn test_integer_id_equality() {
    let id1 = SequenceId(100);
    let id2 = SequenceId(100);
    let id3 = SequenceId(200);

    assert_eq!(id1, id2);
    assert_ne!(id1, id3);
}

#[test]
fn test_integer_id_implements_copy() {
    let id1 = SequenceId(42);
    let id2 = id1; // Copy, not move

    assert_eq!(id1, id2);
    assert_eq!(id1.0, 42); // id1 is still usable
}

// ============================================================================
// String ID Types
// ============================================================================

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct EmailId(String);

impl AggregateIdTrait for EmailId {
    fn to_uuid(&self) -> Uuid {
        // For testing: create deterministic UUID from string hash
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        let mut hasher = DefaultHasher::new();
        self.0.hash(&mut hasher);
        let hash = hasher.finish();

        let mut bytes = [0u8; 16];
        bytes[0..8].copy_from_slice(&hash.to_le_bytes());
        Uuid::from_bytes(bytes)
    }
}

impl std::fmt::Display for EmailId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(&self.0, f)
    }
}

#[test]
fn test_string_id_display() {
    let id = EmailId("user@example.com".to_string());
    assert_eq!(id.to_string(), "user@example.com");
}

#[test]
fn test_string_id_equality() {
    let id1 = EmailId("test@example.com".to_string());
    let id2 = EmailId("test@example.com".to_string());
    let id3 = EmailId("other@example.com".to_string());

    assert_eq!(id1, id2);
    assert_ne!(id1, id3);
}

#[test]
fn test_string_id_clone() {
    let id1 = EmailId("test@example.com".to_string());
    let id2 = id1.clone();

    assert_eq!(id1, id2);
    assert_eq!(id1.0, "test@example.com");
}

// ============================================================================
// Custom Types with Display
// ============================================================================

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct CustomValue {
    prefix: &'static str,
    value: u32,
}

impl std::fmt::Display for CustomValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}-{}", self.prefix, self.value)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct CustomId(CustomValue);

impl AggregateIdTrait for CustomId {
    fn to_uuid(&self) -> Uuid {
        // For testing: create deterministic UUID from custom value
        let mut bytes = [0u8; 16];
        bytes[0..4].copy_from_slice(&self.0.value.to_le_bytes());
        // Add prefix bytes
        for (i, &byte) in self.0.prefix.as_bytes().iter().take(12).enumerate() {
            bytes[i + 4] = byte;
        }
        Uuid::from_bytes(bytes)
    }
}

impl std::fmt::Display for CustomId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(&self.0, f)
    }
}

#[test]
fn test_custom_type_display() {
    let id = CustomId(CustomValue {
        prefix: "TEST",
        value: 123,
    });

    assert_eq!(id.to_string(), "TEST-123");
}

// ============================================================================
// Type Safety Tests
// ============================================================================

#[test]
fn test_type_safety() {
    let user_id = UserId(Uuid::new_v4());
    let order_id = OrderId(Uuid::new_v4());

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
    let id1 = UserId(Uuid::new_v4());
    let id2 = UserId(Uuid::new_v4());

    users.insert(id1, "Alice");
    users.insert(id2, "Bob");

    assert_eq!(users.get(&id1), Some(&"Alice"));
    assert_eq!(users.get(&id2), Some(&"Bob"));
}

#[test]
fn test_use_in_vec() {
    let ids = [UserId(Uuid::new_v4()),
        UserId(Uuid::new_v4()),
        UserId(Uuid::new_v4())];

    assert_eq!(ids.len(), 3);
}

#[test]
fn test_use_in_hashset() {
    use std::collections::HashSet;

    let mut set = HashSet::new();
    let id = UserId(Uuid::new_v4());

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

    let user_id = UserId(Uuid::new_v4());
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
    assert_display::<SequenceId>();
    assert_display::<EmailId>();
}

#[test]
fn test_format_macro() {
    let id = SequenceId(42);

    // Test various format! macro usage
    assert_eq!(format!("{id}"), "42");
    assert_eq!(format!("ID: {id}"), "ID: 42");
}
