use super::*;
use serde_json::json;

#[test]
fn test_metadata_new() {
    let metadata = EventMetadata::new();

    assert!(metadata.correlation_id.is_none());
    assert!(metadata.causation_id.is_none());
    assert!(metadata.additional.is_none());
}

#[test]
fn test_metadata_with_correlation_id() {
    let metadata = EventMetadata::new().with_correlation_id("req-123");

    assert_eq!(metadata.correlation_id.as_deref(), Some("req-123"));
}

#[test]
fn test_metadata_with_causation_id() {
    let causation_id = Uuid::new_v4();
    let metadata = EventMetadata::new().with_causation_id(causation_id);

    assert_eq!(metadata.causation_id, Some(causation_id));
}

#[test]
fn test_metadata_with_additional() {
    let additional = json!({"key": "value"});
    let metadata = EventMetadata::new().with_additional(additional.clone());

    assert_eq!(metadata.additional, Some(additional));
}

#[test]
fn test_metadata_builder_chain() {
    let causation_id = Uuid::new_v4();
    let additional = json!({"test": true});

    let metadata = EventMetadata::new()
        .with_correlation_id("req-123")
        .with_causation_id(causation_id)
        .with_additional(additional.clone());

    assert_eq!(metadata.correlation_id.as_deref(), Some("req-123"));
    assert_eq!(metadata.causation_id, Some(causation_id));
    assert_eq!(metadata.additional, Some(additional));
}

#[test]
fn test_metadata_default() {
    let metadata = EventMetadata::default();

    assert!(metadata.correlation_id.is_none());
    assert!(metadata.causation_id.is_none());
}

#[test]
fn test_metadata_serialization() {
    let metadata = EventMetadata::new()
        .with_correlation_id("req-123")
        .with_causation_id(Uuid::nil());

    let json = serde_json::to_string(&metadata).unwrap();
    let deserialized: EventMetadata = serde_json::from_str(&json).unwrap();

    assert_eq!(metadata.correlation_id, deserialized.correlation_id);
    assert_eq!(metadata.causation_id, deserialized.causation_id);
}

#[test]
fn test_event_envelope_new() {
    let id = Uuid::new_v4();
    let aggregate_id = Uuid::new_v4();
    let event_data = json!({"value": 42});

    let envelope = EventEnvelope::new(
        id,
        aggregate_id,
        "TestAggregate".to_string(),
        "TestEvent".to_string(),
        EventVersion::new(1),
        event_data.clone(),
    );

    assert_eq!(envelope.id, id);
    assert_eq!(envelope.aggregate_id, aggregate_id);
    assert_eq!(envelope.aggregate_type, "TestAggregate");
    assert_eq!(envelope.event_type, "TestEvent");
    assert_eq!(envelope.event_version, EventVersion::new(1));
    assert_eq!(envelope.event_data, event_data);
    assert!(envelope.created_by.is_none());
    assert!(envelope.metadata.is_none());
}

#[test]
fn test_event_envelope_with_created_by() {
    let user_id = Uuid::new_v4();
    let envelope = EventEnvelope::new(
        Uuid::new_v4(),
        Uuid::new_v4(),
        "Test".to_string(),
        "TestEvent".to_string(),
        EventVersion::new(1),
        json!({}),
    )
    .with_created_by(user_id);

    assert_eq!(envelope.created_by, Some(user_id));
}

#[test]
fn test_event_envelope_with_metadata() {
    let metadata = EventMetadata::new().with_correlation_id(Uuid::new_v4());

    let envelope = EventEnvelope::new(
        Uuid::new_v4(),
        Uuid::new_v4(),
        "Test".to_string(),
        "TestEvent".to_string(),
        EventVersion::new(1),
        json!({}),
    )
    .with_metadata(metadata.clone());

    assert_eq!(envelope.metadata, Some(metadata));
}

#[test]
fn test_event_envelope_with_created_at() {
    let timestamp = Utc::now();
    let envelope = EventEnvelope::new(
        Uuid::new_v4(),
        Uuid::new_v4(),
        "Test".to_string(),
        "TestEvent".to_string(),
        EventVersion::new(1),
        json!({}),
    )
    .with_created_at(timestamp);

    assert_eq!(envelope.created_at, timestamp);
}

#[test]
fn test_event_envelope_builder_chain() {
    let user_id = Uuid::new_v4();
    let metadata = EventMetadata::new();
    let timestamp = Utc::now();

    let envelope = EventEnvelope::new(
        Uuid::new_v4(),
        Uuid::new_v4(),
        "Test".to_string(),
        "TestEvent".to_string(),
        EventVersion::new(1),
        json!({}),
    )
    .with_created_by(user_id)
    .with_metadata(metadata.clone())
    .with_created_at(timestamp);

    assert_eq!(envelope.created_by, Some(user_id));
    assert_eq!(envelope.metadata, Some(metadata));
    assert_eq!(envelope.created_at, timestamp);
}

#[test]
fn test_event_envelope_serialization() {
    let envelope = EventEnvelope::new(
        Uuid::nil(),
        Uuid::nil(),
        "Test".to_string(),
        "TestEvent".to_string(),
        EventVersion::new(1),
        json!({"key": "value"}),
    );

    let json = serde_json::to_string(&envelope).unwrap();
    let deserialized: EventEnvelope = serde_json::from_str(&json).unwrap();

    assert_eq!(envelope.id, deserialized.id);
    assert_eq!(envelope.aggregate_id, deserialized.aggregate_id);
    assert_eq!(envelope.event_type, deserialized.event_type);
}

#[test]
fn test_event_envelope_with_complex_data() {
    let complex_data = json!({
        "user": {
            "id": "123",
            "email": "test@example.com",
            "roles": ["admin", "user"]
        },
        "metadata": {
            "timestamp": "2024-01-01T00:00:00Z",
            "version": 2
        }
    });

    let envelope = EventEnvelope::new(
        Uuid::new_v4(),
        Uuid::new_v4(),
        "User".to_string(),
        "UserUpdated".to_string(),
        EventVersion::new(2),
        complex_data.clone(),
    );

    assert_eq!(envelope.event_data, complex_data);
}

#[test]
fn test_metadata_with_causation_chain() {
    let chain = vec![Uuid::new_v4(), Uuid::new_v4()];
    let metadata = EventMetadata::new().with_causation_chain(chain.clone());

    assert_eq!(metadata.causation_chain, chain);
    assert_eq!(metadata.cascade_depth(), 2);
}

#[test]
fn test_metadata_cascade_depth_empty() {
    let metadata = EventMetadata::new();
    assert_eq!(metadata.cascade_depth(), 0);
}

#[test]
fn test_metadata_child_metadata() {
    let parent_event_id = Uuid::new_v4();
    let parent = EventMetadata::new().with_correlation_id("req-123");

    let child = parent.child_metadata(parent_event_id);

    assert_eq!(child.correlation_id.as_deref(), Some("req-123"));
    assert_eq!(child.causation_id, Some(parent_event_id));
    assert_eq!(child.causation_chain.len(), 1);
    assert_eq!(child.causation_chain[0], parent_event_id);
}

#[test]
fn test_metadata_child_metadata_without_correlation_uses_parent_id() {
    let parent_event_id = Uuid::new_v4();
    let parent = EventMetadata::new();

    let child = parent.child_metadata(parent_event_id);

    assert_eq!(child.correlation_id, Some(parent_event_id.to_string()));
    assert_eq!(child.causation_id, Some(parent_event_id));
}

#[test]
fn test_metadata_child_metadata_extends_chain() {
    let root_id = Uuid::new_v4();
    let parent_id = Uuid::new_v4();
    let grandchild_parent_id = Uuid::new_v4();

    let root = EventMetadata::new();
    let parent = root.child_metadata(root_id);
    let grandchild = parent.child_metadata(parent_id);
    let great_grandchild = grandchild.child_metadata(grandchild_parent_id);

    assert_eq!(great_grandchild.causation_chain.len(), 3);
    assert_eq!(great_grandchild.causation_chain[0], root_id);
    assert_eq!(great_grandchild.causation_chain[1], parent_id);
    assert_eq!(great_grandchild.causation_chain[2], grandchild_parent_id);
    assert_eq!(great_grandchild.cascade_depth(), 3);
}

#[test]
fn test_metadata_causation_chain_serialization() {
    let chain = vec![Uuid::nil(), Uuid::new_v4()];
    let metadata = EventMetadata::new()
        .with_correlation_id("req-123")
        .with_causation_chain(chain.clone());

    let json = serde_json::to_string(&metadata).unwrap();
    let deserialized: EventMetadata = serde_json::from_str(&json).unwrap();

    assert_eq!(deserialized.causation_chain, chain);
}

#[test]
fn test_metadata_empty_causation_chain_not_serialized() {
    let metadata = EventMetadata::new();
    let json = serde_json::to_string(&metadata).unwrap();

    // Empty causation_chain should be skipped
    assert!(!json.contains("causation_chain"));
}

#[test]
fn test_metadata_deserialize_without_causation_chain() {
    // Old-format metadata without causation_chain should deserialize fine
    let json = r#"{"correlation_id":null,"causation_id":null,"timestamp":"2024-01-01T00:00:00Z"}"#;
    let metadata: EventMetadata = serde_json::from_str(json).unwrap();

    assert!(metadata.causation_chain.is_empty());
}

#[test]
fn test_metadata_deserialize_legacy_uuid_correlation_id() {
    let id = Uuid::new_v4();
    let json = format!(
        r#"{{"correlation_id":"{id}","causation_id":null,"timestamp":"2024-01-01T00:00:00Z"}}"#
    );
    let metadata: EventMetadata = serde_json::from_str(&json).unwrap();

    assert_eq!(metadata.correlation_id, Some(id.to_string()));
}

#[test]
fn test_metadata_deserialize_null_correlation_id() {
    let json = r#"{"correlation_id":null,"causation_id":null,"timestamp":"2024-01-01T00:00:00Z"}"#;
    let metadata: EventMetadata = serde_json::from_str(json).unwrap();

    assert_eq!(metadata.correlation_id, None);
}

#[test]
fn test_event_envelope_with_causation() {
    let causation_id = Uuid::new_v4();
    let correlation_id = "req-123";
    let chain = vec![Uuid::new_v4(), causation_id];

    let envelope = EventEnvelope::new(
        Uuid::new_v4(),
        Uuid::new_v4(),
        "Test".to_string(),
        "TestEvent".to_string(),
        crate::EventVersion::new(1),
        json!({}),
    )
    .with_causation(causation_id, correlation_id, chain.clone());

    let metadata = envelope.metadata.unwrap();
    assert_eq!(metadata.causation_id, Some(causation_id));
    assert_eq!(metadata.correlation_id.as_deref(), Some(correlation_id));
    assert_eq!(metadata.causation_chain, chain);
}

#[test]
fn test_event_envelope_with_causation_preserves_existing_metadata() {
    let causation_id = Uuid::new_v4();
    let correlation_id = "req-123";
    let additional = json!({"key": "value"});

    let envelope = EventEnvelope::new(
        Uuid::new_v4(),
        Uuid::new_v4(),
        "Test".to_string(),
        "TestEvent".to_string(),
        crate::EventVersion::new(1),
        json!({}),
    )
    .with_metadata(EventMetadata::new().with_additional(additional.clone()))
    .with_causation(causation_id, correlation_id, vec![]);

    let metadata = envelope.metadata.unwrap();
    assert_eq!(metadata.causation_id, Some(causation_id));
    assert_eq!(metadata.correlation_id.as_deref(), Some(correlation_id));
    assert_eq!(metadata.additional, Some(additional));
}

#[test]
fn test_version_in_envelope() {
    let v1_envelope = EventEnvelope::new(
        Uuid::new_v4(),
        Uuid::new_v4(),
        "Test".to_string(),
        "TestEvent".to_string(),
        EventVersion::new(1),
        json!({}),
    );

    let v2_envelope = EventEnvelope::new(
        Uuid::new_v4(),
        Uuid::new_v4(),
        "Test".to_string(),
        "TestEvent".to_string(),
        EventVersion::new(2),
        json!({}),
    );

    assert_eq!(v1_envelope.event_version, EventVersion::new(1));
    assert_eq!(v2_envelope.event_version, EventVersion::new(2));
    assert!(v2_envelope.event_version > v1_envelope.event_version);
}

fn full_metadata() -> EventMetadata {
    let mut metadata = EventMetadata::new()
        .with_correlation_id("req-1")
        .with_tenant("acme")
        .with_command("PlaceOrder")
        .with_correlation_path(vec!["root".to_string(), "leaf".to_string()])
        .with_causation_id(Uuid::new_v4())
        .with_additional(json!({"ip": "10.0.0.1"}));
    metadata.causation_chain = vec![Uuid::new_v4()];
    metadata
}

#[test]
fn test_metadata_builders_set_tenant_command_and_path() {
    let metadata = EventMetadata::new()
        .with_tenant("acme")
        .with_command("PlaceOrder")
        .with_correlation_path(vec!["a".to_string(), "b".to_string()]);

    assert_eq!(metadata.tenant.as_deref(), Some("acme"));
    assert_eq!(metadata.command.as_deref(), Some("PlaceOrder"));
    assert_eq!(metadata.correlation_path, vec!["a", "b"]);
}

#[test]
fn test_metadata_new_fields_round_trip_through_json() {
    let metadata = full_metadata();

    let json = serde_json::to_string(&metadata).unwrap();
    let deserialized: EventMetadata = serde_json::from_str(&json).unwrap();

    assert_eq!(deserialized, metadata);
}

#[test]
fn test_metadata_empty_new_fields_are_not_serialized() {
    let json = serde_json::to_string(&EventMetadata::new()).unwrap();

    assert!(!json.contains("tenant"));
    assert!(!json.contains("command"));
    assert!(!json.contains("correlation_path"));
}

#[test]
fn test_metadata_deserialize_without_new_fields() {
    let json = r#"{"correlation_id":null,"causation_id":null,"timestamp":"2024-01-01T00:00:00Z"}"#;
    let metadata: EventMetadata = serde_json::from_str(json).unwrap();

    assert_eq!(metadata.tenant, None);
    assert_eq!(metadata.command, None);
    assert!(metadata.correlation_path.is_empty());
}

#[test]
fn test_fill_missing_from_keeps_every_receiver_field() {
    let mut receiver = full_metadata();
    let expected = receiver.clone();
    let other = EventMetadata::new()
        .with_correlation_id("req-2")
        .with_tenant("globex")
        .with_command("Other")
        .with_correlation_path(vec!["x".to_string()])
        .with_causation_id(Uuid::new_v4())
        .with_causation_chain(vec![Uuid::new_v4()])
        .with_additional(json!({"ip": "10.0.0.2"}));

    receiver.fill_missing_from(&other);

    assert_eq!(receiver, expected);
}

#[test]
fn test_fill_missing_from_fills_empty_options_and_path() {
    let mut receiver = EventMetadata::new();
    let other = full_metadata();

    receiver.fill_missing_from(&other);

    assert_eq!(receiver.correlation_id, other.correlation_id);
    assert_eq!(receiver.tenant, other.tenant);
    assert_eq!(receiver.command, other.command);
    assert_eq!(receiver.correlation_path, other.correlation_path);
}

#[test]
fn test_fill_missing_from_fills_each_option_independently() {
    let mut receiver = EventMetadata::new().with_tenant("acme");
    let other = EventMetadata::new()
        .with_tenant("globex")
        .with_command("PlaceOrder");

    receiver.fill_missing_from(&other);

    assert_eq!(receiver.tenant.as_deref(), Some("acme"));
    assert_eq!(receiver.command.as_deref(), Some("PlaceOrder"));
}

#[test]
fn test_fill_missing_from_takes_causation_pair_when_receiver_has_none() {
    let mut receiver = EventMetadata::new();
    let other = full_metadata();

    receiver.fill_missing_from(&other);

    assert_eq!(receiver.causation_id, other.causation_id);
    assert_eq!(receiver.causation_chain, other.causation_chain);
}

#[test]
fn test_fill_missing_from_keeps_causation_pair_when_receiver_has_an_id() {
    let id = Uuid::new_v4();
    let mut receiver = EventMetadata::new().with_causation_id(id);
    let other = full_metadata();

    receiver.fill_missing_from(&other);

    assert_eq!(receiver.causation_id, Some(id));
    assert!(receiver.causation_chain.is_empty());
}

#[test]
fn test_fill_missing_from_keeps_causation_pair_when_receiver_has_a_chain() {
    let chain = vec![Uuid::new_v4()];
    let mut receiver = EventMetadata::new().with_causation_chain(chain.clone());
    let other = full_metadata();

    receiver.fill_missing_from(&other);

    assert_eq!(receiver.causation_id, None);
    assert_eq!(receiver.causation_chain, chain);
}

#[test]
fn test_fill_missing_from_merges_additional_objects_receiver_winning() {
    let mut receiver = EventMetadata::new().with_additional(json!({"a": 1, "b": 2}));
    let other = EventMetadata::new().with_additional(json!({"b": 9, "c": 3}));

    receiver.fill_missing_from(&other);

    assert_eq!(receiver.additional, Some(json!({"a": 1, "b": 2, "c": 3})));
}

#[test]
fn test_fill_missing_from_takes_additional_when_receiver_has_none() {
    let mut receiver = EventMetadata::new();
    let other = EventMetadata::new().with_additional(json!({"a": 1}));

    receiver.fill_missing_from(&other);

    assert_eq!(receiver.additional, Some(json!({"a": 1})));
}

#[test]
fn test_fill_missing_from_keeps_non_object_additional() {
    let mut receiver = EventMetadata::new().with_additional(json!("note"));
    let other = EventMetadata::new().with_additional(json!({"a": 1}));

    receiver.fill_missing_from(&other);

    assert_eq!(receiver.additional, Some(json!("note")));
}

#[test]
fn test_fill_missing_from_keeps_additional_when_other_is_not_an_object() {
    let mut receiver = EventMetadata::new().with_additional(json!({"a": 1}));
    let other = EventMetadata::new().with_additional(json!("note"));

    receiver.fill_missing_from(&other);

    assert_eq!(receiver.additional, Some(json!({"a": 1})));
}

#[test]
fn test_fill_missing_from_keeps_receiver_timestamp() {
    let mut receiver = EventMetadata::new();
    let timestamp = receiver.timestamp;
    let mut other = EventMetadata::new();
    other.timestamp = timestamp + chrono::Duration::seconds(60);

    receiver.fill_missing_from(&other);

    assert_eq!(receiver.timestamp, timestamp);
}

#[test]
fn test_child_metadata_carries_tenant_correlation_and_path_only() {
    let parent = EventMetadata::new()
        .with_correlation_id("req-1")
        .with_tenant("acme")
        .with_command("PlaceOrder")
        .with_correlation_path(vec!["root".to_string()])
        .with_additional(json!({"ip": "10.0.0.1"}));

    let child = parent.child_metadata(Uuid::new_v4());

    assert_eq!(child.correlation_id.as_deref(), Some("req-1"));
    assert_eq!(child.tenant.as_deref(), Some("acme"));
    assert_eq!(child.correlation_path, vec!["root"]);
    assert_eq!(child.command, None);
    assert_eq!(child.additional, None);
}
