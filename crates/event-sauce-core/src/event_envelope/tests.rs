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
    let correlation_id = Uuid::new_v4();
    let metadata = EventMetadata::new().with_correlation_id(correlation_id);

    assert_eq!(metadata.correlation_id, Some(correlation_id));
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
    let correlation_id = Uuid::new_v4();
    let causation_id = Uuid::new_v4();
    let additional = json!({"test": true});

    let metadata = EventMetadata::new()
        .with_correlation_id(correlation_id)
        .with_causation_id(causation_id)
        .with_additional(additional.clone());

    assert_eq!(metadata.correlation_id, Some(correlation_id));
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
        .with_correlation_id(Uuid::nil())
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
    let correlation_id = Uuid::new_v4();
    let parent = EventMetadata::new().with_correlation_id(correlation_id);

    let child = parent.child_metadata(parent_event_id);

    assert_eq!(child.correlation_id, Some(correlation_id));
    assert_eq!(child.causation_id, Some(parent_event_id));
    assert_eq!(child.causation_chain.len(), 1);
    assert_eq!(child.causation_chain[0], parent_event_id);
}

#[test]
fn test_metadata_child_metadata_without_correlation_uses_parent_id() {
    let parent_event_id = Uuid::new_v4();
    let parent = EventMetadata::new();

    let child = parent.child_metadata(parent_event_id);

    assert_eq!(child.correlation_id, Some(parent_event_id));
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
        .with_correlation_id(Uuid::nil())
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
fn test_event_envelope_with_causation() {
    let causation_id = Uuid::new_v4();
    let correlation_id = Uuid::new_v4();
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
    assert_eq!(metadata.correlation_id, Some(correlation_id));
    assert_eq!(metadata.causation_chain, chain);
}

#[test]
fn test_event_envelope_with_causation_preserves_existing_metadata() {
    let causation_id = Uuid::new_v4();
    let correlation_id = Uuid::new_v4();
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
    assert_eq!(metadata.correlation_id, Some(correlation_id));
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
