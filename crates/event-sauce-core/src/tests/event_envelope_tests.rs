use super::*;
use serde_json::json;

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
