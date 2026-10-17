use super::*;
use event_sauce_core::EventEnvelope;
use sqlx::types::Json;

fn bare_row() -> EventRow {
    EventRow {
        id: 1,
        event_id: uuid::Uuid::new_v4(),
        aggregate_id: uuid::Uuid::new_v4(),
        aggregate_type: "Order".to_string(),
        event_type: "Created".to_string(),
        event_version: 1,
        event_data: serde_json::json!({}),
        created_by: None,
        created_at: chrono::Utc::now(),
        correlation_id: None,
        causation_id: None,
        metadata: None,
        causation_chain: None,
        tenant: None,
        command: None,
        correlation_path: None,
    }
}

#[test]
fn row_without_metadata_columns_has_no_metadata() {
    assert!(EventEnvelope::from(bare_row()).metadata.is_none());
}

#[test]
fn row_with_empty_json_chains_has_no_metadata() {
    let mut row = bare_row();
    row.causation_chain = Some(Json(Vec::new()));
    row.correlation_path = Some(Json(Vec::new()));

    assert!(EventEnvelope::from(row).metadata.is_none());
}

fn additional_of(value: serde_json::Value) -> Option<serde_json::Value> {
    let mut row = bare_row();
    row.metadata = Some(value);
    EventEnvelope::from(row)
        .metadata
        .and_then(|m| m.additional)
        .map(serde_json::Value::Object)
}

#[test]
fn object_metadata_reads_back_unchanged() {
    assert_eq!(
        additional_of(serde_json::json!({"k": 1})),
        Some(serde_json::json!({"k": 1}))
    );
}

#[test]
fn non_object_metadata_is_kept_under_the_value_key() {
    assert_eq!(
        additional_of(serde_json::json!("note")),
        Some(serde_json::json!({"value": "note"}))
    );
    assert_eq!(
        additional_of(serde_json::json!([1, 2])),
        Some(serde_json::json!({"value": [1, 2]}))
    );
}

#[test]
fn null_metadata_reads_as_no_metadata() {
    assert_eq!(additional_of(serde_json::Value::Null), None);
}

#[test]
fn any_single_metadata_column_yields_metadata() {
    let setters: Vec<fn(&mut EventRow)> = vec![
        |r| r.correlation_id = Some("c".to_string()),
        |r| r.causation_id = Some(uuid::Uuid::new_v4()),
        |r| r.metadata = Some(serde_json::json!({"k": 1})),
        |r| r.tenant = Some("acme".to_string()),
        |r| r.command = Some("Place".to_string()),
        |r| r.causation_chain = Some(Json(vec![uuid::Uuid::new_v4()])),
        |r| r.correlation_path = Some(Json(vec!["root".to_string()])),
    ];

    for set in setters {
        let mut row = bare_row();
        set(&mut row);
        assert!(EventEnvelope::from(row).metadata.is_some());
    }
}
