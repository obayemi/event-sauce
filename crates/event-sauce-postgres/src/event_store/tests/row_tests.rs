use super::*;
use event_sauce_core::{EventEnvelope, EventMetadata, EventVersion, StreamCommit};
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

fn commit_of(events: Vec<EventEnvelope>, expected_version: i64) -> StreamCommit {
    StreamCommit {
        stream_id: event_sauce_core::StreamId::new("Order", uuid::Uuid::new_v4()),
        events,
        expected_version: event_sauce_core::AggregateVersion::new(expected_version),
        claims: vec![],
        clear_claims: false,
    }
}

fn envelope(stream: &StreamCommit, metadata: Option<EventMetadata>) -> EventEnvelope {
    let mut event = EventEnvelope::new(
        uuid::Uuid::new_v4(),
        stream.stream_id.aggregate_id(),
        "Order".to_owned(),
        "Created".to_owned(),
        EventVersion::new(2),
        serde_json::json!({"k": 1}),
    );
    event.metadata = metadata;
    event
}

#[test]
fn columns_number_stream_versions_from_the_expected_version() {
    let mut commit = commit_of(vec![], 3);
    commit.events = vec![envelope(&commit, None), envelope(&commit, None)];

    let columns = EventColumns::from_commit(&commit);

    assert_eq!(columns.stream_versions, vec![3, 4]);
    assert_eq!(columns.event_versions, vec![2, 2]);
    assert_eq!(columns.aggregate_types, vec!["Order", "Order"]);
    assert_eq!(columns.event_ids.len(), 2);
}

#[test]
fn columns_of_events_without_metadata_are_all_null() {
    let mut commit = commit_of(vec![], 0);
    commit.events = vec![envelope(&commit, None)];

    let columns = EventColumns::from_commit(&commit);

    assert_eq!(columns.correlation_ids, vec![None]);
    assert_eq!(columns.causation_ids, vec![None]);
    assert!(columns.metadatas[0].is_none());
    assert!(columns.causation_chains[0].is_none());
    assert_eq!(columns.tenants, vec![None]);
    assert_eq!(columns.commands, vec![None]);
    assert!(columns.correlation_paths[0].is_none());
}

#[test]
fn columns_project_every_metadata_field() {
    let cause = uuid::Uuid::new_v4();
    let mut metadata = EventMetadata::new();
    metadata.correlation_id = Some("corr".to_owned());
    metadata.causation_id = Some(cause);
    metadata.causation_chain = vec![cause];
    metadata.tenant = Some("acme".to_owned());
    metadata.command = Some("Place".to_owned());
    metadata.correlation_path = vec!["root".to_owned()];
    metadata.additional = serde_json::json!({"k": 1}).as_object().cloned();
    let mut commit = commit_of(vec![], 0);
    commit.events = vec![envelope(&commit, Some(metadata))];

    let columns = EventColumns::from_commit(&commit);

    assert_eq!(columns.correlation_ids, vec![Some("corr".to_owned())]);
    assert_eq!(columns.causation_ids, vec![Some(cause)]);
    assert_eq!(columns.causation_chains[0].as_ref().unwrap().0, vec![cause]);
    assert_eq!(columns.tenants, vec![Some("acme".to_owned())]);
    assert_eq!(columns.commands, vec![Some("Place".to_owned())]);
    assert_eq!(
        columns.correlation_paths[0].as_ref().unwrap().0,
        vec!["root".to_owned()]
    );
    assert_eq!(
        columns.metadatas[0].as_ref().unwrap().0,
        serde_json::json!({"k": 1}).as_object().cloned().unwrap()
    );
}

#[test]
fn empty_chains_are_written_as_null() {
    let mut commit = commit_of(vec![], 0);
    commit.events = vec![envelope(&commit, Some(EventMetadata::new()))];

    let columns = EventColumns::from_commit(&commit);

    assert!(columns.causation_chains[0].is_none());
    assert!(columns.correlation_paths[0].is_none());
}
