use crate::event_context::{current_snapshot, ContextSnapshot};
use crate::EventContext;
use serde_json::json;

fn snapshot_of(ctx: EventContext<impl Sized>) -> Option<ContextSnapshot> {
    ctx.in_scope(current_snapshot)
}

#[test]
fn empty_macro_builds_an_empty_nested_context() {
    assert_eq!(snapshot_of(event_context!()), None);
}

#[test]
fn root_builds_a_root_context_hiding_the_surroundings() {
    let snapshot = EventContext::new()
        .with("outer", 1_u8)
        .in_scope(|| snapshot_of(event_context!(root, correlation_id = "c")));
    let snapshot = snapshot.unwrap();
    assert_eq!(snapshot.correlation_id.as_deref(), Some("c"));
    assert_eq!(snapshot.additional, None);
}

#[test]
fn bare_root_is_accepted() {
    let snapshot = EventContext::new()
        .with("outer", 1_u8)
        .in_scope(|| snapshot_of(event_context!(root)));
    assert_eq!(snapshot, None);
}

#[test]
fn tenant_after_root_sets_the_tenant() {
    let snapshot = snapshot_of(event_context!(root, tenant = "acme")).unwrap();
    assert_eq!(snapshot.tenant.as_deref(), Some("acme"));
}

#[test]
fn correlation_id_sets_the_correlation_id() {
    let snapshot = snapshot_of(event_context!(correlation_id = "req-1")).unwrap();
    assert_eq!(snapshot.correlation_id.as_deref(), Some("req-1"));
}

#[test]
fn identifier_key_stores_the_value() {
    let snapshot = snapshot_of(event_context!(region = "eu")).unwrap();
    assert_eq!(snapshot.additional, Some(json!({"region": "eu"})));
}

#[test]
fn string_literal_key_stores_the_value() {
    let snapshot = snapshot_of(event_context!("http.method" = "GET")).unwrap();
    assert_eq!(snapshot.additional, Some(json!({"http.method": "GET"})));
}

#[test]
fn percent_value_stores_the_display_form() {
    let id = 7_u32;
    let snapshot = snapshot_of(event_context!(user = %id)).unwrap();
    assert_eq!(snapshot.additional, Some(json!({"user": "7"})));
}

#[test]
fn percent_value_with_a_string_literal_key_stores_the_display_form() {
    let snapshot = snapshot_of(event_context!("a.b" = %1.5_f64)).unwrap();
    assert_eq!(snapshot.additional, Some(json!({"a.b": "1.5"})));
}

#[test]
fn bare_identifier_uses_its_name_as_key() {
    let attempt = 3_u32;
    let snapshot = snapshot_of(event_context!(attempt)).unwrap();
    assert_eq!(snapshot.additional, Some(json!({"attempt": 3})));
}

#[test]
fn percent_identifier_uses_its_name_as_key_and_display_form() {
    let attempt = 3_u32;
    let snapshot = snapshot_of(event_context!(%attempt)).unwrap();
    assert_eq!(snapshot.additional, Some(json!({"attempt": "3"})));
}

#[test]
fn fields_combine_in_any_order_with_a_trailing_comma() {
    let attempt = 2_u32;
    let snapshot = snapshot_of(event_context!(
        root,
        region = "eu",
        %attempt,
        tenant = "acme",
        correlation_id = "req-1",
    ))
    .unwrap();
    assert_eq!(snapshot.tenant.as_deref(), Some("acme"));
    assert_eq!(snapshot.correlation_id.as_deref(), Some("req-1"));
    assert_eq!(
        snapshot.additional,
        Some(json!({"region": "eu", "attempt": "2"}))
    );
}

#[test]
fn value_expressions_may_contain_commas_and_calls() {
    let snapshot = snapshot_of(event_context!(sum = [1_u8, 2].len(), other = "x")).unwrap();
    assert_eq!(snapshot.additional, Some(json!({"sum": 2, "other": "x"})));
}
