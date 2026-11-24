use super::*;
use crate::event_context::current_snapshot;
#[cfg(feature = "event-sourcing")]
use crate::EventEnvelope;

fn snapshot() -> crate::EventMetadata {
    current_snapshot().expect("a context is active")
}

#[cfg(feature = "event-sourcing")]
fn envelope_with(metadata: Option<crate::EventMetadata>) -> EventEnvelope {
    let mut envelope = crate::test_fixtures::create_test_envelope("Placed", "Order");
    envelope.metadata = metadata;
    envelope
}

#[cfg(feature = "event-sourcing")]
fn reaction_of(envelope: &EventEnvelope) -> crate::EventMetadata {
    envelope
        .metadata
        .clone()
        .unwrap_or_default()
        .child_metadata(envelope.id)
}

#[cfg(feature = "event-sourcing")]
#[test]
fn a_reaction_context_carries_tenant_id_path_and_policy_name() {
    let metadata = crate::EventMetadata::new()
        .with_correlation_id("req")
        .with_correlation_path(vec!["req".into(), "import".into()]);
    let metadata = crate::EventMetadata {
        tenant: Some("acme".into()),
        ..metadata
    };
    reaction_context(&reaction_of(&envelope_with(Some(metadata))), "notify").in_scope(|| {
        let snap = snapshot();
        assert_eq!(snap.tenant.as_deref(), Some("acme"));
        assert_eq!(snap.correlation_id.as_deref(), Some("req"));
        assert_eq!(snap.correlation_path, ["req", "import"]);
        assert_eq!(snap.command.as_deref(), Some("notify"));
    });
}

#[cfg(feature = "event-sourcing")]
#[test]
fn a_reaction_context_without_a_path_starts_it_at_the_id() {
    let metadata = crate::EventMetadata::new().with_correlation_id("req");
    reaction_context(&reaction_of(&envelope_with(Some(metadata))), "notify").in_scope(|| {
        assert_eq!(snapshot().correlation_path, ["req"]);
    });
}

#[cfg(feature = "event-sourcing")]
#[test]
fn a_reaction_context_without_metadata_starts_at_the_source_event_id() {
    let envelope = envelope_with(None);
    reaction_context(&reaction_of(&envelope), "notify").in_scope(|| {
        assert_eq!(snapshot().correlation_path, [envelope.id.to_string()]);
    });
}

#[test]
fn an_execution_context_generates_a_correlation_id_when_none_is_active() {
    let context = execution_context(Some("place-order"));
    context.in_scope(|| {
        let snap = snapshot();
        assert_eq!(snap.command.as_deref(), Some("place-order"));
        assert_eq!(snap.correlation_path.len(), 1);
        assert_eq!(
            snap.correlation_id.as_deref(),
            snap.correlation_path.first().map(String::as_str)
        );
    });
}

#[test]
fn an_execution_context_keeps_the_active_correlation_id() {
    EventContext::new().correlation_id("req-1").in_scope(|| {
        execution_context(None).in_scope(|| {
            assert_eq!(snapshot().correlation_path, ["req-1"]);
            assert_eq!(snapshot().command, None);
        });
    });
}
