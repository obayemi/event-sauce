use super::*;
use serde_json::{json, Map, Value};
use std::panic::{catch_unwind, AssertUnwindSafe};

fn object(value: Value) -> Map<String, Value> {
    match value {
        Value::Object(map) => map,
        other => panic!("not an object: {other}"),
    }
}

fn snapshot() -> crate::EventMetadata {
    current_snapshot().expect("a context is active")
}

#[test]
fn no_active_context_has_no_snapshot() {
    assert_eq!(current_snapshot(), None);
    assert!(!has_correlation_id());
}

#[test]
fn a_context_without_fields_has_no_snapshot() {
    EventContext::new().in_scope(|| assert_eq!(current_snapshot(), None));
}

#[test]
fn in_scope_exposes_every_field() {
    EventContext::root()
        .tenant("acme")
        .correlation_id("req-1")
        .command("place-order")
        .with("region", "eu")
        .in_scope(|| {
            let snap = snapshot();
            assert_eq!(snap.tenant.as_deref(), Some("acme"));
            assert_eq!(snap.correlation_id.as_deref(), Some("req-1"));
            assert_eq!(snap.correlation_path, ["req-1"]);
            assert_eq!(snap.command.as_deref(), Some("place-order"));
            assert_eq!(snap.additional, Some(object(json!({"region": "eu"}))));
            assert!(has_correlation_id());
        });
    assert_eq!(current_snapshot(), None);
}

#[test]
fn in_scope_returns_the_closure_result() {
    assert_eq!(EventContext::new().in_scope(|| 7), 7);
}

#[test]
fn the_outermost_correlation_id_wins() {
    EventContext::new().correlation_id("outer").in_scope(|| {
        EventContext::new().correlation_id("inner").in_scope(|| {
            assert_eq!(snapshot().correlation_id.as_deref(), Some("outer"));
        });
    });
}

#[test]
fn an_inner_frame_can_supply_the_first_correlation_id() {
    EventContext::new().with("a", 1).in_scope(|| {
        assert!(!has_correlation_id());
        EventContext::new().correlation_id("inner").in_scope(|| {
            assert_eq!(snapshot().correlation_id.as_deref(), Some("inner"));
            assert!(has_correlation_id());
        });
    });
}

#[test]
fn the_correlation_path_lists_every_frame_without_consecutive_duplicates() {
    EventContext::new().correlation_id("a").in_scope(|| {
        EventContext::new().correlation_id("a").in_scope(|| {
            EventContext::new().correlation_id("b").in_scope(|| {
                EventContext::new().correlation_id("a").in_scope(|| {
                    assert_eq!(snapshot().correlation_path, ["a", "b", "a"]);
                });
            });
        });
    });
}

#[test]
fn the_innermost_tenant_wins() {
    EventContext::root().tenant("outer").in_scope(|| {
        EventContext::new().with("k", 1).in_scope(|| {
            assert_eq!(snapshot().tenant.as_deref(), Some("outer"));
        });
        EventContext::root().tenant("inner").in_scope(|| {
            assert_eq!(snapshot().tenant.as_deref(), Some("inner"));
        });
    });
}

#[test]
fn the_innermost_command_wins() {
    EventContext::new().command("outer").in_scope(|| {
        EventContext::new().command("inner").in_scope(|| {
            assert_eq!(snapshot().command.as_deref(), Some("inner"));
        });
        EventContext::new().with("k", 1).in_scope(|| {
            assert_eq!(snapshot().command.as_deref(), Some("outer"));
        });
    });
}

#[test]
fn additional_values_merge_with_the_innermost_winning() {
    EventContext::new().with("a", 1).with("b", 1).in_scope(|| {
        EventContext::new().with("b", 2).with("c", 3).in_scope(|| {
            assert_eq!(
                Value::Object(snapshot().additional.unwrap()),
                json!({"a": 1, "b": 2, "c": 3})
            );
        });
        assert_eq!(
            Value::Object(snapshot().additional.unwrap()),
            json!({"a": 1, "b": 1})
        );
    });
}

#[test]
fn a_root_frame_discards_everything_outside_it() {
    EventContext::root()
        .tenant("acme")
        .correlation_id("outer")
        .command("outer-cmd")
        .with("k", 1)
        .in_scope(|| {
            EventContext::root().in_scope(|| assert_eq!(current_snapshot(), None));
            EventContext::root()
                .correlation_id("fresh")
                .with("other", 2)
                .in_scope(|| {
                    let snap = snapshot();
                    assert_eq!(snap.tenant, None);
                    assert_eq!(snap.correlation_path, ["fresh"]);
                    assert_eq!(snap.command, None);
                    assert_eq!(snap.additional, Some(object(json!({"other": 2}))));
                });
        });
}

#[test]
fn frames_nested_under_a_root_see_only_the_root_and_later_frames() {
    EventContext::new().correlation_id("ignored").in_scope(|| {
        EventContext::root().correlation_id("base").in_scope(|| {
            EventContext::new().correlation_id("child").in_scope(|| {
                let snap = snapshot();
                assert_eq!(snap.correlation_id.as_deref(), Some("base"));
                assert_eq!(snap.correlation_path, ["base", "child"]);
            });
        });
    });
}

#[test]
fn entered_guards_restore_the_previous_context() {
    let outer = EventContext::new().with("level", 1).entered();
    {
        let inner = EventContext::new().with("level", 2).entered();
        assert_eq!(
            Value::Object(snapshot().additional.unwrap()),
            json!({"level": 2})
        );
        drop(inner);
    }
    assert_eq!(
        Value::Object(snapshot().additional.unwrap()),
        json!({"level": 1})
    );
    drop(outer);
    assert_eq!(current_snapshot(), None);
}

#[test]
fn the_stack_is_restored_after_a_panic() {
    let outcome = catch_unwind(AssertUnwindSafe(|| {
        EventContext::new().with("k", 1).in_scope(|| {
            EventContext::new().with("j", 2).in_scope(|| panic!("boom"));
        });
    }));
    assert!(outcome.is_err());
    assert_eq!(current_snapshot(), None);
}

#[test]
fn current_reproduces_the_effective_context_on_an_empty_stack() {
    let (expected, rebuilt) = EventContext::root()
        .tenant("acme")
        .correlation_id("req")
        .with("k", "v")
        .in_scope(|| {
            EventContext::new()
                .command("cmd")
                .with("j", 1)
                .in_scope(|| (snapshot(), EventContext::current()))
        });
    assert_eq!(current_snapshot(), None);
    rebuilt.in_scope(|| {
        let snap = snapshot();
        assert_eq!(snap.tenant, expected.tenant);
        assert_eq!(snap.correlation_path, expected.correlation_path);
        assert_eq!(snap.command, expected.command);
        assert_eq!(snap.additional, expected.additional);
    });
}

#[test]
fn current_without_a_context_is_empty() {
    EventContext::current().in_scope(|| assert_eq!(current_snapshot(), None));
}

#[test]
fn current_is_a_root_that_discards_the_surrounding_context() {
    let captured = EventContext::new()
        .correlation_id("a")
        .in_scope(EventContext::current);
    EventContext::new()
        .correlation_id("other")
        .with("x", 1)
        .in_scope(|| {
            captured.in_scope(|| {
                assert_eq!(snapshot().correlation_id.as_deref(), Some("a"));
                assert!(snapshot().additional.is_none());
            });
        });
}

#[test]
fn default_is_an_empty_nested_context() {
    EventContext::default().in_scope(|| assert_eq!(current_snapshot(), None));
}

#[test]
fn current_keeps_the_full_correlation_path() {
    let captured = EventContext::new().correlation_id("a").in_scope(|| {
        EventContext::new()
            .correlation_id("b")
            .in_scope(EventContext::current)
    });
    captured.in_scope(|| {
        assert_eq!(snapshot().correlation_path, ["a", "b"]);
        assert_eq!(snapshot().correlation_id.as_deref(), Some("a"));
        assert!(has_correlation_id());
        EventContext::new().correlation_id("c").in_scope(|| {
            assert_eq!(snapshot().correlation_path, ["a", "b", "c"]);
        });
    });
}

#[test]
fn with_accepts_any_json_convertible_value() {
    EventContext::new()
        .with("flag", true)
        .with("count", 2_u32)
        .with("tags", vec!["a", "b"])
        .with("none", None::<String>)
        .with("shown", display(7))
        .in_scope(|| {
            assert_eq!(
                Value::Object(snapshot().additional.unwrap()),
                json!({
                    "flag": true,
                    "count": 2,
                    "tags": ["a", "b"],
                    "none": null,
                    "shown": "7",
                })
            );
        });
}
