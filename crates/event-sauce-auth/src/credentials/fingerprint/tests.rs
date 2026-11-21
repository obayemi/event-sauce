use super::*;

fn of(fields: &[&str]) -> [u8; 32] {
    let mut fingerprint = Fingerprint::new();
    for field in fields {
        fingerprint.str_field(field);
    }
    fingerprint.finish()
}

#[test]
fn moving_a_boundary_between_fields_changes_the_fingerprint() {
    assert_ne!(of(&["ab", "c"]), of(&["a", "bc"]));
}

#[test]
fn an_empty_field_is_not_nothing() {
    assert_ne!(of(&["a", ""]), of(&["a"]));
    assert_ne!(of(&[""]), of(&[]));
}

#[test]
fn the_same_fields_give_the_same_fingerprint() {
    assert_eq!(of(&["a", "b"]), of(&["a", "b"]));
}

#[test]
fn str_field_feeds_the_utf8_bytes() {
    let mut text = Fingerprint::new();
    text.str_field("é");
    let mut bytes = Fingerprint::new();
    bytes.field("é".as_bytes());
    assert_eq!(text.finish(), bytes.finish());
}

#[test]
fn fields_chain() {
    let mut chained = Fingerprint::new();
    chained.field(b"a").str_field("b");
    assert_eq!(chained.finish(), of(&["a", "b"]));
}

#[test]
fn debug_hides_the_digest_state() {
    let mut fingerprint = Fingerprint::new();
    fingerprint.str_field("secret");
    assert_eq!(format!("{fingerprint:?}"), "Fingerprint { .. }");
}
