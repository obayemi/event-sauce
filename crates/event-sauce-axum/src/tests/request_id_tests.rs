use super::*;

#[test]
fn parse_accepts_ids_of_one_to_128_allowed_characters() {
    for ok in ["a", "req-123", "A.b_c-9", &"x".repeat(128)] {
        assert_eq!(RequestId::parse(ok).unwrap().as_str(), ok);
    }
}

#[test]
fn parse_rejects_empty_oversized_and_unsafe_ids() {
    for bad in ["", &"x".repeat(129), "a b", "a/b", "é", "a\nb", "a;b"] {
        assert!(RequestId::parse(bad).is_none(), "{bad:?}");
    }
}

#[test]
fn generated_ids_are_well_formed_and_distinct() {
    let (a, b) = (RequestId::generate(), RequestId::generate());
    assert_ne!(a, b);
    assert!(RequestId::parse(a.as_str()).is_some());
    assert!(uuid::Uuid::parse_str(a.as_str()).is_ok());
}

#[test]
fn display_renders_the_raw_id() {
    assert_eq!(RequestId::parse("req-1").unwrap().to_string(), "req-1");
}
