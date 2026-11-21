use super::*;

#[test]
fn a_key_joins_its_namespace_and_id_with_a_colon() {
    let key = LoginKey::new("pending", "7b0e");

    assert_eq!(key.as_str(), "pending:7b0e");
    assert_eq!(key.to_string(), "pending:7b0e");
}

#[test]
fn a_key_accepts_any_displayable_id() {
    assert_eq!(LoginKey::new("totp", 42_u64).as_str(), "totp:42");
}

#[test]
fn keys_with_the_same_parts_are_equal() {
    assert_eq!(LoginKey::new("pending", "a"), LoginKey::new("pending", "a"));
    assert_ne!(LoginKey::new("pending", "a"), LoginKey::new("totp", "a"));
}

#[test]
fn a_backend_error_reports_its_context_and_keeps_its_source() {
    let error = LoginStoreError::backend("redis down", std::io::Error::other("refused"));

    assert_eq!(error.to_string(), "login store backend failed: redis down");
    assert!(std::error::Error::source(&error).is_some());
}
