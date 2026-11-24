use super::*;

#[test]
fn the_default_schema_is_event_sauce_login() {
    assert_eq!(DEFAULT_SCHEMA, "event_sauce_login");
}

#[test]
fn plain_lowercase_identifiers_are_valid_schema_names() {
    for name in ["event_sauce_login", "_auth", "a", "login2"] {
        assert!(validate_schema(name).is_ok(), "{name}");
    }
}

#[test]
fn names_that_would_need_quoting_are_refused() {
    for name in [
        "", "1login", "Login", "a-b", "a b", "a.b", "a\"b", "é", "a;drop",
    ] {
        assert_eq!(
            validate_schema(name),
            Err(InvalidSchemaName(name.to_owned())),
            "{name}"
        );
    }
}

#[test]
fn names_longer_than_postgres_identifiers_are_refused() {
    assert!(validate_schema(&"a".repeat(63)).is_ok());
    assert!(validate_schema(&"a".repeat(64)).is_err());
}
