use super::*;

#[test]
fn keys_are_stored_under_the_default_prefix() {
    assert_eq!(
        storage_key(DEFAULT_PREFIX, &LoginKey::new("pending", "a")),
        "event_sauce:login:pending:a"
    );
}

#[test]
fn a_custom_prefix_is_prepended_verbatim() {
    assert_eq!(
        storage_key("app:", &LoginKey::new("totp", "u:1")),
        "app:totp:u:1"
    );
}
