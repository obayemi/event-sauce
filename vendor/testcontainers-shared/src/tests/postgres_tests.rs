use uuid::Uuid;

use super::{database_name, database_url};
use crate::server::Address;

#[test]
fn database_url_points_at_the_named_database() {
    let address = Address::new("localhost", 5432);

    assert_eq!(
        database_url(&address, "test_x"),
        "postgresql://postgres:postgres@localhost:5432/test_x"
    );
}

#[test]
fn database_name_is_a_valid_unquoted_identifier() {
    let name = database_name(Uuid::new_v4());

    assert!(name.starts_with("test_"));
    assert!(name
        .chars()
        .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_'));
}

#[test]
fn database_names_differ_between_ids() {
    assert_ne!(database_name(Uuid::new_v4()), database_name(Uuid::new_v4()));
}
