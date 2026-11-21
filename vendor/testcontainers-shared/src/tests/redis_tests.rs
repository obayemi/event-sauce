use super::{database_url, server_args, DATABASES};
use crate::server::Address;

#[test]
fn database_url_selects_the_logical_database() {
    let address = Address::new("localhost", 6379);

    assert_eq!(database_url(&address, 7), "redis://localhost:6379/7");
}

#[test]
fn server_args_raise_the_database_count() {
    let args = server_args();

    let position = args
        .iter()
        .position(|arg| *arg == "--databases")
        .expect("--databases is set");
    assert_eq!(args[position + 1], DATABASES.to_string());
}
