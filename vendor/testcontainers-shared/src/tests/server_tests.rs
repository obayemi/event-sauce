use super::{container_name, Address};

#[test]
fn container_name_carries_the_kind_the_process_and_the_attempt() {
    assert_eq!(
        container_name("postgres", 4242, 3),
        "testcontainers_shared_postgres_4242_3"
    );
}

#[test]
fn container_names_differ_between_attempts() {
    assert_ne!(container_name("redis", 1, 1), container_name("redis", 1, 2));
}

#[test]
fn an_address_renders_as_host_and_port() {
    let address = Address::new("localhost", 5432);

    assert_eq!(address.to_string(), "localhost:5432");
}
