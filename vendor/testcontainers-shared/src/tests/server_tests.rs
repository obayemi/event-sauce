use bollard::errors::Error as DockerError;
use testcontainers::{core::error::ClientError, TestcontainersError};
use uuid::Uuid;

use super::{container_name, may_have_left_a_container, request_name, Address, Process};

fn process(id: u32, token: &str) -> Process {
    Process {
        id,
        token: token.to_owned(),
    }
}

fn create_failure(status_code: u16) -> TestcontainersError {
    TestcontainersError::Client(ClientError::CreateContainer(
        DockerError::DockerResponseServerError {
            status_code,
            message: String::new(),
        },
    ))
}

#[test]
fn container_name_carries_the_kind_the_process_and_the_attempt() {
    assert_eq!(
        container_name("postgres", &process(4242, "c0ffee00"), 3),
        "testcontainers_shared_postgres_4242_c0ffee00_3"
    );
}

#[test]
fn container_names_differ_between_attempts() {
    let process = process(1, "token");

    assert_ne!(
        container_name("redis", &process, 1),
        container_name("redis", &process, 2)
    );
}

#[test]
fn container_names_differ_between_processes_sharing_an_id() {
    assert_ne!(
        container_name("redis", &process(1, "first"), 1),
        container_name("redis", &process(1, "second"), 1)
    );
}

#[test]
fn the_current_process_keeps_its_token() {
    assert_eq!(
        Process::current().to_string(),
        Process::current().to_string()
    );
    assert!(Process::current()
        .to_string()
        .starts_with(&format!("{}_", std::process::id())));
}

#[test]
fn a_name_conflict_leaves_no_container_of_ours() {
    assert!(!may_have_left_a_container(&create_failure(409)));
}

#[test]
fn any_other_failure_may_leave_a_container_behind() {
    assert!(may_have_left_a_container(&create_failure(500)));
    assert!(may_have_left_a_container(&TestcontainersError::other(
        "port already allocated"
    )));
}

#[test]
fn an_address_renders_as_host_and_port() {
    let address = Address::new("localhost", 5432);

    assert_eq!(address.to_string(), "localhost:5432");
}

#[test]
fn request_name_keeps_the_name_a_caller_chose() {
    assert_eq!(request_name(Some("mine"), Uuid::new_v4(), 2), "mine");
}

#[test]
fn request_name_names_an_unnamed_request_after_the_process() {
    let name = request_name(None, Uuid::new_v4(), 2);

    assert!(name.starts_with("testcontainers_shared_dedicated_"));
    assert!(name.ends_with(&format!("_{}_2", Process::current())));
}

#[test]
fn request_names_of_unnamed_requests_differ_between_ids() {
    assert_ne!(
        request_name(None, Uuid::new_v4(), 1),
        request_name(None, Uuid::new_v4(), 1)
    );
}
