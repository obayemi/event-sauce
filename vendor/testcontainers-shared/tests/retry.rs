//! `start_with_retry` against a real Docker daemon.

use std::net::TcpListener;

use bollard::{query_parameters::InspectContainerOptions, Docker};
use testcontainers::{core::IntoContainerPort, ImageExt};
use testcontainers_modules::postgres::Postgres;
use testcontainers_shared::start_with_retry;
use uuid::Uuid;

#[tokio::test]
async fn a_start_that_keeps_failing_leaves_no_container_behind() {
    let taken = TcpListener::bind("0.0.0.0:0").unwrap();
    let host_port = taken.local_addr().unwrap().port();
    let prefix = format!("testcontainers_shared_retry_{}", Uuid::new_v4().simple());
    let mut names = Vec::new();

    let started = start_with_retry(|attempt| {
        let name = format!("{prefix}_{attempt}");
        names.push(name.clone());
        Postgres::default()
            .with_tag("16-alpine")
            .with_mapped_port(host_port, 5432.tcp())
            .with_container_name(name)
    })
    .await;

    assert!(started.is_err());
    let docker = Docker::connect_with_defaults().unwrap();
    for name in names {
        let found = docker
            .inspect_container(&name, None::<InspectContainerOptions>)
            .await;
        assert!(found.is_err(), "{name} was left behind");
    }
}
