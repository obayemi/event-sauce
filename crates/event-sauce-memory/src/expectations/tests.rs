use super::check_expectations;
use event_sauce_core::{AggregateVersion, StreamId};
use uuid::Uuid;

fn stream() -> StreamId {
    StreamId::new("Acct", Uuid::new_v4())
}

#[test]
fn accepts_expectations_matching_the_load_time_version() {
    let (a, b) = (stream(), stream());
    let expectations = [
        (a.clone(), AggregateVersion::new(2)),
        (b, AggregateVersion::initial()),
    ];

    let result = check_expectations(&expectations, |id| {
        if *id == a {
            AggregateVersion::new(2)
        } else {
            AggregateVersion::initial()
        }
    });

    assert!(result.is_ok());
}

#[test]
fn accepts_an_empty_expectation_list() {
    assert!(check_expectations(&[], |_| AggregateVersion::new(9)).is_ok());
}

#[test]
fn rejects_the_first_mismatching_expectation() {
    let expectations = [(stream(), AggregateVersion::new(1))];

    let error = check_expectations(&expectations, |_| AggregateVersion::new(3))
        .expect_err("a stale expectation must conflict");

    assert!(error.is_concurrency_conflict(), "got {error:?}");
}
