use std::time::Duration;

use super::*;

#[derive(Serialize, Deserialize)]
struct Slow;

impl LoginState for Slow {
    const TAG: &'static str = "test.slow";
    const TTL: LoginTtl = LoginTtl::ONE_HOUR;
}

#[test]
fn done_is_tagged_with_the_library_namespace() {
    assert_eq!(Done::TAG, "event_sauce.done");
}

#[test]
fn a_state_waits_five_minutes_unless_it_says_otherwise() {
    assert_eq!(Done::TTL.as_duration(), Duration::from_secs(300));
    assert_eq!(Slow::TTL.as_duration(), Duration::from_secs(3_600));
}

#[test]
fn done_round_trips_through_json() {
    let json = serde_json::to_string(&Done).unwrap();

    assert_eq!(serde_json::from_str::<Done>(&json).unwrap(), Done);
}

fn assert_complete<S: Complete>() {}

#[test]
fn done_ends_a_login() {
    assert_complete::<Done>();
}
