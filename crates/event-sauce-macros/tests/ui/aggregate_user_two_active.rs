#[derive(Debug, thiserror::Error)]
#[error("error")]
struct TestError;

impl event_sauce_core::AggregateError for TestError {}

#[event_sauce_macros::aggregate(event = "TestEvent", error = "TestError", init, user)]
struct TestAggregate {
    #[id]
    id: event_sauce_core::EntityId,
    #[user(identity)]
    identity: event_sauce_auth::Identity<event_sauce_auth::Password<event_sauce_auth::Email>>,
    #[user(active)]
    enabled: bool,
    #[user(active)]
    suspended: bool,
}

fn main() {}
