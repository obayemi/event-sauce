#[derive(Debug, thiserror::Error)]
#[error("error")]
struct TestError;

impl event_sauce_core::AggregateError for TestError {}

#[event_sauce_macros::aggregate(event = "TestEvent", error = "TestError", snapshots(every = 0))]
struct TestAggregate {
    #[id]
    id: event_sauce_core::EntityId,
}

fn main() {}
