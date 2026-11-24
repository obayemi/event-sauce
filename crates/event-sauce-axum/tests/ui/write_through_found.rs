use event_sauce_axum::{Found, PathAggregate};
use event_sauce_core::AggregateRoot;

fn write<A: PathAggregate, S>(mut found: Found<A, S>) {
    let _: &mut AggregateRoot<A> = &mut found;
}

fn main() {}
