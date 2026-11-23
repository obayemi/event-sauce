use std::marker::PhantomData;

use event_sauce_auth::UserAggregate;
use event_sauce_axum::{Access, Authorized};
use event_sauce_core::{Aggregate, AggregateRoot};

struct Owner<B, U>(PhantomData<fn() -> (B, U)>);

impl<B: Aggregate, U: UserAggregate> Access<B> for Owner<B, U> {
    type User = U;

    fn allows(_aggregate: &AggregateRoot<B>, _user: &AggregateRoot<U>) -> bool {
        true
    }
}

fn authorize<A: Aggregate, B: Aggregate, U: UserAggregate, S>(
    _authorized: Option<Authorized<A, Owner<B, U>, S>>,
) {
}

fn main() {}
