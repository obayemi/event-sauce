use std::sync::Arc;

use event_sauce_auth::{CacheError, UserAggregate, UserCache};
use event_sauce_core::{AggregateRoot, AggregateVersion, EntityId};

async fn ask<U: UserAggregate>(
    cache: &dyn UserCache<U>,
    id: EntityId,
    held: &AggregateRoot<U>,
) -> Result<Option<Arc<AggregateRoot<U>>>, CacheError> {
    let _: AggregateVersion = held.version();
    cache.get(id, held.version()).await
}

fn main() {}
