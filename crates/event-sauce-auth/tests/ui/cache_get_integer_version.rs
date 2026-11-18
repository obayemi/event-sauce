use std::sync::Arc;

use event_sauce_auth::{CacheError, UserAggregate, UserCache};
use event_sauce_core::{AggregateRoot, EntityId};

async fn ask<U: UserAggregate>(
    cache: &dyn UserCache<U>,
    id: EntityId,
) -> Result<Option<Arc<AggregateRoot<U>>>, CacheError> {
    cache.get(id, 3).await
}

fn main() {}
