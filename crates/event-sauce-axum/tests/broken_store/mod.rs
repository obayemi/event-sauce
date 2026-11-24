//! An event store whose every operation fails, to answer what a handler
//! does when the store is down.

use async_trait::async_trait;
use event_sauce_core::{
    AggregateClaim, AggregateVersion, EntityId, Error, EventEnvelope, EventLogEntry, EventStore,
    Position, Result, StreamId,
};
use futures::stream::Empty;

pub struct BrokenStore;

fn down() -> Error {
    Error::custom("the store is down")
}

#[async_trait]
impl EventStore for BrokenStore {
    async fn append(
        &self,
        _stream_id: StreamId,
        _events: Vec<EventEnvelope>,
        _expected_version: AggregateVersion,
        _claims: Vec<AggregateClaim>,
        _clear_claims: bool,
    ) -> Result<()> {
        Err(down())
    }

    async fn load_stream(
        &self,
        _stream_id: StreamId,
        _from_version: AggregateVersion,
    ) -> Result<impl futures::Stream<Item = Result<EventEnvelope>> + Send> {
        Err::<Empty<Result<EventEnvelope>>, _>(down())
    }

    async fn stream_all(
        &self,
        _from_position: Position,
    ) -> Result<impl futures::Stream<Item = Result<EventLogEntry>> + Send> {
        Err::<Empty<Result<EventLogEntry>>, _>(down())
    }

    async fn resolve_claim(&self, _claim: &AggregateClaim) -> Result<Option<EntityId>> {
        Err(down())
    }

    async fn get_version(&self, _stream_id: StreamId) -> Result<AggregateVersion> {
        Err(down())
    }
}
