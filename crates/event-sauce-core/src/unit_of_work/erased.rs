//! The registry's type-erased view of a repository: what a unit of work
//! calls to load, prepare and expect an aggregate without knowing its
//! persistence style.

use std::any::Any;

use async_trait::async_trait;

use super::identity_map::{downcast, Held, Returned};
use super::{AtomicCommit, Expectation, PreparedWrite, StreamWrite, TransactionalStore};
use crate::commit_source::CommitSource;
use crate::{
    Aggregate, AggregateRoot, AggregateVersion, DeletedAggregateRoot, EntityId, EventStore, Loaded,
    Result, Snapshot, StateStore, StateStoredRepository, StreamId,
};

/// Aggregate `A`'s persistence against a borrowed [`TransactionalStore`].
///
/// Loads an aggregate, prepares its write without persisting it, and builds
/// the version assertion for one the unit of work only read. Borrows the
/// store per call: a [`TransactionalStore`] hands out `&Events` and
/// `&States`, never the `Arc` a repository owns.
#[async_trait]
pub(super) trait Persistence<T, A: Aggregate>: Send + Sync {
    async fn load_any(&self, store: &T, id: EntityId) -> Result<Loaded<A>>;

    async fn prepare(&self, store: &T, root: &AggregateRoot<A>) -> Result<Option<PreparedWrite>>;

    async fn prepare_deleted(
        &self,
        store: &T,
        root: &DeletedAggregateRoot<A>,
    ) -> Result<Option<PreparedWrite>>;

    fn expect(&self, id: EntityId, version: AggregateVersion) -> Expectation;
}

/// Persists an aggregate as an event stream in the pair's event store.
pub(super) struct EventSourced;

/// Persists an aggregate as a state row in the pair's state store.
pub(super) struct StateStored;

#[async_trait]
impl<T, A> Persistence<T, A> for EventSourced
where
    T: TransactionalStore,
    A: Aggregate + serde::Serialize + serde::de::DeserializeOwned,
    A::DeletedState: serde::Serialize + serde::de::DeserializeOwned,
    A::Event: serde::Serialize + serde::de::DeserializeOwned,
{
    async fn load_any(&self, store: &T, id: EntityId) -> Result<Loaded<A>> {
        crate::event_store::load_any(store.event_store(), id).await
    }

    async fn prepare(&self, store: &T, root: &AggregateRoot<A>) -> Result<Option<PreparedWrite>> {
        prepare_stream_write(store.event_store(), root).await
    }

    async fn prepare_deleted(
        &self,
        store: &T,
        root: &DeletedAggregateRoot<A>,
    ) -> Result<Option<PreparedWrite>> {
        prepare_stream_write(store.event_store(), root).await
    }

    fn expect(&self, id: EntityId, version: AggregateVersion) -> Expectation {
        stream_expectation::<A>(id, version)
    }
}

#[async_trait]
impl<T, A> Persistence<T, A> for StateStored
where
    T: TransactionalStore,
    T::States: 'static,
    A: Aggregate + serde::Serialize + serde::de::DeserializeOwned,
    A::DeletedState: serde::Serialize + serde::de::DeserializeOwned,
    A::Event: serde::Serialize + serde::de::DeserializeOwned,
{
    async fn load_any(&self, store: &T, id: EntityId) -> Result<Loaded<A>> {
        StateStoredRepository::<T::States, A>::load_any_from(store.state_store(), id).await
    }

    async fn prepare(&self, _store: &T, root: &AggregateRoot<A>) -> Result<Option<PreparedWrite>> {
        prepare_state_write::<T::States, A, _>(root)
    }

    async fn prepare_deleted(
        &self,
        _store: &T,
        root: &DeletedAggregateRoot<A>,
    ) -> Result<Option<PreparedWrite>> {
        prepare_state_write::<T::States, A, _>(root)
    }

    fn expect(&self, id: EntityId, version: AggregateVersion) -> Expectation {
        state_expectation::<A>(id, version)
    }
}

async fn prepare_stream_write<S, A, R>(store: &S, root: &R) -> Result<Option<PreparedWrite>>
where
    S: EventStore + ?Sized,
    A: Aggregate,
    A::Event: serde::Serialize,
    R: CommitSource<A>,
{
    Ok(crate::event_store::prepare_commit(store, root)
        .await?
        .map(|prepared| {
            PreparedWrite::Stream(StreamWrite {
                commit: prepared.commit,
                snapshot: prepared.snapshot,
            })
        }))
}

fn prepare_state_write<S, A, R>(root: &R) -> Result<Option<PreparedWrite>>
where
    S: StateStore + 'static,
    A: Aggregate + serde::Serialize + serde::de::DeserializeOwned,
    A::DeletedState: serde::Serialize + serde::de::DeserializeOwned,
    A::Event: serde::Serialize + serde::de::DeserializeOwned,
    R: CommitSource<A>,
{
    Ok(StateStoredRepository::<S, A>::prepare(root)?.map(PreparedWrite::State))
}

fn stream_expectation<A: Aggregate>(id: EntityId, version: AggregateVersion) -> Expectation {
    Expectation::Stream {
        stream_id: StreamId::new(A::aggregate_type(), id.as_uuid()),
        version,
    }
}

fn state_expectation<A: Aggregate>(id: EntityId, version: AggregateVersion) -> Expectation {
    Expectation::State {
        stream_id: StreamId::new(A::aggregate_type(), id.as_uuid()),
        version,
    }
}

/// One registered aggregate type: its [`Persistence`], whichever style.
pub(super) struct Registration<T, A> {
    pub(super) persistence: Box<dyn Persistence<T, A>>,
}

/// A [`Registration`] with its aggregate type erased, so the registry can
/// hold every registered type in one map.
#[async_trait]
pub(super) trait Registered<T>: Send + Sync {
    /// Lets [`UnitOfWorkStore`](super::UnitOfWorkStore) recover the typed
    /// [`Registration`] once the caller names the aggregate type.
    fn as_any(&self) -> &dyn Any;

    /// Adds what committing `returned` takes to `commit`: its write if it
    /// has pending events, otherwise an expectation at its loaded version
    /// (nothing for a root this unit of work created and left untouched).
    async fn stage(
        &self,
        store: &T,
        returned: Returned,
        commit: &mut AtomicCommit,
    ) -> Result<Option<Snapshot>>;
}

#[async_trait]
impl<T, A> Registered<T> for Registration<T, A>
where
    T: TransactionalStore + 'static,
    A: Aggregate + 'static,
{
    fn as_any(&self) -> &dyn Any {
        self
    }

    async fn stage(
        &self,
        store: &T,
        returned: Returned,
        commit: &mut AtomicCommit,
    ) -> Result<Option<Snapshot>> {
        let write = match returned.root {
            Held::Active(root) => {
                let root: AggregateRoot<A> = downcast(root);
                self.persistence.prepare(store, &root).await?
            }
            Held::Deleted(root) => {
                let root: DeletedAggregateRoot<A> = downcast(root);
                self.persistence.prepare_deleted(store, &root).await?
            }
        };
        Ok(match (write, returned.loaded) {
            (Some(write), _) => commit.push_write(write),
            (None, Some(version)) => {
                commit.push_expectation(self.persistence.expect(returned.key.1, version));
                None
            }
            (None, None) => None,
        })
    }
}

#[cfg(test)]
mod tests;
