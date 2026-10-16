//! [`UnitOfWork`] — one attempt at a command spanning several aggregates.

use super::identity_map::IdentityMap;
use super::{AtomicCommit, Tracked, TransactionalStore, UnitOfWorkError, UnitOfWorkStore};
use crate::{
    Aggregate, AggregateRoot, EntityId, EntityIdFor, Lifecycle, Loaded, UninitAggregateRoot,
};

/// The aggregates one attempt of a [`UnitOfWorkCommand`](super::UnitOfWorkCommand)
/// loads, creates and changes, committed together once the command returns.
///
/// Created by [`UnitOfWorkStore::execute`] for each attempt and handed to the
/// command by reference. Every aggregate it hands out is a [`Tracked`]
/// handle; once the command returns `Ok`, each one is committed in a single
/// atomic commit:
///
/// - an aggregate with pending events is written, under optimistic
///   concurrency against the version it was loaded at;
/// - an aggregate loaded but left unchanged is only asserted to still be at
///   that version, so a command whose decision rests on it conflicts — and
///   retries — if it changed concurrently;
/// - an aggregate [created](Self::create) here is written as new, and
///   conflicts if it already exists.
///
/// A [peeked](Self::peek) aggregate is read-only and not tracked at all.
///
/// # Examples
///
/// See [`UnitOfWorkStore`].
pub struct UnitOfWork<'s, T: TransactionalStore> {
    store: &'s UnitOfWorkStore<T>,
    map: IdentityMap,
}

impl<'s, T: TransactionalStore + 'static> UnitOfWork<'s, T> {
    pub(super) fn new(store: &'s UnitOfWorkStore<T>) -> Self {
        Self {
            store,
            map: IdentityMap::default(),
        }
    }

    /// Checks out active aggregate `id`, loading it from the store unless
    /// this unit of work already holds it.
    ///
    /// The first load records the version the aggregate was read at; the
    /// commit asserts it is unchanged, or writes the aggregate's pending
    /// events against it. A later load of an aggregate whose handle was
    /// dropped hands out the same, already modified root rather than a fresh
    /// copy.
    ///
    /// # Errors
    ///
    /// - [`UnitOfWorkError::Unregistered`] if `A` was not registered.
    /// - [`UnitOfWorkError::Lifecycle`] if the aggregate is missing or
    ///   deleted (`expected` is [`Lifecycle::Active`]).
    /// - [`UnitOfWorkError::AlreadyCheckedOut`] if a live [`Tracked`] handle
    ///   already holds it.
    /// - [`UnitOfWorkError::DeleteFailed`] if a failed
    ///   [`Tracked::delete_with`] consumed it.
    /// - [`UnitOfWorkError::Store`] if loading fails.
    pub async fn load<A: Aggregate + 'static>(
        &self,
        id: impl EntityIdFor<A> + Send,
    ) -> Result<Tracked<'_, A>, UnitOfWorkError> {
        let id = id.entity_id();
        let persistence = self.store.persistence::<A>()?;
        if let Some(held) = self.map.check_out_held::<A>(id) {
            return Ok(Tracked::new(held?, &self.map));
        }
        let root = match persistence.load_any(self.store.store(), id).await {
            Ok(Loaded::Active(root)) => root,
            Ok(Loaded::Deleted(_)) => return Err(not_active(Lifecycle::Deleted)),
            Err(e) if e.is_not_found() => return Err(not_active(Lifecycle::Missing)),
            Err(e) => return Err(e.into()),
        };
        Ok(Tracked::new(self.map.check_out_loaded(root)?, &self.map))
    }

    /// Reads aggregate `id`'s committed state, active or deleted, without
    /// tracking it: the commit neither writes it nor asserts its version.
    ///
    /// Returns `None` if the aggregate does not exist. Ignores any change
    /// this unit of work has made to it.
    ///
    /// # Errors
    ///
    /// Returns [`UnitOfWorkError::Unregistered`] if `A` was not registered,
    /// or [`UnitOfWorkError::Store`] if loading fails.
    pub async fn peek<A: Aggregate + 'static>(
        &self,
        id: impl EntityIdFor<A> + Send,
    ) -> Result<Option<Loaded<A>>, UnitOfWorkError> {
        let persistence = self.store.persistence::<A>()?;
        match persistence
            .load_any(self.store.store(), id.entity_id())
            .await
        {
            Ok(loaded) => Ok(Some(loaded)),
            Err(e) if e.is_not_found() => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    /// Creates aggregate `id`: `f` initializes it (typically with
    /// [`UninitAggregateRoot::apply_init`]) and the commit writes it as new.
    ///
    /// Nothing is read from the store: if the aggregate already exists there,
    /// the commit fails with a concurrency conflict.
    ///
    /// # Errors
    ///
    /// - [`UnitOfWorkError::Unregistered`] if `A` was not registered.
    /// - [`UnitOfWorkError::Domain`] if `f` fails.
    /// - [`UnitOfWorkError::Lifecycle`] if this unit of work already holds
    ///   the aggregate (`expected` is [`Lifecycle::Missing`]).
    /// - [`UnitOfWorkError::Store`] if `f` returns a root for another id.
    pub fn create<A: Aggregate + 'static>(
        &self,
        id: EntityId,
        f: impl FnOnce(UninitAggregateRoot<A>) -> Result<AggregateRoot<A>, A::Error>,
    ) -> Result<Tracked<'_, A>, UnitOfWorkError> {
        self.store.persistence::<A>()?;
        let root = f(UninitAggregateRoot::new(id))?;
        if root.entity_id() != id {
            return Err(crate::Error::invalid_state(format!(
                "create returned aggregate {} for requested id {id}",
                root.entity_id()
            ))
            .into());
        }
        self.map.check_out_created::<A>(id)?;
        Ok(Tracked::new(root, &self.map))
    }

    /// Commits every change this unit of work holds, atomically.
    ///
    /// Takes `self` by value, so every [`Tracked`] handle borrowing it has
    /// been dropped (or leaked). Commits nothing when no aggregate has
    /// pending events: an assertion that unchanged aggregates are unchanged
    /// guards no write. Event-sourced snapshots are saved best-effort once
    /// the commit is durable.
    pub(super) async fn commit(self) -> Result<(), UnitOfWorkError> {
        let store = self.store.store();
        let mut commit = AtomicCommit::new();
        for returned in self.map.into_returned()? {
            self.store
                .registered(returned.key.0)
                .stage(store, returned, &mut commit)
                .await?;
        }
        if commit.stream_writes.is_empty() && commit.state_writes.is_empty() {
            return Ok(());
        }
        let snapshots: Vec<_> = commit
            .stream_writes
            .iter_mut()
            .filter_map(|write| write.snapshot.take())
            .collect();
        store.commit_atomic(commit).await?;
        for snapshot in snapshots {
            crate::event_store::save_snapshot_best_effort(store.event_store(), snapshot).await;
        }
        Ok(())
    }
}

fn not_active(found: Lifecycle) -> UnitOfWorkError {
    UnitOfWorkError::Lifecycle {
        expected: Lifecycle::Active,
        found,
    }
}

impl<T: TransactionalStore> std::fmt::Debug for UnitOfWork<'_, T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("UnitOfWork").finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests;
