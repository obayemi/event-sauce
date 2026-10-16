//! [`Tracked`] — an aggregate checked out of a unit of work.

use std::ops::{Deref, DerefMut};

use super::identity_map::{Held, IdentityMap, SlotState};
use super::UnitOfWorkError;
use crate::{Aggregate, AggregateRoot, DeletedAggregateRoot};

/// An active aggregate checked out of a [`UnitOfWork`](super::UnitOfWork).
///
/// Dereferences to the [`AggregateRoot`], so events are applied to it as to
/// any root. Dropping the handle checks the root — and every event applied
/// to it — back into the unit of work, which commits it once the command
/// returns. While a handle is alive, loading the same aggregate again fails
/// with [`UnitOfWorkError::AlreadyCheckedOut`]; once it is dropped, loading
/// it again hands out the same, already modified root.
///
/// `Tracked` is [`Send`], so a handle can be held across an `.await`.
///
/// # Examples
///
/// See [`UnitOfWorkStore`](super::UnitOfWorkStore).
pub struct Tracked<'u, A: Aggregate + 'static> {
    root: Option<AggregateRoot<A>>,
    map: &'u IdentityMap,
}

impl<'u, A: Aggregate + 'static> Tracked<'u, A> {
    pub(super) fn new(root: AggregateRoot<A>, map: &'u IdentityMap) -> Self {
        Self {
            root: Some(root),
            map,
        }
    }

    /// Deletes the aggregate: `f` turns the root into its deleted state
    /// (typically with [`AggregateRoot::apply_delete`]), which the unit of
    /// work commits.
    ///
    /// # Errors
    ///
    /// Returns [`UnitOfWorkError::Domain`] with the aggregate's error if `f`
    /// fails. `f` took the root by value, so the unit of work no longer
    /// holds one for this aggregate: a command that swallows this error and
    /// returns `Ok` anyway fails to commit with
    /// [`UnitOfWorkError::DeleteFailed`], rather than silently committing
    /// nothing for the aggregate.
    pub fn delete_with<F>(mut self, f: F) -> Result<(), UnitOfWorkError>
    where
        F: FnOnce(AggregateRoot<A>) -> Result<DeletedAggregateRoot<A>, A::Error>,
    {
        let root = self.take();
        let id = root.entity_id();
        let (state, result) = match f(root) {
            Ok(deleted) => (SlotState::Held(Held::deleted(deleted)), Ok(())),
            Err(e) => (SlotState::Consumed(A::aggregate_type()), Err(e.into())),
        };
        self.map.check_in::<A>(id, state);
        result
    }

    fn take(&mut self) -> AggregateRoot<A> {
        self.root
            .take()
            .unwrap_or_else(|| unreachable!("only drop or delete_with take the root, once"))
    }
}

impl<A: Aggregate + 'static> Deref for Tracked<'_, A> {
    type Target = AggregateRoot<A>;

    fn deref(&self) -> &Self::Target {
        self.root
            .as_ref()
            .unwrap_or_else(|| unreachable!("the root is only taken when the handle is consumed"))
    }
}

impl<A: Aggregate + 'static> DerefMut for Tracked<'_, A> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.root
            .as_mut()
            .unwrap_or_else(|| unreachable!("the root is only taken when the handle is consumed"))
    }
}

impl<A: Aggregate + 'static> Drop for Tracked<'_, A> {
    fn drop(&mut self) {
        if let Some(root) = self.root.take() {
            let id = root.entity_id();
            self.map
                .check_in::<A>(id, SlotState::Held(Held::active(root)));
        }
    }
}

impl<A: Aggregate + std::fmt::Debug + 'static> std::fmt::Debug for Tracked<'_, A> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Tracked").field("root", &self.root).finish()
    }
}

#[cfg(test)]
mod tests;
