//! [`IdentityMap`] — the aggregates a unit of work has handed out, keyed by
//! type and id, and where each one currently is.

use std::any::{Any, TypeId};
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::sync::{Mutex, MutexGuard, PoisonError};

use super::UnitOfWorkError;
use crate::{
    Aggregate, AggregateRoot, AggregateType, AggregateVersion, DeletedAggregateRoot, EntityId,
    Lifecycle,
};

/// An aggregate's identity within a unit of work.
pub(super) type Key = (TypeId, EntityId);

/// A root the unit of work holds, type-erased so aggregates of several types
/// share one map. Always an `AggregateRoot<A>` or a
/// `DeletedAggregateRoot<A>` for the `A` whose `TypeId` keys it.
pub(super) enum Held {
    Active(Box<dyn Any + Send>),
    Deleted(Box<dyn Any + Send>),
}

impl Held {
    pub(super) fn active<A: Aggregate + 'static>(root: AggregateRoot<A>) -> Self {
        Self::Active(Box::new(root))
    }

    pub(super) fn deleted<A: Aggregate + 'static>(root: DeletedAggregateRoot<A>) -> Self {
        Self::Deleted(Box::new(root))
    }
}

/// Recovers the concrete root a [`Held`] boxed.
///
/// # Panics
///
/// Panics if `root` is not an `R`: every slot is keyed by its aggregate's
/// `TypeId`, so a mismatch is a bug in the unit of work, never a caller error.
pub(super) fn downcast<R: 'static>(root: Box<dyn Any + Send>) -> R {
    *root
        .downcast()
        .unwrap_or_else(|_| unreachable!("a slot always holds the root its TypeId keys"))
}

/// Where a handed-out aggregate currently is.
pub(super) enum SlotState {
    /// A live [`Tracked`](super::Tracked) handle holds it.
    CheckedOut,
    /// The unit of work holds it, ready to be checked out again or committed.
    Held(Held),
    /// A failed [`Tracked::delete_with`](super::Tracked::delete_with) consumed it.
    Consumed(AggregateType),
}

/// One aggregate the unit of work has handed out.
pub(super) struct Slot {
    /// The version it was loaded at, or `None` if this unit of work created it.
    pub(super) loaded: Option<AggregateVersion>,
    pub(super) state: SlotState,
}

impl Slot {
    /// Takes the active root out, leaving the slot checked out.
    fn check_out<A: Aggregate + 'static>(
        &mut self,
        id: EntityId,
    ) -> Result<AggregateRoot<A>, UnitOfWorkError> {
        match std::mem::replace(&mut self.state, SlotState::CheckedOut) {
            SlotState::Held(Held::Active(root)) => Ok(downcast(root)),
            SlotState::CheckedOut => Err(UnitOfWorkError::AlreadyCheckedOut {
                aggregate_type: A::aggregate_type(),
                id,
            }),
            deleted @ SlotState::Held(Held::Deleted(_)) => {
                self.state = deleted;
                Err(UnitOfWorkError::Lifecycle {
                    expected: Lifecycle::Active,
                    found: Lifecycle::Deleted,
                })
            }
            SlotState::Consumed(aggregate_type) => {
                self.state = SlotState::Consumed(aggregate_type.clone());
                Err(UnitOfWorkError::DeleteFailed { aggregate_type, id })
            }
        }
    }

    fn lifecycle(&self) -> Lifecycle {
        match self.state {
            SlotState::CheckedOut | SlotState::Held(Held::Active(_)) => Lifecycle::Active,
            SlotState::Held(Held::Deleted(_)) | SlotState::Consumed(_) => Lifecycle::Deleted,
        }
    }
}

/// A slot ready to be committed: the unit of work holds its root.
pub(super) struct Returned {
    pub(super) key: Key,
    pub(super) loaded: Option<AggregateVersion>,
    pub(super) root: Held,
}

/// Every aggregate a unit of work has handed out.
///
/// The lock is only ever held for a synchronous slot update, never across an
/// `.await`.
#[derive(Default)]
pub(super) struct IdentityMap {
    slots: Mutex<HashMap<Key, Slot>>,
}

impl IdentityMap {
    fn lock(&self) -> MutexGuard<'_, HashMap<Key, Slot>> {
        self.slots.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Checks out the root this unit of work already holds for `id`, or
    /// `None` if it has not handed `A`'s `id` out yet.
    pub(super) fn check_out_held<A: Aggregate + 'static>(
        &self,
        id: EntityId,
    ) -> Option<Result<AggregateRoot<A>, UnitOfWorkError>> {
        self.lock()
            .get_mut(&(TypeId::of::<A>(), id))
            .map(|slot| slot.check_out(id))
    }

    /// Records a freshly loaded root as checked out. Should another load of
    /// the same aggregate have won the race to record it, `root` is discarded
    /// and the recorded one is checked out instead.
    pub(super) fn check_out_loaded<A: Aggregate + 'static>(
        &self,
        root: AggregateRoot<A>,
    ) -> Result<AggregateRoot<A>, UnitOfWorkError> {
        let id = root.entity_id();
        let mut slots = self.lock();
        match slots.entry((TypeId::of::<A>(), id)) {
            Entry::Occupied(mut slot) => slot.get_mut().check_out(id),
            Entry::Vacant(slot) => {
                slot.insert(Slot {
                    loaded: Some(root.version()),
                    state: SlotState::CheckedOut,
                });
                Ok(root)
            }
        }
    }

    /// Records a newly created root as checked out, refusing an aggregate
    /// this unit of work already holds.
    pub(super) fn check_out_created<A: Aggregate + 'static>(
        &self,
        id: EntityId,
    ) -> Result<(), UnitOfWorkError> {
        let mut slots = self.lock();
        match slots.entry((TypeId::of::<A>(), id)) {
            Entry::Occupied(slot) => Err(UnitOfWorkError::Lifecycle {
                expected: Lifecycle::Missing,
                found: slot.get().lifecycle(),
            }),
            Entry::Vacant(slot) => {
                slot.insert(Slot {
                    loaded: None,
                    state: SlotState::CheckedOut,
                });
                Ok(())
            }
        }
    }

    /// Puts a checked-out aggregate's slot into `state`.
    pub(super) fn check_in<A: 'static>(&self, id: EntityId, state: SlotState) {
        if let Some(slot) = self.lock().get_mut(&(TypeId::of::<A>(), id)) {
            slot.state = state;
        }
    }

    /// Hands back every held root for committing.
    ///
    /// # Errors
    ///
    /// Returns [`UnitOfWorkError::LeakedHandle`] if any slot is still checked
    /// out, or [`UnitOfWorkError::DeleteFailed`] if a `delete_with` consumed a
    /// root.
    pub(super) fn into_returned(self) -> Result<Vec<Returned>, UnitOfWorkError> {
        let slots = self
            .slots
            .into_inner()
            .unwrap_or_else(PoisonError::into_inner);
        let mut returned = Vec::with_capacity(slots.len());
        let mut leaked = 0;
        let mut consumed = None;
        for (key, slot) in slots {
            match slot.state {
                SlotState::Held(root) => returned.push(Returned {
                    key,
                    loaded: slot.loaded,
                    root,
                }),
                SlotState::CheckedOut => leaked += 1,
                SlotState::Consumed(aggregate_type) => consumed = Some((aggregate_type, key.1)),
            }
        }
        if leaked > 0 {
            return Err(UnitOfWorkError::LeakedHandle(leaked));
        }
        match consumed {
            Some((aggregate_type, id)) => Err(UnitOfWorkError::DeleteFailed { aggregate_type, id }),
            None => Ok(returned),
        }
    }
}
