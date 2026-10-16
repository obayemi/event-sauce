//! [`UnitOfWorkStore`] — the registry of aggregate types a unit of work can
//! hold, and the retry loop that runs commands against it.

use std::any::TypeId;
use std::collections::HashMap;
use std::num::NonZeroU32;

use super::erased::{EventSourced, Persistence, Registered, Registration, StateStored};
use super::{TransactionalStore, UnitOfWork, UnitOfWorkCommand, UnitOfWorkError};
use crate::commands::run_with_retry;
use crate::event_context::{execution_context, WithEventContextExt};
use crate::{Aggregate, RetryPolicy};

/// The outcome of a successful [`UnitOfWorkStore::execute`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct UnitOfWorkExecuted {
    /// How many attempts the command took, including the first. Always `1`
    /// unless a commit was retried after a concurrency conflict.
    pub attempts: NonZeroU32,
}

/// A [`TransactionalStore`] together with the aggregate types a
/// [`UnitOfWork`] may hold, and how each one is persisted.
///
/// Built once at the composition root by registering every aggregate type
/// as [event-sourced](Self::event_sourced) or
/// [state-stored](Self::state_stored); commands then load and change any mix
/// of them through [`execute`](Self::execute) without ever naming a
/// persistence style.
///
/// # Examples
///
/// ```
/// # use async_trait::async_trait;
/// # use event_sauce_core::{
/// #     Aggregate, AggregateError, DefaultEntity, DomainEvent, Entity, EntityId, EventApplicator,
/// #     EventVersion, TransactionalStore,
/// # };
/// # use serde::{Deserialize, Serialize};
/// # use thiserror::Error;
/// # #[derive(Debug, Serialize, Deserialize)]
/// # struct Counter { id: EntityId, value: i32 }
/// # impl Entity for Counter {
/// #     fn new(id: EntityId) -> Self { Self { id, value: 0 } }
/// #     fn entity_id(&self) -> EntityId { self.id }
/// # }
/// # impl DefaultEntity for Counter {}
/// # #[derive(Debug, Clone, Serialize, Deserialize)]
/// # enum CounterEvent { Incremented { amount: i32 } }
/// # impl DomainEvent for CounterEvent {
/// #     type Aggregate = Counter;
/// #     fn event_type(&self) -> &'static str { "Counter" }
/// #     fn event_version(&self) -> EventVersion { EventVersion::new(1) }
/// #     fn occurred_at(&self) -> chrono::DateTime<chrono::Utc> { chrono::Utc::now() }
/// # }
/// # impl EventApplicator<Counter> for CounterEvent {
/// #     fn dispatch(&self, c: &mut Counter) -> Result<(), CounterError> {
/// #         self.dispatch_unchecked(c);
/// #         Ok(())
/// #     }
/// #     fn dispatch_unchecked(&self, c: &mut Counter) {
/// #         let CounterEvent::Incremented { amount } = self;
/// #         c.value += amount;
/// #     }
/// # }
/// # #[derive(Debug, Error)]
/// # #[error("counter error")]
/// # struct CounterError;
/// # impl AggregateError for CounterError {}
/// # impl Aggregate for Counter {
/// #     type Event = CounterEvent;
/// #     type Error = CounterError;
/// #     type DeletedState = Self;
/// # }
/// use event_sauce_core::{RetryPolicy, UnitOfWork, UnitOfWorkCommand, UnitOfWorkError, UnitOfWorkStore};
///
/// /// Moves `amount` from one counter to another, atomically.
/// struct Transfer { from: EntityId, to: EntityId, amount: i32 }
///
/// #[async_trait]
/// impl<T: TransactionalStore + 'static> UnitOfWorkCommand<T> for Transfer {
///     async fn handle(&self, uow: &UnitOfWork<'_, T>, _ctx: &()) -> Result<(), UnitOfWorkError> {
///         let mut from = uow.load::<Counter>(self.from).await?;
///         let mut to = uow.load::<Counter>(self.to).await?;
///         from.apply(CounterEvent::Incremented { amount: -self.amount })?;
///         to.apply(CounterEvent::Incremented { amount: self.amount })?;
///         Ok(())
///     }
/// }
///
/// # async fn run<T: TransactionalStore + 'static>(stores: T) -> Result<(), UnitOfWorkError> {
/// let store = UnitOfWorkStore::new(stores).event_sourced::<Counter>();
/// let (from, to) = (EntityId::new(), EntityId::new());
/// let transfer = Transfer { from, to, amount: 5 };
/// let executed = store.execute(&transfer, &(), RetryPolicy::default()).await?;
/// assert_eq!(executed.attempts.get(), 1);
/// # Ok(())
/// # }
/// ```
pub struct UnitOfWorkStore<T: TransactionalStore> {
    store: T,
    registry: HashMap<TypeId, Box<dyn Registered<T>>>,
}

impl<T: TransactionalStore + 'static> UnitOfWorkStore<T> {
    /// Wraps `store` with no aggregate type registered yet.
    #[must_use]
    pub fn new(store: T) -> Self {
        Self {
            store,
            registry: HashMap::new(),
        }
    }

    /// Registers `A` as event-sourced: persisted as an event stream in the
    /// store's [`event_store`](TransactionalStore::event_store).
    ///
    /// Registering a type that is already registered replaces its
    /// registration: the last call wins.
    #[must_use]
    pub fn event_sourced<A>(self) -> Self
    where
        A: Aggregate + serde::Serialize + serde::de::DeserializeOwned + 'static,
        A::DeletedState: serde::Serialize + serde::de::DeserializeOwned,
        A::Event: serde::Serialize + serde::de::DeserializeOwned,
    {
        self.register::<A>(Box::new(EventSourced))
    }

    /// Registers `A` as state-stored: persisted as a single state row in the
    /// store's [`state_store`](TransactionalStore::state_store).
    ///
    /// Registering a type that is already registered replaces its
    /// registration: the last call wins.
    #[must_use]
    pub fn state_stored<A>(self) -> Self
    where
        A: Aggregate + serde::Serialize + serde::de::DeserializeOwned + 'static,
        A::DeletedState: serde::Serialize + serde::de::DeserializeOwned,
        A::Event: serde::Serialize + serde::de::DeserializeOwned,
    {
        self.register::<A>(Box::new(StateStored))
    }

    fn register<A: Aggregate + 'static>(mut self, persistence: Box<dyn Persistence<T, A>>) -> Self {
        self.registry
            .insert(TypeId::of::<A>(), Box::new(Registration { persistence }));
        self
    }

    /// The wrapped transactional store.
    #[must_use]
    pub fn store(&self) -> &T {
        &self.store
    }

    /// Runs `command` against a fresh [`UnitOfWork`] and commits what it
    /// changed, retrying the whole command on a concurrency conflict as
    /// `retry` allows.
    ///
    /// # Errors
    ///
    /// Returns the command's own error without committing anything, or the
    /// commit's error — a concurrency conflict only once `retry` gave up.
    pub async fn execute<C, Ctx>(
        &self,
        command: &C,
        ctx: &Ctx,
        retry: RetryPolicy,
    ) -> Result<UnitOfWorkExecuted, UnitOfWorkError>
    where
        C: UnitOfWorkCommand<T, Ctx> + ?Sized,
        Ctx: Sync,
    {
        let (handled, attempts) = run_with_retry(
            &retry,
            || async {
                let uow = UnitOfWork::new(self);
                match command.handle(&uow, ctx).await {
                    Ok(()) => uow.commit().await.map(Ok),
                    Err(e) => Ok(Err(e)),
                }
            },
            UnitOfWorkError::is_concurrency_conflict,
        )
        .with_event_context(execution_context(command.name()))
        .await?;
        handled?;
        Ok(UnitOfWorkExecuted { attempts })
    }

    /// `A`'s registered persistence.
    pub(super) fn persistence<A: Aggregate + 'static>(
        &self,
    ) -> Result<&dyn Persistence<T, A>, UnitOfWorkError> {
        let registered =
            self.registry
                .get(&TypeId::of::<A>())
                .ok_or_else(|| UnitOfWorkError::Unregistered {
                    aggregate_type: A::aggregate_type(),
                })?;
        let registration = registered
            .as_any()
            .downcast_ref::<Registration<T, A>>()
            .unwrap_or_else(|| unreachable!("a registration is keyed by its aggregate's TypeId"));
        Ok(registration.persistence.as_ref())
    }

    /// The registration of the aggregate type `type_id` names.
    pub(super) fn registered(&self, type_id: TypeId) -> &dyn Registered<T> {
        self.registry
            .get(&type_id)
            .unwrap_or_else(|| unreachable!("only registered types are ever handed out"))
            .as_ref()
    }
}

impl<T: TransactionalStore> std::fmt::Debug for UnitOfWorkStore<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("UnitOfWorkStore")
            .field("registered", &self.registry.len())
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests;
