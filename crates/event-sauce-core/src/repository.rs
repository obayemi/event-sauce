//! Repository pattern for high-level aggregate persistence.
//!
//! [`Repository`] is the persistence-style-agnostic trait that application
//! code should depend on: it exposes aggregate lifecycle operations
//! (`load`, `save`, `modify`, `create_*`, …) without committing to *how*
//! aggregates are persisted.
//!
//! [`EventSourcedRepository`] is its event-store-backed implementation:
//! `load` replays the aggregate's event stream (optionally from a snapshot)
//! and `save` appends the pending events with optimistic concurrency control.

#[cfg(feature = "event-sourcing")]
use std::marker::PhantomData;
#[cfg(feature = "event-sourcing")]
use std::sync::Arc;

use async_trait::async_trait;

#[cfg(feature = "event-sourcing")]
use crate::{
    event_store::{count_events, load, load_any, load_deleted},
    EventStore, StreamId,
};
use crate::{
    Aggregate, AggregateRoot, AggregateVersion, DefaultEntity, DeletedAggregateRoot, EntityId,
    EntityIdFor, InitEvent, Loaded, Result, UninitAggregateRoot,
};
#[cfg(feature = "commands")]
use crate::{Command, Error, ExecuteError, Executed, Lifecycle, RetryPolicy, Root};

/// Adapts a one-shot [`Repository::modify`] closure into a [`Command`], so
/// `modify` can be driven by [`Repository::execute`] with
/// [`RetryPolicy::none()`] instead of duplicating the load/apply/save cycle.
///
/// `execute` never retries under [`RetryPolicy::none()`], so
/// [`handle`](Command::handle) runs at most once; the `Mutex`es exist only to
/// let a `&self` call move the `FnOnce` out and hand its result back, not for
/// real contention.
#[cfg(feature = "commands")]
struct ModifyCommand<F, R> {
    f: std::sync::Mutex<Option<F>>,
    result: std::sync::Mutex<Option<R>>,
}

#[cfg(feature = "commands")]
impl<F, R> ModifyCommand<F, R> {
    fn new(f: F) -> Self {
        Self {
            f: std::sync::Mutex::new(Some(f)),
            result: std::sync::Mutex::new(None),
        }
    }

    fn take_result(&self) -> R {
        self.result
            .lock()
            .unwrap()
            .take()
            .expect("execute() calls handle() exactly once before returning Ok")
    }
}

#[cfg(feature = "commands")]
impl<A, F, R> Command<A> for ModifyCommand<F, R>
where
    A: Aggregate,
    F: FnOnce(&mut AggregateRoot<A>) -> std::result::Result<R, A::Error> + Send,
    R: Send,
{
    fn handle(
        &self,
        root: Root<A>,
        _ctx: &(),
    ) -> std::result::Result<Root<A>, crate::CommandError<A::Error>> {
        let mut active = root.active()?;
        let f = self
            .f
            .lock()
            .unwrap()
            .take()
            .expect("ModifyCommand::handle is only ever called once");
        let result = f(&mut active)?;
        *self.result.lock().unwrap() = Some(result);
        Ok(active.into())
    }
}

/// The [`modify_deleted`](Repository::modify_deleted) counterpart of
/// [`ModifyCommand`], narrowing to [`Root::Deleted`] instead of
/// [`Root::Active`].
#[cfg(feature = "commands")]
struct ModifyDeletedCommand<F, R> {
    f: std::sync::Mutex<Option<F>>,
    result: std::sync::Mutex<Option<R>>,
}

#[cfg(feature = "commands")]
impl<F, R> ModifyDeletedCommand<F, R> {
    fn new(f: F) -> Self {
        Self {
            f: std::sync::Mutex::new(Some(f)),
            result: std::sync::Mutex::new(None),
        }
    }

    fn take_result(&self) -> R {
        self.result
            .lock()
            .unwrap()
            .take()
            .expect("execute() calls handle() exactly once before returning Ok")
    }
}

#[cfg(feature = "commands")]
impl<A, F, R> Command<A> for ModifyDeletedCommand<F, R>
where
    A: Aggregate,
    F: FnOnce(&mut DeletedAggregateRoot<A>) -> std::result::Result<R, A::Error> + Send,
    R: Send,
{
    fn handle(
        &self,
        root: Root<A>,
        _ctx: &(),
    ) -> std::result::Result<Root<A>, crate::CommandError<A::Error>> {
        let mut deleted = root.deleted()?;
        let f = self
            .f
            .lock()
            .unwrap()
            .take()
            .expect("ModifyDeletedCommand::handle is only ever called once");
        let result = f(&mut deleted)?;
        *self.result.lock().unwrap() = Some(result);
        Ok(deleted.into())
    }
}

/// Reconstructs the `Error` [`Repository::modify`] and
/// [`Repository::modify_deleted`] have always returned for a lifecycle
/// mismatch — `execute()` only knows the mismatch, not the request that
/// caused it, so the specific `Error` is rebuilt here from `entity_id` and
/// the lifecycle actually found.
#[cfg(feature = "commands")]
fn modify_lifecycle_error<A: Aggregate>(entity_id: EntityId, found: Lifecycle) -> Error {
    match found {
        Lifecycle::Missing => Error::not_found(A::aggregate_type().as_str(), entity_id.to_string()),
        Lifecycle::Deleted => {
            Error::aggregate_deleted(A::aggregate_type().to_string(), entity_id.to_string())
        }
        Lifecycle::Active => Error::invalid_state("Aggregate is not deleted"),
    }
}

/// Maps an [`ExecuteError`] onto the [`ModifyError`](crate::ModifyError)
/// `modify`/`modify_deleted` have always returned, preserving every variant's
/// prior meaning exactly.
#[cfg(feature = "commands")]
fn map_modify_error<A: Aggregate>(
    entity_id: EntityId,
    err: ExecuteError<A::Error>,
) -> crate::ModifyError<A::Error> {
    match err {
        ExecuteError::Domain(e) => crate::ModifyError::Domain(e),
        ExecuteError::Lifecycle { found, .. } => {
            crate::ModifyError::Load(modify_lifecycle_error::<A>(entity_id, found))
        }
        ExecuteError::Load(e) => crate::ModifyError::Load(e),
        ExecuteError::Save(e) => crate::ModifyError::Save(e),
    }
}

/// Persistence-style-agnostic aggregate persistence.
///
/// Application code written against this trait runs unchanged whether
/// aggregates are event-sourced or state-stored — the persistence style is
/// chosen once, at the composition root, by constructing the matching
/// implementation (e.g. [`EventSourcedRepository`]).
///
/// The `load`/`save` family are the per-implementation primitives; `modify`,
/// `modify_deleted`, and the `create_*` constructors are generic and provided
/// as default methods.
///
/// # Examples
///
/// ```ignore
/// async fn rename_user<R: Repository<User>>(repo: &R, id: EntityId, name: String) -> Result<()> {
///     repo.modify(id, |user| user.rename(name)).await?;
///     Ok(())
/// }
/// ```
#[async_trait]
pub trait Repository<A: Aggregate>: Send + Sync {
    /// Loads an active aggregate.
    ///
    /// Accepts both raw `EntityId` and typed IDs implementing `EntityIdFor<A>`.
    ///
    /// # Errors
    ///
    /// Returns `Error::NotFound` if the aggregate doesn't exist, or an error
    /// if the store operation or deserialization fails. Returns
    /// `Error::InvalidState` if the stored state cannot be restored, for
    /// instance a state-store row saved at a version below 1.
    async fn load_by_id(&self, id: EntityId) -> Result<AggregateRoot<A>>;

    /// Loads an aggregate, returning its lifecycle state.
    ///
    /// Returns `Loaded::Active` for active aggregates or `Loaded::Deleted`
    /// for deleted ones.
    ///
    /// Accepts both raw `EntityId` and typed IDs implementing `EntityIdFor<A>`.
    ///
    /// # Errors
    ///
    /// Returns an error if the aggregate doesn't exist or deserialization fails,
    /// and `Error::InvalidState` if the stored state cannot be restored, for
    /// instance a state-store row saved at a version below 1.
    async fn load_any_by_id(&self, id: EntityId) -> Result<Loaded<A>>;

    /// Loads a deleted aggregate.
    ///
    /// Accepts both raw `EntityId` and typed IDs implementing `EntityIdFor<A>`.
    ///
    /// # Errors
    ///
    /// Returns `Error::InvalidState` if the aggregate is still active, or an
    /// error if the aggregate doesn't exist or deserialization fails.
    async fn load_deleted_by_id(&self, id: EntityId) -> Result<DeletedAggregateRoot<A>>;

    /// Persists an aggregate root's pending changes and clears them on success.
    ///
    /// # Errors
    ///
    /// Returns an error if serialization, the store operation, or concurrency
    /// control fails.
    async fn save(&self, aggregate: &mut AggregateRoot<A>) -> Result<()>;

    /// Persists several aggregate roots as one logical write.
    ///
    /// On backends with real transactions (e.g. `PostgreSQL`) the whole set
    /// commits atomically: all aggregates are saved or none are. The in-memory
    /// backends apply them per-aggregate (not atomic). Aggregates with no
    /// pending changes are skipped.
    ///
    /// # Errors
    ///
    /// Returns an error if serialization, the store operation, or concurrency
    /// control fails. On atomic backends a failure rolls the whole batch back.
    async fn save_all(&self, aggregates: &mut [&mut AggregateRoot<A>]) -> Result<()>;

    /// Persists a deleted aggregate root's pending changes and clears them on success.
    ///
    /// # Errors
    ///
    /// Returns an error if serialization, the store operation, or concurrency
    /// control fails.
    async fn save_deleted(&self, aggregate: &mut DeletedAggregateRoot<A>) -> Result<()>;

    /// Checks if an aggregate exists.
    ///
    /// The default answers from [`load_any_by_id`](Self::load_any_by_id), so a
    /// deleted aggregate still exists. Override it when the store can answer without
    /// materializing the aggregate — reading a whole entity to return a `bool` is
    /// worth avoiding on a hot path.
    ///
    /// # Errors
    ///
    /// Returns an error if the store operation fails.
    async fn exists_by_id(&self, id: EntityId) -> Result<bool> {
        match self.load_any_by_id(id).await {
            Ok(_) => Ok(true),
            Err(e) if e.is_not_found() => Ok(false),
            Err(e) => Err(e),
        }
    }

    /// Gets the current version of an aggregate.
    ///
    /// The default reads it off [`load_any_by_id`](Self::load_any_by_id). Override it
    /// when the store can answer without materializing the aggregate — a version is
    /// one column, and loading a whole entity for it is worth avoiding.
    ///
    /// # Errors
    ///
    /// Returns an error if the store operation fails.
    async fn version_by_id(&self, id: EntityId) -> Result<AggregateVersion> {
        Ok(self.load_any_by_id(id).await?.version())
    }

    /// Runs a [`Command`](crate::Command) against the aggregate `id` names,
    /// retrying its save when the backend reports a concurrency conflict.
    ///
    /// Each attempt repeats the same cycle: load the aggregate's current
    /// [`Root`](crate::Root) — synthesizing [`Root::Missing`](crate::Root::Missing)
    /// from an `Error::NotFound` load, since nothing saved yet does not mean
    /// the command should be refused; it may be the one that creates the
    /// aggregate — run `command` against it, then save whatever lifecycle
    /// state it returns (`save` for [`Root::Active`](crate::Root::Active),
    /// `save_deleted` for [`Root::Deleted`](crate::Root::Deleted), nothing for
    /// [`Root::Missing`](crate::Root::Missing)). Only a save-time concurrency
    /// conflict is retried, reloading and re-running `command` from scratch
    /// each time, up to `retry`'s attempt budget; every other failure returns
    /// immediately.
    ///
    /// Accepts both raw `EntityId` and typed IDs implementing `EntityIdFor<A>`.
    ///
    /// # Errors
    ///
    /// Returns [`ExecuteError::Load`] if loading fails for a reason other
    /// than "nothing saved yet", [`ExecuteError::Domain`] or
    /// [`ExecuteError::Lifecycle`] if `command` refuses the aggregate's
    /// current lifecycle state, and [`ExecuteError::Save`] if saving still
    /// fails once `retry` is exhausted (or immediately, for a non-concurrency
    /// save failure). Also returns [`ExecuteError::Save`] if `command`
    /// returns a [`Root`](crate::Root) under a different [`EntityId`] than
    /// `id` — it is rejected rather than saved under the wrong identity.
    ///
    /// # Examples
    ///
    /// ```
    /// # use event_sauce_core::{
    /// #     Aggregate, AggregateError, AggregateRoot, Command, CommandError, DefaultEntity,
    /// #     DomainEvent, Entity, EntityId, EventApplicator, EventVersion, Repository, RetryPolicy,
    /// #     Root,
    /// # };
    /// # use serde::{Deserialize, Serialize};
    /// # use thiserror::Error;
    /// # use std::sync::Arc;
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
    /// #     fn event_type(&self) -> &'static str { "Incremented" }
    /// #     fn event_version(&self) -> EventVersion { EventVersion::new(1) }
    /// #     fn occurred_at(&self) -> chrono::DateTime<chrono::Utc> { chrono::Utc::now() }
    /// # }
    /// # impl EventApplicator<Counter> for CounterEvent {
    /// #     fn dispatch(&self, c: &mut Counter) -> Result<(), CounterError> {
    /// #         match self { CounterEvent::Incremented { amount } => c.value += amount }
    /// #         Ok(())
    /// #     }
    /// #     fn dispatch_unchecked(&self, c: &mut Counter) {
    /// #         match self { CounterEvent::Incremented { amount } => c.value += amount }
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
    /// struct Increment(i32);
    /// impl Command<Counter> for Increment {
    ///     fn handle(
    ///         &self,
    ///         root: Root<Counter>,
    ///         _ctx: &(),
    ///     ) -> Result<Root<Counter>, CommandError<CounterError>> {
    ///         let mut root = root.active()?;
    ///         root.apply(CounterEvent::Incremented { amount: self.0 })?;
    ///         Ok(root.into())
    ///     }
    /// }
    ///
    /// # async fn run<R: Repository<Counter>>(repo: &R) {
    /// let id = EntityId::new();
    /// let executed = repo
    ///     .execute(id, &Increment(5), &(), RetryPolicy::default())
    ///     .await
    ///     .unwrap();
    /// assert_eq!(executed.attempts.get(), 1);
    /// assert_eq!(executed.root.active().unwrap().value, 5);
    /// # }
    /// ```
    #[cfg(feature = "commands")]
    async fn execute<I, C, Ctx>(
        &self,
        id: I,
        command: &C,
        ctx: &Ctx,
        retry: RetryPolicy,
    ) -> std::result::Result<Executed<A>, ExecuteError<A::Error>>
    where
        I: EntityIdFor<A> + Send,
        C: Command<A, Ctx>,
        Ctx: Sync,
        Self: Sized,
    {
        let id = id.entity_id();
        let max_attempts = retry.max_attempts().get();
        let mut attempt = 1u32;

        loop {
            let root = match self.load_any_by_id(id).await {
                Ok(Loaded::Active(root)) => Root::Active(root),
                Ok(Loaded::Deleted(root)) => Root::Deleted(root),
                Err(e) if e.is_not_found() => Root::Missing(self.create_uninit_with_id(id)),
                Err(e) => return Err(ExecuteError::Load(e)),
            };

            let mut outcome = command.handle(root, ctx).map_err(|err| match err {
                crate::CommandError::Domain(e) => ExecuteError::Domain(e),
                crate::CommandError::Lifecycle { expected, found } => {
                    ExecuteError::Lifecycle { expected, found }
                }
            })?;

            if outcome.entity_id() != id {
                return Err(ExecuteError::Save(Error::invalid_state(format!(
                    "command returned aggregate {} for requested id {id}",
                    outcome.entity_id()
                ))));
            }

            let saved = match &mut outcome {
                Root::Active(root) => self.save(root).await,
                Root::Deleted(root) => self.save_deleted(root).await,
                Root::Missing(_) => Ok(()),
            };

            match saved {
                Ok(()) => {
                    return Ok(Executed {
                        root: outcome,
                        attempts: std::num::NonZeroU32::new(attempt)
                            .expect("attempt starts at 1 and only grows"),
                    });
                }
                Err(e) if e.is_concurrency_conflict() && attempt < max_attempts => {
                    let backoff = retry.backoff(attempt);
                    attempt += 1;
                    if !backoff.is_zero() {
                        tokio::time::sleep(backoff).await;
                    }
                }
                Err(e) => return Err(ExecuteError::Save(e)),
            }
        }
    }

    /// Loads an aggregate, applies a closure that mutates it, and saves the result.
    ///
    /// The most common service command handler pattern, condensed into a single
    /// call. The closure receives the loaded aggregate and may run aggregate
    /// commands on it or apply events directly.
    ///
    /// Returns `Result<R, ModifyError<A::Error>>`, which distinguishes three
    /// failure modes:
    ///
    /// - [`ModifyError::Load`](crate::ModifyError::Load) — the store failed
    ///   while loading the aggregate.
    /// - [`ModifyError::Domain`](crate::ModifyError::Domain) — the closure returned
    ///   an aggregate-defined error `A::Error`.
    /// - [`ModifyError::Save`](crate::ModifyError::Save) — the store failed
    ///   while persisting the changes.
    ///
    /// Callers that want typed-error handling can match on the variants directly.
    /// Callers whose functions return `Result<_, Error>` can still use `?` thanks
    /// to the `From<ModifyError<E>> for Error` blanket, which flattens the error
    /// transparently.
    ///
    /// Accepts both raw `EntityId` and typed IDs implementing `EntityIdFor<A>`.
    ///
    /// # Errors
    ///
    /// Returns `ModifyError::Load` if loading fails, `ModifyError::Domain` if the
    /// closure fails, or `ModifyError::Save` if saving fails.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// // Typed matching:
    /// match repo.modify(order_id, |order| order.add_item("laptop".into(), 1, 120_000)).await {
    ///     Ok(()) => {}
    ///     Err(ModifyError::Domain(OrderError::InsufficientStock)) => { /* handle */ }
    ///     Err(e) => return Err(e.into()),
    /// }
    ///
    /// // Ergonomic ? in Result<_, Error> functions:
    /// let value = repo.modify(order_id, |order| {
    ///     order.add_item("laptop".into(), 1, 120_000)?;
    ///     Ok(order.total())
    /// }).await?;
    /// ```
    async fn modify<I, F, R>(
        &self,
        id: I,
        f: F,
    ) -> std::result::Result<R, crate::ModifyError<A::Error>>
    where
        I: EntityIdFor<A> + Send,
        Self: Sized,
        F: FnOnce(&mut AggregateRoot<A>) -> std::result::Result<R, A::Error> + Send,
        R: Send,
    {
        #[cfg(feature = "commands")]
        {
            let entity_id = id.entity_id();
            let command = ModifyCommand::new(f);
            self.execute(entity_id, &command, &(), RetryPolicy::none())
                .await
                .map(|_executed| command.take_result())
                .map_err(|e| map_modify_error::<A>(entity_id, e))
        }
        #[cfg(not(feature = "commands"))]
        {
            let mut aggregate = self.load(id).await.map_err(crate::ModifyError::Load)?;
            let result = f(&mut aggregate).map_err(crate::ModifyError::Domain)?;
            self.save(&mut aggregate)
                .await
                .map_err(crate::ModifyError::Save)?;
            Ok(result)
        }
    }

    /// Loads a deleted aggregate, applies a closure, and saves any new pending changes.
    ///
    /// Mirrors [`modify`](Self::modify) for the deleted-state lifecycle.
    /// Note: deleted aggregates cannot accept further events through `apply()` —
    /// the closure is mostly useful for inspecting state or appending
    /// metadata-only events.
    ///
    /// Returns `Result<R, ModifyError<A::Error>>` with the same failure-mode
    /// split as [`modify`](Self::modify); `ModifyError::Load` also covers
    /// `Error::InvalidState` when the aggregate is still active.
    ///
    /// Accepts both raw `EntityId` and typed IDs implementing `EntityIdFor<A>`.
    ///
    /// # Errors
    ///
    /// Returns `ModifyError::Load` if the aggregate is not deleted or cannot be
    /// loaded, `ModifyError::Domain` if the closure fails, or `ModifyError::Save`
    /// if saving fails.
    async fn modify_deleted<I, F, R>(
        &self,
        id: I,
        f: F,
    ) -> std::result::Result<R, crate::ModifyError<A::Error>>
    where
        I: EntityIdFor<A> + Send,
        Self: Sized,
        F: FnOnce(&mut DeletedAggregateRoot<A>) -> std::result::Result<R, A::Error> + Send,
        R: Send,
    {
        #[cfg(feature = "commands")]
        {
            let entity_id = id.entity_id();
            let command = ModifyDeletedCommand::new(f);
            self.execute(entity_id, &command, &(), RetryPolicy::none())
                .await
                .map(|_executed| command.take_result())
                .map_err(|e| map_modify_error::<A>(entity_id, e))
        }
        #[cfg(not(feature = "commands"))]
        {
            let mut deleted = self
                .load_deleted(id)
                .await
                .map_err(crate::ModifyError::Load)?;
            let result = f(&mut deleted).map_err(crate::ModifyError::Domain)?;
            self.save_deleted(&mut deleted)
                .await
                .map_err(crate::ModifyError::Save)?;
            Ok(result)
        }
    }

    /// Creates an uninitialized aggregate root with a random `EntityId`.
    ///
    /// Use with aggregates that require init events.
    #[must_use]
    fn create_uninit(&self) -> UninitAggregateRoot<A> {
        UninitAggregateRoot::new(EntityId::new())
    }

    /// Creates an uninitialized aggregate root with the given `EntityId`.
    ///
    /// Use with aggregates that require init events.
    #[must_use]
    fn create_uninit_with_id(&self, id: EntityId) -> UninitAggregateRoot<A> {
        UninitAggregateRoot::new(id)
    }

    /// Creates and immediately initializes an aggregate with an init event.
    ///
    /// Convenience for `create_uninit()` + `apply_init()` in one step.
    ///
    /// # Errors
    ///
    /// Returns an error if init event validation fails.
    fn create_with<E: InitEvent<A> + Into<A::Event>>(
        &self,
        event: E,
    ) -> std::result::Result<AggregateRoot<A>, A::Error>
    where
        Self: Sized,
    {
        UninitAggregateRoot::new(EntityId::new()).apply_init(event)
    }

    /// Creates and immediately initializes an aggregate with a given ID and init event.
    ///
    /// # Errors
    ///
    /// Returns an error if init event validation fails.
    fn create_with_id_and<E: InitEvent<A> + Into<A::Event>>(
        &self,
        id: EntityId,
        event: E,
    ) -> std::result::Result<AggregateRoot<A>, A::Error>
    where
        Self: Sized,
    {
        UninitAggregateRoot::new(id).apply_init(event)
    }

    /// Creates a new aggregate root with a random `EntityId`.
    ///
    /// Requires `DefaultEntity`. For init-event aggregates, use
    /// [`create_uninit()`](Self::create_uninit) or [`create_with()`](Self::create_with).
    #[must_use]
    fn create(&self) -> AggregateRoot<A>
    where
        A: DefaultEntity,
    {
        AggregateRoot::new(EntityId::new())
    }

    /// Creates a new aggregate root with the given `EntityId`.
    ///
    /// Requires `DefaultEntity`. For init-event aggregates, use
    /// [`create_uninit_with_id()`](Self::create_uninit_with_id) or
    /// [`create_with_id_and()`](Self::create_with_id_and).
    #[must_use]
    fn create_with_id(&self, id: EntityId) -> AggregateRoot<A>
    where
        A: DefaultEntity,
    {
        AggregateRoot::new(id)
    }

    /// Loads an active aggregate by its typed id.
    ///
    /// # Errors
    ///
    /// What [`Repository::load_by_id`] gives.
    async fn load<I: EntityIdFor<A> + Send>(&self, id: I) -> Result<AggregateRoot<A>>
    where
        Self: Sized,
    {
        self.load_by_id(id.entity_id()).await
    }

    /// Loads an aggregate by its typed id, returning its lifecycle state.
    ///
    /// # Errors
    ///
    /// What [`Repository::load_any_by_id`] gives.
    async fn load_any<I: EntityIdFor<A> + Send>(&self, id: I) -> Result<Loaded<A>>
    where
        Self: Sized,
    {
        self.load_any_by_id(id.entity_id()).await
    }

    /// Loads a deleted aggregate by its typed id.
    ///
    /// # Errors
    ///
    /// What [`Repository::load_deleted_by_id`] gives.
    async fn load_deleted<I: EntityIdFor<A> + Send>(&self, id: I) -> Result<DeletedAggregateRoot<A>>
    where
        Self: Sized,
    {
        self.load_deleted_by_id(id.entity_id()).await
    }

    /// Whether an aggregate with this typed id exists.
    ///
    /// # Errors
    ///
    /// What [`Repository::exists_by_id`] gives.
    async fn exists<I: EntityIdFor<A> + Send>(&self, id: I) -> Result<bool>
    where
        Self: Sized,
    {
        self.exists_by_id(id.entity_id()).await
    }

    /// The stored version of the aggregate with this typed id.
    ///
    /// # Errors
    ///
    /// What [`Repository::version_by_id`] gives.
    async fn get_version<I: EntityIdFor<A> + Send>(&self, id: I) -> Result<AggregateVersion>
    where
        Self: Sized,
    {
        self.version_by_id(id.entity_id()).await
    }
}

/// Event-store-backed [`Repository`] implementation.
///
/// Wraps an `EventStore` and provides type-safe operations for a specific
/// aggregate type: `load` reconstructs the aggregate by replaying its events
/// (optionally from a snapshot), `save` appends the pending events with
/// optimistic concurrency control.
///
/// # Type Parameters
///
/// - `S`: The event store implementation
/// - `A`: The aggregate type (entity)
///
/// # Examples
///
/// ```ignore
/// let repo = store.repository::<User>();
///
/// // Create a new aggregate
/// let mut user = repo.create();
/// user.apply(UserCreatedEvent { name: "Alice".into(), timestamp: Utc::now() })?;
/// repo.save(&mut user).await?;
///
/// // Load an existing aggregate
/// let loaded = repo.load(user.entity_id()).await?;
/// ```
#[cfg(feature = "event-sourcing")]
#[derive(Debug)]
pub struct EventSourcedRepository<S, A> {
    store: Arc<S>,
    _phantom: PhantomData<A>,
}

#[cfg(feature = "event-sourcing")]
impl<S, A> EventSourcedRepository<S, A>
where
    S: EventStore + 'static,
    A: Aggregate + serde::Serialize + serde::de::DeserializeOwned,
    A::DeletedState: serde::Serialize + serde::de::DeserializeOwned,
    A::Event: serde::Serialize + serde::de::DeserializeOwned,
{
    /// Creates a new repository wrapping the given event store.
    #[must_use]
    pub fn new(store: Arc<S>) -> Self {
        Self {
            store,
            _phantom: PhantomData,
        }
    }

    /// Counts the number of events for an aggregate.
    ///
    /// Event-sourcing-specific: only meaningful when aggregates are persisted
    /// as event streams, so this lives on the concrete repository rather than
    /// the [`Repository`] trait.
    ///
    /// Accepts both raw `EntityId` and typed IDs implementing `EntityIdFor<A>`.
    ///
    /// # Errors
    ///
    /// Returns an error if the event store operation fails.
    pub async fn count_events(&self, id: impl EntityIdFor<A>) -> Result<usize> {
        let stream_id = StreamId::new(A::aggregate_type(), id.entity_id().as_uuid());
        count_events(&*self.store, stream_id).await
    }
}

#[cfg(feature = "event-sourcing")]
#[async_trait]
impl<S, A> Repository<A> for EventSourcedRepository<S, A>
where
    S: EventStore + 'static,
    A: Aggregate + serde::Serialize + serde::de::DeserializeOwned,
    A::DeletedState: serde::Serialize + serde::de::DeserializeOwned,
    A::Event: serde::Serialize + serde::de::DeserializeOwned,
{
    async fn load_by_id(&self, id: EntityId) -> Result<AggregateRoot<A>> {
        load(&*self.store, id).await
    }

    async fn load_any_by_id(&self, id: EntityId) -> Result<Loaded<A>> {
        load_any(&*self.store, id).await
    }

    async fn load_deleted_by_id(&self, id: EntityId) -> Result<DeletedAggregateRoot<A>> {
        load_deleted(&*self.store, id).await
    }

    async fn save(&self, aggregate: &mut AggregateRoot<A>) -> Result<()> {
        self.store.commit(aggregate).await
    }

    async fn save_all(&self, aggregates: &mut [&mut AggregateRoot<A>]) -> Result<()> {
        let mut prepared = Vec::with_capacity(aggregates.len());
        for aggregate in aggregates.iter_mut() {
            if let Some(commit) =
                crate::event_store::prepare_commit(&*self.store, &**aggregate).await?
            {
                prepared.push(commit);
            }
        }
        crate::event_store::flush_prepared_batch(&*self.store, prepared).await?;
        for aggregate in aggregates.iter_mut() {
            aggregate.clear_pending_events();
        }
        Ok(())
    }

    async fn save_deleted(&self, aggregate: &mut DeletedAggregateRoot<A>) -> Result<()> {
        self.store.commit_deleted(aggregate).await
    }

    async fn exists_by_id(&self, id: EntityId) -> Result<bool> {
        let stream_id = StreamId::new(A::aggregate_type(), id.as_uuid());
        self.store.stream_exists(stream_id).await
    }

    async fn version_by_id(&self, id: EntityId) -> Result<AggregateVersion> {
        let stream_id = StreamId::new(A::aggregate_type(), id.as_uuid());
        self.store.get_version(stream_id).await
    }
}

#[cfg(feature = "event-sourcing")]
impl<S, A> Clone for EventSourcedRepository<S, A> {
    fn clone(&self) -> Self {
        Self {
            store: Arc::clone(&self.store),
            _phantom: PhantomData,
        }
    }
}

#[cfg(all(test, feature = "event-sourcing"))]
mod tests {
    use super::*;
    use crate::test_fixtures::{
        MockEventStore, SimpleTestDelete, SimpleTestEntity, SimpleTestEvent, SimpleTestInit,
    };
    use crate::EntityId;

    #[tokio::test]
    async fn test_repository_create_uninit_and_with_id() {
        let store = Arc::new(MockEventStore::new());
        let repo = store.repository::<SimpleTestEntity>();

        let uninit = repo.create_uninit();
        let generated_id = uninit.entity_id();

        let id = EntityId::new();
        let uninit_with_id = repo.create_uninit_with_id(id);
        assert_eq!(uninit_with_id.entity_id(), id);
        assert_ne!(generated_id, id);
    }

    #[tokio::test]
    async fn test_repository_create_with_init_event() {
        let store = Arc::new(MockEventStore::new());
        let repo = store.repository::<SimpleTestEntity>();

        let mut aggregate = repo.create_with(SimpleTestInit { value: 7 }).unwrap();
        assert_eq!(aggregate.value, 7);

        repo.save(&mut aggregate).await.unwrap();
        let loaded = repo.load(aggregate.entity_id()).await.unwrap();
        assert_eq!(loaded.value, 7);
    }

    #[tokio::test]
    async fn test_repository_create_with_id_and_init_event() {
        let store = Arc::new(MockEventStore::new());
        let repo = store.repository::<SimpleTestEntity>();

        let id = EntityId::new();
        let aggregate = repo
            .create_with_id_and(id, SimpleTestInit { value: 3 })
            .unwrap();
        assert_eq!(aggregate.entity_id(), id);
        assert_eq!(aggregate.value, 3);
    }

    #[tokio::test]
    async fn test_repository_modify_deleted_roundtrip() {
        let store = Arc::new(MockEventStore::new());
        let repo = store.repository::<SimpleTestEntity>();

        let id = EntityId::new();
        let mut aggregate = repo.create_with_id(id);
        aggregate
            .apply(SimpleTestEvent::Created { value: 12 })
            .unwrap();
        repo.save(&mut aggregate).await.unwrap();

        let aggregate = repo.load(id).await.unwrap();
        let mut deleted = aggregate.apply_delete(SimpleTestDelete).unwrap();
        repo.save_deleted(&mut deleted).await.unwrap();

        let value = repo
            .modify_deleted(id, |deleted| Ok(deleted.state().value))
            .await
            .unwrap();
        assert_eq!(value, 12);
    }

    #[tokio::test]
    async fn test_repository_new() {
        let store = Arc::new(MockEventStore::new());
        let _repo = EventSourcedRepository::<MockEventStore, SimpleTestEntity>::new(store);
    }

    #[tokio::test]
    async fn test_repository_save_and_load() {
        let store = Arc::new(MockEventStore::new());
        let repo = EventSourcedRepository::<MockEventStore, SimpleTestEntity>::new(store);

        let test_id = EntityId::new();
        let mut aggregate = AggregateRoot::<SimpleTestEntity>::new(test_id);
        aggregate
            .apply(SimpleTestEvent::Created { value: 5 })
            .unwrap();

        repo.save(&mut aggregate).await.unwrap();

        let loaded = repo.load(test_id).await.unwrap();
        assert_eq!(loaded.value, 5);
    }

    #[tokio::test]
    async fn test_generic_code_runs_against_repository_trait() {
        // The acceptance criterion of the state-store split: application code
        // written against `R: Repository<A>` must be persistence-agnostic.
        async fn set_value<R: Repository<SimpleTestEntity>>(
            repo: &R,
            id: EntityId,
            value: i32,
        ) -> Result<i32> {
            let mut aggregate = repo.create_with_id(id);
            aggregate.apply(SimpleTestEvent::Created { value })?;
            repo.save(&mut aggregate).await?;
            let loaded = repo.load(id).await?;
            Ok(loaded.value)
        }

        let store = Arc::new(MockEventStore::new());
        let repo = store.repository::<SimpleTestEntity>();

        let id = EntityId::new();
        assert_eq!(set_value(&repo, id, 41).await.unwrap(), 41);
    }

    #[tokio::test]
    async fn test_repository_exists() {
        let store = Arc::new(MockEventStore::new());
        let repo = EventSourcedRepository::<MockEventStore, SimpleTestEntity>::new(store);

        let test_id = EntityId::new();

        assert!(!repo.exists(test_id).await.unwrap());

        let mut aggregate = AggregateRoot::<SimpleTestEntity>::new(test_id);
        aggregate
            .apply(SimpleTestEvent::Created { value: 5 })
            .unwrap();
        repo.save(&mut aggregate).await.unwrap();

        assert!(repo.exists(test_id).await.unwrap());
    }

    #[tokio::test]
    async fn test_repository_get_version() {
        let store = Arc::new(MockEventStore::new());
        let repo = EventSourcedRepository::<MockEventStore, SimpleTestEntity>::new(store);

        let test_id = EntityId::new();
        let mut aggregate = AggregateRoot::<SimpleTestEntity>::new(test_id);
        aggregate
            .apply(SimpleTestEvent::Created { value: 5 })
            .unwrap();
        aggregate
            .apply(SimpleTestEvent::Updated { value: 8 })
            .unwrap();

        repo.save(&mut aggregate).await.unwrap();

        let version = repo.get_version(test_id).await.unwrap();
        assert_eq!(version.as_i64(), 2);
    }

    #[tokio::test]
    async fn test_repository_count_events() {
        let store = Arc::new(MockEventStore::new());
        let repo = EventSourcedRepository::<MockEventStore, SimpleTestEntity>::new(store);

        let test_id = EntityId::new();
        let mut aggregate = AggregateRoot::<SimpleTestEntity>::new(test_id);
        aggregate
            .apply(SimpleTestEvent::Created { value: 5 })
            .unwrap();
        aggregate
            .apply(SimpleTestEvent::Updated { value: 8 })
            .unwrap();
        aggregate
            .apply(SimpleTestEvent::Updated { value: 10 })
            .unwrap();

        repo.save(&mut aggregate).await.unwrap();

        let count = repo.count_events(test_id).await.unwrap();
        assert_eq!(count, 3);
    }

    #[tokio::test]
    async fn test_repository_from_store_convenience() {
        let store = Arc::new(MockEventStore::new());
        // Use the convenience method instead of EventSourcedRepository::<S, A>::new()
        let repo = store.repository::<SimpleTestEntity>();

        let test_id = EntityId::new();
        let mut aggregate = AggregateRoot::<SimpleTestEntity>::new(test_id);
        aggregate
            .apply(SimpleTestEvent::Created { value: 7 })
            .unwrap();

        repo.save(&mut aggregate).await.unwrap();
        let loaded = repo.load(test_id).await.unwrap();
        assert_eq!(loaded.value, 7);
    }

    #[tokio::test]
    async fn test_repository_create() {
        let store = Arc::new(MockEventStore::new());
        let repo = store.repository::<SimpleTestEntity>();

        let mut aggregate = repo.create();
        aggregate
            .apply(SimpleTestEvent::Created { value: 99 })
            .unwrap();

        repo.save(&mut aggregate).await.unwrap();
        let loaded = repo.load(aggregate.entity_id()).await.unwrap();
        assert_eq!(loaded.value, 99);
    }

    #[tokio::test]
    async fn test_repository_create_with_id() {
        let store = Arc::new(MockEventStore::new());
        let repo = store.repository::<SimpleTestEntity>();

        let id = EntityId::new();
        let mut aggregate = repo.create_with_id(id);
        assert_eq!(aggregate.entity_id(), id);

        aggregate
            .apply(SimpleTestEvent::Created { value: 42 })
            .unwrap();

        repo.save(&mut aggregate).await.unwrap();
        let loaded = repo.load(id).await.unwrap();
        assert_eq!(loaded.value, 42);
    }

    #[tokio::test]
    async fn test_repository_clone_shares_arc_store() {
        let store = Arc::new(MockEventStore::new());
        let repo = EventSourcedRepository::<MockEventStore, SimpleTestEntity>::new(store);
        let repo_clone = repo.clone();

        let test_id = EntityId::new();
        let mut aggregate = AggregateRoot::<SimpleTestEntity>::new(test_id);
        aggregate
            .apply(SimpleTestEvent::Created { value: 42 })
            .unwrap();

        // Save through the original repository
        repo.save(&mut aggregate).await.unwrap();

        // Load through the cloned repository — should see the same data
        let loaded = repo_clone.load(test_id).await.unwrap();
        assert_eq!(loaded.value, 42);
    }

    #[tokio::test]
    async fn test_repository_modify_loads_mutates_and_saves() {
        let store = Arc::new(MockEventStore::new());
        let repo = store.repository::<SimpleTestEntity>();

        let id = EntityId::new();
        let mut aggregate = AggregateRoot::<SimpleTestEntity>::new(id);
        aggregate
            .apply(SimpleTestEvent::Created { value: 1 })
            .unwrap();
        repo.save(&mut aggregate).await.unwrap();

        let returned = repo
            .modify(id, |agg| {
                agg.apply(SimpleTestEvent::Updated { value: 99 })?;
                Ok(agg.value)
            })
            .await
            .unwrap();

        assert_eq!(returned, 99);
        let reloaded = repo.load(id).await.unwrap();
        assert_eq!(reloaded.value, 99);
    }

    #[tokio::test]
    async fn test_repository_modify_propagates_aggregate_error() {
        let store = Arc::new(MockEventStore::new());
        let repo = store.repository::<SimpleTestEntity>();

        let id = EntityId::new();
        let mut aggregate = AggregateRoot::<SimpleTestEntity>::new(id);
        aggregate
            .apply(SimpleTestEvent::Created { value: 1 })
            .unwrap();
        repo.save(&mut aggregate).await.unwrap();

        let result = repo
            .modify(id, |_agg| -> std::result::Result<(), _> {
                Err(crate::test_fixtures::SimpleTestError)
            })
            .await;

        assert!(result.is_err());
        let reloaded = repo.load(id).await.unwrap();
        assert_eq!(reloaded.value, 1, "aggregate not mutated on closure error");
    }

    #[tokio::test]
    async fn test_repository_modify_domain_error_is_typed() {
        let store = Arc::new(MockEventStore::new());
        let repo = store.repository::<SimpleTestEntity>();

        let id = EntityId::new();
        let mut aggregate = AggregateRoot::<SimpleTestEntity>::new(id);
        aggregate
            .apply(SimpleTestEvent::Created { value: 1 })
            .unwrap();
        repo.save(&mut aggregate).await.unwrap();

        let result = repo
            .modify(id, |_agg| -> std::result::Result<(), _> {
                Err(crate::test_fixtures::SimpleTestError)
            })
            .await;

        // The error should be the Domain variant, not Load or Save.
        assert!(result.as_ref().unwrap_err().is_domain());
        assert!(matches!(
            result.unwrap_err(),
            crate::ModifyError::Domain(crate::test_fixtures::SimpleTestError)
        ));
    }

    #[tokio::test]
    async fn test_repository_modify_domain_error_flattens_via_from() {
        // Verify backward-compat: ? in a function returning Result<_, Error> still works.
        // The inner helper is declared before any statements to satisfy clippy::items_after_statements.
        async fn try_modify(
            repo: &crate::EventSourcedRepository<MockEventStore, SimpleTestEntity>,
            id: EntityId,
        ) -> crate::Result<()> {
            repo.modify(id, |_agg| -> std::result::Result<(), _> {
                Err(crate::test_fixtures::SimpleTestError)
            })
            .await?;
            Ok(())
        }

        let store = Arc::new(MockEventStore::new());
        let repo = store.repository::<SimpleTestEntity>();

        let id = EntityId::new();
        let mut aggregate = AggregateRoot::<SimpleTestEntity>::new(id);
        aggregate
            .apply(SimpleTestEvent::Created { value: 1 })
            .unwrap();
        repo.save(&mut aggregate).await.unwrap();

        let err = try_modify(&repo, id).await.unwrap_err();
        assert!(matches!(err, crate::Error::InvalidState(_)));
    }

    #[tokio::test]
    async fn test_repository_modify_on_missing_aggregate_is_load_not_found() {
        let store = Arc::new(MockEventStore::new());
        let repo = store.repository::<SimpleTestEntity>();

        let id = EntityId::new();
        let err = repo
            .modify(id, |agg| agg.apply(SimpleTestEvent::Updated { value: 1 }))
            .await
            .unwrap_err();

        match err {
            crate::ModifyError::Load(e) => {
                assert!(e.is_not_found(), "expected NotFound, got {e:?}");
            }
            other => panic!("expected ModifyError::Load, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn test_repository_modify_on_deleted_aggregate_is_load_aggregate_deleted() {
        let store = Arc::new(MockEventStore::new());
        let repo = store.repository::<SimpleTestEntity>();

        let id = EntityId::new();
        let mut aggregate = AggregateRoot::<SimpleTestEntity>::new(id);
        aggregate
            .apply(SimpleTestEvent::Created { value: 1 })
            .unwrap();
        repo.save(&mut aggregate).await.unwrap();
        let mut deleted = repo
            .load(id)
            .await
            .unwrap()
            .apply_delete(crate::test_fixtures::SimpleTestDelete)
            .unwrap();
        repo.save_deleted(&mut deleted).await.unwrap();

        let err = repo
            .modify(id, |agg| agg.apply(SimpleTestEvent::Updated { value: 2 }))
            .await
            .unwrap_err();

        match err {
            crate::ModifyError::Load(e) => {
                assert!(
                    e.is_aggregate_deleted(),
                    "expected AggregateDeleted, got {e:?}"
                );
            }
            other => panic!("expected ModifyError::Load, got {other:?}"),
        }
    }

    /// Wraps a [`MockEventStore`], delegating loads but always failing
    /// `append` with a claim conflict — for pinning that `Repository::modify`
    /// surfaces a save-time claim conflict through `ModifyError::Save`
    /// unchanged.
    struct ClaimConflictEventStore {
        inner: Arc<MockEventStore>,
    }

    #[async_trait]
    impl EventStore for ClaimConflictEventStore {
        async fn append(
            &self,
            _stream_id: StreamId,
            _events: Vec<crate::EventEnvelope>,
            _expected_version: AggregateVersion,
            _claims: Vec<crate::AggregateClaim>,
            _clear_claims: bool,
        ) -> Result<()> {
            Err(crate::Error::claim_conflict(
                "Profile.email",
                serde_json::json!("taken@example.com"),
                None,
            ))
        }

        async fn load_stream(
            &self,
            stream_id: StreamId,
            from_version: AggregateVersion,
        ) -> Result<impl futures::Stream<Item = Result<crate::EventEnvelope>> + Send> {
            self.inner.load_stream(stream_id, from_version).await
        }

        async fn stream_all(
            &self,
            from_position: crate::Position,
        ) -> Result<impl futures::Stream<Item = Result<crate::EventLogEntry>> + Send> {
            self.inner.stream_all(from_position).await
        }

        async fn get_version(&self, stream_id: StreamId) -> Result<AggregateVersion> {
            self.inner.get_version(stream_id).await
        }
    }

    #[tokio::test]
    async fn test_repository_modify_save_claim_conflict_is_decodable() {
        let inner = Arc::new(MockEventStore::new());
        let setup = inner.repository::<SimpleTestEntity>();
        let id = EntityId::new();
        let mut aggregate = AggregateRoot::<SimpleTestEntity>::new(id);
        aggregate
            .apply(SimpleTestEvent::Created { value: 1 })
            .unwrap();
        setup.save(&mut aggregate).await.unwrap();

        let repo = EventSourcedRepository::<ClaimConflictEventStore, SimpleTestEntity>::new(
            Arc::new(ClaimConflictEventStore { inner }),
        );
        let err = repo
            .modify(id, |agg| agg.apply(SimpleTestEvent::Updated { value: 2 }))
            .await
            .unwrap_err();

        let conflict = err.save_claim_conflict().expect("claim conflict");
        assert_eq!(conflict.claim_type, "Profile.email");
    }

    #[tokio::test]
    async fn test_repository_load_any_returns_active() {
        let store = Arc::new(MockEventStore::new());
        let repo = store.repository::<SimpleTestEntity>();

        let id = EntityId::new();
        let mut aggregate = AggregateRoot::<SimpleTestEntity>::new(id);
        aggregate
            .apply(SimpleTestEvent::Created { value: 10 })
            .unwrap();
        repo.save(&mut aggregate).await.unwrap();

        let loaded = repo.load_any(id).await.unwrap();
        assert!(loaded.is_active());
    }

    #[tokio::test]
    async fn test_repository_load_deleted_errors_for_active() {
        let store = Arc::new(MockEventStore::new());
        let repo = store.repository::<SimpleTestEntity>();

        let id = EntityId::new();
        let mut aggregate = AggregateRoot::<SimpleTestEntity>::new(id);
        aggregate
            .apply(SimpleTestEvent::Created { value: 10 })
            .unwrap();
        repo.save(&mut aggregate).await.unwrap();

        let result = repo.load_deleted(id).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_repository_save_deleted() {
        let store = Arc::new(MockEventStore::new());
        let repo = store.repository::<SimpleTestEntity>();

        let id = EntityId::new();
        let entity = SimpleTestEntity { id, value: 42 };
        let pending = vec![crate::aggregate_root::PendingEvent {
            event: SimpleTestEvent::Updated { value: 0 },
            actor_id: None,
            metadata: None,
        }];
        let mut deleted = DeletedAggregateRoot::from_delete_with_pending(
            entity,
            id,
            AggregateVersion::new(1),
            pending,
            false,
        );

        repo.save_deleted(&mut deleted).await.unwrap();
        assert!(deleted.pending_events().is_empty());
    }

    /// A store whose `append` fails once every stream has been attempted
    /// `succeed_first` times, for testing that a batch conflict does not
    /// silently clear aggregates that were never actually persisted.
    struct FailingBatchStore {
        succeed_first: usize,
        calls: std::sync::atomic::AtomicUsize,
    }

    #[async_trait]
    impl EventStore for FailingBatchStore {
        async fn append(
            &self,
            _stream_id: StreamId,
            _events: Vec<crate::EventEnvelope>,
            expected_version: AggregateVersion,
            _claims: Vec<crate::AggregateClaim>,
            _clear_claims: bool,
        ) -> Result<()> {
            let call = self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            if call >= self.succeed_first {
                return Err(crate::Error::concurrency_conflict(
                    expected_version,
                    AggregateVersion::new(expected_version.as_i64() + 1),
                ));
            }
            Ok(())
        }

        async fn load_stream(
            &self,
            _stream_id: StreamId,
            _from_version: AggregateVersion,
        ) -> Result<impl futures::Stream<Item = Result<crate::EventEnvelope>> + Send> {
            Ok(futures::stream::empty())
        }

        async fn stream_all(
            &self,
            _from_position: crate::Position,
        ) -> Result<impl futures::Stream<Item = Result<crate::EventLogEntry>> + Send> {
            Ok(futures::stream::empty())
        }

        async fn get_version(&self, _stream_id: StreamId) -> Result<AggregateVersion> {
            Ok(AggregateVersion::initial())
        }
    }

    #[tokio::test]
    async fn save_all_skips_aggregates_with_no_pending_events() {
        let store = Arc::new(MockEventStore::new());
        let repo = store.repository::<SimpleTestEntity>();

        let mut dirty = AggregateRoot::<SimpleTestEntity>::new(EntityId::new());
        dirty.apply(SimpleTestEvent::Created { value: 1 }).unwrap();
        let mut clean = AggregateRoot::<SimpleTestEntity>::new(EntityId::new());

        repo.save_all(&mut [&mut dirty, &mut clean]).await.unwrap();

        assert!(dirty.pending_events().is_empty());
        assert!(clean.pending_events().is_empty());
        assert_eq!(
            store.get_events().len(),
            1,
            "a clean aggregate must not be persisted by save_all"
        );
    }

    #[tokio::test]
    async fn save_all_keeps_every_pending_event_when_the_batch_fails() {
        let store = Arc::new(FailingBatchStore {
            succeed_first: 1,
            calls: std::sync::atomic::AtomicUsize::new(0),
        });
        let repo = EventSourcedRepository::<FailingBatchStore, SimpleTestEntity>::new(store);

        let mut first = AggregateRoot::<SimpleTestEntity>::new(EntityId::new());
        first.apply(SimpleTestEvent::Created { value: 1 }).unwrap();
        let mut second = AggregateRoot::<SimpleTestEntity>::new(EntityId::new());
        second.apply(SimpleTestEvent::Created { value: 2 }).unwrap();

        let result = repo.save_all(&mut [&mut first, &mut second]).await;

        assert!(result.is_err());
        assert_eq!(
            first.pending_events().len(),
            1,
            "the whole batch failed: the first aggregate's own append \
             succeeded but the batch as a whole must not be treated as saved"
        );
        assert_eq!(
            second.pending_events().len(),
            1,
            "a batch conflict must not clear an aggregate that was never persisted"
        );
    }

    #[tokio::test]
    async fn save_all_clears_pending_events_once_the_batch_succeeds() {
        let store = Arc::new(FailingBatchStore {
            succeed_first: usize::MAX,
            calls: std::sync::atomic::AtomicUsize::new(0),
        });
        let repo = EventSourcedRepository::<FailingBatchStore, SimpleTestEntity>::new(store);

        let mut first = AggregateRoot::<SimpleTestEntity>::new(EntityId::new());
        first.apply(SimpleTestEvent::Created { value: 1 }).unwrap();
        let mut second = AggregateRoot::<SimpleTestEntity>::new(EntityId::new());
        second.apply(SimpleTestEvent::Created { value: 2 }).unwrap();

        repo.save_all(&mut [&mut first, &mut second]).await.unwrap();

        assert!(first.pending_events().is_empty());
        assert!(second.pending_events().is_empty());
    }
}

#[cfg(all(
    test,
    feature = "commands",
    any(feature = "event-sourcing", feature = "state-store")
))]
mod execute_tests {
    use super::*;
    use crate::test_fixtures::{
        SimpleTestEntity, SimpleTestError, SimpleTestEvent, SimpleTestInit,
    };
    use crate::CommandError;

    fn update_to(
        value: i32,
    ) -> impl Fn(
        Root<SimpleTestEntity>,
        &(),
    ) -> std::result::Result<Root<SimpleTestEntity>, CommandError<SimpleTestError>> {
        move |root, _ctx| {
            let mut active = root.active()?;
            active.apply(SimpleTestEvent::Updated { value })?;
            Ok(active.into())
        }
    }

    fn create_with(
        value: i32,
    ) -> impl Fn(
        Root<SimpleTestEntity>,
        &(),
    ) -> std::result::Result<Root<SimpleTestEntity>, CommandError<SimpleTestError>> {
        move |root, _ctx| {
            let uninit = root.missing()?;
            Ok(uninit.apply_init(SimpleTestInit { value })?.into())
        }
    }

    #[cfg(feature = "event-sourcing")]
    fn delete() -> impl Fn(
        Root<SimpleTestEntity>,
        &(),
    )
        -> std::result::Result<Root<SimpleTestEntity>, CommandError<SimpleTestError>> {
        |root, _ctx| {
            let active = root.active()?;
            Ok(active
                .apply_delete(crate::test_fixtures::SimpleTestDelete)?
                .into())
        }
    }

    #[cfg(feature = "event-sourcing")]
    fn reject() -> impl Fn(
        Root<SimpleTestEntity>,
        &(),
    )
        -> std::result::Result<Root<SimpleTestEntity>, CommandError<SimpleTestError>> {
        |root, _ctx| {
            let mut active = root.active()?;
            active.apply(SimpleTestEvent::Rejected)?;
            Ok(active.into())
        }
    }

    #[cfg(feature = "event-sourcing")]
    fn swap_identity() -> impl Fn(
        Root<SimpleTestEntity>,
        &(),
    ) -> std::result::Result<
        Root<SimpleTestEntity>,
        CommandError<SimpleTestError>,
    > {
        |root, _ctx| {
            let _active = root.active()?;
            Ok(Root::Active(AggregateRoot::<SimpleTestEntity>::new(
                EntityId::new(),
            )))
        }
    }

    #[cfg(feature = "event-sourcing")]
    mod event_sourced {
        use super::*;
        use crate::event_store::EventStore;
        use crate::test_fixtures::MockEventStore;
        use crate::{AggregateClaim, EventEnvelope, EventLogEntry, Position};

        /// Wraps a [`MockEventStore`], failing `append`'s first `failures`
        /// calls with a concurrency conflict regardless of the version
        /// actually stored, before delegating to the inner store.
        struct FailNTimesEventStore {
            inner: Arc<MockEventStore>,
            remaining_failures: std::sync::Mutex<usize>,
        }

        impl FailNTimesEventStore {
            fn new(inner: Arc<MockEventStore>, failures: usize) -> Self {
                Self {
                    inner,
                    remaining_failures: std::sync::Mutex::new(failures),
                }
            }
        }

        #[async_trait]
        impl EventStore for FailNTimesEventStore {
            async fn append(
                &self,
                stream_id: StreamId,
                events: Vec<EventEnvelope>,
                expected_version: AggregateVersion,
                claims: Vec<AggregateClaim>,
                clear_claims: bool,
            ) -> Result<()> {
                let should_fail = {
                    let mut remaining = self.remaining_failures.lock().unwrap();
                    if *remaining > 0 {
                        *remaining -= 1;
                        true
                    } else {
                        false
                    }
                };
                if should_fail {
                    return Err(Error::concurrency_conflict(
                        expected_version,
                        AggregateVersion::new(expected_version.as_i64() + 1),
                    ));
                }
                self.inner
                    .append(stream_id, events, expected_version, claims, clear_claims)
                    .await
            }

            async fn load_stream(
                &self,
                stream_id: StreamId,
                from_version: AggregateVersion,
            ) -> Result<impl futures::Stream<Item = Result<EventEnvelope>> + Send> {
                self.inner.load_stream(stream_id, from_version).await
            }

            async fn stream_all(
                &self,
                from_position: Position,
            ) -> Result<impl futures::Stream<Item = Result<EventLogEntry>> + Send> {
                self.inner.stream_all(from_position).await
            }

            async fn get_version(&self, stream_id: StreamId) -> Result<AggregateVersion> {
                self.inner.get_version(stream_id).await
            }
        }

        #[tokio::test]
        async fn succeeds_on_the_first_attempt() {
            let store = Arc::new(MockEventStore::new());
            let repo = store.repository::<SimpleTestEntity>();
            let id = EntityId::new();
            let mut seed = AggregateRoot::<SimpleTestEntity>::new(id);
            seed.apply(SimpleTestEvent::Created { value: 1 }).unwrap();
            repo.save(&mut seed).await.unwrap();

            let executed = repo
                .execute(id, &update_to(42), &(), RetryPolicy::default())
                .await
                .unwrap();

            assert_eq!(executed.attempts.get(), 1);
            assert_eq!(executed.root.active().unwrap().value, 42);
            assert_eq!(repo.load(id).await.unwrap().value, 42);
        }

        #[tokio::test]
        async fn creates_from_missing_via_an_init_command() {
            let store = Arc::new(MockEventStore::new());
            let repo = store.repository::<SimpleTestEntity>();
            let id = EntityId::new();

            let executed = repo
                .execute(id, &create_with(7), &(), RetryPolicy::default())
                .await
                .unwrap();

            assert_eq!(executed.attempts.get(), 1);
            assert_eq!(executed.root.active().unwrap().value, 7);
            assert_eq!(repo.load(id).await.unwrap().value, 7);
        }

        #[tokio::test]
        async fn deletes_an_active_aggregate() {
            let store = Arc::new(MockEventStore::new());
            let repo = store.repository::<SimpleTestEntity>();
            let id = EntityId::new();
            let mut seed = AggregateRoot::<SimpleTestEntity>::new(id);
            seed.apply(SimpleTestEvent::Created { value: 1 }).unwrap();
            repo.save(&mut seed).await.unwrap();

            let executed = repo
                .execute(id, &delete(), &(), RetryPolicy::default())
                .await
                .unwrap();

            assert!(matches!(executed.root, Root::Deleted(_)));
            assert!(repo.load_any(id).await.unwrap().is_deleted());
        }

        #[tokio::test]
        async fn domain_refusal_is_not_retried_and_saves_nothing() {
            let store = Arc::new(MockEventStore::new());
            let repo = store.repository::<SimpleTestEntity>();
            let id = EntityId::new();
            let mut seed = AggregateRoot::<SimpleTestEntity>::new(id);
            seed.apply(SimpleTestEvent::Created { value: 1 }).unwrap();
            repo.save(&mut seed).await.unwrap();

            let err = repo
                .execute(id, &reject(), &(), RetryPolicy::default())
                .await
                .unwrap_err();

            assert!(matches!(err, ExecuteError::Domain(SimpleTestError)));
            assert_eq!(repo.load(id).await.unwrap().value, 1);
        }

        #[tokio::test]
        async fn lifecycle_refusal_is_reported() {
            let store = Arc::new(MockEventStore::new());
            let repo = store.repository::<SimpleTestEntity>();
            let id = EntityId::new();
            let mut seed = AggregateRoot::<SimpleTestEntity>::new(id);
            seed.apply(SimpleTestEvent::Created { value: 1 }).unwrap();
            repo.save(&mut seed).await.unwrap();

            // Entity is active; `create_with` requires it to be missing.
            let err = repo
                .execute(id, &create_with(1), &(), RetryPolicy::default())
                .await
                .unwrap_err();

            assert!(matches!(
                err,
                ExecuteError::Lifecycle {
                    expected: crate::Lifecycle::Missing,
                    found: crate::Lifecycle::Active,
                }
            ));
        }

        #[tokio::test(start_paused = true)]
        async fn retries_once_after_a_concurrency_conflict_then_succeeds() {
            let inner = Arc::new(MockEventStore::new());
            let setup = inner.repository::<SimpleTestEntity>();
            let id = EntityId::new();
            let mut seed = AggregateRoot::<SimpleTestEntity>::new(id);
            seed.apply(SimpleTestEvent::Created { value: 1 }).unwrap();
            setup.save(&mut seed).await.unwrap();

            let flaky = Arc::new(FailNTimesEventStore::new(Arc::clone(&inner), 1));
            let repo = flaky.repository::<SimpleTestEntity>();

            let executed = repo
                .execute(id, &update_to(99), &(), RetryPolicy::default())
                .await
                .unwrap();

            assert_eq!(executed.attempts.get(), 2);
            assert_eq!(executed.root.active().unwrap().value, 99);
        }

        #[tokio::test(start_paused = true)]
        async fn conflicts_exhausted_returns_save_error_after_max_attempts() {
            let inner = Arc::new(MockEventStore::new());
            let setup = inner.repository::<SimpleTestEntity>();
            let id = EntityId::new();
            let mut seed = AggregateRoot::<SimpleTestEntity>::new(id);
            seed.apply(SimpleTestEvent::Created { value: 1 }).unwrap();
            setup.save(&mut seed).await.unwrap();

            let flaky = Arc::new(FailNTimesEventStore::new(Arc::clone(&inner), usize::MAX));
            let repo = flaky.repository::<SimpleTestEntity>();
            let retry =
                RetryPolicy::default().with_max_attempts(std::num::NonZeroU32::new(3).unwrap());

            let err = repo
                .execute(id, &update_to(99), &(), retry)
                .await
                .unwrap_err();

            assert!(err.is_concurrency_conflict());
            assert!(matches!(err, ExecuteError::Save(_)));
        }

        #[tokio::test]
        async fn non_concurrency_save_error_is_not_retried() {
            let store = Arc::new(MockEventStore::new());
            let repo = store.repository::<SimpleTestEntity>();
            let id = EntityId::new();
            let mut seed = AggregateRoot::<SimpleTestEntity>::new(id);
            seed.apply(SimpleTestEvent::Created { value: 1 }).unwrap();
            repo.save(&mut seed).await.unwrap();
            // Poison the aggregate so the next save fails for a reason other
            // than a concurrency conflict.
            let mut loaded = repo.load(id).await.unwrap();
            assert!(loaded.apply(SimpleTestEvent::Rejected).is_err());
            assert!(loaded.is_poisoned());

            // Craft a command that hands back this already-poisoned root.
            let poisoned = std::sync::Mutex::new(Some(loaded));
            let command = move |_root: Root<SimpleTestEntity>, _ctx: &()| {
                Ok(Root::Active(poisoned.lock().unwrap().take().unwrap()))
            };

            let err = repo
                .execute(id, &command, &(), RetryPolicy::default())
                .await
                .unwrap_err();

            assert!(!err.is_concurrency_conflict());
            assert!(matches!(err, ExecuteError::Save(_)));
        }

        #[tokio::test]
        async fn load_error_other_than_not_found_is_reported() {
            let store = Arc::new(MockEventStore::new());
            let id = EntityId::new();
            store.add_event(EventEnvelope::new(
                uuid::Uuid::new_v4(),
                id.as_uuid(),
                "SimpleTestEntity".to_string(),
                "SimpleTestEntity.Updated".to_string(),
                crate::EventVersion::new(1),
                serde_json::json!({ "totally": "unrecognized" }),
            ));
            let repo = store.repository::<SimpleTestEntity>();

            let err = repo
                .execute(id, &update_to(1), &(), RetryPolicy::default())
                .await
                .unwrap_err();

            assert!(matches!(err, ExecuteError::Load(e) if !e.is_not_found()));
        }

        #[tokio::test]
        async fn id_mismatch_is_reported_as_a_save_error() {
            let store = Arc::new(MockEventStore::new());
            let repo = store.repository::<SimpleTestEntity>();
            let id = EntityId::new();
            let mut seed = AggregateRoot::<SimpleTestEntity>::new(id);
            seed.apply(SimpleTestEvent::Created { value: 1 }).unwrap();
            repo.save(&mut seed).await.unwrap();

            let err = repo
                .execute(id, &swap_identity(), &(), RetryPolicy::default())
                .await
                .unwrap_err();

            assert!(matches!(err, ExecuteError::Save(e) if e.is_invalid_state()));
        }
    }

    #[cfg(feature = "state-store")]
    mod state_stored {
        use super::*;
        use crate::test_fixtures::MockStateStore;
        use crate::{StateCommit, StateStore, StreamId};
        use std::sync::Arc;

        /// Wraps a [`MockStateStore`], failing `save`'s first `failures`
        /// calls with a concurrency conflict before delegating to the inner
        /// store.
        struct FailNTimesStateStore {
            inner: Arc<MockStateStore>,
            remaining_failures: std::sync::Mutex<usize>,
        }

        impl FailNTimesStateStore {
            fn new(inner: Arc<MockStateStore>, failures: usize) -> Self {
                Self {
                    inner,
                    remaining_failures: std::sync::Mutex::new(failures),
                }
            }
        }

        #[async_trait]
        impl StateStore for FailNTimesStateStore {
            async fn load(&self, stream_id: StreamId) -> Result<Option<crate::StoredState>> {
                self.inner.load(stream_id).await
            }

            async fn save(&self, commit: StateCommit) -> Result<()> {
                let should_fail = {
                    let mut remaining = self.remaining_failures.lock().unwrap();
                    if *remaining > 0 {
                        *remaining -= 1;
                        true
                    } else {
                        false
                    }
                };
                if should_fail {
                    return Err(Error::concurrency_conflict(
                        commit.expected_version,
                        commit.expected_version,
                    ));
                }
                self.inner.save(commit).await
            }
        }

        #[tokio::test]
        async fn succeeds_on_the_first_attempt() {
            let store = Arc::new(MockStateStore::new());
            let repo = store.repository::<SimpleTestEntity>();
            let id = EntityId::new();
            let mut seed = repo.create_with_id(id);
            seed.apply(SimpleTestEvent::Created { value: 1 }).unwrap();
            repo.save(&mut seed).await.unwrap();

            let executed = repo
                .execute(id, &update_to(42), &(), RetryPolicy::default())
                .await
                .unwrap();

            assert_eq!(executed.attempts.get(), 1);
            assert_eq!(repo.load(id).await.unwrap().value, 42);
        }

        #[tokio::test(start_paused = true)]
        async fn retries_once_after_a_concurrency_conflict_then_succeeds() {
            let inner = Arc::new(MockStateStore::new());
            let setup = inner.repository::<SimpleTestEntity>();
            let id = EntityId::new();
            let mut seed = setup.create_with_id(id);
            seed.apply(SimpleTestEvent::Created { value: 1 }).unwrap();
            setup.save(&mut seed).await.unwrap();

            let flaky = Arc::new(FailNTimesStateStore::new(Arc::clone(&inner), 1));
            let repo = flaky.repository::<SimpleTestEntity>();

            let executed = repo
                .execute(id, &update_to(99), &(), RetryPolicy::default())
                .await
                .unwrap();

            assert_eq!(executed.attempts.get(), 2);
        }

        #[tokio::test(start_paused = true)]
        async fn conflicts_exhausted_returns_save_error_after_max_attempts() {
            let inner = Arc::new(MockStateStore::new());
            let setup = inner.repository::<SimpleTestEntity>();
            let id = EntityId::new();
            let mut seed = setup.create_with_id(id);
            seed.apply(SimpleTestEvent::Created { value: 1 }).unwrap();
            setup.save(&mut seed).await.unwrap();

            let flaky = Arc::new(FailNTimesStateStore::new(Arc::clone(&inner), usize::MAX));
            let repo = flaky.repository::<SimpleTestEntity>();
            let retry =
                RetryPolicy::default().with_max_attempts(std::num::NonZeroU32::new(3).unwrap());

            let err = repo
                .execute(id, &update_to(99), &(), retry)
                .await
                .unwrap_err();

            assert!(err.is_concurrency_conflict());
        }

        #[tokio::test]
        async fn creates_from_missing_via_an_init_command() {
            let store = Arc::new(MockStateStore::new());
            let repo = store.repository::<SimpleTestEntity>();
            let id = EntityId::new();

            let executed = repo
                .execute(id, &create_with(5), &(), RetryPolicy::default())
                .await
                .unwrap();

            assert_eq!(executed.root.active().unwrap().value, 5);
        }
    }
}
