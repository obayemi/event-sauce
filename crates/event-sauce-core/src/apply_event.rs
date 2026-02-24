//! Apply event trait for event application logic.
//!
//! Provides the `ApplyEvent` trait that allows events to define their own
//! application logic, making events self-contained and reducing boilerplate.

use crate::Aggregate;

#[cfg(test)]
use crate::AggregateError;

/// Trait for applying events to aggregates.
///
/// This trait allows each event type to define how it should be applied
/// to an aggregate, promoting encapsulation and reducing the need for
/// large match statements in aggregate code.
///
/// # Type Parameters
///
/// - `A`: The aggregate type this event applies to (must implement `Aggregate`)
///
/// The error type is automatically derived from `A::Error`.
///
/// # Pattern
///
/// Events should implement this trait to define:
/// 1. How to validate the event can be applied (optional, via `validate`)
/// 2. How to apply the event to update aggregate state (via `apply`)
///
/// The `validate` method has a default implementation that always succeeds,
/// making validation optional. Only implement it when your event needs
/// validation logic.
///
/// # Examples
///
/// ## Simple Event Without Validation
///
/// ```
/// use event_sauce_core::{Aggregate, ApplyEvent, AggregateError, AggregateId, DomainEvent, EventApplicator, Version};
/// use thiserror::Error;
/// use chrono::Utc;
/// use std::fmt;
/// use uuid::Uuid;
///
/// #[derive(Debug, Clone, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
/// struct CounterId(Uuid);
/// impl CounterId {
///     fn new() -> Self { Self(Uuid::new_v4()) }
/// }
/// impl fmt::Display for CounterId {
///     fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
///         write!(f, "Counter-{}", self.0)
///     }
/// }
/// impl AggregateId for CounterId {
///     fn to_uuid(&self) -> Uuid { self.0 }
///     fn from_uuid(uuid: Uuid) -> Self { Self(uuid) }
/// }
///
/// #[derive(Debug, Error)]
/// #[error("Counter error")]
/// enum CounterError {}
/// impl AggregateError for CounterError {}
///
/// #[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
/// struct CounterState { value: i32 }
///
/// #[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
/// enum CounterEvent {
///     Incremented { amount: i32 },
/// }
///
/// impl DomainEvent for CounterEvent {
///     type Aggregate = Counter;
///     fn event_type(&self) -> &'static str { "Incremented" }
///     fn event_version(&self) -> u64 { 1 }
///     fn occurred_at(&self) -> chrono::DateTime<Utc> { Utc::now() }
/// }
///
/// impl EventApplicator<Counter> for CounterEvent {
///     fn dispatch(&self, counter: &mut Counter) -> Result<(), CounterError> {
///         match self {
///             CounterEvent::Incremented { amount } => counter.state.value += amount,
///         }
///         Ok(())
///     }
///     fn dispatch_unchecked(&self, counter: &mut Counter) {
///         match self {
///             CounterEvent::Incremented { amount } => counter.state.value += amount,
///         }
///     }
/// }
///
/// struct Counter {
///     id: CounterId,
///     state: CounterState,
///     version: Version,
///     pending_events: Vec<CounterEvent>,
/// }
///
/// impl Aggregate for Counter {
///     type Event = CounterEvent;
///     type Id = CounterId;
///     type Error = CounterError;
///     type State = CounterState;
///
///     fn new(id: Self::Id) -> Self {
///         Self { id, state: CounterState { value: 0 }, version: Version::initial(), pending_events: Vec::new() }
///     }
///     fn aggregate_id(&self) -> &Self::Id { &self.id }
///     fn version(&self) -> Version { self.version }
///     fn pending_events(&self) -> &[Self::Event] { &self.pending_events }
///     fn clear_pending_events(&mut self) { self.pending_events.clear(); }
///     fn push_pending_event(&mut self, event: Self::Event) { self.pending_events.push(event); }
///     fn increment_version(&mut self) { self.version = self.version.next(); }
///     fn state(&self) -> &Self::State { &self.state }
///     fn from_snapshot(id: Self::Id, version: Version, state: Self::State) -> Self {
///         Self { id, state, version, pending_events: Vec::new() }
///     }
/// }
///
/// struct Incremented {
///     amount: i32,
/// }
///
/// impl ApplyEvent<Counter> for Incremented {
///     fn apply(&self, counter: &mut Counter) {
///         counter.state.value += self.amount;
///     }
/// }
/// ```
///
/// ## Event With Validation
///
/// ```
/// use event_sauce_core::{Aggregate, ApplyEvent, AggregateError, AggregateId, DomainEvent, EventApplicator, Version};
/// use thiserror::Error;
/// use chrono::Utc;
/// use std::fmt;
/// use uuid::Uuid;
///
/// #[derive(Debug, Clone, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
/// struct AccountId(Uuid);
/// impl AccountId {
///     fn new() -> Self { Self(Uuid::new_v4()) }
/// }
/// impl fmt::Display for AccountId {
///     fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
///         write!(f, "Account-{}", self.0)
///     }
/// }
/// impl AggregateId for AccountId {
///     fn to_uuid(&self) -> Uuid { self.0 }
///     fn from_uuid(uuid: Uuid) -> Self { Self(uuid) }
/// }
///
/// #[derive(Debug, Error)]
/// enum BankAccountError {
///     #[error("Insufficient funds")]
///     InsufficientFunds,
///     #[error("Account closed")]
///     AccountClosed,
/// }
/// impl AggregateError for BankAccountError {}
///
/// #[derive(Debug, PartialEq, Clone, serde::Serialize, serde::Deserialize)]
/// enum AccountStatus {
///     Active,
///     Closed,
/// }
///
/// #[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
/// struct BankAccountState {
///     balance: i64,
///     status: AccountStatus,
/// }
///
/// #[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
/// enum BankAccountEvent {
///     Withdrawn { amount: i64 },
/// }
///
/// impl DomainEvent for BankAccountEvent {
///     type Aggregate = BankAccount;
///     fn event_type(&self) -> &'static str { "MoneyWithdrawn" }
///     fn event_version(&self) -> u64 { 1 }
///     fn occurred_at(&self) -> chrono::DateTime<Utc> { Utc::now() }
/// }
///
/// impl EventApplicator<BankAccount> for BankAccountEvent {
///     fn dispatch(&self, account: &mut BankAccount) -> Result<(), BankAccountError> {
///         match self {
///             BankAccountEvent::Withdrawn { amount } => account.state.balance -= amount,
///         }
///         Ok(())
///     }
///     fn dispatch_unchecked(&self, account: &mut BankAccount) {
///         match self {
///             BankAccountEvent::Withdrawn { amount } => account.state.balance -= amount,
///         }
///     }
/// }
///
/// struct BankAccount {
///     id: AccountId,
///     state: BankAccountState,
///     version: Version,
///     pending_events: Vec<BankAccountEvent>,
/// }
///
/// impl Aggregate for BankAccount {
///     type Event = BankAccountEvent;
///     type Id = AccountId;
///     type Error = BankAccountError;
///     type State = BankAccountState;
///
///     fn new(id: Self::Id) -> Self {
///         Self {
///             id,
///             state: BankAccountState { balance: 0, status: AccountStatus::Active },
///             version: Version::initial(),
///             pending_events: Vec::new(),
///         }
///     }
///     fn aggregate_id(&self) -> &Self::Id { &self.id }
///     fn version(&self) -> Version { self.version }
///     fn pending_events(&self) -> &[Self::Event] { &self.pending_events }
///     fn clear_pending_events(&mut self) { self.pending_events.clear(); }
///     fn push_pending_event(&mut self, event: Self::Event) { self.pending_events.push(event); }
///     fn increment_version(&mut self) { self.version = self.version.next(); }
///     fn state(&self) -> &Self::State { &self.state }
///     fn from_snapshot(id: Self::Id, version: Version, state: Self::State) -> Self {
///         Self { id, state, version, pending_events: Vec::new() }
///     }
/// }
///
/// struct MoneyWithdrawn {
///     amount: i64,
/// }
///
/// impl ApplyEvent<BankAccount> for MoneyWithdrawn {
///     fn validate(&self, account: &BankAccount) -> Result<(), <BankAccount as Aggregate>::Error> {
///         if account.state.status == AccountStatus::Closed {
///             return Err(BankAccountError::AccountClosed);
///         }
///         if account.state.balance < self.amount {
///             return Err(BankAccountError::InsufficientFunds);
///         }
///         Ok(())
///     }
///
///     fn apply(&self, account: &mut BankAccount) {
///         account.state.balance -= self.amount;
///     }
/// }
/// ```
///
/// ## Usage in Aggregates
///
/// ```
/// use event_sauce_core::{Aggregate, ApplyEvent, AggregateError, AggregateId, DomainEvent, EventApplicator, Version};
/// use thiserror::Error;
/// use chrono::Utc;
/// use std::fmt;
/// use uuid::Uuid;
///
/// #[derive(Debug, Clone, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
/// struct CounterId(Uuid);
/// impl CounterId {
///     fn new() -> Self { Self(Uuid::new_v4()) }
/// }
/// impl fmt::Display for CounterId {
///     fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
///         write!(f, "Counter-{}", self.0)
///     }
/// }
/// impl AggregateId for CounterId {
///     fn to_uuid(&self) -> Uuid { self.0 }
///     fn from_uuid(uuid: Uuid) -> Self { Self(uuid) }
/// }
///
/// #[derive(Debug, Error)]
/// #[error("Counter error")]
/// enum CounterError {}
/// impl AggregateError for CounterError {}
///
/// #[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
/// struct CounterState { value: i32 }
///
/// #[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
/// enum CounterEvent {
///     Incremented { amount: i32 },
/// }
///
/// impl DomainEvent for CounterEvent {
///     type Aggregate = Counter;
///     fn event_type(&self) -> &'static str { "Incremented" }
///     fn event_version(&self) -> u64 { 1 }
///     fn occurred_at(&self) -> chrono::DateTime<Utc> { Utc::now() }
/// }
///
/// impl EventApplicator<Counter> for CounterEvent {
///     fn dispatch(&self, counter: &mut Counter) -> Result<(), CounterError> {
///         match self {
///             CounterEvent::Incremented { amount } => counter.state.value += amount,
///         }
///         Ok(())
///     }
///     fn dispatch_unchecked(&self, counter: &mut Counter) {
///         match self {
///             CounterEvent::Incremented { amount } => counter.state.value += amount,
///         }
///     }
/// }
///
/// struct Counter {
///     id: CounterId,
///     state: CounterState,
///     version: Version,
///     pending_events: Vec<CounterEvent>,
/// }
///
/// impl Aggregate for Counter {
///     type Event = CounterEvent;
///     type Id = CounterId;
///     type Error = CounterError;
///     type State = CounterState;
///
///     fn new(id: Self::Id) -> Self {
///         Self { id, state: CounterState { value: 0 }, version: Version::initial(), pending_events: Vec::new() }
///     }
///     fn aggregate_id(&self) -> &Self::Id { &self.id }
///     fn version(&self) -> Version { self.version }
///     fn pending_events(&self) -> &[Self::Event] { &self.pending_events }
///     fn clear_pending_events(&mut self) { self.pending_events.clear(); }
///     fn push_pending_event(&mut self, event: Self::Event) { self.pending_events.push(event); }
///     fn increment_version(&mut self) { self.version = self.version.next(); }
///     fn state(&self) -> &Self::State { &self.state }
///     fn from_snapshot(id: Self::Id, version: Version, state: Self::State) -> Self {
///         Self { id, state, version, pending_events: Vec::new() }
///     }
/// }
///
/// struct Incremented {
///     amount: i32,
/// }
///
/// impl ApplyEvent<Counter> for Incremented {
///     fn apply(&self, counter: &mut Counter) {
///         counter.state.value += self.amount;
///     }
/// }
///
/// impl Counter {
///     fn increment(&mut self, amount: i32) -> Result<(), CounterError> {
///         let event = Incremented { amount };
///
///         // Validate before applying
///         event.validate(self)?;
///
///         // Apply the event
///         event.apply(self);
///
///         Ok(())
///     }
/// }
/// ```
pub trait ApplyEvent<A: Aggregate> {
    /// Validates that the event can be applied to the aggregate.
    ///
    /// This method checks business rules and invariants without modifying
    /// the aggregate's state. If validation fails, it returns an error.
    ///
    /// The default implementation always succeeds, making validation optional.
    /// Only override this method when your event requires validation.
    ///
    /// # Arguments
    ///
    /// * `aggregate` - Immutable reference to the aggregate to validate against
    ///
    /// # Returns
    ///
    /// * `Ok(())` if the event can be applied
    /// * `Err(E)` if validation fails
    ///
    /// # Errors
    ///
    /// Returns an error of type `A::Error` if the event fails validation based on
    /// the aggregate's current state or business rules.
    ///
    /// # Examples
    ///
    /// ```
    /// use event_sauce_core::{Aggregate, ApplyEvent, AggregateError, AggregateId, DomainEvent, EventApplicator, Version};
    /// use thiserror::Error;
    /// use chrono::Utc;
    /// use std::fmt;
    /// use uuid::Uuid;
    ///
    /// #[derive(Debug, Clone, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
    /// struct CounterId(Uuid);
    /// impl CounterId {
    ///     fn new() -> Self { Self(Uuid::new_v4()) }
    /// }
    /// impl fmt::Display for CounterId {
    ///     fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    ///         write!(f, "Counter-{}", self.0)
    ///     }
    /// }
    /// impl AggregateId for CounterId {
    ///     fn to_uuid(&self) -> Uuid { self.0 }
    ///     fn from_uuid(uuid: Uuid) -> Self { Self(uuid) }
    /// }
    ///
    /// #[derive(Debug, Error)]
    /// enum CounterError {
    ///     #[error("Invalid amount: {0}")]
    ///     InvalidAmount(i32),
    /// }
    /// impl AggregateError for CounterError {}
    ///
    /// #[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
    /// struct CounterState { value: i32 }
    ///
    /// #[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
    /// enum CounterEvent {
    ///     Incremented { amount: i32 },
    /// }
    ///
    /// impl DomainEvent for CounterEvent {
    ///     type Aggregate = Counter;
    ///     fn event_type(&self) -> &'static str { "Incremented" }
    ///     fn event_version(&self) -> u64 { 1 }
    ///     fn occurred_at(&self) -> chrono::DateTime<Utc> { Utc::now() }
    /// }
    ///
    /// impl EventApplicator<Counter> for CounterEvent {
    ///     fn dispatch(&self, counter: &mut Counter) -> Result<(), CounterError> {
    ///         match self {
    ///             CounterEvent::Incremented { amount } => counter.state.value += amount,
    ///         }
    ///         Ok(())
    ///     }
    ///     fn dispatch_unchecked(&self, counter: &mut Counter) {
    ///         match self {
    ///             CounterEvent::Incremented { amount } => counter.state.value += amount,
    ///         }
    ///     }
    /// }
    ///
    /// struct Counter {
    ///     id: CounterId,
    ///     state: CounterState,
    ///     version: Version,
    ///     pending_events: Vec<CounterEvent>,
    /// }
    ///
    /// impl Aggregate for Counter {
    ///     type Event = CounterEvent;
    ///     type Id = CounterId;
    ///     type Error = CounterError;
    ///     type State = CounterState;
    ///
    ///     fn new(id: Self::Id) -> Self {
    ///         Self { id, state: CounterState { value: 0 }, version: Version::initial(), pending_events: Vec::new() }
    ///     }
    ///     fn aggregate_id(&self) -> &Self::Id { &self.id }
    ///     fn version(&self) -> Version { self.version }
    ///     fn pending_events(&self) -> &[Self::Event] { &self.pending_events }
    ///     fn clear_pending_events(&mut self) { self.pending_events.clear(); }
    ///     fn push_pending_event(&mut self, event: Self::Event) { self.pending_events.push(event); }
    ///     fn increment_version(&mut self) { self.version = self.version.next(); }
    ///     fn state(&self) -> &Self::State { &self.state }
    ///     fn from_snapshot(id: Self::Id, version: Version, state: Self::State) -> Self {
    ///         Self { id, state, version, pending_events: Vec::new() }
    ///     }
    /// }
    ///
    /// struct Incremented {
    ///     amount: i32,
    /// }
    ///
    /// impl ApplyEvent<Counter> for Incremented {
    ///     fn validate(&self, _counter: &Counter) -> Result<(), <Counter as Aggregate>::Error> {
    ///         if self.amount <= 0 {
    ///             return Err(CounterError::InvalidAmount(self.amount));
    ///         }
    ///         Ok(())
    ///     }
    ///
    ///     fn apply(&self, counter: &mut Counter) {
    ///         counter.state.value += self.amount;
    ///     }
    /// }
    /// ```
    fn validate(&self, _aggregate: &A) -> Result<(), A::Error> {
        Ok(())
    }

    /// Applies the event to the aggregate, updating its state.
    ///
    /// This method performs the actual state transformation. It assumes
    /// that validation has already been performed (or is not needed).
    ///
    /// This method should be pure and deterministic - given the same
    /// aggregate state and event, it should always produce the same result.
    ///
    /// # Arguments
    ///
    /// * `aggregate` - Mutable reference to the aggregate to update
    ///
    /// # Examples
    ///
    /// ```
    /// use event_sauce_core::{Aggregate, ApplyEvent, AggregateError, AggregateId, DomainEvent, EventApplicator, Version};
    /// use thiserror::Error;
    /// use chrono::Utc;
    /// use std::fmt;
    /// use uuid::Uuid;
    ///
    /// #[derive(Debug, Clone, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
    /// struct CounterId(Uuid);
    /// impl CounterId {
    ///     fn new() -> Self { Self(Uuid::new_v4()) }
    /// }
    /// impl fmt::Display for CounterId {
    ///     fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    ///         write!(f, "Counter-{}", self.0)
    ///     }
    /// }
    /// impl AggregateId for CounterId {
    ///     fn to_uuid(&self) -> Uuid { self.0 }
    ///     fn from_uuid(uuid: Uuid) -> Self { Self(uuid) }
    /// }
    ///
    /// #[derive(Debug, Error)]
    /// #[error("Counter error")]
    /// enum CounterError {}
    /// impl AggregateError for CounterError {}
    ///
    /// #[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
    /// struct CounterState { value: i32 }
    ///
    /// #[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
    /// enum CounterEvent {
    ///     Incremented { amount: i32 },
    /// }
    ///
    /// impl DomainEvent for CounterEvent {
    ///     type Aggregate = Counter;
    ///     fn event_type(&self) -> &'static str { "Incremented" }
    ///     fn event_version(&self) -> u64 { 1 }
    ///     fn occurred_at(&self) -> chrono::DateTime<Utc> { Utc::now() }
    /// }
    ///
    /// impl EventApplicator<Counter> for CounterEvent {
    ///     fn dispatch(&self, counter: &mut Counter) -> Result<(), CounterError> {
    ///         match self {
    ///             CounterEvent::Incremented { amount } => counter.state.value += amount,
    ///         }
    ///         Ok(())
    ///     }
    ///     fn dispatch_unchecked(&self, counter: &mut Counter) {
    ///         match self {
    ///             CounterEvent::Incremented { amount } => counter.state.value += amount,
    ///         }
    ///     }
    /// }
    ///
    /// struct Counter {
    ///     id: CounterId,
    ///     state: CounterState,
    ///     version: Version,
    ///     pending_events: Vec<CounterEvent>,
    /// }
    ///
    /// impl Aggregate for Counter {
    ///     type Event = CounterEvent;
    ///     type Id = CounterId;
    ///     type Error = CounterError;
    ///     type State = CounterState;
    ///
    ///     fn new(id: Self::Id) -> Self {
    ///         Self { id, state: CounterState { value: 0 }, version: Version::initial(), pending_events: Vec::new() }
    ///     }
    ///     fn aggregate_id(&self) -> &Self::Id { &self.id }
    ///     fn version(&self) -> Version { self.version }
    ///     fn pending_events(&self) -> &[Self::Event] { &self.pending_events }
    ///     fn clear_pending_events(&mut self) { self.pending_events.clear(); }
    ///     fn push_pending_event(&mut self, event: Self::Event) { self.pending_events.push(event); }
    ///     fn increment_version(&mut self) { self.version = self.version.next(); }
    ///     fn state(&self) -> &Self::State { &self.state }
    ///     fn from_snapshot(id: Self::Id, version: Version, state: Self::State) -> Self {
    ///         Self { id, state, version, pending_events: Vec::new() }
    ///     }
    /// }
    ///
    /// struct Incremented {
    ///     amount: i32,
    /// }
    ///
    /// impl ApplyEvent<Counter> for Incremented {
    ///     fn apply(&self, counter: &mut Counter) {
    ///         counter.state.value += self.amount;
    ///     }
    /// }
    ///
    /// let mut counter = Counter::new(CounterId::new());
    /// let event = Incremented { amount: 5 };
    /// event.apply(&mut counter);
    /// assert_eq!(counter.state.value, 5);
    /// ```
    fn apply(&self, aggregate: &mut A);

    /// Validates the aggregate state after the event has been applied.
    ///
    /// This method allows checking business invariants after state changes.
    /// Unlike `validate()` which checks pre-conditions, `post_validate()`
    /// checks post-conditions and invariants.
    ///
    /// The default implementation always succeeds, making post-validation optional.
    /// Only override this method when your event requires post-application validation.
    ///
    /// # Arguments
    ///
    /// * `aggregate` - Immutable reference to the aggregate after the event was applied
    ///
    /// # Returns
    ///
    /// * `Ok(())` if the post-conditions are satisfied
    /// * `Err(E)` if validation fails
    ///
    /// # Errors
    ///
    /// Returns an error of type `A::Error` if the aggregate state violates
    /// business invariants after applying the event.
    ///
    /// # Examples
    ///
    /// ```
    /// use event_sauce_core::{Aggregate, ApplyEvent, AggregateError, AggregateId, DomainEvent, EventApplicator, Version};
    /// use thiserror::Error;
    /// use chrono::Utc;
    /// use std::fmt;
    /// use uuid::Uuid;
    ///
    /// #[derive(Debug, Clone, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
    /// struct AccountId(Uuid);
    /// impl AccountId {
    ///     fn new() -> Self { Self(Uuid::new_v4()) }
    /// }
    /// impl fmt::Display for AccountId {
    ///     fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    ///         write!(f, "Account-{}", self.0)
    ///     }
    /// }
    /// impl AggregateId for AccountId {
    ///     fn to_uuid(&self) -> Uuid { self.0 }
    ///     fn from_uuid(uuid: Uuid) -> Self { Self(uuid) }
    /// }
    ///
    /// #[derive(Debug, Error)]
    /// enum BankAccountError {
    ///     #[error("Balance cannot be negative: {0}")]
    ///     NegativeBalance(i64),
    /// }
    /// impl AggregateError for BankAccountError {}
    ///
    /// #[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
    /// struct BankAccountState {
    ///     balance: i64,
    /// }
    ///
    /// #[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
    /// enum BankAccountEvent {
    ///     Withdrawn { amount: i64 },
    /// }
    ///
    /// impl DomainEvent for BankAccountEvent {
    ///     type Aggregate = BankAccount;
    ///     fn event_type(&self) -> &'static str { "MoneyWithdrawn" }
    ///     fn event_version(&self) -> u64 { 1 }
    ///     fn occurred_at(&self) -> chrono::DateTime<Utc> { Utc::now() }
    /// }
    ///
    /// impl EventApplicator<BankAccount> for BankAccountEvent {
    ///     fn dispatch(&self, account: &mut BankAccount) -> Result<(), BankAccountError> {
    ///         match self {
    ///             BankAccountEvent::Withdrawn { amount } => account.state.balance -= amount,
    ///         }
    ///         Ok(())
    ///     }
    ///     fn dispatch_unchecked(&self, account: &mut BankAccount) {
    ///         match self {
    ///             BankAccountEvent::Withdrawn { amount } => account.state.balance -= amount,
    ///         }
    ///     }
    /// }
    ///
    /// struct BankAccount {
    ///     id: AccountId,
    ///     state: BankAccountState,
    ///     version: Version,
    ///     pending_events: Vec<BankAccountEvent>,
    /// }
    ///
    /// impl Aggregate for BankAccount {
    ///     type Event = BankAccountEvent;
    ///     type Id = AccountId;
    ///     type Error = BankAccountError;
    ///     type State = BankAccountState;
    ///
    ///     fn new(id: Self::Id) -> Self {
    ///         Self {
    ///             id,
    ///             state: BankAccountState { balance: 0 },
    ///             version: Version::initial(),
    ///             pending_events: Vec::new(),
    ///         }
    ///     }
    ///     fn aggregate_id(&self) -> &Self::Id { &self.id }
    ///     fn version(&self) -> Version { self.version }
    ///     fn pending_events(&self) -> &[Self::Event] { &self.pending_events }
    ///     fn clear_pending_events(&mut self) { self.pending_events.clear(); }
    ///     fn push_pending_event(&mut self, event: Self::Event) { self.pending_events.push(event); }
    ///     fn increment_version(&mut self) { self.version = self.version.next(); }
    ///     fn state(&self) -> &Self::State { &self.state }
    ///     fn from_snapshot(id: Self::Id, version: Version, state: Self::State) -> Self {
    ///         Self { id, state, version, pending_events: Vec::new() }
    ///     }
    /// }
    ///
    /// struct MoneyWithdrawn {
    ///     amount: i64,
    /// }
    ///
    /// impl ApplyEvent<BankAccount> for MoneyWithdrawn {
    ///     fn apply(&self, account: &mut BankAccount) {
    ///         account.state.balance -= self.amount;
    ///     }
    ///
    ///     fn post_validate(&self, account: &BankAccount) -> Result<(), <BankAccount as Aggregate>::Error> {
    ///         if account.state.balance < 0 {
    ///             return Err(BankAccountError::NegativeBalance(account.state.balance));
    ///         }
    ///         Ok(())
    ///     }
    /// }
    /// ```
    fn post_validate(&self, _aggregate: &A) -> Result<(), A::Error> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{AggregateId, DomainEvent, Version};
    use chrono::Utc;
    use thiserror::Error;

    #[derive(Debug, Error)]
    #[error("Test error: {0}")]
    struct TestError(String);

    impl AggregateError for TestError {}

    #[derive(Debug, PartialEq, Clone, serde::Serialize, serde::Deserialize)]
    enum Status {
        Active,
        Inactive,
    }

    #[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
    struct TestState {
        value: i32,
        status: Status,
    }

    #[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
    enum TestEvent {
        Simple { amount: i32 },
        Validated { amount: i32 },
        PostValidated { amount: i32, max_value: i32 },
    }

    impl DomainEvent for TestEvent {
        type Aggregate = TestAggregate;

        fn event_type(&self) -> &'static str {
            match self {
                TestEvent::Simple { .. } => "Simple",
                TestEvent::Validated { .. } => "Validated",
                TestEvent::PostValidated { .. } => "PostValidated",
            }
        }

        fn event_version(&self) -> u64 {
            1
        }

        fn occurred_at(&self) -> chrono::DateTime<Utc> {
            Utc::now()
        }
    }

    struct TestAggregate {
        id: crate::DefaultAggregateId,
        state: TestState,
        version: Version,
        pending_events: Vec<TestEvent>,
    }

    impl crate::EventApplicator<TestAggregate> for TestEvent {
        fn dispatch(&self, aggregate: &mut TestAggregate) -> Result<(), TestError> {
            match self {
                TestEvent::Simple { amount }
                | TestEvent::Validated { amount }
                | TestEvent::PostValidated { amount, .. } => {
                    aggregate.state.value += amount;
                }
            }
            Ok(())
        }

        fn dispatch_unchecked(&self, aggregate: &mut TestAggregate) {
            match self {
                TestEvent::Simple { amount }
                | TestEvent::Validated { amount }
                | TestEvent::PostValidated { amount, .. } => {
                    aggregate.state.value += amount;
                }
            }
        }
    }

    impl crate::Aggregate for TestAggregate {
        type Id = crate::DefaultAggregateId;
        type Event = TestEvent;
        type Error = TestError;
        type State = TestState;

        fn new(id: Self::Id) -> Self {
            Self {
                id,
                state: TestState {
                    value: 0,
                    status: Status::Active,
                },
                version: Version::initial(),
                pending_events: Vec::new(),
            }
        }

        fn aggregate_id(&self) -> &Self::Id {
            &self.id
        }

        fn version(&self) -> Version {
            self.version
        }

        fn pending_events(&self) -> &[Self::Event] {
            &self.pending_events
        }

        fn clear_pending_events(&mut self) {
            self.pending_events.clear();
        }

        fn push_pending_event(&mut self, event: Self::Event) {
            self.pending_events.push(event);
        }

        fn increment_version(&mut self) {
            self.version = self.version.next();
        }

        fn state(&self) -> &Self::State {
            &self.state
        }

        fn from_snapshot(id: Self::Id, version: Version, state: Self::State) -> Self {
            Self {
                id,
                state,
                version,
                pending_events: Vec::new(),
            }
        }
    }

    struct SimpleEvent {
        amount: i32,
    }

    impl ApplyEvent<TestAggregate> for SimpleEvent {
        fn apply(&self, aggregate: &mut TestAggregate) {
            aggregate.state.value += self.amount;
        }
    }

    struct ValidatedEvent {
        amount: i32,
    }

    impl ApplyEvent<TestAggregate> for ValidatedEvent {
        fn validate(
            &self,
            aggregate: &TestAggregate,
        ) -> Result<(), <TestAggregate as crate::Aggregate>::Error> {
            if aggregate.state.status == Status::Inactive {
                return Err(TestError("Aggregate is inactive".to_string()));
            }
            if self.amount < 0 {
                return Err(TestError("Amount cannot be negative".to_string()));
            }
            Ok(())
        }

        fn apply(&self, aggregate: &mut TestAggregate) {
            aggregate.state.value += self.amount;
        }
    }

    #[test]
    fn test_apply_event_without_validation() {
        let mut aggregate = TestAggregate::new(crate::DefaultAggregateId::new());
        aggregate.state.value = 10;
        aggregate.state.status = Status::Active;

        let event = SimpleEvent { amount: 5 };

        event.apply(&mut aggregate);

        assert_eq!(aggregate.state.value, 15);
    }

    #[test]
    fn test_apply_event_default_validation_succeeds() {
        let mut aggregate = TestAggregate::new(crate::DefaultAggregateId::new());
        aggregate.state.value = 10;
        aggregate.state.status = Status::Active;

        let event = SimpleEvent { amount: 5 };

        let result = event.validate(&aggregate);

        assert!(result.is_ok());
    }

    #[test]
    fn test_apply_event_with_successful_validation() {
        let mut aggregate = TestAggregate::new(crate::DefaultAggregateId::new());
        aggregate.state.value = 10;
        aggregate.state.status = Status::Active;

        let event = ValidatedEvent { amount: 5 };

        assert!(event.validate(&aggregate).is_ok());

        event.apply(&mut aggregate);

        assert_eq!(aggregate.state.value, 15);
    }

    #[test]
    fn test_apply_event_validation_fails_on_inactive_status() {
        let mut aggregate = TestAggregate::new(crate::DefaultAggregateId::new());
        aggregate.state.value = 10;
        aggregate.state.status = Status::Inactive;

        let event = ValidatedEvent { amount: 5 };

        let result = event.validate(&aggregate);

        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err().to_string(),
            "Test error: Aggregate is inactive"
        );
    }

    #[test]
    fn test_apply_event_validation_fails_on_negative_amount() {
        let mut aggregate = TestAggregate::new(crate::DefaultAggregateId::new());
        aggregate.state.value = 10;
        aggregate.state.status = Status::Active;

        let event = ValidatedEvent { amount: -5 };

        let result = event.validate(&aggregate);

        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err().to_string(),
            "Test error: Amount cannot be negative"
        );
    }

    #[test]
    fn test_apply_event_apply_without_validation() {
        // Apply should work even if validation would fail
        // (for event replay scenarios)
        let mut aggregate = TestAggregate::new(crate::DefaultAggregateId::new());
        aggregate.state.value = 10;
        aggregate.state.status = Status::Inactive;

        let event = ValidatedEvent { amount: 5 };

        // Validation would fail
        assert!(event.validate(&aggregate).is_err());

        // But apply still works (for replay)
        event.apply(&mut aggregate);

        assert_eq!(aggregate.state.value, 15);
    }

    #[test]
    fn test_apply_event_multiple_applications() {
        let mut aggregate = TestAggregate::new(crate::DefaultAggregateId::new());
        aggregate.state.value = 0;
        aggregate.state.status = Status::Active;

        let event1 = SimpleEvent { amount: 5 };
        let event2 = SimpleEvent { amount: 10 };
        let event3 = SimpleEvent { amount: 3 };

        event1.apply(&mut aggregate);
        event2.apply(&mut aggregate);
        event3.apply(&mut aggregate);

        assert_eq!(aggregate.state.value, 18);
    }

    #[test]
    fn test_apply_event_is_deterministic() {
        let event = SimpleEvent { amount: 5 };

        let mut aggregate1 = TestAggregate::new(crate::DefaultAggregateId::new());
        aggregate1.state.value = 10;
        aggregate1.state.status = Status::Active;
        event.apply(&mut aggregate1);

        let mut aggregate2 = TestAggregate::new(crate::DefaultAggregateId::new());
        aggregate2.state.value = 10;
        aggregate2.state.status = Status::Active;
        event.apply(&mut aggregate2);

        assert_eq!(aggregate1.state.value, aggregate2.state.value);
    }

    // Tests for post_validate
    struct PostValidatedEvent {
        amount: i32,
        max_value: i32,
    }

    impl ApplyEvent<TestAggregate> for PostValidatedEvent {
        fn apply(&self, aggregate: &mut TestAggregate) {
            aggregate.state.value += self.amount;
        }

        fn post_validate(
            &self,
            aggregate: &TestAggregate,
        ) -> Result<(), <TestAggregate as crate::Aggregate>::Error> {
            if aggregate.state.value > self.max_value {
                return Err(TestError(format!(
                    "Value {} exceeds maximum {}",
                    aggregate.state.value, self.max_value
                )));
            }
            Ok(())
        }
    }

    #[test]
    fn test_post_validate_default_succeeds() {
        let mut aggregate = TestAggregate::new(crate::DefaultAggregateId::new());
        aggregate.state.value = 10;
        aggregate.state.status = Status::Active;

        let event = SimpleEvent { amount: 5 };

        let result = event.post_validate(&aggregate);

        assert!(result.is_ok());
    }

    #[test]
    fn test_post_validate_succeeds() {
        let mut aggregate = TestAggregate::new(crate::DefaultAggregateId::new());
        aggregate.state.value = 10;
        aggregate.state.status = Status::Active;

        let event = PostValidatedEvent {
            amount: 5,
            max_value: 20,
        };

        event.apply(&mut aggregate);
        let result = event.post_validate(&aggregate);

        assert!(result.is_ok());
        assert_eq!(aggregate.state.value, 15);
    }

    #[test]
    fn test_post_validate_fails() {
        let mut aggregate = TestAggregate::new(crate::DefaultAggregateId::new());
        aggregate.state.value = 10;
        aggregate.state.status = Status::Active;

        let event = PostValidatedEvent {
            amount: 15,
            max_value: 20,
        };

        event.apply(&mut aggregate);
        let result = event.post_validate(&aggregate);

        assert!(result.is_err());
        assert_eq!(aggregate.state.value, 25); // State was modified
        assert_eq!(
            result.unwrap_err().to_string(),
            "Test error: Value 25 exceeds maximum 20"
        );
    }

    #[test]
    fn test_post_validate_checks_invariants() {
        let mut aggregate = TestAggregate::new(crate::DefaultAggregateId::new());
        aggregate.state.value = 18;
        aggregate.state.status = Status::Active;

        let event = PostValidatedEvent {
            amount: 1,
            max_value: 20,
        };

        event.apply(&mut aggregate);
        assert!(event.post_validate(&aggregate).is_ok());

        // Now push over the limit
        let event2 = PostValidatedEvent {
            amount: 2,
            max_value: 20,
        };
        event2.apply(&mut aggregate);
        assert!(event2.post_validate(&aggregate).is_err());
    }
}
