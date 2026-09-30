//! Proves that a caller depending only on `event-sauce` and `serde` can use
//! its exported macros by full path, with no direct dependency of its own
//! on `pastey`, `uuid`, `serde_json` or `async-trait`, and reaches `chrono`
//! only through the public [`event_sauce::chrono`] re-export rather than a
//! direct dependency either.
//!
//! If any macro body here resolved `pastey`, `uuid`, `serde_json` or
//! `async-trait` through a bare path instead of `$crate::__private::...`,
//! or recursed into itself by bare name instead of `$crate::...`, this
//! crate would fail to compile because none of those crates are in its
//! `Cargo.toml`. Every expansion arm that only appears under a marker
//! (`policy!`, `@upcast`, `@init`, `@actor(T)`, `@delete`, and their
//! combinations) is instantiated below, not just the plain, marker-less
//! case: a bare crate path hiding in one of those arms would otherwise
//! leave every other test in the workspace passing.

use serde::{Deserialize, Serialize};

/// `event_sauce::chrono`'s instant type, spelled out once so a non-`@clock`
/// command can take it as a parameter without this crate adding `chrono` to
/// its own `Cargo.toml`.
type Instant = event_sauce::chrono::DateTime<event_sauce::chrono::Utc>;

#[derive(Debug)]
struct CounterError;

impl std::fmt::Display for CounterError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "counter error")
    }
}

impl std::error::Error for CounterError {}
impl event_sauce::AggregateError for CounterError {}

#[derive(Debug, Serialize, Deserialize)]
struct Counter {
    id: event_sauce::EntityId,
    value: i32,
}

impl event_sauce::Entity for Counter {
    fn new(id: event_sauce::EntityId) -> Self {
        Self { id, value: 0 }
    }

    fn entity_id(&self) -> event_sauce::EntityId {
        self.id
    }
}

impl event_sauce::DefaultEntity for Counter {}

impl event_sauce::Aggregate for Counter {
    type Event = CounterEvent;
    type Error = CounterError;
    type DeletedState = Self;
}

event_sauce::define_events! {
    enum CounterEvent for Counter {
        Incremented {
            amount: i32,
        } => |counter, event| {
            counter.value += event.amount;
        },
    }
}

event_sauce::command_handler! {
    impl Counter {
        @clock fn increment(amount: i32) -> IncrementedEvent { amount };
    }
}

/// Error shared by every aggregate below — its only job is to exist, so the
/// generated `Result<_, Error>` signatures have a concrete type to name.
#[derive(Debug)]
struct VaultError;

impl std::fmt::Display for VaultError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "vault error")
    }
}

impl std::error::Error for VaultError {}
impl event_sauce::AggregateError for VaultError {}

impl From<event_sauce::SpecificationError<Vault>> for VaultError {
    fn from(_err: event_sauce::SpecificationError<Vault>) -> Self {
        VaultError
    }
}

/// Stands in as the `@actor(T)` parameter for every actor-flavoured
/// event/command below. Its own event is hand-written rather than
/// macro-generated — `@init`, `@actor` and the rest are exercised on
/// `Vault` instead, this type only needs to exist.
#[derive(Debug, Serialize, Deserialize)]
struct Operator {
    id: event_sauce::EntityId,
}

impl event_sauce::Entity for Operator {
    fn new(id: event_sauce::EntityId) -> Self {
        Self { id }
    }

    fn entity_id(&self) -> event_sauce::EntityId {
        self.id
    }
}

impl event_sauce::DefaultEntity for Operator {}

#[derive(Debug, Clone, Serialize, Deserialize)]
enum OperatorEvent {
    Noop,
}

impl event_sauce::DomainEvent for OperatorEvent {
    type Aggregate = Operator;

    fn event_type(&self) -> &'static str {
        "Operator.Noop"
    }

    fn event_version(&self) -> event_sauce::EventVersion {
        event_sauce::EventVersion::new(1)
    }

    fn occurred_at(&self) -> Instant {
        event_sauce::chrono::Utc::now()
    }
}

impl event_sauce::EventApplicator<Operator> for OperatorEvent {
    fn dispatch(&self, _operator: &mut Operator) -> Result<(), VaultError> {
        Ok(())
    }

    fn dispatch_unchecked(&self, _operator: &mut Operator) {}
}

impl event_sauce::Aggregate for Operator {
    type Event = OperatorEvent;
    type Error = VaultError;
    type DeletedState = Self;
}

event_sauce::spec!(
    VaultNotLocked for Vault, "vault must not be locked",
    |vault| { !vault.locked }
);

/// Main aggregate: one `define_events!` enum with an enum-level `@upcast`
/// and every variant shape `command_handler!` supports.
#[derive(Debug, Serialize, Deserialize)]
struct Vault {
    id: event_sauce::EntityId,
    label: String,
    locked: bool,
}

impl event_sauce::Entity for Vault {
    fn entity_id(&self) -> event_sauce::EntityId {
        self.id
    }
}

impl event_sauce::Aggregate for Vault {
    type Event = VaultEvent;
    type Error = VaultError;
    type DeletedState = Self;
}

event_sauce::define_events! {
    enum VaultEvent for Vault {
        @upcast |_event_type, _from_version, _data| {}

        Opened {
            label: String,
        }
        @init
        @version(2)
        @encrypted_fields(label)
        @aliases("Vault.Created")
        => |id, event| {
            Vault { id, label: event.label.clone(), locked: false }
        },

        Provisioned {
            owner: String,
        }
        @init
        @actor(Operator)
        @validate |_actor, _event| {
            return Ok(());
        }
        => |id, event| {
            Vault { id, label: format!("owned-by-{}", event.owner), locked: false }
        },

        Renamed {
            new_label: String,
        }
        @occurred_at(renamed_at)
        @validate_spec(VaultNotLocked)
        @post_validate |_vault, _event| {
            return Ok(());
        }
        => |vault, event| {
            vault.label = event.new_label.clone();
        },

        Locked {}
        @actor(Operator)
        @validate |vault, _actor, _event| {
            if vault.locked {
                return Err(VaultError);
            }
            return Ok(());
        }
        => |vault, _event| {
            vault.locked = true;
        },

        Emptied {
            reason: String,
        }
        @delete
        @validate |vault, _event| {
            if vault.locked {
                return Err(VaultError);
            }
            return Ok(());
        }
        => |vault, _event| {
            vault
        },

        Seized {
            reason: String,
        }
        @delete
        @actor(Operator)
        @validate |_vault, _actor, _event| {
            return Ok(());
        }
        => |vault, _event| {
            vault
        },
    }
}

event_sauce::command_handler! {
    impl Vault {
        @clock @init fn open(label: String) -> OpenedEvent { label };
        @clock @init @actor(Operator) fn provision(owner: String) -> ProvisionedEvent { owner };
        fn rename(renamed_at: Instant, new_label: String) -> RenamedEvent { renamed_at, new_label };
        @clock @actor(Operator) fn lock() -> LockedEvent { };
        @clock @delete fn empty(reason: String) -> EmptiedEvent { reason };
        @clock @delete @actor(Operator) fn seize(reason: String) -> SeizedEvent { reason };
    }
}

event_sauce::policy! {
    VaultLockPolicy {
        on LockedEvent |_event, _ctx| {
            Ok(())
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Aggregate that exists only to type `#[event(aggregate = ...)]` below:
    /// the derive needs a concrete `Aggregate` whose `Event` is exactly that
    /// enum, and nothing else here touches it.
    #[derive(Debug, Serialize, Deserialize)]
    struct Ledger {
        id: event_sauce::EntityId,
    }

    impl event_sauce::Entity for Ledger {
        fn new(id: event_sauce::EntityId) -> Self {
            Self { id }
        }

        fn entity_id(&self) -> event_sauce::EntityId {
            self.id
        }
    }

    impl event_sauce::DefaultEntity for Ledger {}

    /// Exercises `#[derive(event_sauce::Event)]`'s generated `DomainEvent`
    /// impl with no `use event_sauce::DomainEvent` anywhere in this crate:
    /// proves the generated code resolves entirely by full path, the way a
    /// bare `#[derive(Event)]` caller depends on it.
    #[derive(event_sauce::Event, Debug, Clone, Serialize, Deserialize, PartialEq)]
    #[event(version = 1, aggregate = "Ledger")]
    enum CounterAudited {
        Recorded { timestamp: Instant },
    }

    impl event_sauce::EventApplicator<Ledger> for CounterAudited {
        fn dispatch(&self, _ledger: &mut Ledger) -> Result<(), CounterError> {
            Ok(())
        }

        fn dispatch_unchecked(&self, _ledger: &mut Ledger) {}
    }

    impl event_sauce::Aggregate for Ledger {
        type Event = CounterAudited;
        type Error = CounterError;
        type DeletedState = Self;
    }

    fn assert_is_policy<S, P>()
    where
        S: event_sauce::EventStore + 'static,
        P: event_sauce::policy::Policy<S>,
    {
    }

    fn an_operator() -> event_sauce::AggregateRoot<Operator> {
        event_sauce::AggregateRoot::<Operator>::new(event_sauce::EntityId::new())
    }

    #[test]
    fn caller_without_direct_macro_dependencies_compiles_and_runs() {
        let mut counter = event_sauce::AggregateRoot::<Counter>::new(event_sauce::EntityId::new());
        counter.increment(5).unwrap();
        assert_eq!(counter.value, 5);
    }

    #[test]
    fn derived_event_round_trips_with_no_domain_event_import() {
        let timestamp = event_sauce::chrono::Utc::now();
        let event = CounterAudited::Recorded { timestamp };

        let envelope = <CounterAudited as event_sauce::DomainEvent>::to_envelope(
            &event,
            event_sauce::EntityId::new().as_uuid(),
        )
        .unwrap();
        let restored = CounterAudited::try_from(&envelope).unwrap();

        assert_eq!(restored, event);
    }

    #[test]
    fn vault_lock_policy_implements_the_policy_trait() {
        assert_is_policy::<event_sauce::memory::InMemoryEventStore, VaultLockPolicy>();
    }

    #[test]
    fn vault_surface_compiles_and_runs() {
        let operator = an_operator();

        let mut vault = Vault::open("secrets".to_string()).unwrap();
        assert_eq!(vault.label, "secrets");

        let provisioned = Vault::provision(&operator, "root".to_string()).unwrap();
        assert_eq!(provisioned.label, "owned-by-root");

        let now = event_sauce::chrono::Utc::now();
        vault.rename(now, "vault-2".to_string()).unwrap();
        assert_eq!(vault.label, "vault-2");

        vault.lock(&operator).unwrap();
        assert!(vault.locked);

        let deleted = provisioned
            .seize(&operator, "compromised".to_string())
            .unwrap();
        assert_eq!(deleted.label, "owned-by-root");
    }

    #[test]
    fn renaming_a_locked_vault_fails_the_spec() {
        let mut vault = Vault::open("secrets".to_string()).unwrap();
        let operator = an_operator();
        vault.lock(&operator).unwrap();

        let now = event_sauce::chrono::Utc::now();
        assert!(vault.rename(now, "new-name".to_string()).is_err());
    }

    #[test]
    fn emptying_a_locked_vault_fails_validation() {
        let mut vault = Vault::open("secrets".to_string()).unwrap();
        let operator = an_operator();
        vault.lock(&operator).unwrap();
        assert!(vault.empty("no longer needed".to_string()).is_err());
    }
}
