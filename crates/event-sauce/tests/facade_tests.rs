//! The facade is the only dependency a consumer needs.
//!
//! Nothing below names `event_sauce_core`, `event_sauce_macros`,
//! `event_sauce_memory`, or `event_sauce_crypto`: every macro, trait, and
//! backend is reached through `event_sauce`, exactly as a downstream crate
//! that depends on the facade alone would write it. If the derive macros ever
//! go back to emitting paths rooted at a sibling crate, these tests stop
//! compiling.

use std::sync::Arc;

use event_sauce::memory::InMemoryEventStore;
use event_sauce::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, AggregateId)]
#[aggregate_id(Ledger)]
struct LedgerId(EntityId);

#[aggregate_error(aggregate = "Ledger")]
#[derive(Debug, thiserror::Error)]
enum LedgerError {
    #[error("amount must be positive: {0}")]
    NonPositiveAmount(i64),
}

#[specification("Amount must be positive")]
fn positive_credit(event: &CreditedEvent) -> bool {
    event.amount > 0
}

define_events! {
    enum LedgerEvent for Ledger {
        Opened {
            owner: String,
        }
        @init
        => |id, event| {
            Ledger {
                id: id.into(),
                owner: event.owner.clone(),
                balance: 0,
            }
        },

        Credited {
            amount: i64,
        }
        @validate |_ledger, event| {
            PositiveCredit.validate_or(event, |_| LedgerError::NonPositiveAmount(event.amount))?;
            Ok(())
        }
        => |ledger, event| {
            ledger.balance += event.amount;
        },
    }
}

#[aggregate(
    event = "LedgerEvent",
    error = "LedgerError",
    init,
    type_name = "ledger"
)]
#[derive(Debug, Serialize, Deserialize)]
struct Ledger {
    #[id]
    id: LedgerId,
    owner: String,
    balance: i64,
}

command_handler! {
    impl Ledger {
        /// Open a ledger for an owner.
        @init fn open(owner: String) -> OpenedEvent { owner };

        /// Credit the ledger.
        fn credit(amount: i64) -> CreditedEvent { amount };
    }
}

#[specification("Ledger must name an owner")]
fn ledger_has_owner(ledger: &Ledger) -> bool {
    !ledger.owner.is_empty()
}

fn require_owner(ledger: &Ledger) -> std::result::Result<(), LedgerError> {
    LedgerHasOwner.check(ledger)?;
    Ok(())
}

#[tokio::test]
async fn aggregate_round_trips_through_the_facade() {
    let store = Arc::new(InMemoryEventStore::builder().build());
    let repo = store.repository::<Ledger>();

    let mut ledger = Ledger::open("alice".to_string()).unwrap();
    ledger.credit(120).unwrap();
    repo.save(&mut ledger).await.unwrap();

    let loaded = repo.load(ledger.entity_id()).await.unwrap();

    assert_eq!(loaded.owner, "alice");
    assert_eq!(loaded.balance, 120);
}

#[test]
fn a_specification_maps_onto_a_domain_error_variant() {
    let mut ledger = Ledger::open("bob".to_string()).unwrap();

    let error = ledger.credit(-5).unwrap_err();

    assert!(matches!(error, LedgerError::NonPositiveAmount(-5)));
}

#[test]
fn the_injected_specification_variant_absorbs_a_failed_check() {
    let ledger = Ledger::open(String::new()).unwrap();

    let error = require_owner(&ledger).unwrap_err();

    assert!(matches!(error, LedgerError::SpecificationFailed(_)));
}

#[test]
fn aggregate_type_override_reaches_the_core_type() {
    assert_eq!(Ledger::aggregate_type(), AggregateType::new("ledger"));
}

#[test]
fn typed_ids_convert_through_the_facade() {
    let entity_id = EntityId::new();
    let ledger_id = LedgerId::from(entity_id);

    assert_eq!(ledger_id.as_entity_id(), entity_id);
}

#[cfg(feature = "crypto")]
#[test]
fn the_bundled_provider_shares_a_module_with_the_crypto_traits() {
    use event_sauce::crypto::{Aes256GcmProvider, CryptoProvider};

    let key = [7u8; 32];
    let sealed = Aes256GcmProvider.encrypt(&key, b"ledger", &[]).unwrap();

    assert_eq!(
        Aes256GcmProvider.decrypt(&key, &sealed, &[]).unwrap(),
        b"ledger"
    );
}
