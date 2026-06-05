//! # Multi-Aggregate Atomic Writes - `Repository::save_all`
//!
//! A single `repo.save()` commits one aggregate. The canonical use case that
//! spans **two** aggregates is a money transfer: debit account A, credit
//! account B. Both changes must land together — leaving A debited while B's
//! credit is lost would lose money.
//!
//! `Repository::save_all` persists several aggregates through a single
//! `EventStore::append_batch`. On the PostgreSQL backend that is one transaction
//! (all-or-nothing); the in-memory backend used here applies them per-stream, so
//! this example demonstrates the API shape and the happy path. See
//! `docs/event-store.md` ("Multi-Aggregate Atomic Writes") for the consistency
//! guarantees per backend.
//!
//! Run with:
//! ```bash
//! cargo run --example save-all
//! ```

use std::sync::Arc;

use event_sauce_core::{
    command_handler, define_events, Aggregate, AggregateError, Entity, EntityId, EventStore,
};
use event_sauce_memory::InMemoryEventStore;
use serde::{Deserialize, Serialize};

#[derive(Debug, thiserror::Error)]
enum AccountError {
    #[error("insufficient funds: balance {balance}, requested {amount}")]
    InsufficientFunds { balance: i64, amount: i64 },
}

impl AggregateError for AccountError {}

#[derive(Debug, Serialize, Deserialize)]
struct Account {
    id: EntityId,
    holder: String,
    balance: i64,
}

impl Entity for Account {
    fn entity_id(&self) -> EntityId {
        self.id
    }
}

impl Aggregate for Account {
    type Event = AccountEvent;
    type Error = AccountError;
    type DeletedState = Self;
}

define_events! {
    enum AccountEvent for Account {
        Opened {
            holder: String,
            opening_balance: i64,
        }
        @init
        => |id, event| {
            Account {
                id,
                holder: event.holder.clone(),
                balance: event.opening_balance,
            }
        },

        Withdrawn {
            amount: i64,
        }
        @validate |account, event| {
            if account.balance < event.amount {
                return Err(AccountError::InsufficientFunds {
                    balance: account.balance,
                    amount: event.amount,
                });
            }
            Ok(())
        }
        => |account, event| {
            account.balance -= event.amount;
        },

        Deposited {
            amount: i64,
        } => |account, event| {
            account.balance += event.amount;
        },
    }
}

command_handler! {
    impl Account {
        @init fn open(holder: String, opening_balance: i64) -> OpenedEvent { holder, opening_balance };
        fn withdraw(amount: i64) -> WithdrawnEvent { amount };
        fn deposit(amount: i64) -> DepositedEvent { amount };
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Event Sauce - Multi-Aggregate Atomic Writes (save_all)\n");

    let store = Arc::new(InMemoryEventStore::new());
    let repo = store.repository::<Account>();

    // Open two accounts and persist both in a single save_all call.
    let mut alice = Account::open("Alice".to_string(), 1_000)?;
    let mut bob = Account::open("Bob".to_string(), 200)?;
    repo.save_all(&mut [&mut alice, &mut bob]).await?;

    let alice_id = alice.entity_id();
    let bob_id = bob.entity_id();

    println!("Opened:");
    println!("  Alice: {}", alice.balance);
    println!("  Bob:   {}\n", bob.balance);

    // === The transfer: debit Alice, credit Bob, saved together ===
    let amount = 300;
    alice.withdraw(amount)?;
    bob.deposit(amount)?;

    // Both aggregates' events are persisted through one append_batch.
    repo.save_all(&mut [&mut alice, &mut bob]).await?;
    println!("Transferred {amount} from Alice to Bob via save_all.\n");

    // Reload both to prove the writes landed.
    let alice = repo.load(alice_id).await?;
    let bob = repo.load(bob_id).await?;
    println!("After transfer:");
    println!("  Alice: {}", alice.balance);
    println!("  Bob:   {}\n", bob.balance);

    assert_eq!(alice.balance, 700);
    assert_eq!(bob.balance, 500);

    // A domain rule rejected before any write keeps both aggregates clean:
    // the withdraw never produces an event, so nothing is buffered for save_all.
    let mut alice = repo.load(alice_id).await?;
    let mut bob = repo.load(bob_id).await?;
    match alice.withdraw(10_000) {
        Ok(()) => println!("ERROR: overdraw should have been rejected"),
        Err(e) => println!("Overdraw rejected up front: {e}"),
    }
    // Only Bob has a pending event; save_all skips the untouched Alice.
    bob.deposit(50)?;
    repo.save_all(&mut [&mut alice, &mut bob]).await?;

    println!("\nDemo complete.");
    Ok(())
}
