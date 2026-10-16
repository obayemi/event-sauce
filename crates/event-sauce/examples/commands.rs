//! # Commands Example — Typed, Lifecycle-Aware Command Handling
//!
//! A [`Command`] receives an aggregate wherever it stands in its lifecycle
//! (missing, active or deleted) and returns it wherever the command leaves
//! it. `Repository::execute` loads, runs the command, saves the result and
//! retries the whole cycle when a concurrent writer wins the race.
//!
//! Run with:
//! ```bash
//! cargo run --example commands --features commands
//! ```

use std::sync::Arc;

use event_sauce::memory::InMemoryEventStore;
use event_sauce::{
    aggregate_command, define_events, Aggregate, AggregateError, Command, CommandError, Entity,
    EntityId, EventStore, ExecuteError, Repository, RetryPolicy, Root,
};
use serde::{Deserialize, Serialize};

#[derive(Debug, thiserror::Error)]
enum AccountError {
    #[error("amount must be positive")]
    NonPositive,
}

impl AggregateError for AccountError {}

#[derive(Debug, Serialize, Deserialize)]
struct Account {
    id: EntityId,
    owner: String,
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
            owner: String,
        }
        @init
        => |id, event| {
            Account { id, owner: event.owner.clone(), balance: 0 }
        },

        Deposited {
            amount: i64,
        }
        @validate |_account, event| {
            if event.amount <= 0 {
                return Err(AccountError::NonPositive);
            }
            return Ok(());
        }
        => |account, event| {
            account.balance += event.amount;
        },

        Closed {}
        @delete
        => |account, _event| {
            account
        },
    }
}

aggregate_command! {
    impl Account {
        @clock @init fn open(owner: String) -> OpenedEvent { owner };
        @clock fn deposit(amount: i64) -> DepositedEvent { amount };
        @clock @delete fn close() -> ClosedEvent { };
    }
}

type Outcome = Result<Root<Account>, CommandError<AccountError>>;

struct Open(&'static str);

impl Command<Account> for Open {
    fn handle(&self, root: Root<Account>, _ctx: &()) -> Outcome {
        Ok(root.missing()?.open(self.0.to_owned())?.into())
    }
}

struct Deposit(i64);

impl Command<Account> for Deposit {
    fn handle(&self, root: Root<Account>, _ctx: &()) -> Outcome {
        let mut account = root.active()?;
        account.deposit(self.0)?;
        Ok(account.into())
    }
}

struct Close;

impl Command<Account> for Close {
    fn handle(&self, root: Root<Account>, _ctx: &()) -> Outcome {
        Ok(root.active()?.close()?.into())
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let store = Arc::new(InMemoryEventStore::new());
    let repo = store.repository::<Account>();
    let id = EntityId::new();
    let retry = RetryPolicy::default();

    repo.execute(id, &Open("ada"), &(), retry.clone()).await?;
    repo.execute(id, &Deposit(40), &(), retry.clone()).await?;
    println!("balance: {}", repo.load(id).await?.balance);

    match repo.execute(id, &Deposit(-1), &(), retry.clone()).await {
        Err(ExecuteError::Domain(error)) => println!("refused: {error}"),
        other => anyhow::bail!("expected a domain refusal, got {other:?}"),
    }

    repo.execute(id, &Close, &(), retry.clone()).await?;
    match repo.execute(id, &Deposit(5), &(), retry).await {
        Err(ExecuteError::Lifecycle { expected, found }) => {
            println!("refused: expected {expected}, found {found}");
        }
        other => anyhow::bail!("expected a lifecycle refusal, got {other:?}"),
    }

    Ok(())
}
