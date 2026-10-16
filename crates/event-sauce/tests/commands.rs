//! System tests driving `Command` and `Repository::execute` through the facade
//! against the in-memory event store.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use event_sauce::memory::InMemoryEventStore;
use event_sauce::{
    aggregate_command, define_events, Aggregate, AggregateError, Command, CommandError, Entity,
    EntityId, EventStore, ExecuteError, Lifecycle, Repository, RetryPolicy, Root,
};
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, thiserror::Error)]
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

struct DepositAfterRival {
    amount: i64,
    store: Arc<InMemoryEventStore>,
    rival_has_written: AtomicBool,
}

impl Command<Account> for DepositAfterRival {
    fn handle(&self, root: Root<Account>, _ctx: &()) -> Outcome {
        let mut account = root.active()?;
        if !self.rival_has_written.swap(true, Ordering::SeqCst) {
            let repo = self.store.repository::<Account>();
            futures::executor::block_on(async {
                let mut rival = repo.load(account.entity_id()).await.unwrap();
                rival.deposit(1_000).unwrap();
                repo.save(&mut rival).await.unwrap();
            });
        }
        account.deposit(self.amount)?;
        Ok(account.into())
    }
}

fn new_repo() -> (
    Arc<InMemoryEventStore>,
    impl Repository<Account> + use<>,
    EntityId,
) {
    let store = Arc::new(InMemoryEventStore::new());
    let repo = store.repository::<Account>();
    (store, repo, EntityId::new())
}

#[tokio::test]
async fn a_command_creates_modifies_and_deletes_an_aggregate() {
    let (_store, repo, id) = new_repo();
    let retry = RetryPolicy::default;

    let created = repo.execute(id, &Open("ada"), &(), retry()).await.unwrap();
    assert_eq!(created.root.lifecycle(), Lifecycle::Active);

    repo.execute(id, &Deposit(40), &(), retry()).await.unwrap();
    assert_eq!(repo.load(id).await.unwrap().balance, 40);

    let closed = repo.execute(id, &Close, &(), retry()).await.unwrap();
    assert_eq!(closed.root.lifecycle(), Lifecycle::Deleted);
    assert!(repo.load_any(id).await.unwrap().is_deleted());
}

#[tokio::test]
async fn a_command_refused_for_its_lifecycle_writes_nothing() {
    let (_store, repo, id) = new_repo();

    let error = repo
        .execute(id, &Deposit(5), &(), RetryPolicy::default())
        .await
        .unwrap_err();

    assert!(matches!(
        error,
        ExecuteError::Lifecycle {
            expected: Lifecycle::Active,
            found: Lifecycle::Missing,
        }
    ));
    assert!(repo.load_any(id).await.unwrap_err().is_not_found());
}

#[tokio::test]
async fn a_domain_refusal_writes_nothing() {
    let (_store, repo, id) = new_repo();
    repo.execute(id, &Open("ada"), &(), RetryPolicy::default())
        .await
        .unwrap();
    let version = repo.get_version(id).await.unwrap();

    let error = repo
        .execute(id, &Deposit(-5), &(), RetryPolicy::default())
        .await
        .unwrap_err();

    assert!(matches!(
        error,
        ExecuteError::Domain(AccountError::NonPositive)
    ));
    assert_eq!(repo.get_version(id).await.unwrap(), version);
    assert_eq!(repo.load(id).await.unwrap().balance, 0);
}

#[tokio::test]
async fn a_concurrent_writer_causes_one_retry_then_success() {
    let (store, repo, id) = new_repo();
    repo.execute(id, &Open("ada"), &(), RetryPolicy::default())
        .await
        .unwrap();
    let command = DepositAfterRival {
        amount: 5,
        store,
        rival_has_written: AtomicBool::new(false),
    };

    let executed = repo
        .execute(id, &command, &(), RetryPolicy::default())
        .await
        .unwrap();

    assert_eq!(executed.attempts.get(), 2);
    assert_eq!(repo.load(id).await.unwrap().balance, 1_005);
}

#[tokio::test]
async fn without_retries_a_concurrent_writer_fails_the_command() {
    let (store, repo, id) = new_repo();
    repo.execute(id, &Open("ada"), &(), RetryPolicy::none())
        .await
        .unwrap();
    let command = DepositAfterRival {
        amount: 5,
        store,
        rival_has_written: AtomicBool::new(false),
    };

    let error = repo
        .execute(id, &command, &(), RetryPolicy::none())
        .await
        .unwrap_err();

    assert!(error.is_concurrency_conflict());
    assert_eq!(repo.load(id).await.unwrap().balance, 1_000);
}
