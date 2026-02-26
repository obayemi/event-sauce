//! Integration tests for the event system with Entity, Aggregate, AggregateRoot, and ApplyEvent.
//!
//! These tests verify:
//! - Aggregate-specific error types
//! - Event validation
//! - Event replay without validation
//! - Business rule enforcement
//! - Type safety throughout the system

use chrono::Utc;
use event_sauce_core::{
    Aggregate, AggregateRoot, AggregateVersion, DomainEvent, Entity, EntityId, EventApplicator,
    EventVersion,
};
use event_sauce_macros::AggregateError;
use serde::{Deserialize, Serialize};
use thiserror::Error;

// ============================================================================
// Test Aggregate: Account
// ============================================================================

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[allow(dead_code)]
enum AccountStatus {
    #[default]
    Active,
    Frozen,
    Closed,
}

#[derive(AggregateError, Debug, Error)]
enum TestAccountError {
    #[error("Insufficient funds: balance={balance}, requested={requested}")]
    InsufficientFunds { balance: i64, requested: i64 },

    #[error("Invalid amount: {0}")]
    InvalidAmount(i64),

    #[error("Account is {0:?}")]
    AccountNotActive(AccountStatus),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
enum TestAccountEvent {
    Opened {
        owner: String,
        initial_balance: i64,
        timestamp: chrono::DateTime<Utc>,
    },
    Deposited {
        amount: i64,
        timestamp: chrono::DateTime<Utc>,
    },
    Withdrawn {
        amount: i64,
        timestamp: chrono::DateTime<Utc>,
    },
    Frozen {
        timestamp: chrono::DateTime<Utc>,
    },
}

impl DomainEvent for TestAccountEvent {
    type Aggregate = TestAccount;

    fn event_type(&self) -> &'static str {
        match self {
            TestAccountEvent::Opened { .. } => "TestAccountOpened",
            TestAccountEvent::Deposited { .. } => "TestAccountDeposited",
            TestAccountEvent::Withdrawn { .. } => "TestAccountWithdrawn",
            TestAccountEvent::Frozen { .. } => "TestAccountFrozen",
        }
    }

    fn event_version(&self) -> EventVersion {
        EventVersion::new(1)
    }

    fn occurred_at(&self) -> chrono::DateTime<Utc> {
        match self {
            TestAccountEvent::Opened { timestamp, .. }
            | TestAccountEvent::Deposited { timestamp, .. }
            | TestAccountEvent::Withdrawn { timestamp, .. }
            | TestAccountEvent::Frozen { timestamp } => *timestamp,
        }
    }
}

impl EventApplicator<TestAccount> for TestAccountEvent {
    fn dispatch(&self, entity: &mut TestAccount) -> Result<(), TestAccountError> {
        match self {
            TestAccountEvent::Opened {
                owner,
                initial_balance,
                ..
            } => {
                entity.owner = owner.clone();
                entity.balance = *initial_balance;
                entity.status = AccountStatus::Active;
            }
            TestAccountEvent::Deposited { amount, .. } => {
                entity.balance += amount;
            }
            TestAccountEvent::Withdrawn { amount, .. } => {
                entity.balance -= amount;
            }
            TestAccountEvent::Frozen { .. } => {
                entity.status = AccountStatus::Frozen;
            }
        }
        Ok(())
    }

    fn dispatch_unchecked(&self, entity: &mut TestAccount) {
        match self {
            TestAccountEvent::Opened {
                owner,
                initial_balance,
                ..
            } => {
                entity.owner = owner.clone();
                entity.balance = *initial_balance;
                entity.status = AccountStatus::Active;
            }
            TestAccountEvent::Deposited { amount, .. } => {
                entity.balance += amount;
            }
            TestAccountEvent::Withdrawn { amount, .. } => {
                entity.balance -= amount;
            }
            TestAccountEvent::Frozen { .. } => {
                entity.status = AccountStatus::Frozen;
            }
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct TestAccount {
    id: EntityId,
    owner: String,
    balance: i64,
    status: AccountStatus,
}

impl Entity for TestAccount {
    fn new(id: EntityId) -> Self {
        Self {
            id,
            owner: String::new(),
            balance: 0,
            status: AccountStatus::Active,
        }
    }

    fn entity_id(&self) -> EntityId {
        self.id
    }
}

impl Aggregate for TestAccount {
    type Event = TestAccountEvent;
    type Error = TestAccountError;
}

/// Opens a new account, returning an AggregateRoot wrapping the entity.
fn open_account(
    id: EntityId,
    owner: String,
    initial_balance: i64,
) -> Result<AggregateRoot<TestAccount>, TestAccountError> {
    if initial_balance < 0 {
        return Err(TestAccountError::InvalidAmount(initial_balance));
    }

    let mut root = AggregateRoot::<TestAccount>::new(id);
    root.apply(TestAccountEvent::Opened {
        owner,
        initial_balance,
        timestamp: Utc::now(),
    })?;

    Ok(root)
}

fn deposit(root: &mut AggregateRoot<TestAccount>, amount: i64) -> Result<(), TestAccountError> {
    if root.status != AccountStatus::Active {
        return Err(TestAccountError::AccountNotActive(root.status));
    }
    if amount <= 0 {
        return Err(TestAccountError::InvalidAmount(amount));
    }
    root.apply(TestAccountEvent::Deposited {
        amount,
        timestamp: Utc::now(),
    })
}

fn withdraw(root: &mut AggregateRoot<TestAccount>, amount: i64) -> Result<(), TestAccountError> {
    if root.status != AccountStatus::Active {
        return Err(TestAccountError::AccountNotActive(root.status));
    }
    if amount <= 0 {
        return Err(TestAccountError::InvalidAmount(amount));
    }
    if root.balance < amount {
        return Err(TestAccountError::InsufficientFunds {
            balance: root.balance,
            requested: amount,
        });
    }
    root.apply(TestAccountEvent::Withdrawn {
        amount,
        timestamp: Utc::now(),
    })
}

fn freeze(root: &mut AggregateRoot<TestAccount>) -> Result<(), TestAccountError> {
    root.apply(TestAccountEvent::Frozen {
        timestamp: Utc::now(),
    })
}

// ============================================================================
// Tests
// ============================================================================

#[test]
fn test_aggregate_error_trait_implementation() {
    let error = TestAccountError::InvalidAmount(-100);

    // Should implement std::error::Error
    let _error_trait: &dyn std::error::Error = &error;

    // Should be Send + Sync
    fn assert_send<T: Send>() {}
    fn assert_sync<T: Sync>() {}
    assert_send::<TestAccountError>();
    assert_sync::<TestAccountError>();
}

#[test]
fn test_aggregate_has_error_type() {
    // Verify the Aggregate trait has the Error associated type
    type ErrorType = <TestAccount as Aggregate>::Error;

    // Create an instance to verify it works
    let _error: ErrorType = TestAccountError::InvalidAmount(0);
}

#[test]
fn test_create_account_with_validation() {
    let id = EntityId::new();
    let account = open_account(id, "Alice".to_string(), 1000).unwrap();

    assert_eq!(account.entity_id(), id);
    assert_eq!(account.owner, "Alice");
    assert_eq!(account.balance, 1000);
    assert_eq!(account.status, AccountStatus::Active);
    assert_eq!(account.version(), AggregateVersion::new(1));
    assert_eq!(account.pending_events().len(), 1);
}

#[test]
fn test_create_account_rejects_negative_balance() {
    let id = EntityId::new();
    let result = open_account(id, "Alice".to_string(), -100);

    assert!(result.is_err());
    match result.unwrap_err() {
        TestAccountError::InvalidAmount(amount) => assert_eq!(amount, -100),
        _ => panic!("Wrong error type"),
    }
}

#[test]
fn test_deposit_validates_positive_amount() {
    let id = EntityId::new();
    let mut account = open_account(id, "Alice".to_string(), 1000).unwrap();

    let result = deposit(&mut account, -50);

    assert!(result.is_err());
    match result.unwrap_err() {
        TestAccountError::InvalidAmount(amount) => assert_eq!(amount, -50),
        _ => panic!("Wrong error type"),
    }
}

#[test]
fn test_deposit_validates_account_status() {
    let id = EntityId::new();
    let mut account = open_account(id, "Alice".to_string(), 1000).unwrap();

    // Freeze the account
    freeze(&mut account).unwrap();
    assert_eq!(account.status, AccountStatus::Frozen);

    // Try to deposit
    let result = deposit(&mut account, 100);

    assert!(result.is_err());
    match result.unwrap_err() {
        TestAccountError::AccountNotActive(status) => assert_eq!(status, AccountStatus::Frozen),
        _ => panic!("Wrong error type"),
    }
}

#[test]
fn test_withdraw_validates_sufficient_funds() {
    let id = EntityId::new();
    let mut account = open_account(id, "Alice".to_string(), 1000).unwrap();

    let result = withdraw(&mut account, 2000);

    assert!(result.is_err());
    match result.unwrap_err() {
        TestAccountError::InsufficientFunds { balance, requested } => {
            assert_eq!(balance, 1000);
            assert_eq!(requested, 2000);
        }
        _ => panic!("Wrong error type"),
    }
}

#[test]
fn test_withdraw_validates_account_status() {
    let id = EntityId::new();
    let mut account = open_account(id, "Alice".to_string(), 1000).unwrap();

    // Freeze the account
    freeze(&mut account).unwrap();

    // Try to withdraw
    let result = withdraw(&mut account, 100);

    assert!(result.is_err());
    match result.unwrap_err() {
        TestAccountError::AccountNotActive(status) => assert_eq!(status, AccountStatus::Frozen),
        _ => panic!("Wrong error type"),
    }
}

#[test]
fn test_successful_transactions_update_state() {
    let id = EntityId::new();
    let mut account = open_account(id, "Alice".to_string(), 1000).unwrap();

    deposit(&mut account, 500).unwrap();
    assert_eq!(account.balance, 1500);
    assert_eq!(account.version(), AggregateVersion::new(2));

    withdraw(&mut account, 300).unwrap();
    assert_eq!(account.balance, 1200);
    assert_eq!(account.version(), AggregateVersion::new(3));

    assert_eq!(account.pending_events().len(), 3);
}

#[test]
fn test_event_replay_with_dispatch_unchecked() {
    let id = EntityId::new();

    // Create events
    let events = vec![
        TestAccountEvent::Opened {
            owner: "Alice".to_string(),
            initial_balance: 1000,
            timestamp: Utc::now(),
        },
        TestAccountEvent::Deposited {
            amount: 500,
            timestamp: Utc::now(),
        },
        TestAccountEvent::Withdrawn {
            amount: 300,
            timestamp: Utc::now(),
        },
    ];

    // Replay events using dispatch_unchecked (no validation)
    let mut entity = TestAccount::new(id);

    for event in &events {
        EventApplicator::dispatch_unchecked(event, &mut entity);
    }

    assert_eq!(entity.owner, "Alice");
    assert_eq!(entity.balance, 1200);
    assert_eq!(entity.status, AccountStatus::Active);
}

#[test]
fn test_apply_vs_apply_unchecked() {
    let id = EntityId::new();
    let mut root1 = open_account(id, "Alice".to_string(), 1000).unwrap();
    root1.clear_pending_events(); // Clear for fair comparison

    let mut entity2 = TestAccount::new(id);
    // Set up entity2 to match root1 state
    EventApplicator::dispatch_unchecked(
        &TestAccountEvent::Opened {
            owner: "Alice".to_string(),
            initial_balance: 1000,
            timestamp: Utc::now(),
        },
        &mut entity2,
    );

    let event = TestAccountEvent::Deposited {
        amount: 500,
        timestamp: Utc::now(),
    };

    // apply on AggregateRoot records the event
    root1.apply(event.clone()).unwrap();
    // dispatch_unchecked on entity doesn't track events
    EventApplicator::dispatch_unchecked(&event, &mut entity2);

    // Both update state the same way
    assert_eq!(root1.balance, entity2.balance);

    // But AggregateRoot tracks pending events
    assert_eq!(root1.pending_events().len(), 1);
}

#[test]
fn test_domain_event_trait_implementation() {
    let event = TestAccountEvent::Deposited {
        amount: 100,
        timestamp: Utc::now(),
    };

    // Should have correct event type
    assert_eq!(event.event_type(), "TestAccountDeposited");

    // Should have correct version
    assert_eq!(event.event_version(), EventVersion::new(1));

    // Should have timestamp
    let _timestamp = event.occurred_at();
}

#[test]
fn test_aggregate_type_safety() {
    let id = EntityId::new();
    let account = open_account(id, "Alice".to_string(), 1000).unwrap();

    // Verify types are correct
    let _id: EntityId = account.entity_id();
    let _version: AggregateVersion = account.version();
    let _events: &[TestAccountEvent] = account.pending_events();
}

#[test]
fn test_business_rules_enforced_consistently() {
    let id = EntityId::new();
    let mut account = open_account(id, "Alice".to_string(), 1000).unwrap();

    // Invalid amount should always fail
    assert!(deposit(&mut account, 0).is_err());
    assert!(deposit(&mut account, -50).is_err());
    assert!(withdraw(&mut account, 0).is_err());
    assert!(withdraw(&mut account, -50).is_err());

    // Insufficient funds should fail
    assert!(withdraw(&mut account, 2000).is_err());

    // After freezing, transactions should fail
    freeze(&mut account).unwrap();
    assert!(deposit(&mut account, 100).is_err());
    assert!(withdraw(&mut account, 100).is_err());
}

#[test]
fn test_version_increments_correctly() {
    let id = EntityId::new();
    let mut account = open_account(id, "Alice".to_string(), 1000).unwrap();

    assert_eq!(account.version(), AggregateVersion::new(1));

    deposit(&mut account, 100).unwrap();
    assert_eq!(account.version(), AggregateVersion::new(2));

    withdraw(&mut account, 50).unwrap();
    assert_eq!(account.version(), AggregateVersion::new(3));

    freeze(&mut account).unwrap();
    assert_eq!(account.version(), AggregateVersion::new(4));
}

#[test]
fn test_pending_events_tracking() {
    let id = EntityId::new();
    let mut account = open_account(id, "Alice".to_string(), 1000).unwrap();

    assert_eq!(account.pending_events().len(), 1);

    deposit(&mut account, 100).unwrap();
    assert_eq!(account.pending_events().len(), 2);

    withdraw(&mut account, 50).unwrap();
    assert_eq!(account.pending_events().len(), 3);

    account.clear_pending_events();
    assert_eq!(account.pending_events().len(), 0);
}
