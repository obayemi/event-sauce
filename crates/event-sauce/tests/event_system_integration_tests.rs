//! Integration tests for the new event system with AggregateError and ApplyEvent traits.
//!
//! These tests verify:
//! - Aggregate-specific error types
//! - Event validation
//! - Event replay without validation
//! - Business rule enforcement
//! - Type safety throughout the system

use chrono::Utc;
use event_sauce_core::{Aggregate, AggregateId, DomainEvent, EventApplicator, Version};
use event_sauce_macros::AggregateError;
use serde::{Deserialize, Serialize};
use std::fmt;
use thiserror::Error;
use uuid::Uuid;

// ============================================================================
// Test Aggregate: Account
// ============================================================================

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
struct TestAccountId(Uuid);

impl Default for TestAccountId {
    fn default() -> Self {
        Self(Uuid::nil())
    }
}

impl TestAccountId {
    fn new() -> Self {
        Self(Uuid::new_v4())
    }
}

impl fmt::Display for TestAccountId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "TestAccount-{}", self.0)
    }
}

impl AggregateId for TestAccountId {
    fn to_uuid(&self) -> Uuid {
        self.0
    }

    fn from_uuid(uuid: Uuid) -> Self {
        Self(uuid)
    }
}

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

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
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

    fn event_version(&self) -> u64 {
        1
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
    fn dispatch(&self, aggregate: &mut TestAccount) -> Result<(), TestAccountError> {
        match self {
            TestAccountEvent::Opened {
                owner,
                initial_balance,
                ..
            } => {
                aggregate.owner = owner.clone();
                aggregate.balance = *initial_balance;
                aggregate.status = AccountStatus::Active;
            }
            TestAccountEvent::Deposited { amount, .. } => {
                aggregate.balance += amount;
            }
            TestAccountEvent::Withdrawn { amount, .. } => {
                aggregate.balance -= amount;
            }
            TestAccountEvent::Frozen { .. } => {
                aggregate.status = AccountStatus::Frozen;
            }
        }
        Ok(())
    }

    fn dispatch_unchecked(&self, aggregate: &mut TestAccount) {
        match self {
            TestAccountEvent::Opened {
                owner,
                initial_balance,
                ..
            } => {
                aggregate.owner = owner.clone();
                aggregate.balance = *initial_balance;
                aggregate.status = AccountStatus::Active;
            }
            TestAccountEvent::Deposited { amount, .. } => {
                aggregate.balance += amount;
            }
            TestAccountEvent::Withdrawn { amount, .. } => {
                aggregate.balance -= amount;
            }
            TestAccountEvent::Frozen { .. } => {
                aggregate.status = AccountStatus::Frozen;
            }
        }
    }
}

#[event_sauce_macros::aggregate(
    id = "TestAccountId",
    event = "TestAccountEvent",
    error = "TestAccountError"
)]
#[derive(Default)]
struct TestAccount {
    id: TestAccountId,
    owner: String,
    balance: i64,
    status: AccountStatus,
}

impl TestAccount {
    fn open(
        id: TestAccountId,
        owner: String,
        initial_balance: i64,
    ) -> Result<Self, TestAccountError> {
        if initial_balance < 0 {
            return Err(TestAccountError::InvalidAmount(initial_balance));
        }

        let mut account = Self::new(id);

        account
            .apply(TestAccountEvent::Opened {
                owner,
                initial_balance,
                timestamp: Utc::now(),
            })
            .unwrap();

        Ok(account)
    }

    fn deposit(&mut self, amount: i64) -> Result<(), TestAccountError> {
        if self.status != AccountStatus::Active {
            return Err(TestAccountError::AccountNotActive(self.status));
        }

        if amount <= 0 {
            return Err(TestAccountError::InvalidAmount(amount));
        }

        self.apply(TestAccountEvent::Deposited {
            amount,
            timestamp: Utc::now(),
        })
        .unwrap();

        Ok(())
    }

    fn withdraw(&mut self, amount: i64) -> Result<(), TestAccountError> {
        if self.status != AccountStatus::Active {
            return Err(TestAccountError::AccountNotActive(self.status));
        }

        if amount <= 0 {
            return Err(TestAccountError::InvalidAmount(amount));
        }

        if self.balance < amount {
            return Err(TestAccountError::InsufficientFunds {
                balance: self.balance,
                requested: amount,
            });
        }

        self.apply(TestAccountEvent::Withdrawn {
            amount,
            timestamp: Utc::now(),
        })
        .unwrap();

        Ok(())
    }

    fn freeze(&mut self) -> Result<(), TestAccountError> {
        self.apply(TestAccountEvent::Frozen {
            timestamp: Utc::now(),
        })
        .unwrap();

        Ok(())
    }
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
    let id = TestAccountId::new();
    let account = TestAccount::open(id.clone(), "Alice".to_string(), 1000).unwrap();

    assert_eq!(account.aggregate_id(), &id);
    assert_eq!(account.owner, "Alice");
    assert_eq!(account.balance, 1000);
    assert_eq!(account.status, AccountStatus::Active);
    assert_eq!(account.version(), Version::new(1));
    assert_eq!(account.pending_events().len(), 1);
}

#[test]
fn test_create_account_rejects_negative_balance() {
    let id = TestAccountId::new();
    let result = TestAccount::open(id, "Alice".to_string(), -100);

    assert!(result.is_err());
    match result.unwrap_err() {
        TestAccountError::InvalidAmount(amount) => assert_eq!(amount, -100),
        _ => panic!("Wrong error type"),
    }
}

#[test]
fn test_deposit_validates_positive_amount() {
    let id = TestAccountId::new();
    let mut account = TestAccount::open(id, "Alice".to_string(), 1000).unwrap();

    let result = account.deposit(-50);

    assert!(result.is_err());
    match result.unwrap_err() {
        TestAccountError::InvalidAmount(amount) => assert_eq!(amount, -50),
        _ => panic!("Wrong error type"),
    }
}

#[test]
fn test_deposit_validates_account_status() {
    let id = TestAccountId::new();
    let mut account = TestAccount::open(id, "Alice".to_string(), 1000).unwrap();

    // Freeze the account
    account.freeze().unwrap();
    assert_eq!(account.status, AccountStatus::Frozen);

    // Try to deposit
    let result = account.deposit(100);

    assert!(result.is_err());
    match result.unwrap_err() {
        TestAccountError::AccountNotActive(status) => assert_eq!(status, AccountStatus::Frozen),
        _ => panic!("Wrong error type"),
    }
}

#[test]
fn test_withdraw_validates_sufficient_funds() {
    let id = TestAccountId::new();
    let mut account = TestAccount::open(id, "Alice".to_string(), 1000).unwrap();

    let result = account.withdraw(2000);

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
    let id = TestAccountId::new();
    let mut account = TestAccount::open(id, "Alice".to_string(), 1000).unwrap();

    // Freeze the account
    account.freeze().unwrap();

    // Try to withdraw
    let result = account.withdraw(100);

    assert!(result.is_err());
    match result.unwrap_err() {
        TestAccountError::AccountNotActive(status) => assert_eq!(status, AccountStatus::Frozen),
        _ => panic!("Wrong error type"),
    }
}

#[test]
fn test_successful_transactions_update_state() {
    let id = TestAccountId::new();
    let mut account = TestAccount::open(id, "Alice".to_string(), 1000).unwrap();

    account.deposit(500).unwrap();
    assert_eq!(account.balance, 1500);
    assert_eq!(account.version(), Version::new(2));

    account.withdraw(300).unwrap();
    assert_eq!(account.balance, 1200);
    assert_eq!(account.version(), Version::new(3));

    assert_eq!(account.pending_events().len(), 3);
}

#[test]
fn test_event_replay_with_apply_unchecked() {
    let id = TestAccountId::new();

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

    // Replay events using apply_unchecked (no validation)
    let mut account = TestAccount::new(id.clone());

    for event in &events {
        account.apply_unchecked(event);
    }

    assert_eq!(account.owner, "Alice");
    assert_eq!(account.balance, 1200);
    assert_eq!(account.version(), Version::new(3));
    assert_eq!(account.status, AccountStatus::Active);
}

#[test]
fn test_apply_vs_apply_unchecked() {
    let id = TestAccountId::new();
    let mut account1 = TestAccount::open(id.clone(), "Alice".to_string(), 1000).unwrap();
    account1.pending_events.clear(); // Clear for fair comparison

    let mut account2 = TestAccount::open(id, "Alice".to_string(), 1000).unwrap();
    account2.pending_events.clear();

    let event = TestAccountEvent::Deposited {
        amount: 500,
        timestamp: Utc::now(),
    };

    // apply records the event while apply_unchecked doesn't
    account1.apply(event.clone()).unwrap();
    account2.apply_unchecked(&event);

    // Both update state and version the same way
    assert_eq!(account1.balance, account2.balance);
    assert_eq!(account1.version(), account2.version());

    // But apply adds to pending events
    assert_eq!(account1.pending_events().len(), 1);
    assert_eq!(account2.pending_events().len(), 0);
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
    assert_eq!(event.event_version(), 1);

    // Should have timestamp
    let _timestamp = event.occurred_at();
}

#[test]
fn test_aggregate_type_safety() {
    let id = TestAccountId::new();
    let account = TestAccount::open(id, "Alice".to_string(), 1000).unwrap();

    // Verify types are correct
    let _id: &TestAccountId = account.aggregate_id();
    let _version: Version = account.version();
    let _events: &[TestAccountEvent] = account.pending_events();
}

#[test]
fn test_business_rules_enforced_consistently() {
    let id = TestAccountId::new();
    let mut account = TestAccount::open(id, "Alice".to_string(), 1000).unwrap();

    // Invalid amount should always fail
    assert!(account.deposit(0).is_err());
    assert!(account.deposit(-50).is_err());
    assert!(account.withdraw(0).is_err());
    assert!(account.withdraw(-50).is_err());

    // Insufficient funds should fail
    assert!(account.withdraw(2000).is_err());

    // After freezing, transactions should fail
    account.freeze().unwrap();
    assert!(account.deposit(100).is_err());
    assert!(account.withdraw(100).is_err());
}

#[test]
fn test_version_increments_correctly() {
    let id = TestAccountId::new();
    let mut account = TestAccount::open(id, "Alice".to_string(), 1000).unwrap();

    assert_eq!(account.version(), Version::new(1));

    account.deposit(100).unwrap();
    assert_eq!(account.version(), Version::new(2));

    account.withdraw(50).unwrap();
    assert_eq!(account.version(), Version::new(3));

    account.freeze().unwrap();
    assert_eq!(account.version(), Version::new(4));
}

#[test]
fn test_pending_events_tracking() {
    let id = TestAccountId::new();
    let mut account = TestAccount::open(id, "Alice".to_string(), 1000).unwrap();

    assert_eq!(account.pending_events().len(), 1);

    account.deposit(100).unwrap();
    assert_eq!(account.pending_events().len(), 2);

    account.withdraw(50).unwrap();
    assert_eq!(account.pending_events().len(), 3);

    account.clear_pending_events();
    assert_eq!(account.pending_events().len(), 0);
}
