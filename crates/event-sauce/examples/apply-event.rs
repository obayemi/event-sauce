//! # Apply Event Example - Manual Event Implementation with ApplyEvent Trait
//!
//! This example demonstrates the **manual approach** to event definition using:
//! - Individual event structs that implement `ApplyEvent` trait
//! - `#[derive(Event)]` on an event enum to auto-generate boilerplate
//! - Manual validation logic in each event's `ApplyEvent` implementation
//! - Repository pattern for type-safe aggregate persistence
//!
//! **Compare this with the `postgres-quickstart.rs` example** which uses the
//! `define_events!` macro for a more declarative, concise approach.
//!
//! ## When to use the manual approach:
//!
//! - Complex validation requiring multiple steps or external data
//! - Events shared across multiple aggregates
//! - Fine-grained control over event application logic
//! - Custom serialization or event transformation needs
//!
//! ## When to use `define_events!` macro:
//!
//! - Simple validation logic (most common case)
//! - Standard event patterns
//! - Prefer less boilerplate (60-80% less code)
//! - Events are specific to one aggregate
//!
//! Run with:
//! ```bash
//! cargo run --example apply-event
//! ```

use std::sync::Arc;

use chrono::{DateTime, Utc};
use event_sauce_core::{
    Aggregate, AggregateRoot, ApplyEvent, DefaultEntity, DomainEvent, Entity, EntityId, EventStore,
};
use event_sauce_macros::AggregateError;
use event_sauce_memory::InMemoryEventStore;
use serde::{Deserialize, Serialize};

// ============================================================================
// Domain Errors
// ============================================================================

/// Bank account domain errors - auto-implements AggregateError trait
#[derive(AggregateError, Debug, thiserror::Error)]
enum BankAccountError {
    #[error("Insufficient funds: balance={balance}, withdrawal={amount}")]
    #[allow(dead_code)]
    InsufficientFunds { balance: i64, amount: i64 },

    #[error("Invalid amount: {0}")]
    InvalidAmount(i64),

    #[error("Account is closed")]
    AccountClosed,

    #[error("Daily withdrawal limit exceeded: limit={limit}, attempted={total}")]
    DailyLimitExceeded { limit: i64, total: i64 },

    #[error("Overdraft limit exceeded: overdraft={overdraft}, attempted={balance}")]
    OverdraftExceeded { overdraft: i64, balance: i64 },
}

// ============================================================================
// Account Status
// ============================================================================

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
enum AccountStatus {
    #[default]
    Active,
    Closed,
}

// ============================================================================
// Individual Event Structs (Manual Implementation)
// ============================================================================

/// Event: Account was opened
#[derive(Debug, Clone, Serialize, Deserialize)]
struct AccountOpenedEvent {
    holder_name: String,
    initial_balance: i64,
    overdraft_limit: i64,
    timestamp: DateTime<Utc>,
}

impl ApplyEvent<BankAccount> for AccountOpenedEvent {
    fn validate(&self, _account: &BankAccount) -> Result<(), BankAccountError> {
        if self.initial_balance < 0 {
            return Err(BankAccountError::InvalidAmount(self.initial_balance));
        }
        if self.overdraft_limit < 0 {
            return Err(BankAccountError::InvalidAmount(self.overdraft_limit));
        }
        Ok(())
    }

    fn apply(&self, account: &mut BankAccount) {
        account.holder_name = self.holder_name.clone();
        account.balance = self.initial_balance;
        account.overdraft_limit = self.overdraft_limit;
        account.status = AccountStatus::Active;
        account.daily_withdrawal_total = 0;
    }
}

/// Event: Money was deposited
#[derive(Debug, Clone, Serialize, Deserialize)]
struct MoneyDepositedEvent {
    amount: i64,
    #[allow(dead_code)]
    description: String,
    timestamp: DateTime<Utc>,
}

impl ApplyEvent<BankAccount> for MoneyDepositedEvent {
    fn validate(&self, account: &BankAccount) -> Result<(), BankAccountError> {
        if account.status == AccountStatus::Closed {
            return Err(BankAccountError::AccountClosed);
        }
        if self.amount <= 0 {
            return Err(BankAccountError::InvalidAmount(self.amount));
        }
        Ok(())
    }

    fn apply(&self, account: &mut BankAccount) {
        account.balance += self.amount;
    }
}

/// Event: Money was withdrawn
#[derive(Debug, Clone, Serialize, Deserialize)]
struct MoneyWithdrawnEvent {
    amount: i64,
    #[allow(dead_code)]
    description: String,
    timestamp: DateTime<Utc>,
}

impl ApplyEvent<BankAccount> for MoneyWithdrawnEvent {
    fn validate(&self, account: &BankAccount) -> Result<(), BankAccountError> {
        if account.status == AccountStatus::Closed {
            return Err(BankAccountError::AccountClosed);
        }
        if self.amount <= 0 {
            return Err(BankAccountError::InvalidAmount(self.amount));
        }

        const DAILY_LIMIT: i64 = 100_000; // $1000 in cents
        let new_daily_total = account.daily_withdrawal_total + self.amount;
        if new_daily_total > DAILY_LIMIT {
            return Err(BankAccountError::DailyLimitExceeded {
                limit: DAILY_LIMIT,
                total: new_daily_total,
            });
        }

        let new_balance = account.balance - self.amount;
        let overdraft_threshold = -(account.overdraft_limit);
        if new_balance < overdraft_threshold {
            return Err(BankAccountError::OverdraftExceeded {
                overdraft: account.overdraft_limit,
                balance: new_balance,
            });
        }

        Ok(())
    }

    fn apply(&self, account: &mut BankAccount) {
        account.balance -= self.amount;
        account.daily_withdrawal_total += self.amount;
    }
}

/// Event: Account was closed
#[derive(Debug, Clone, Serialize, Deserialize)]
struct AccountClosedEvent {
    #[allow(dead_code)]
    reason: String,
    timestamp: DateTime<Utc>,
}

impl ApplyEvent<BankAccount> for AccountClosedEvent {
    fn validate(&self, account: &BankAccount) -> Result<(), BankAccountError> {
        if account.status == AccountStatus::Closed {
            return Err(BankAccountError::AccountClosed);
        }
        if account.balance < 0 {
            return Err(BankAccountError::InvalidAmount(account.balance));
        }
        Ok(())
    }

    fn apply(&self, account: &mut BankAccount) {
        account.status = AccountStatus::Closed;
    }
}

// ============================================================================
// Event Enum with derive(Event)
// ============================================================================

/// Event enum that wraps all account events
///
/// The `#[derive(Event)]` macro auto-generates:
/// - `DomainEvent` trait implementation
/// - `event_type()` method returning "BankAccount.Opened", etc.
/// - `event_version()` returning the specified version
/// - `occurred_at()` extracting timestamp from each variant
/// - `Into` implementations for each event struct
/// - `EventApplicator` impl that delegates to `ApplyEvent` trait
#[derive(Debug, Clone, Serialize, Deserialize, event_sauce_macros::Event)]
#[event(version = 1, aggregate = "BankAccount")]
enum BankAccountEvent {
    Opened(AccountOpenedEvent),
    Deposited(MoneyDepositedEvent),
    Withdrawn(MoneyWithdrawnEvent),
    Closed(AccountClosedEvent),
}

// ============================================================================
// Aggregate Definition (new Entity + Aggregate pattern)
// ============================================================================

/// Bank account entity
///
/// Uses the new Entity + Aggregate pattern:
/// - `Entity` provides identity (EntityId) and construction
/// - `Aggregate` associates Event + Error types
/// - `AggregateRoot<BankAccount>` provides infrastructure (version, pending events)
#[derive(Debug, Clone, Serialize, Deserialize)]
struct BankAccount {
    id: EntityId,
    holder_name: String,
    balance: i64,
    overdraft_limit: i64,
    status: AccountStatus,
    daily_withdrawal_total: i64,
}

impl Entity for BankAccount {
    fn new(id: EntityId) -> Self {
        Self {
            id,
            holder_name: String::new(),
            balance: 0,
            overdraft_limit: 0,
            status: AccountStatus::Active,
            daily_withdrawal_total: 0,
        }
    }

    fn entity_id(&self) -> EntityId {
        self.id
    }
}

impl DefaultEntity for BankAccount {}

impl Aggregate for BankAccount {
    type Event = BankAccountEvent;
    type Error = BankAccountError;
}

// ============================================================================
// Command Methods
// ============================================================================

/// Helper to create an opened account wrapped in `AggregateRoot`
fn open_account(
    holder_name: String,
    initial_balance: i64,
    overdraft_limit: i64,
) -> Result<AggregateRoot<BankAccount>, BankAccountError> {
    let mut root = AggregateRoot::<BankAccount>::new(EntityId::new());

    root.apply(AccountOpenedEvent {
        holder_name,
        initial_balance,
        overdraft_limit,
        timestamp: Utc::now(),
    })?;

    Ok(root)
}

// ============================================================================
// Main Demo
// ============================================================================

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Event Sauce - Apply Event Example (Manual Implementation)\n");
    println!("This example demonstrates:");
    println!("  - Manual event structs implementing ApplyEvent trait");
    println!("  - Event enum with #[derive(Event)]");
    println!("  - Complex validation logic in ApplyEvent implementations");
    println!("  - Pre-validation and state transformation separation");
    println!("  - Repository pattern for type-safe persistence\n");

    // Setup in-memory event store using builder pattern
    println!("  Initializing in-memory event store...\n");
    let store = Arc::new(InMemoryEventStore::builder().build());

    // Create repository for type-safe aggregate persistence
    println!("  Creating repository...\n");
    let repo = store.repository::<BankAccount>();

    println!("=== Opening Account ===\n");

    // Open account with $100 initial deposit and $500 overdraft
    let mut account = open_account(
        "Alice Johnson".to_string(),
        10_000, // $100.00
        50_000, // $500.00 overdraft limit
    )?;
    println!("  Account opened for: {}", account.holder_name);
    println!("   Balance: ${:.2}", account.balance as f64 / 100.0);
    println!(
        "   Overdraft limit: ${:.2}",
        account.overdraft_limit as f64 / 100.0
    );
    repo.save(&mut account).await?;

    println!("\n=== Deposits and Withdrawals ===\n");

    // Make some deposits
    account.apply(MoneyDepositedEvent {
        amount: 25_000,
        description: "Salary deposit".to_string(),
        timestamp: Utc::now(),
    })?;
    println!("  Deposited $250.00 (Salary)");
    repo.save(&mut account).await?;

    account.apply(MoneyDepositedEvent {
        amount: 5_000,
        description: "Freelance payment".to_string(),
        timestamp: Utc::now(),
    })?;
    println!("  Deposited $50.00 (Freelance)");
    repo.save(&mut account).await?;

    println!("   Current balance: ${:.2}", account.balance as f64 / 100.0);

    // Make some withdrawals
    account.apply(MoneyWithdrawnEvent {
        amount: 15_000,
        description: "Rent payment".to_string(),
        timestamp: Utc::now(),
    })?;
    println!("\n  Withdrew $150.00 (Rent)");
    repo.save(&mut account).await?;

    account.apply(MoneyWithdrawnEvent {
        amount: 8_000,
        description: "Groceries".to_string(),
        timestamp: Utc::now(),
    })?;
    println!("  Withdrew $80.00 (Groceries)");
    repo.save(&mut account).await?;

    println!("   Current balance: ${:.2}", account.balance as f64 / 100.0);
    println!(
        "   Daily withdrawal total: ${:.2}",
        account.daily_withdrawal_total as f64 / 100.0
    );

    println!("\n=== Testing Overdraft ===\n");

    // Try to withdraw more than balance but within overdraft limit
    account.apply(MoneyWithdrawnEvent {
        amount: 30_000,
        description: "Emergency expense".to_string(),
        timestamp: Utc::now(),
    })?;
    println!("  Withdrew $300.00 (Emergency - using overdraft)");
    println!(
        "   Current balance: ${:.2} (overdraft)",
        account.balance as f64 / 100.0
    );
    repo.save(&mut account).await?;

    println!("\n=== Testing Validation ===\n");

    // Try to exceed overdraft limit
    println!("  Attempting to withdraw $1000.00 (exceeds overdraft)...");
    match account.apply(MoneyWithdrawnEvent {
        amount: 100_000,
        description: "Large expense".to_string(),
        timestamp: Utc::now(),
    }) {
        Ok(()) => println!("   ERROR: Should have failed!"),
        Err(e) => println!("   Validation prevented: {e}"),
    }

    // Try to exceed daily withdrawal limit
    println!("\n  Attempting to withdraw $800.00 (exceeds daily limit)...");
    match account.apply(MoneyWithdrawnEvent {
        amount: 80_000,
        description: "Another large expense".to_string(),
        timestamp: Utc::now(),
    }) {
        Ok(()) => println!("   ERROR: Should have failed!"),
        Err(e) => println!("   Validation prevented: {e}"),
    }

    println!("\n=== Testing Account Closure ===\n");

    // Try to close account with negative balance
    println!("  Attempting to close account with negative balance...");
    match account.apply(AccountClosedEvent {
        reason: "Moving banks".to_string(),
        timestamp: Utc::now(),
    }) {
        Ok(()) => println!("   ERROR: Should have failed!"),
        Err(e) => println!("   Validation prevented: {e}"),
    }

    // Deposit to bring balance positive
    account.apply(MoneyDepositedEvent {
        amount: 50_000,
        description: "Final deposit before closure".to_string(),
        timestamp: Utc::now(),
    })?;
    println!("\n  Deposited $500.00 to clear balance");
    println!("   Current balance: ${:.2}", account.balance as f64 / 100.0);
    repo.save(&mut account).await?;

    // Now close the account
    account.apply(AccountClosedEvent {
        reason: "Moving banks".to_string(),
        timestamp: Utc::now(),
    })?;
    println!("\n  Account closed successfully");
    println!("   Status: {:?}", account.status);
    repo.save(&mut account).await?;

    // Try to deposit after closure
    println!("\n  Attempting to deposit after closure...");
    match account.apply(MoneyDepositedEvent {
        amount: 10_000,
        description: "Late deposit".to_string(),
        timestamp: Utc::now(),
    }) {
        Ok(()) => println!("   ERROR: Should have failed!"),
        Err(e) => println!("   Validation prevented: {e}"),
    }

    println!("\n=== Event Replay ===\n");

    // Load account from repository to verify event sourcing
    let account_id = account.entity_id();
    let replayed_account = repo.load(account_id).await?;

    println!("  Replayed account from repository:");
    println!("   Holder: {}", replayed_account.holder_name);
    println!(
        "   Balance: ${:.2}",
        replayed_account.balance as f64 / 100.0
    );
    println!("   Status: {:?}", replayed_account.status);
    println!("   Version: {}", replayed_account.version());
    println!(
        "   Pending events: {}",
        replayed_account.pending_events().len()
    );

    println!("\n=== Repository Features ===\n");

    // Demonstrate repository features
    println!("Repository API examples:");

    // Check existence
    let account_exists = repo.exists(account_id).await?;
    println!("  Account exists: {account_exists}");

    // Get version
    let account_version = repo.get_version(account_id).await?;
    println!("  Account version: {}", account_version.as_u64());

    // Count events
    let event_count = repo.count_events(account_id).await?;
    println!("  Total events: {event_count}");

    println!("\n  Demo complete!");
    println!("\n  Key Takeaways:");
    println!("   - Each event struct implements ApplyEvent trait");
    println!("   - validate() method contains business logic");
    println!("   - apply() method performs state transformation");
    println!("   - #[derive(Event)] generates boilerplate for the enum");
    println!("   - Repository provides type-safe aggregate persistence");
    println!("   - Provides maximum control and flexibility");

    Ok(())
}
