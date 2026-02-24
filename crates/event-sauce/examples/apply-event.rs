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
//! ✅ Complex validation requiring multiple steps or external data
//! ✅ Events shared across multiple aggregates
//! ✅ Fine-grained control over event application logic
//! ✅ Custom serialization or event transformation needs
//!
//! ## When to use `define_events!` macro:
//!
//! ✅ Simple validation logic (most common case)
//! ✅ Standard event patterns
//! ✅ Prefer less boilerplate (60-80% less code)
//! ✅ Events are specific to one aggregate
//!
//! Run with:
//! ```bash
//! cargo run --example apply-event
//! ```

use std::sync::Arc;

use chrono::{DateTime, Utc};
use event_sauce_core::{Aggregate, ApplyEvent, DomainEvent, Repository};
use event_sauce_macros::{AggregateError, AggregateId};
use event_sauce_memory::InMemoryEventStore;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

// ============================================================================
// Aggregate ID
// ============================================================================

/// Bank account aggregate ID - auto-implements AggregateId trait and Display
#[derive(AggregateId, Debug, Clone, Copy, PartialEq, Eq, Hash, Default, Serialize, Deserialize)]
#[display("Account-{}")]
struct AccountId(Uuid);

impl AccountId {
    fn new() -> Self {
        Self(Uuid::new_v4())
    }
}

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
        // Validate initial deposit is non-negative
        if self.initial_balance < 0 {
            return Err(BankAccountError::InvalidAmount(self.initial_balance));
        }

        // Validate overdraft limit is non-negative
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
    description: String,
    timestamp: DateTime<Utc>,
}

impl ApplyEvent<BankAccount> for MoneyDepositedEvent {
    fn validate(&self, account: &BankAccount) -> Result<(), BankAccountError> {
        // Ensure account is active
        if account.status == AccountStatus::Closed {
            return Err(BankAccountError::AccountClosed);
        }

        // Validate deposit amount is positive
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
    description: String,
    timestamp: DateTime<Utc>,
}

impl ApplyEvent<BankAccount> for MoneyWithdrawnEvent {
    fn validate(&self, account: &BankAccount) -> Result<(), BankAccountError> {
        // Ensure account is active
        if account.status == AccountStatus::Closed {
            return Err(BankAccountError::AccountClosed);
        }

        // Validate withdrawal amount is positive
        if self.amount <= 0 {
            return Err(BankAccountError::InvalidAmount(self.amount));
        }

        // Check daily withdrawal limit (example: $1000/day)
        const DAILY_LIMIT: i64 = 100_000; // $1000 in cents
        let new_daily_total = account.daily_withdrawal_total + self.amount;
        if new_daily_total > DAILY_LIMIT {
            return Err(BankAccountError::DailyLimitExceeded {
                limit: DAILY_LIMIT,
                total: new_daily_total,
            });
        }

        // Check if withdrawal would exceed overdraft limit
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
    reason: String,
    timestamp: DateTime<Utc>,
}

impl ApplyEvent<BankAccount> for AccountClosedEvent {
    fn validate(&self, account: &BankAccount) -> Result<(), BankAccountError> {
        // Can't close an already closed account
        if account.status == AccountStatus::Closed {
            return Err(BankAccountError::AccountClosed);
        }

        // Must have zero or positive balance to close
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
/// - DomainEvent trait implementation
/// - event_type() method returning "BankAccount.Opened", "BankAccount.Deposited", etc.
/// - event_version() returning the specified version
/// - occurred_at() extracting timestamp from each variant
/// - Into implementations for each event struct
/// - apply_event() method that delegates to ApplyEvent trait
#[derive(Debug, Clone, Serialize, Deserialize, event_sauce_macros::Event)]
#[event(version = 1, aggregate = "BankAccount")]
enum BankAccountEvent {
    Opened(AccountOpenedEvent),
    Deposited(MoneyDepositedEvent),
    Withdrawn(MoneyWithdrawnEvent),
    Closed(AccountClosedEvent),
}

// ============================================================================
// Aggregate Definition
// ============================================================================

/// Bank account aggregate state
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct BankAccountState {
    holder_name: String,
    balance: i64,
    overdraft_limit: i64,
    status: AccountStatus,
    daily_withdrawal_total: i64,
}

/// Bank account aggregate
///
/// This aggregate uses the MANUAL approach for events:
/// - Each event is a separate struct implementing ApplyEvent
/// - The BankAccountEvent enum wraps all events and derives Event
/// - Validation logic is in each event's ApplyEvent::validate()
/// - State transformation is in each event's ApplyEvent::apply()
#[derive(Debug, Serialize, Deserialize)]
struct BankAccount {
    id: AccountId,
    holder_name: String,
    balance: i64,
    overdraft_limit: i64,
    status: AccountStatus,
    daily_withdrawal_total: i64,
    version: event_sauce_core::Version,
    pending_events: Vec<BankAccountEvent>,
}

impl Aggregate for BankAccount {
    type Event = BankAccountEvent;
    type Id = AccountId;
    type Error = BankAccountError;
    type State = BankAccountState;

    fn new(id: Self::Id) -> Self {
        Self {
            id,
            holder_name: String::new(),
            balance: 0,
            overdraft_limit: 0,
            status: AccountStatus::Active,
            daily_withdrawal_total: 0,
            version: event_sauce_core::Version::initial(),
            pending_events: Vec::new(),
        }
    }

    fn aggregate_id(&self) -> &Self::Id {
        &self.id
    }

    fn version(&self) -> event_sauce_core::Version {
        self.version
    }

    fn pending_events(&self) -> &[Self::Event] {
        &self.pending_events
    }

    fn clear_pending_events(&mut self) {
        self.pending_events.clear();
    }

    fn apply<E: Into<Self::Event>>(&mut self, event: E) -> Result<(), Self::Error> {
        let event = event.into();
        self.apply_internal(&event)?;
        self.pending_events.push(event);
        Ok(())
    }

    fn apply_internal(&mut self, event: &Self::Event) -> Result<(), Self::Error> {
        // The apply_event() method is auto-generated by #[derive(Event)]
        // It delegates to each event's ApplyEvent implementation
        self.apply_event(event)?;
        self.version = self.version.next();
        Ok(())
    }

    fn state(&self) -> &Self::State {
        // For this example, we'll create state on-the-fly
        // In production, you'd want to optimize this
        unimplemented!("State extraction not needed for this example")
    }

    fn from_snapshot(
        id: Self::Id,
        version: event_sauce_core::Version,
        _state: Self::State,
    ) -> Self {
        // Simplified for this example
        let mut account = Self::new(id);
        account.version = version;
        account
    }
}

// ============================================================================
// Command Methods (Manual Implementation)
// ============================================================================

impl BankAccount {
    /// Opens a new bank account
    fn open(
        holder_name: String,
        initial_balance: i64,
        overdraft_limit: i64,
    ) -> Result<Self, BankAccountError> {
        let id = AccountId::new();
        let mut account = Self::new(id);

        let event = AccountOpenedEvent {
            holder_name,
            initial_balance,
            overdraft_limit,
            timestamp: Utc::now(),
        };

        account.apply(event)?;
        Ok(account)
    }

    /// Deposits money into the account
    fn deposit(&mut self, amount: i64, description: String) -> Result<(), BankAccountError> {
        let event = MoneyDepositedEvent {
            amount,
            description,
            timestamp: Utc::now(),
        };

        self.apply(event)
    }

    /// Withdraws money from the account
    fn withdraw(&mut self, amount: i64, description: String) -> Result<(), BankAccountError> {
        let event = MoneyWithdrawnEvent {
            amount,
            description,
            timestamp: Utc::now(),
        };

        self.apply(event)
    }

    /// Closes the account
    fn close(&mut self, reason: String) -> Result<(), BankAccountError> {
        let event = AccountClosedEvent {
            reason,
            timestamp: Utc::now(),
        };

        self.apply(event)
    }
}

// ============================================================================
// Main Demo
// ============================================================================

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🏦 Event Sauce - Apply Event Example (Manual Implementation)\n");
    println!("This example demonstrates:");
    println!("  ✓ Manual event structs implementing ApplyEvent trait");
    println!("  ✓ Event enum with #[derive(Event)]");
    println!("  ✓ Complex validation logic in ApplyEvent implementations");
    println!("  ✓ Pre-validation and state transformation separation");
    println!("  ✓ Repository pattern for type-safe persistence\n");

    // Setup in-memory event store using builder pattern
    println!("🗄️  Initializing in-memory event store...\n");
    let store = Arc::new(InMemoryEventStore::builder().build());

    // Create repository for type-safe aggregate persistence
    println!("🔧 Creating repository...\n");
    let repo = Repository::<InMemoryEventStore, BankAccount>::new(Arc::clone(&store));

    println!("=== Opening Account ===\n");

    // Open account with $100 initial deposit and $500 overdraft
    let mut account = BankAccount::open(
        "Alice Johnson".to_string(),
        10_000, // $100.00
        50_000, // $500.00 overdraft limit
    )?;
    println!("👤 Account opened for: {}", account.holder_name);
    println!("   Balance: ${:.2}", account.balance as f64 / 100.0);
    println!(
        "   Overdraft limit: ${:.2}",
        account.overdraft_limit as f64 / 100.0
    );
    repo.save(&mut account).await?;

    println!("\n=== Deposits and Withdrawals ===\n");

    // Make some deposits
    account.deposit(25_000, "Salary deposit".to_string())?;
    println!("💰 Deposited $250.00 (Salary)");
    repo.save(&mut account).await?;

    account.deposit(5_000, "Freelance payment".to_string())?;
    println!("💰 Deposited $50.00 (Freelance)");
    repo.save(&mut account).await?;

    println!("   Current balance: ${:.2}", account.balance as f64 / 100.0);

    // Make some withdrawals
    account.withdraw(15_000, "Rent payment".to_string())?;
    println!("\n💸 Withdrew $150.00 (Rent)");
    repo.save(&mut account).await?;

    account.withdraw(8_000, "Groceries".to_string())?;
    println!("💸 Withdrew $80.00 (Groceries)");
    repo.save(&mut account).await?;

    println!("   Current balance: ${:.2}", account.balance as f64 / 100.0);
    println!(
        "   Daily withdrawal total: ${:.2}",
        account.daily_withdrawal_total as f64 / 100.0
    );

    println!("\n=== Testing Overdraft ===\n");

    // Try to withdraw more than balance but within overdraft limit
    account.withdraw(30_000, "Emergency expense".to_string())?;
    println!("💸 Withdrew $300.00 (Emergency - using overdraft)");
    println!(
        "   Current balance: ${:.2} (overdraft)",
        account.balance as f64 / 100.0
    );
    repo.save(&mut account).await?;

    println!("\n=== Testing Validation ===\n");

    // Try to exceed overdraft limit
    println!("❌ Attempting to withdraw $1000.00 (exceeds overdraft)...");
    match account.withdraw(100_000, "Large expense".to_string()) {
        Ok(_) => println!("   ERROR: Should have failed!"),
        Err(e) => println!("   ✓ Validation prevented: {e}"),
    }

    // Try to exceed daily withdrawal limit
    println!("\n❌ Attempting to withdraw $800.00 (exceeds daily limit)...");
    match account.withdraw(80_000, "Another large expense".to_string()) {
        Ok(_) => println!("   ERROR: Should have failed!"),
        Err(e) => println!("   ✓ Validation prevented: {e}"),
    }

    println!("\n=== Testing Account Closure ===\n");

    // Try to close account with negative balance
    println!("❌ Attempting to close account with negative balance...");
    match account.close("Moving banks".to_string()) {
        Ok(_) => println!("   ERROR: Should have failed!"),
        Err(e) => println!("   ✓ Validation prevented: {e}"),
    }

    // Deposit to bring balance positive
    account.deposit(50_000, "Final deposit before closure".to_string())?;
    println!("\n💰 Deposited $500.00 to clear balance");
    println!("   Current balance: ${:.2}", account.balance as f64 / 100.0);
    repo.save(&mut account).await?;

    // Now close the account
    account.close("Moving banks".to_string())?;
    println!("\n🔒 Account closed successfully");
    println!("   Status: {:?}", account.status);
    repo.save(&mut account).await?;

    // Try to deposit after closure
    println!("\n❌ Attempting to deposit after closure...");
    match account.deposit(10_000, "Late deposit".to_string()) {
        Ok(_) => println!("   ERROR: Should have failed!"),
        Err(e) => println!("   ✓ Validation prevented: {e}"),
    }

    println!("\n=== Event Replay ===\n");

    // Load account from repository to verify event sourcing
    let account_id = *account.aggregate_id();
    let replayed_account = repo.load(account_id).await?;

    println!("📼 Replayed account from repository:");
    println!("   Holder: {}", replayed_account.holder_name);
    println!(
        "   Balance: ${:.2}",
        replayed_account.balance as f64 / 100.0
    );
    println!("   Status: {:?}", replayed_account.status);
    println!("   Version: {}", replayed_account.version);
    println!(
        "   Pending events: {}",
        replayed_account.pending_events().len()
    );

    println!("\n=== Repository Features ===\n");

    // Demonstrate repository features
    println!("Repository API examples:");

    // Check existence
    let account_exists = repo.exists(account_id).await?;
    println!("  • Account exists: {account_exists}");

    // Get version
    let account_version = repo.get_version(account_id).await?;
    println!("  • Account version: {}", account_version.as_i32());

    // Count events
    let event_count = repo.count_events(account_id).await?;
    println!("  • Total events: {event_count}");

    println!("\n✨ Demo complete!");
    println!("\n💡 Key Takeaways:");
    println!("   • Each event struct implements ApplyEvent trait");
    println!("   • validate() method contains business logic");
    println!("   • apply() method performs state transformation");
    println!("   • #[derive(Event)] generates boilerplate for the enum");
    println!("   • Repository provides type-safe aggregate persistence");
    println!("   • Provides maximum control and flexibility");

    Ok(())
}
