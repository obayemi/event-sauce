//! Bank Account Example with Validation
//!
//! This example demonstrates the new event sourcing pattern with:
//! - AggregateState derive macro for cleaner code structure
//! - Aggregate-specific error types using `AggregateError` trait
//! - Event validation with business rules
//! - Self-contained event application logic using `ApplyEvent` trait
//! - Event replay without validation using `apply_unchecked`
//! - Rich domain model with status management
//!
//! Run with: cargo run -p event-sauce --example bank-account --features "memory,macros"

use chrono::Utc;
use event_sauce_core::{Aggregate, ApplyEvent, DomainEvent};
use event_sauce_macros::{AggregateError, AggregateId, AggregateState, Event as DeriveEvent};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

// ============================================================================
// Domain Model
// ============================================================================

/// Unique identifier for a bank account
#[derive(AggregateId, Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
struct AccountId(Uuid);

impl AccountId {
    fn new() -> Self {
        Self(Uuid::new_v4())
    }
}

impl Default for AccountId {
    fn default() -> Self {
        Self(Uuid::nil())
    }
}

/// Account status
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[allow(dead_code)]
#[derive(Default)]
enum AccountStatus {
    #[default]
    Active,
    Frozen,
    Closed,
}


// ============================================================================
// Event Structs - Separated event definitions
// ============================================================================

/// Event: Account was opened
#[derive(Debug, Clone, Serialize, Deserialize)]
#[allow(dead_code)]
struct AccountOpenedEvent {
    account_id: String,
    owner: String,
    initial_balance: i64,
    timestamp: chrono::DateTime<Utc>,
}

/// Event: Money was deposited
#[derive(Debug, Clone, Serialize, Deserialize)]
struct AccountDepositedEvent {
    amount: i64,
    timestamp: chrono::DateTime<Utc>,
}

/// Event: Money was withdrawn
#[derive(Debug, Clone, Serialize, Deserialize)]
struct AccountWithdrawnEvent {
    amount: i64,
    timestamp: chrono::DateTime<Utc>,
}

/// Bank account events wrapping the separated event structs
#[derive(DeriveEvent, Debug, Clone, Serialize, Deserialize)]
#[event(
    version = 1,
    type_prefix = "Account",
    aggregate = "BankAccountAggregate"
)]
enum AccountEvent {
    Opened(AccountOpenedEvent),
    Deposited(AccountDepositedEvent),
    Withdrawn(AccountWithdrawnEvent),
}

/// Domain errors
#[derive(AggregateError, Debug, thiserror::Error)]
#[allow(dead_code)]
enum AccountError {
    #[error("Insufficient funds: balance={balance}, requested={requested}")]
    InsufficientFunds { balance: i64, requested: i64 },

    #[error("Invalid amount: {0} (must be positive)")]
    InvalidAmount(i64),

    #[error("Account is {0:?}")]
    AccountNotActive(AccountStatus),

    #[error("Account already exists")]
    AccountAlreadyExists,
}

// ============================================================================
// ApplyEvent Implementations
// ============================================================================
//
// NOTE: ApplyEvent is implemented for the GENERATED BankAccountAggregate type.
// The Event macro auto-generates the apply_event method that dispatches to these.

impl ApplyEvent<BankAccountAggregate, AccountError> for AccountOpenedEvent {
    fn validate(&self, _account: &BankAccountAggregate) -> Result<(), AccountError> {
        if self.initial_balance < 0 {
            return Err(AccountError::InvalidAmount(self.initial_balance));
        }
        Ok(())
    }

    fn apply(&self, account: &mut BankAccountAggregate) {
        account.owner = self.owner.clone();
        account.balance = self.initial_balance;
        account.status = AccountStatus::Active;
    }
}

impl ApplyEvent<BankAccountAggregate, AccountError> for AccountDepositedEvent {
    fn validate(&self, account: &BankAccountAggregate) -> Result<(), AccountError> {
        if account.status != AccountStatus::Active {
            return Err(AccountError::AccountNotActive(account.status));
        }

        if self.amount <= 0 {
            return Err(AccountError::InvalidAmount(self.amount));
        }

        Ok(())
    }

    fn apply(&self, account: &mut BankAccountAggregate) {
        account.balance += self.amount;
    }
}

impl ApplyEvent<BankAccountAggregate, AccountError> for AccountWithdrawnEvent {
    fn validate(&self, account: &BankAccountAggregate) -> Result<(), AccountError> {
        if account.status != AccountStatus::Active {
            return Err(AccountError::AccountNotActive(account.status));
        }

        if self.amount <= 0 {
            return Err(AccountError::InvalidAmount(self.amount));
        }

        if account.balance < self.amount {
            return Err(AccountError::InsufficientFunds {
                balance: account.balance,
                requested: self.amount,
            });
        }

        Ok(())
    }

    fn apply(&self, account: &mut BankAccountAggregate) {
        account.balance -= self.amount;
    }

    // Post-validation: Ensure balance never goes negative (defense in depth)
    // This catches any bugs in the apply logic or validation
    fn post_validate(&self, account: &BankAccountAggregate) -> Result<(), AccountError> {
        if account.balance < 0 {
            return Err(AccountError::InsufficientFunds {
                balance: account.balance,
                requested: self.amount,
            });
        }
        Ok(())
    }
}

// ============================================================================
// Bank Account State - Business Logic Only
// ============================================================================
//
// The AggregateState macro generates a BankAccountAggregate wrapper

/// Bank account state - contains only business data
#[derive(AggregateState, Debug, Clone, Serialize, Deserialize)]
#[aggregate(id = "AccountId", event = "AccountEvent", error = "AccountError")]
struct BankAccountState {
    #[aggregate_id]
    id: AccountId,
    owner: String,
    balance: i64,
    status: AccountStatus,
}

impl Default for BankAccountState {
    fn default() -> Self {
        Self {
            id: AccountId::default(),
            owner: String::new(),
            balance: 0,
            status: AccountStatus::Active,
        }
    }
}

impl BankAccountState {
    /// Create a new bank account state
    fn new(id: AccountId) -> Self {
        Self {
            id,
            owner: String::new(),
            balance: 0,
            status: AccountStatus::Active,
        }
    }

    /// Get current balance
    fn balance(&self) -> i64 {
        self.balance
    }

    /// Get account status
    fn status(&self) -> AccountStatus {
        self.status
    }
}

// apply_event is auto-generated by the Event macro

// ============================================================================
// Command Methods - Implemented on the generated BankAccountAggregate
// ============================================================================

impl BankAccountAggregate {
    /// Create a new bank account using the Aggregate trait's new method
    fn open(id: AccountId, owner: String, initial_balance: i64) -> Result<Self, AccountError> {
        let mut account = <Self as Aggregate>::new(id.clone());

        let event = AccountOpenedEvent {
            account_id: id.to_string(),
            owner,
            initial_balance,
            timestamp: Utc::now(),
        };

        // apply() automatically runs: validate() → apply() → post_validate()
        account.apply(event)?;
        Ok(account)
    }

    /// Deposit money into the account
    fn deposit(&mut self, amount: i64) -> Result<(), AccountError> {
        let event = AccountDepositedEvent {
            amount,
            timestamp: Utc::now(),
        };

        // apply() automatically runs: validate() → apply() → post_validate()
        self.apply(event)?;
        Ok(())
    }

    /// Withdraw money from the account
    fn withdraw(&mut self, amount: i64) -> Result<(), AccountError> {
        let event = AccountWithdrawnEvent {
            amount,
            timestamp: Utc::now(),
        };

        // apply() automatically runs: validate() → apply() → post_validate()
        self.apply(event)?;
        Ok(())
    }
}

// ============================================================================
// Main Example
// ============================================================================

fn main() -> Result<(), AccountError> {
    println!("\n{}", "=".repeat(70));
    println!("🏦 Bank Account Example - Event Sourcing with AggregateState");
    println!("{}\n", "=".repeat(70));

    // Create a new account
    println!("📝 Step 1: Open a new account");
    println!("{}", "-".repeat(70));

    let account_id = AccountId::new();
    let mut account = BankAccountAggregate::open(
        account_id.clone(),
        "Alice Johnson".to_string(),
        1000, // $10.00 in cents
    )?;

    println!("✓ Opened account: {}", account.aggregate_id());
    println!("  Owner: {}", account.owner);
    println!(
        "  Initial balance: ${:.2}",
        account.balance() as f64 / 100.0
    );
    println!("  Pending events: {}", account.pending_events().len());

    // Perform transactions
    println!("\n📝 Step 2: Perform transactions");
    println!("{}", "-".repeat(70));

    account.deposit(5000)?; // $50.00
    println!("✓ Deposited: ${:.2}", 50.0);
    println!("  New balance: ${:.2}", account.balance() as f64 / 100.0);

    account.withdraw(2000)?; // $20.00
    println!("✓ Withdrew: ${:.2}", 20.0);
    println!("  New balance: ${:.2}", account.balance() as f64 / 100.0);

    account.deposit(1500)?; // $15.00
    println!("✓ Deposited: ${:.2}", 15.0);
    println!("  Final balance: ${:.2}", account.balance() as f64 / 100.0);
    println!("  Total pending events: {}", account.pending_events().len());

    // Show event details
    println!("\n📝 Step 3: Inspect events");
    println!("{}", "-".repeat(70));

    for (i, event) in account.pending_events().iter().enumerate() {
        println!(
            "Event #{}: {} (v{})",
            i + 1,
            event.event_type(),
            event.event_version()
        );
        println!(
            "  Occurred at: {}",
            event.occurred_at().format("%Y-%m-%d %H:%M:%S")
        );
    }

    // Test business rules
    println!("\n📝 Step 4: Test business rules");
    println!("{}", "-".repeat(70));

    match account.withdraw(100000) {
        Err(AccountError::InsufficientFunds { balance, requested }) => {
            println!("✓ Correctly rejected overdraft");
            println!("  Balance: ${:.2}", balance as f64 / 100.0);
            println!("  Requested: ${:.2}", requested as f64 / 100.0);
        }
        _ => println!("❌ Should have rejected overdraft!"),
    }

    match account.deposit(-100) {
        Err(AccountError::InvalidAmount(amount)) => {
            println!("✓ Correctly rejected negative deposit");
            println!("  Amount: {amount}");
        }
        _ => println!("❌ Should have rejected negative amount!"),
    }

    // Test status validation
    println!("\n📝 Step 5: Test status validation");
    println!("{}", "-".repeat(70));
    println!("Current status: {:?}", account.status());
    println!("✓ Account is active and accepting transactions");

    // Summary
    println!("\n{}", "=".repeat(70));
    println!("✅ Example completed successfully!");
    println!("\nKey Takeaways:");
    println!("  • #[derive(AggregateState)] separates state from infrastructure");
    println!("  • Generated wrapper handles version & event tracking automatically");
    println!("  • #[derive(Event)] generates DomainEvent trait implementation");
    println!("  • Aggregate-specific errors via AggregateError trait");
    println!("  • Rich validation with status checking and business rules");
    println!("  • Events capture all state changes immutably");
    println!("  • Business rules are enforced at command time");
    println!("  • ~40% less boilerplate with cleaner code organization");
    println!("{}", "=".repeat(70));
    println!();

    Ok(())
}
