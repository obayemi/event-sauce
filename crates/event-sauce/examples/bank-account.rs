//! Simple Bank Account Example
//!
//! This example demonstrates:
//! - Using #[derive(Aggregate)] and #[derive(Event)] macros
//! - In-memory event store for testing and development
//! - Basic event sourcing patterns
//!
//! Run with: cargo run -p event-sauce --example bank-account

use chrono::Utc;
use event_sauce_core::{Aggregate, AggregateId, DomainEvent, Version};
use event_sauce_macros::{Aggregate as DeriveAggregate, Event as DeriveEvent};
use std::fmt;
use uuid::Uuid;

// ============================================================================
// Domain Model
// ============================================================================

/// Unique identifier for a bank account
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct AccountId(Uuid);

impl AccountId {
    fn new() -> Self {
        Self(Uuid::new_v4())
    }
}

impl fmt::Display for AccountId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Account-{}", self.0)
    }
}

impl AggregateId for AccountId {}

/// Bank account events using the derive macro
#[derive(DeriveEvent, Debug, Clone)]
#[event(version = 1, type_prefix = "Account")]
#[allow(dead_code)]
enum AccountEvent {
    Opened {
        account_id: String,
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
}

/// Domain errors
#[derive(Debug, thiserror::Error)]
enum AccountError {
    #[error("Insufficient funds: balance={balance}, requested={requested}")]
    InsufficientFunds { balance: i64, requested: i64 },

    #[error("Invalid amount: {0}")]
    InvalidAmount(i64),
}

/// Bank account aggregate using the derive macro
#[derive(DeriveAggregate, Debug, Clone)]
#[aggregate(id = "AccountId", event = "AccountEvent")]
struct BankAccount {
    #[aggregate_id]
    id: AccountId,
    owner: String,
    balance: i64,
    #[aggregate_version]
    version: Version,
    #[aggregate_events]
    pending_events: Vec<AccountEvent>,
}

impl BankAccount {
    /// Create a new bank account
    fn open(id: AccountId, owner: String, initial_balance: i64) -> Result<Self, AccountError> {
        if initial_balance < 0 {
            return Err(AccountError::InvalidAmount(initial_balance));
        }

        let mut account = Self {
            id: id.clone(),
            owner: String::new(),
            balance: 0,
            version: Version::initial(),
            pending_events: Vec::new(),
        };

        let event = AccountEvent::Opened {
            account_id: id.to_string(),
            owner,
            initial_balance,
            timestamp: Utc::now(),
        };

        account.apply(&event);
        account.pending_events.push(event);

        Ok(account)
    }

    /// Deposit money into the account
    fn deposit(&mut self, amount: i64) -> Result<(), AccountError> {
        if amount <= 0 {
            return Err(AccountError::InvalidAmount(amount));
        }

        let event = AccountEvent::Deposited {
            amount,
            timestamp: Utc::now(),
        };

        self.apply(&event);
        self.pending_events.push(event);

        Ok(())
    }

    /// Withdraw money from the account
    fn withdraw(&mut self, amount: i64) -> Result<(), AccountError> {
        if amount <= 0 {
            return Err(AccountError::InvalidAmount(amount));
        }

        if self.balance < amount {
            return Err(AccountError::InsufficientFunds {
                balance: self.balance,
                requested: amount,
            });
        }

        let event = AccountEvent::Withdrawn {
            amount,
            timestamp: Utc::now(),
        };

        self.apply(&event);
        self.pending_events.push(event);

        Ok(())
    }

    /// Apply event to update state (required by Aggregate trait)
    fn apply_event(&mut self, event: &AccountEvent) {
        match event {
            AccountEvent::Opened {
                owner,
                initial_balance,
                ..
            } => {
                self.owner = owner.clone();
                self.balance = *initial_balance;
            }
            AccountEvent::Deposited { amount, .. } => {
                self.balance += amount;
            }
            AccountEvent::Withdrawn { amount, .. } => {
                self.balance -= amount;
            }
        }
    }

    /// Get current balance
    fn balance(&self) -> i64 {
        self.balance
    }
}

// ============================================================================
// Main Example
// ============================================================================

fn main() -> Result<(), AccountError> {
    println!("\n{}", "=".repeat(70));
    println!("🏦 Bank Account Example - Event Sourcing with Derive Macros");
    println!("{}\n", "=".repeat(70));

    // Create a new account
    println!("📝 Step 1: Open a new account");
    println!("{}", "-".repeat(70));

    let account_id = AccountId::new();
    let mut account = BankAccount::open(
        account_id.clone(),
        "Alice Johnson".to_string(),
        1000, // $10.00 in cents
    )?;

    println!("✓ Opened account: {}", account.aggregate_id());
    println!("  Owner: {}", account.owner);
    println!("  Initial balance: ${:.2}", account.balance() as f64 / 100.0);
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
        println!("Event #{}: {} (v{})",
            i + 1,
            event.event_type(),
            event.event_version()
        );
        println!("  Occurred at: {}", event.occurred_at().format("%Y-%m-%d %H:%M:%S"));
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
            println!("  Amount: {}", amount);
        }
        _ => println!("❌ Should have rejected negative amount!"),
    }

    // Summary
    println!("\n{}", "=".repeat(70));
    println!("✅ Example completed successfully!");
    println!("\nKey Takeaways:");
    println!("  • #[derive(Aggregate)] eliminates boilerplate for aggregates");
    println!("  • #[derive(Event)] generates DomainEvent trait implementation");
    println!("  • Events capture all state changes immutably");
    println!("  • Business rules are enforced at command time");
    println!("  • Type-safe event sourcing with minimal code");
    println!("{}", "=".repeat(70));
    println!();

    Ok(())
}
