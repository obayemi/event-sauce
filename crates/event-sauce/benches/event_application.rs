//! Benchmarks for event application performance
//!
//! This benchmark suite measures the performance of:
//! - Event application with validation (apply)
//! - Event application without validation (apply_unchecked)
//! - Full event replay scenarios
//!
//! Run with: cargo bench --bench event_application

use chrono::Utc;
use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use event_sauce_core::{Aggregate, AggregateError, AggregateId, Version};
use event_sauce_macros::{Aggregate as DeriveAggregate, Event as DeriveEvent};
use std::fmt;
use uuid::Uuid;

// ============================================================================
// Benchmark Aggregate: BankAccount
// ============================================================================

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct BenchAccountId(Uuid);

impl BenchAccountId {
    fn new() -> Self {
        Self(Uuid::new_v4())
    }
}

impl fmt::Display for BenchAccountId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "BenchAccount-{}", self.0)
    }
}

impl AggregateId for BenchAccountId {}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[allow(dead_code)]
enum AccountStatus {
    Active,
    Frozen,
    Closed,
}

#[derive(Debug, thiserror::Error)]
enum BenchAccountError {
    #[error("Insufficient funds: balance={balance}, requested={requested}")]
    InsufficientFunds { balance: i64, requested: i64 },

    #[error("Invalid amount: {0}")]
    InvalidAmount(i64),

    #[error("Account is {0:?}")]
    AccountNotActive(AccountStatus),
}

impl AggregateError for BenchAccountError {}

#[derive(DeriveEvent, Debug, Clone)]
#[event(version = 1, type_prefix = "BenchAccount")]
enum BenchAccountEvent {
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
}

#[derive(DeriveAggregate, Debug, Clone)]
#[aggregate(
    id = "BenchAccountId",
    event = "BenchAccountEvent",
    error = "BenchAccountError"
)]
struct BenchAccount {
    #[aggregate_id]
    id: BenchAccountId,
    owner: String,
    balance: i64,
    status: AccountStatus,
    #[aggregate_version]
    version: Version,
    #[aggregate_events]
    pending_events: Vec<BenchAccountEvent>,
}

impl BenchAccount {
    fn open(id: BenchAccountId, owner: String, initial_balance: i64) -> Result<Self, BenchAccountError> {
        if initial_balance < 0 {
            return Err(BenchAccountError::InvalidAmount(initial_balance));
        }

        let mut account = Self {
            id,
            owner: String::new(),
            balance: 0,
            status: AccountStatus::Active,
            version: Version::initial(),
            pending_events: Vec::new(),
        };

        let event = BenchAccountEvent::Opened {
            owner,
            initial_balance,
            timestamp: Utc::now(),
        };

        account.apply(&event);
        account.pending_events.push(event);

        Ok(account)
    }

    fn deposit(&mut self, amount: i64) -> Result<(), BenchAccountError> {
        if self.status != AccountStatus::Active {
            return Err(BenchAccountError::AccountNotActive(self.status));
        }

        if amount <= 0 {
            return Err(BenchAccountError::InvalidAmount(amount));
        }

        let event = BenchAccountEvent::Deposited {
            amount,
            timestamp: Utc::now(),
        };

        self.apply(&event);
        self.pending_events.push(event);

        Ok(())
    }

    fn withdraw(&mut self, amount: i64) -> Result<(), BenchAccountError> {
        if self.status != AccountStatus::Active {
            return Err(BenchAccountError::AccountNotActive(self.status));
        }

        if amount <= 0 {
            return Err(BenchAccountError::InvalidAmount(amount));
        }

        if self.balance < amount {
            return Err(BenchAccountError::InsufficientFunds {
                balance: self.balance,
                requested: amount,
            });
        }

        let event = BenchAccountEvent::Withdrawn {
            amount,
            timestamp: Utc::now(),
        };

        self.apply(&event);
        self.pending_events.push(event);

        Ok(())
    }

    fn apply_event(&mut self, event: &BenchAccountEvent) {
        match event {
            BenchAccountEvent::Opened {
                owner,
                initial_balance,
                ..
            } => {
                self.owner = owner.clone();
                self.balance = *initial_balance;
                self.status = AccountStatus::Active;
            }
            BenchAccountEvent::Deposited { amount, .. } => {
                self.balance += amount;
            }
            BenchAccountEvent::Withdrawn { amount, .. } => {
                self.balance -= amount;
            }
        }
    }

    fn from_events(id: BenchAccountId, events: Vec<BenchAccountEvent>) -> Self {
        let mut account = Self {
            id,
            owner: String::new(),
            balance: 0,
            status: AccountStatus::Active,
            version: Version::initial(),
            pending_events: Vec::new(),
        };

        for event in events {
            account.apply_unchecked(&event);
        }

        account
    }
}

// ============================================================================
// Benchmark Functions
// ============================================================================

fn bench_single_event_apply(c: &mut Criterion) {
    let mut group = c.benchmark_group("single_event");

    let id = BenchAccountId::new();
    let mut account = BenchAccount::open(id, "Alice".to_string(), 1000).unwrap();
    account.pending_events.clear();

    let event = BenchAccountEvent::Deposited {
        amount: 100,
        timestamp: Utc::now(),
    };

    group.bench_function("apply", |b| {
        b.iter(|| {
            let mut acc = account.clone();
            acc.apply(black_box(&event));
        });
    });

    group.bench_function("apply_unchecked", |b| {
        b.iter(|| {
            let mut acc = account.clone();
            acc.apply_unchecked(black_box(&event));
        });
    });

    group.finish();
}

fn bench_event_replay(c: &mut Criterion) {
    let mut group = c.benchmark_group("event_replay");

    // Test different event counts
    for event_count in [10, 100, 1000].iter() {
        // Generate events
        let events: Vec<BenchAccountEvent> = std::iter::once(BenchAccountEvent::Opened {
            owner: "Alice".to_string(),
            initial_balance: 10000,
            timestamp: Utc::now(),
        })
        .chain(
            (0..*event_count).map(|i| {
                if i % 2 == 0 {
                    BenchAccountEvent::Deposited {
                        amount: 10,
                        timestamp: Utc::now(),
                    }
                } else {
                    BenchAccountEvent::Withdrawn {
                        amount: 5,
                        timestamp: Utc::now(),
                    }
                }
            }),
        )
        .collect();

        group.throughput(Throughput::Elements(*event_count as u64));

        group.bench_with_input(
            BenchmarkId::new("apply", event_count),
            &events,
            |b, events| {
                b.iter(|| {
                    let mut account = BenchAccount {
                        id: BenchAccountId::new(),
                        owner: String::new(),
                        balance: 0,
                        status: AccountStatus::Active,
                        version: Version::initial(),
                        pending_events: Vec::new(),
                    };

                    for event in events {
                        account.apply(black_box(event));
                    }

                    black_box(account)
                });
            },
        );

        group.bench_with_input(
            BenchmarkId::new("apply_unchecked", event_count),
            &events,
            |b, events| {
                b.iter(|| {
                    let mut account = BenchAccount {
                        id: BenchAccountId::new(),
                        owner: String::new(),
                        balance: 0,
                        status: AccountStatus::Active,
                        version: Version::initial(),
                        pending_events: Vec::new(),
                    };

                    for event in events {
                        account.apply_unchecked(black_box(event));
                    }

                    black_box(account)
                });
            },
        );
    }

    group.finish();
}

fn bench_from_events(c: &mut Criterion) {
    let mut group = c.benchmark_group("from_events");

    for event_count in [10, 100, 1000].iter() {
        let events: Vec<BenchAccountEvent> = std::iter::once(BenchAccountEvent::Opened {
            owner: "Alice".to_string(),
            initial_balance: 10000,
            timestamp: Utc::now(),
        })
        .chain(
            (0..*event_count).map(|i| {
                if i % 2 == 0 {
                    BenchAccountEvent::Deposited {
                        amount: 10,
                        timestamp: Utc::now(),
                    }
                } else {
                    BenchAccountEvent::Withdrawn {
                        amount: 5,
                        timestamp: Utc::now(),
                    }
                }
            }),
        )
        .collect();

        group.throughput(Throughput::Elements(*event_count as u64));

        group.bench_with_input(
            BenchmarkId::from_parameter(event_count),
            &events,
            |b, events| {
                b.iter(|| {
                    let account = BenchAccount::from_events(
                        BenchAccountId::new(),
                        black_box(events.clone()),
                    );
                    black_box(account)
                });
            },
        );
    }

    group.finish();
}

fn bench_business_operations(c: &mut Criterion) {
    let mut group = c.benchmark_group("business_operations");

    let id = BenchAccountId::new();
    let account = BenchAccount::open(id, "Alice".to_string(), 10000).unwrap();

    group.bench_function("deposit", |b| {
        b.iter(|| {
            let mut acc = account.clone();
            acc.pending_events.clear();
            acc.deposit(black_box(100)).unwrap();
        });
    });

    group.bench_function("withdraw", |b| {
        b.iter(|| {
            let mut acc = account.clone();
            acc.pending_events.clear();
            acc.withdraw(black_box(50)).unwrap();
        });
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_single_event_apply,
    bench_event_replay,
    bench_from_events,
    bench_business_operations
);
criterion_main!(benches);
