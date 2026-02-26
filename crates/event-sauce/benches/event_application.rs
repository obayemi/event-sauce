//! Benchmarks for event application performance
//!
//! This benchmark suite measures the performance of:
//! - Event application with validation (apply via `AggregateRoot`)
//! - Event application without validation (`dispatch_unchecked`)
//! - Full event replay scenarios
//!
//! Run with: cargo bench --bench event_application

use chrono::Utc;
use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use event_sauce_core::{
    Aggregate, AggregateRoot, DefaultEntity, DomainEvent, Entity, EntityId, EventApplicator,
};
use event_sauce_macros::AggregateError;

// ============================================================================
// Benchmark Aggregate: BankAccount
// ============================================================================

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[allow(dead_code)]
enum AccountStatus {
    Active,
    Frozen,
    Closed,
}

#[derive(AggregateError, Debug, thiserror::Error)]
enum BenchAccountError {
    #[error("Insufficient funds: balance={balance}, requested={requested}")]
    InsufficientFunds { balance: i64, requested: i64 },

    #[error("Invalid amount: {0}")]
    InvalidAmount(i64),

    #[error("Account is {0:?}")]
    AccountNotActive(AccountStatus),
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
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

impl DomainEvent for BenchAccountEvent {
    type Aggregate = BenchAccount;

    fn event_type(&self) -> &'static str {
        match self {
            BenchAccountEvent::Opened { .. } => "BenchAccount.Opened",
            BenchAccountEvent::Deposited { .. } => "BenchAccount.Deposited",
            BenchAccountEvent::Withdrawn { .. } => "BenchAccount.Withdrawn",
        }
    }

    fn event_version(&self) -> event_sauce_core::EventVersion {
        event_sauce_core::EventVersion::new(1)
    }

    fn occurred_at(&self) -> chrono::DateTime<Utc> {
        match self {
            BenchAccountEvent::Opened { timestamp, .. }
            | BenchAccountEvent::Deposited { timestamp, .. }
            | BenchAccountEvent::Withdrawn { timestamp, .. } => *timestamp,
        }
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct BenchAccount {
    id: EntityId,
    owner: String,
    balance: i64,
    status: AccountStatus,
}

impl Entity for BenchAccount {
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

impl DefaultEntity for BenchAccount {}

impl Aggregate for BenchAccount {
    type Event = BenchAccountEvent;
    type Error = BenchAccountError;
}

impl EventApplicator<BenchAccount> for BenchAccountEvent {
    fn dispatch(&self, entity: &mut BenchAccount) -> Result<(), BenchAccountError> {
        match self {
            BenchAccountEvent::Opened {
                owner,
                initial_balance,
                ..
            } => {
                entity.owner = owner.clone();
                entity.balance = *initial_balance;
                entity.status = AccountStatus::Active;
            }
            BenchAccountEvent::Deposited { amount, .. } => {
                entity.balance += amount;
            }
            BenchAccountEvent::Withdrawn { amount, .. } => {
                entity.balance -= amount;
            }
        }
        Ok(())
    }

    fn dispatch_unchecked(&self, entity: &mut BenchAccount) {
        match self {
            BenchAccountEvent::Opened {
                owner,
                initial_balance,
                ..
            } => {
                entity.owner = owner.clone();
                entity.balance = *initial_balance;
                entity.status = AccountStatus::Active;
            }
            BenchAccountEvent::Deposited { amount, .. } => {
                entity.balance += amount;
            }
            BenchAccountEvent::Withdrawn { amount, .. } => {
                entity.balance -= amount;
            }
        }
    }
}

fn open_account(
    id: EntityId,
    owner: String,
    initial_balance: i64,
) -> Result<AggregateRoot<BenchAccount>, BenchAccountError> {
    if initial_balance < 0 {
        return Err(BenchAccountError::InvalidAmount(initial_balance));
    }

    let mut root = AggregateRoot::<BenchAccount>::new(id);
    root.apply(BenchAccountEvent::Opened {
        owner,
        initial_balance,
        timestamp: Utc::now(),
    })?;
    Ok(root)
}

fn deposit(root: &mut AggregateRoot<BenchAccount>, amount: i64) -> Result<(), BenchAccountError> {
    if root.status != AccountStatus::Active {
        return Err(BenchAccountError::AccountNotActive(root.status));
    }
    if amount <= 0 {
        return Err(BenchAccountError::InvalidAmount(amount));
    }
    root.apply(BenchAccountEvent::Deposited {
        amount,
        timestamp: Utc::now(),
    })
}

fn withdraw(root: &mut AggregateRoot<BenchAccount>, amount: i64) -> Result<(), BenchAccountError> {
    if root.status != AccountStatus::Active {
        return Err(BenchAccountError::AccountNotActive(root.status));
    }
    if amount <= 0 {
        return Err(BenchAccountError::InvalidAmount(amount));
    }
    if root.balance < amount {
        return Err(BenchAccountError::InsufficientFunds {
            balance: root.balance,
            requested: amount,
        });
    }
    root.apply(BenchAccountEvent::Withdrawn {
        amount,
        timestamp: Utc::now(),
    })
}

fn from_events(id: EntityId, events: &[BenchAccountEvent]) -> BenchAccount {
    let mut entity = BenchAccount::new(id);
    for event in events {
        EventApplicator::dispatch_unchecked(event, &mut entity);
    }
    entity
}

// ============================================================================
// Benchmark Functions
// ============================================================================

fn bench_single_event_apply(c: &mut Criterion) {
    let mut group = c.benchmark_group("single_event");

    let id = EntityId::new();
    let account = open_account(id, "Alice".to_string(), 1000).unwrap();

    let event = BenchAccountEvent::Deposited {
        amount: 100,
        timestamp: Utc::now(),
    };

    group.bench_function("apply", |b| {
        b.iter(|| {
            let mut acc = account.clone();
            acc.apply(black_box(event.clone())).unwrap();
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
        .chain((0..*event_count).map(|i| {
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
        }))
        .collect();

        group.throughput(Throughput::Elements(*event_count as u64));

        group.bench_with_input(
            BenchmarkId::new("apply", event_count),
            &events,
            |b, events| {
                b.iter(|| {
                    let mut root = AggregateRoot::<BenchAccount>::new(EntityId::new());

                    for event in events {
                        root.apply(black_box(event.clone())).unwrap();
                    }

                    black_box(root)
                });
            },
        );

        group.bench_with_input(
            BenchmarkId::new("dispatch_unchecked", event_count),
            &events,
            |b, events| {
                b.iter(|| {
                    let mut entity = BenchAccount::new(EntityId::new());

                    for event in events {
                        EventApplicator::dispatch_unchecked(black_box(event), &mut entity);
                    }

                    black_box(entity)
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
        .chain((0..*event_count).map(|i| {
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
        }))
        .collect();

        group.throughput(Throughput::Elements(*event_count as u64));

        group.bench_with_input(
            BenchmarkId::from_parameter(event_count),
            &events,
            |b, events| {
                b.iter(|| {
                    let entity = from_events(EntityId::new(), black_box(events));
                    black_box(entity)
                });
            },
        );
    }

    group.finish();
}

fn bench_business_operations(c: &mut Criterion) {
    let mut group = c.benchmark_group("business_operations");

    let id = EntityId::new();
    let account = open_account(id, "Alice".to_string(), 10000).unwrap();

    group.bench_function("deposit", |b| {
        b.iter(|| {
            let mut acc = account.clone();
            acc.clear_pending_events();
            deposit(&mut acc, black_box(100)).unwrap();
        });
    });

    group.bench_function("withdraw", |b| {
        b.iter(|| {
            let mut acc = account.clone();
            acc.clear_pending_events();
            withdraw(&mut acc, black_box(50)).unwrap();
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
