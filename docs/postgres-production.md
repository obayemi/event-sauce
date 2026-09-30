# PostgreSQL Production Setup Guide

Complete guide to deploying event-sauce with PostgreSQL in production environments.

## Table of Contents

- [Quick Start](#quick-start)
- [Schema Isolation Strategy](#schema-isolation-strategy)
- [Connection Pool Configuration](#connection-pool-configuration)
- [Snapshot Configuration](#snapshot-configuration)
- [Migration Management](#migration-management)
- [Performance Tuning](#performance-tuning)
- [Monitoring and Observability](#monitoring-and-observability)
- [High Availability Setup](#high-availability-setup)
- [Backup and Recovery](#backup-and-recovery)
- [Security Best Practices](#security-best-practices)

## Quick Start

### Basic Setup (Recommended)

The simplest production-ready setup uses `PostgresBackend` — a single entry point
that creates the connection pool, event store, checkpoint store, and runs all migrations:

```rust
use event_sauce::postgres::PostgresBackend;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let backend = PostgresBackend::setup(&std::env::var("DATABASE_URL")?, "event_sauce").await?;

    // Everything is ready to use
    let event_store = backend.event_store();
    let checkpoint_store = backend.checkpoint_store();
    let pool = backend.pool();

    Ok(())
}
```

This creates:
- `event_sauce.events` - Event storage table
- `event_sauce.snapshots` - Snapshot storage table
- `event_sauce.checkpoints` - Checkpoint tracking table
- `event_sauce._event_sauce_migrations` - Migration tracking
- `event_sauce._checkpoint_migrations` - Checkpoint migration tracking

Your application's tables remain in the `public` schema with separate migrations.

### Custom Configuration

For production environments with specific requirements, use the builder:

```rust
use event_sauce::postgres::PostgresBackend;
use event_sauce::SnapshotConfig;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let backend = PostgresBackend::builder()
        .database_url(&std::env::var("DATABASE_URL")?)
        .schema("event_sauce")
        .snapshot_config(SnapshotConfig::disabled()) // Custom snapshot config
        .build()
        .await?;

    Ok(())
}
```

### Individual Store Setup

For fine-grained control (e.g., separate connection pools per store), create stores individually:

```rust
use event_sauce::postgres::{PostgresEventStore, PostgresCheckpointStore};
use event_sauce::{SnapshotConfig, EveryNEvents};
use sqlx::postgres::{PgPool, PgPoolOptions};
use std::sync::Arc;
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let pool = PgPoolOptions::new()
        .max_connections(50)
        .min_connections(5)
        .acquire_timeout(Duration::from_secs(30))
        .idle_timeout(Duration::from_secs(600))
        .max_lifetime(Duration::from_secs(1800))
        .connect(&std::env::var("DATABASE_URL")?)
        .await?;

    let checkpoint_store = PostgresCheckpointStore::builder()
        .pool(pool.clone())
        .schema("event_sauce")
        .build()?;
    checkpoint_store.migrate().await?;

    let event_store = PostgresEventStore::builder()
        .pool(pool)
        .schema("event_sauce")
        .snapshot_config(SnapshotConfig::builder()
            .default_strategy(EveryNEvents::try_new(50).expect("50 != 0"))
            .build())
        .checkpoint_store(Arc::new(checkpoint_store))
        .build()?;
    event_store.migrate().await?;

    Ok(())
}
```

## Schema Isolation Strategy

### Why Schema Isolation?

Event-sauce uses a **dedicated PostgreSQL schema** by default to avoid conflicts with your application:

**Benefits:**
- ✅ **No migration conflicts** - Your app and event-sauce have separate migration tables
- ✅ **Clear separation** - Event sourcing tables are isolated
- ✅ **Easy cleanup** - Drop entire schema without affecting app data
- ✅ **Permission control** - Can restrict access per schema
- ✅ **Schema-level backups** - Backup event store independently

### Schema Architecture

```sql
-- Your application schema (default: public)
public._sqlx_migrations         -- Your app's migrations
public.users                    -- Your app's tables
public.products
...

-- Event-sauce schema (default: event_sauce)
event_sauce._event_sauce_migrations  -- Event-sauce migrations
event_sauce.events              -- Event storage
event_sauce.snapshots           -- Snapshot storage
```

### Schema Configuration Options

```rust
// Option 1: Default isolated schema (recommended)
let store = PostgresEventStore::new(pool);
// Uses: event_sauce schema

// Option 2: Custom schema name
let store = PostgresEventStore::builder()
    .pool(pool)
    .schema("my_events")
    .build()?;

// Option 3: Use public schema (not recommended for production)
let store = PostgresEventStore::builder()
    .pool(pool)
    .schema("public")
    .build()?;
```

### Multiple Event Stores

You can run multiple isolated event stores in the same database:

```rust
// Event store for orders domain
let orders_store = PostgresEventStore::builder()
    .pool(pool.clone())
    .schema("orders_events")
    .build()?;
orders_store.migrate().await?;

// Event store for inventory domain
let inventory_store = PostgresEventStore::builder()
    .pool(pool.clone())
    .schema("inventory_events")
    .build()?;
inventory_store.migrate().await?;
```

This gives you:
- **Domain isolation** - Separate event stores per bounded context
- **Independent scaling** - Different pools/configs per domain
- **Granular access control** - Schema-level permissions

## Connection Pool Configuration

### Production Pool Settings

```rust
use sqlx::postgres::PgPoolOptions;
use std::time::Duration;

let pool = PgPoolOptions::new()
    // Connection limits
    .max_connections(50)           // Max pool size (adjust based on load)
    .min_connections(5)            // Keep connections warm

    // Timeouts
    .acquire_timeout(Duration::from_secs(30))  // Max wait for connection
    .idle_timeout(Duration::from_secs(600))    // Close idle connections after 10min
    .max_lifetime(Duration::from_secs(1800))   // Recycle connections after 30min

    // Connection testing
    .test_before_acquire(true)     // Verify connection health

    .connect(&database_url)
    .await?;
```

### Sizing Guidelines

**Connection pool size calculation:**
```
connections = ((core_count * 2) + effective_spindle_count)
```

For typical web apps:
- **Small** (1-2 cores): 10-20 connections
- **Medium** (4-8 cores): 20-50 connections
- **Large** (16+ cores): 50-100 connections

**Monitor these metrics:**
- Pool utilization (should be < 80% typically)
- Connection wait time (should be < 100ms)
- Connection errors (should be near 0)

### Environment-Specific Configuration

```rust
fn get_pool_config() -> PgPoolOptions {
    match std::env::var("ENVIRONMENT").as_deref() {
        Ok("production") => PgPoolOptions::new()
            .max_connections(50)
            .min_connections(10)
            .acquire_timeout(Duration::from_secs(30)),

        Ok("staging") => PgPoolOptions::new()
            .max_connections(20)
            .min_connections(5)
            .acquire_timeout(Duration::from_secs(30)),

        _ => PgPoolOptions::new()  // Development defaults
            .max_connections(5)
            .acquire_timeout(Duration::from_secs(5)),
    }
}
```

## Snapshot Configuration

Snapshots dramatically improve read performance for aggregates with many events.

### Understanding Snapshots

**Without snapshots:**
```
Load aggregate → Replay 1000 events → Return aggregate (slow!)
```

**With snapshots (every 100 events):**
```
Load aggregate → Load snapshot at event 900 → Replay 100 events → Return (fast!)
```

### Snapshot Strategies

```rust
use event_sauce::{SnapshotConfig, EveryNEvents};

// Strategy 1: Snapshot every N events (recommended)
let config = SnapshotConfig::builder()
    .default_strategy(EveryNEvents::try_new(100).expect("100 != 0"))  // Snapshot every 100 events
    .use_snapshots_on_load(true)
    .build();

// Strategy 2: More frequent snapshots for hot aggregates
let config = SnapshotConfig::builder()
    .default_strategy(EveryNEvents::try_new(50).expect("50 != 0"))   // More frequent
    .use_snapshots_on_load(true)
    .build();

// Strategy 3: Disable snapshots (for testing or low-event aggregates)
let config = SnapshotConfig::disabled();
```

### Choosing Snapshot Frequency

| Aggregate Profile | Events/Day | Recommended Frequency |
|-------------------|------------|----------------------|
| Hot (frequently accessed) | 1000+ | Every 25-50 events |
| Warm (regular access) | 100-1000 | Every 50-100 events |
| Cold (rare access) | < 100 | Every 100-200 events or disabled |

### Performance Impact

**Snapshot every 50 events:**
- 📊 Storage: +2% (snapshots are small)
- ⚡ Load time: 95% faster for aggregates with 500+ events
- 💾 Write cost: +2% (one extra write every 50 events)

**Best practice:** Start with 100 and tune based on your load patterns.

## Migration Management

### Initial Setup

```rust
// In your application startup
let store = PostgresEventStore::new(pool);

// Run migrations (idempotent - safe to call on every startup)
store.migrate().await?;
```

### Migration Strategy

Event-sauce migrations are **idempotent** and **isolated**:

```sql
-- Creates schema if it doesn't exist
CREATE SCHEMA IF NOT EXISTS event_sauce;

-- Creates migration tracking table
CREATE TABLE IF NOT EXISTS event_sauce._event_sauce_migrations (
    version BIGINT PRIMARY KEY,
    description TEXT NOT NULL,
    applied_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);

-- Creates tables with IF NOT EXISTS
CREATE TABLE IF NOT EXISTS event_sauce.events (...);
CREATE TABLE IF NOT EXISTS event_sauce.snapshots (...);
```

### CI/CD Integration

**Option 1: Application-managed (recommended):**
```yaml
# docker-compose.yml or k8s manifest
services:
  app:
    image: your-app:latest
    command: ["./your-app"]  # Calls store.migrate() on startup
```

**Option 2: Separate migration job:**
```rust
// migrations.rs
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let pool = PgPool::connect(&std::env::var("DATABASE_URL")?).await?;
    let store = PostgresEventStore::new(pool);
    store.migrate().await?;
    println!("Migrations complete!");
    Ok(())
}
```

```yaml
# k8s job
apiVersion: batch/v1
kind: Job
metadata:
  name: event-sauce-migrations
spec:
  template:
    spec:
      containers:
      - name: migrate
        image: your-app:latest
        command: ["./migrations"]
      restartPolicy: OnFailure
```

### Rollback Strategy

Event-sauce tables are **append-only** by design, making rollbacks safe:

1. **Schema rollback:**
   ```sql
   -- Drop schema (destructive - backup first!)
   DROP SCHEMA event_sauce CASCADE;
   ```

2. **Application rollback:**
   - Events are never deleted
   - Old application versions can still read events
   - New event types are ignored by old code

## Performance Tuning

### Database Indexes

Event-sauce creates optimized indexes automatically:

```sql
-- Aggregate lookup (most common query)
CREATE INDEX idx_events_aggregate
    ON event_sauce.events(aggregate_id, aggregate_type);

-- Aggregate + version (for loading from version)
CREATE INDEX idx_events_aggregate_version
    ON event_sauce.events(aggregate_id, aggregate_type, stream_version);

-- Event type filtering
CREATE INDEX idx_events_type
    ON event_sauce.events(event_type);

-- Time-based queries
CREATE INDEX idx_events_created_at
    ON event_sauce.events(created_at);

-- Correlation tracking
CREATE INDEX idx_events_correlation_id
    ON event_sauce.events(correlation_id)
    WHERE correlation_id IS NOT NULL;  -- Partial index for sparse data
```

### PostgreSQL Configuration

**postgresql.conf tuning for event sourcing:**

```ini
# Memory settings (adjust based on available RAM)
shared_buffers = 4GB              # 25% of RAM
effective_cache_size = 12GB       # 75% of RAM
work_mem = 64MB                   # Per operation
maintenance_work_mem = 1GB        # For vacuum, index creation

# Write-ahead log (WAL) settings
wal_buffers = 16MB
max_wal_size = 4GB
min_wal_size = 1GB

# Checkpoint settings (tune for write-heavy workload)
checkpoint_completion_target = 0.9
checkpoint_timeout = 15min

# Query planner
random_page_cost = 1.1            # For SSD storage
effective_io_concurrency = 200    # For SSD storage

# Connections
max_connections = 200             # Match your application needs
```

### Vacuum Strategy

Event tables are **append-only**, making autovacuum efficient:

```sql
-- Configure autovacuum for event tables
ALTER TABLE event_sauce.events SET (
    autovacuum_vacuum_scale_factor = 0.05,  -- Vacuum at 5% dead tuples
    autovacuum_analyze_scale_factor = 0.02  -- Analyze more frequently
);

ALTER TABLE event_sauce.snapshots SET (
    autovacuum_vacuum_scale_factor = 0.1    -- Snapshots update in place
);
```

### Query Optimization

**Efficient event loading:**
```rust
// GOOD: Load only what you need
let events = store.load_stream(stream_id, AggregateVersion::new(10)).await?;

// GOOD: Use snapshots
let config = SnapshotConfig::builder()
    .default_strategy(EveryNEvents::try_new(100).expect("100 != 0"))
    .build();

// AVOID: Loading all events for a high-event aggregate
let events = store.load_stream(stream_id, AggregateVersion::initial()).await?;
// Use snapshots instead!
```

## Monitoring and Observability

### Key Metrics to Track

```rust
use tracing::{info, warn};

// 1. Event write latency
let start = std::time::Instant::now();
store.append(stream_id, events, version).await?;
info!("Event append took: {:?}", start.elapsed());

// 2. Event count per aggregate
let version = store.get_version(stream_id).await?;
if version.as_i64() > 1000 {
    warn!("High event count for aggregate: {}", stream_id);
}

// 3. Snapshot usage
info!("Loaded from snapshot at version: {}", snapshot.version);
```

### PostgreSQL Monitoring Queries

```sql
-- 1. Events per aggregate distribution
SELECT
    aggregate_type,
    COUNT(DISTINCT aggregate_id) as aggregate_count,
    AVG(stream_version) as avg_events_per_aggregate,
    MAX(stream_version) as max_events
FROM event_sauce.events
GROUP BY aggregate_type;

-- 2. Event ingestion rate
SELECT
    date_trunc('hour', created_at) as hour,
    COUNT(*) as events_per_hour
FROM event_sauce.events
WHERE created_at > NOW() - INTERVAL '24 hours'
GROUP BY hour
ORDER BY hour DESC;

-- 3. Snapshot effectiveness
SELECT
    COUNT(DISTINCT aggregate_id) as total_aggregates,
    (SELECT COUNT(*) FROM event_sauce.snapshots) as snapshots_stored,
    ROUND(100.0 * (SELECT COUNT(*) FROM event_sauce.snapshots) /
          COUNT(DISTINCT aggregate_id), 2) as snapshot_coverage_pct
FROM event_sauce.events;

-- 4. Table sizes
SELECT
    schemaname,
    tablename,
    pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) as total_size,
    pg_size_pretty(pg_relation_size(schemaname||'.'||tablename)) as table_size,
    pg_size_pretty(pg_indexes_size(schemaname||'.'||tablename)) as indexes_size
FROM pg_tables
WHERE schemaname = 'event_sauce';
```

### Alerts to Configure

1. **High write latency** (> 100ms p99)
2. **Connection pool exhaustion** (> 80% utilization)
3. **Aggregate event count** (> 10,000 events)
4. **Failed appends** (concurrency conflicts or DB errors)
5. **Snapshot coverage** (< 50% of hot aggregates)

## High Availability Setup

### Read Replicas

**Command-side loads and appends must go to the primary.** A replica lags
the primary by a variable, unbounded amount, so an aggregate loaded from a
replica can be stale: a command validated against that stale state and then
appended (necessarily to the primary — replicas are read-only) either
produces a decision based on facts that no longer hold, or simply loses the
optimistic-concurrency race against whatever *did* reach the primary in the
meantime, over and over, under load. The same staleness applies to a
replica-backed [`CryptoKeyStore`](https://docs.rs/event-sauce-core/latest/event_sauce_core/trait.CryptoKeyStore.html):
a key rotated or deleted (crypto-shredded) on the primary may not have
replicated yet, so a load could transiently succeed with a key that should
already be gone.

Replicas are for **read models and event-log queries only** — the workloads
that already tolerate eventual consistency because they're derived, not the
source of truth:

```rust
use sqlx::PgPool;
use event_sauce::postgres::{PostgresEventStore, PostgresEventLogQuery};

// Primary pool — all command-side loads, saves, and appends
let primary_pool = PgPool::connect(&primary_url).await?;
let store = std::sync::Arc::new(PostgresEventStore::new(primary_pool));
let repo = store.repository::<Order>();
let mut order = repo.load(order_id).await?; // Always the primary
order.confirm()?;
repo.save(&mut order).await?; // Always the primary

// Replica pool — read models and audit/event-log queries only
let replica_pool = PgPool::connect(&replica_url).await?;
let replica_store = PostgresEventStore::new(replica_pool);
let log_query = PostgresEventLogQuery::new(replica_store.pool().clone(), replica_store.schema().to_string());
let page = log_query.query_events(Default::default()).await?;
```

### Connection Pooling with PgBouncer

**PgBouncer configuration:**
```ini
[databases]
events = host=postgres-primary port=5432 dbname=events

[pgbouncer]
pool_mode = transaction          # Fine for repo.load/save and migrate() —
                                  # NOT for listen_for_events (see below)
max_client_conn = 1000
default_pool_size = 25
reserve_pool_size = 5
reserve_pool_timeout = 3
```

Transaction-mode pooling hands out a backend connection per transaction, so
it's exactly what `repo.load`/`repo.save` and `store.migrate()` need — a
migration's advisory lock is itself transaction-scoped, which is why
`migrate()` is safe to run concurrently from every replica of your app, even
behind a transaction-mode PgBouncer.

`listen_for_events` is different: it opens one `PgListener` connection and
holds a session-level `LISTEN` registration on it for as long as the worker
runs. Transaction-mode pooling cycles the backend connection between
transactions, so that registration does not survive past the first
transaction boundary — `NOTIFY` wake-ups silently stop arriving, with no
error to signal it. Give `listen_for_events` its own pool (or a direct,
unpooled connection) pointed at a `session`-mode PgBouncer database, or bypass
PgBouncer for it entirely:

```ini
[databases]
events_session = host=postgres-primary port=5432 dbname=events pool_mode=session
```

Either way, treat `NOTIFY` as a latency optimization, not a delivery
guarantee: keep a polling fallback (a periodic `run_leased_projection`/
`run_postgres_projection` tick regardless of notifications) so a dropped or
misconfigured listener degrades to polling latency instead of silently
stalling.

### Which Components Are Multi-Node Safe

Running more than one instance of your app (multiple pods, multiple
processes) is safe for commands and for most background work, but not
uniformly — check each component against this table before scaling
horizontally:

| Component | Multi-node safe? | Why |
|---|---|---|
| `repo.load`/`repo.save`, `EventStore::commit` | ✅ Yes | Optimistic concurrency (`ConcurrencyConflict`) plus, on Postgres, a DB-enforced unique constraint make a lost write impossible even under a race. |
| `Repository::save_all` (Postgres) | ✅ Yes | Runs the whole batch in one transaction under a single append serialization lock. |
| `run_leased_projection`, `run_sticky_projection` | ✅ Yes | Lease-fenced: only the current lease holder's checkpoint writes are accepted; a losing racer's write is rejected, not silently overwritten. |
| Policy outbox (`claim_batch` / `mark_done` / `mark_failed`) | ✅ Yes | `FOR UPDATE SKIP LOCKED` partitions work across workers; acks are fenced on `locked_by`, so a worker whose lease already expired can't falsely ack a row a different worker has since reclaimed. |
| `dispatch_policies_to_outbox` | ✅ Yes | Each policy tracks its own lease and fenced checkpoint (`__policy_outbox_dispatcher:{policy}`), so every instance can call it: each policy is dispatched by whichever instance currently holds its lease, and the rest sit out that policy without blocking on it. |
| `prune_done` | ✅ Yes | Deletes strictly by `status = 'done' AND updated_at < cutoff`; concurrent claims/acks on other rows are unaffected. |
| `BackoffPolicy` (retry backoff for outbox handlers) | ✅ Yes | Pure computation of a delay from an attempt count; no shared state to race on. |
| `store.migrate()` (any store) | ✅ Yes, even behind PgBouncer transaction mode | Guarded by a transaction-scoped `pg_advisory_xact_lock`; safe to call from every instance on every startup. |
| `run_postgres_projection` (unleased) | ❌ No | Explicitly documented as **not** taking a lease — two instances race on the checkpoint and double-apply events. Use `run_leased_projection` instead whenever more than one instance might run the same projection. |
| `PolicyRunner::process_pending` | ❌ No | Resolves and advances policy checkpoints with no lease of its own. Run it from exactly one instance (a dedicated worker, a leader-elected pod, or a single cron-triggered job), not from every app replica. |
| `listen_for_events` behind a transaction-mode PgBouncer | ❌ No (silently) | See above — the session-level `LISTEN` doesn't survive transaction-mode pooling. Safe once pointed at a session-mode connection. |

### Kubernetes Deployment

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: event-sauce-app
spec:
  replicas: 3
  template:
    spec:
      containers:
      - name: app
        image: your-app:latest
        env:
        - name: DATABASE_URL
          valueFrom:
            secretKeyRef:
              name: postgres-secret
              key: connection-string
        - name: POOL_MAX_CONNECTIONS
          value: "20"  # 3 replicas * 20 = 60 total connections
```

## Backup and Recovery

### Backup Strategy

**1. Continuous WAL archiving:**
```ini
# postgresql.conf
archive_mode = on
archive_command = 'cp %p /backup/wal/%f'
```

**2. Schema-specific backup:**
```bash
# Backup only event-sauce schema
pg_dump -h localhost -U postgres \
    -n event_sauce \
    -F c \
    -f event_sauce_backup_$(date +%Y%m%d).dump \
    your_database

# Backup with compression
pg_dump -h localhost -U postgres \
    -n event_sauce \
    -F c -Z 9 \
    -f event_sauce_backup.dump.gz \
    your_database
```

**3. Point-in-time recovery:**
```bash
# Restore to specific time
pg_restore -h localhost -U postgres \
    -n event_sauce \
    -d your_database \
    event_sauce_backup.dump
```

### Disaster Recovery

**Event sourcing provides natural DR capabilities:**

1. **Events are immutable** - No data corruption from application bugs
2. **Rebuild from events** - Recreate any state by replaying events
3. **Schema-isolated** - Restore event-sauce independently

```rust
// Rebuild projections from events
async fn rebuild_projections(store: &PostgresEventStore) -> Result<()> {
    let mut stream = store.stream_all(Position::start()).await?;

    // `stream_all` yields `EventLogEntry { position, envelope }`. Checkpoint
    // `entry.position` (the real global id) to resume; never reconstruct it.
    while let Some(entry) = stream.next().await {
        let entry = entry?;
        // Rebuild projection state
        projection.handle_event(&entry.envelope).await?;
    }

    Ok(())
}
```

## Security Best Practices

### Connection Security

**1. Use SSL/TLS:**
```rust
let pool = PgPool::connect(
    "postgresql://user:pass@host:5432/db?sslmode=require"
).await?;
```

**2. Use connection string from secrets:**
```rust
use std::env;

let database_url = env::var("DATABASE_URL")
    .expect("DATABASE_URL must be set");
let pool = PgPool::connect(&database_url).await?;
```

### Database Permissions

**Principle of least privilege:**

```sql
-- Create dedicated user for event-sauce
CREATE USER event_sauce_app WITH PASSWORD 'secure_password';

-- Grant schema access
GRANT USAGE ON SCHEMA event_sauce TO event_sauce_app;

-- Grant table permissions
GRANT SELECT, INSERT ON event_sauce.events TO event_sauce_app;
GRANT SELECT, INSERT, UPDATE ON event_sauce.snapshots TO event_sauce_app;
GRANT ALL ON event_sauce._event_sauce_migrations TO event_sauce_app;

-- Grant sequence access (for auto-increment IDs)
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA event_sauce TO event_sauce_app;
```

**Read-only user for replicas:**
```sql
CREATE USER event_sauce_readonly WITH PASSWORD 'readonly_password';
GRANT USAGE ON SCHEMA event_sauce TO event_sauce_readonly;
GRANT SELECT ON ALL TABLES IN SCHEMA event_sauce TO event_sauce_readonly;
```

### Network Security

**1. Use connection pooler (PgBouncer):**
- Reduces connection overhead
- Adds connection-level access control
- Protects against connection exhaustion

**2. Firewall rules:**
```bash
# Only allow app servers to connect to PostgreSQL
iptables -A INPUT -p tcp --dport 5432 -s 10.0.1.0/24 -j ACCEPT
iptables -A INPUT -p tcp --dport 5432 -j DROP
```

**3. Use private networks (VPC):**
- Keep PostgreSQL in private subnet
- No public internet access
- App servers in same VPC

### Audit Logging

Event-sauce provides built-in audit trail:

```rust
// Every event includes audit metadata
let event = EventEnvelope {
    created_by: Some(user_id),           // Who created the event
    metadata: Some(EventMetadata {
        correlation_id: Some(request_id), // Request tracking
        causation_id: Some(parent_event_id), // Causality chain
        timestamp: Utc::now(),            // When it happened
        additional: Some(json!({
            "ip": "192.168.1.1",
            "user_agent": "...",
        })),
    }),
    ..event
};
```

Query audit trail:
```sql
-- Track all events by user
SELECT * FROM event_sauce.events
WHERE created_by = '...'
ORDER BY created_at DESC;

-- Trace request flow
SELECT * FROM event_sauce.events
WHERE correlation_id = '...'
ORDER BY created_at ASC;
```

## Production Checklist

Before going live, ensure:

- [ ] **Schema isolation** configured (not using `public`)
- [ ] **Connection pool** sized appropriately for load
- [ ] **Snapshots** enabled with reasonable frequency
- [ ] **Migrations** run and verified
- [ ] **Indexes** created (automatic with `migrate()`)
- [ ] **SSL/TLS** enabled for database connections
- [ ] **Monitoring** configured (metrics + alerts)
- [ ] **Backups** automated and tested
- [ ] **Read replicas** configured for read-heavy workloads
- [ ] **Permissions** follow least privilege principle
- [ ] **Load testing** performed with realistic data volumes
- [ ] **Disaster recovery** plan documented and tested

## Example Production Configuration

Complete production-ready setup:

```rust
use event_sauce::postgres::PostgresBackend;
use tracing::info;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();

    let database_url = std::env::var("DATABASE_URL")?;

    info!("Setting up PostgreSQL backend...");
    let backend = PostgresBackend::setup(&database_url, "event_sauce").await?;
    info!("Backend ready — event store, checkpoint store, and migrations complete");

    // Access components as needed
    let _event_store = backend.event_store();
    let _checkpoint_store = backend.checkpoint_store();
    let _pool = backend.pool();

    // Your application logic here
    // ...

    Ok(())
}
```

## Need Help?

- 📚 [Architecture Overview](architecture.md)
- 🚀 [Getting Started Guide](getting-started.md)
- 💾 [Event Store Documentation](event-store.md)
- 🔧 [GitHub Issues](https://github.com/obayemi/event-sauce/issues)

---

**Production-Ready PostgreSQL Event Sourcing with event-sauce** 🚀
