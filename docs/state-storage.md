# State Storage (Using event-sauce Without Event Sourcing)

Not every application needs an event log. Some systems have no audit
requirements, no replay use case, and no appetite for operating one — they
just need well-modeled aggregates with validation, optimistic concurrency,
and a clean persistence boundary. event-sauce supports this directly: the
same domain layer can be persisted **state-stored** (one row of current
state per aggregate, CRUD-style) instead of event-sourced.

The promise is strict: **application code is identical in both modes.**
Aggregates, events, commands, `define_events!`, `command_handler!`, and every
call through the `Repository` trait are byte-for-byte the same. The only
thing that changes is which store you construct at the composition root.

Everything on this page ships today: the `Repository` trait with its
`EventSourcedRepository` and `StateStoredRepository` implementations, the
`StateStore`/`StateCommit`/`StoredState`/`StateProjection` core types, and
the `InMemoryStateStore` and `PostgresStateStore` backends. For a complete
runnable demonstration, see `examples/state-stored-order.rs`.

## Choosing a persistence style

Both styles run the exact same aggregate code. What differs is what the
store remembers:

- **Event-sourced** — the store keeps the full history of events. State is
  derived (replayed) from that history. You get an audit trail, time travel,
  retroactive replay into new read models, and crypto-shredding.
- **State-stored** — the store keeps only the *current* state, one row per
  aggregate, versioned for optimistic locking. Events still flow through
  `save()` — they feed in-transaction projections and the outbox — but they
  are **not persisted as a log**. Once the transaction commits, the events
  are gone; only their effects remain.

Pick event sourcing when history is part of your requirements (compliance,
debugging via causation traces, rebuilding read models from day one, GDPR
crypto-shredding). Pick state storage when it is not: you save the
operational cost of an append-only log, snapshots-as-cache, and checkpointed
consumers, and your storage footprint stays proportional to the number of
aggregates instead of the number of changes.

You can also start state-stored and treat the switch to event sourcing as a
data migration later — the domain code will not change. (Migration tooling
between the two styles is explicitly out of scope for now; the point is that
no *code* is throwaway.)

## One codebase, two composition roots

Application code depends only on `R: Repository<A>`:

```rust
async fn add_item<R: Repository<Order>>(
    repo: &R,
    order_id: EntityId,
    item_id: String,
    quantity: u32,
    price: i64,
) -> Result<()> {
    repo.modify(order_id, |order| order.add_item(item_id, quantity, price))
        .await?;
    Ok(())
}
```

The composition root is the single place where the persistence style is
chosen:

```rust
// Event-sourced: streams, snapshots, policies, checkpoints, audit log.
let store = Arc::new(PostgresEventStore::builder().pool(pool.clone()).build()?);
let repo = store.repository::<Order>(); // EventSourcedRepository<_, Order>

// State-stored: one versioned row per aggregate.
let store = Arc::new(PostgresStateStore::builder().pool(pool.clone()).build()?);
store.migrate().await?;
let repo = store.repository::<Order>(); // StateStoredRepository<_, Order>

// Either way, the application code above runs unchanged:
add_item(&repo, order_id, "laptop".into(), 1, 120_000).await?;
```

This works because `AggregateRoot::apply()` applies events **eagerly**: the
aggregate always carries current state alongside its buffered
`pending_events`. The event-sourced repository persists the buffered events;
the state-stored repository serializes the entity itself and uses
`AggregateVersion` as a plain optimistic-lock row version. Concurrency
conflicts surface as the same `ConcurrencyConflict` error in both modes.

## Feature matrix

Be honest with yourself about this table before choosing — the right-hand
column's gaps are structural, not roadmap items:

| Capability | Event-sourced | State-stored |
|---|---|---|
| Aggregates, events, commands, `define_events!`, `command_handler!` | ✅ | ✅ |
| `Repository` trait (`load` / `save` / `modify` / `create_*`) | ✅ | ✅ |
| Optimistic concurrency (`ConcurrencyConflict`) | ✅ | ✅ |
| Claims (cross-aggregate uniqueness) | ✅ | ✅ |
| Deletion lifecycle (`DeletedState`, tombstones) | ✅ | ✅ |
| Schema evolution (`snapshot_version`) | ✅ | ✅ |
| Projections of the **present** (from now on) | ✅ checkpointed | ✅ in-transaction |
| Projections of the **past** (retroactive replay into a new read model) | ✅ | ❌ (backfill from current state only) |
| Durable side effects (policies / external I/O) | ✅ policy outbox | ✅ transactional outbox |
| Audit log, causation tracing, time travel | ✅ | ❌ |
| Crypto-shredding / encrypted aggregates | ✅ | ⏳ parity pending — currently rejected |

"Projections of the present" deserves emphasis: a state-stored read model can
reflect everything that happens *from the moment its projection is
registered*, plus whatever you backfill from current state. It can never
answer "what did this look like in March?" — that information was discarded
at commit time. If a future requirement might need the past, that is an
argument for event sourcing today, because the past cannot be added
retroactively.

## How state storage works

The state store persists one versioned row per aggregate. The row shape
mirrors the existing `Snapshot` shape (id, type, JSON state, deleted flag,
schema version) — but where a snapshot is a *cache* in front of the event
log, a state row is the *source of truth*:

```rust
/// One aggregate's state write (the state-path analog of StreamCommit).
pub struct StateCommit {
    pub state: StoredState,                 // id, type, serialized state,
                                            // new version, is_deleted,
                                            // schema version
    pub expected_version: AggregateVersion, // optimistic lock
    pub events: Vec<EventEnvelope>,         // feed projections + outbox;
                                            // NOT stored as a log
    pub claims: Vec<AggregateClaim>,
    pub clear_claims: bool,
}

/// Primitive current-state persistence.
#[async_trait]
pub trait StateStore: Send + Sync {
    async fn load(&self, id: StreamId) -> Result<Option<StoredState>>;
    async fn save(&self, commit: StateCommit) -> Result<()>;
    async fn save_batch(&self, commits: Vec<StateCommit>) -> Result<()>;
    async fn get_version(&self, id: StreamId) -> Result<AggregateVersion>;
    async fn exists(&self, id: StreamId) -> Result<bool>;
}
```

`StateStoredRepository<S: StateStore, A>` implements `Repository<A>` on top
of these primitives: `load` fetches and deserializes the row, `save` drains
the aggregate's pending events into envelopes, serializes the entity, and
hands the store a `StateCommit`. `save_deleted` writes a tombstone row
(`is_deleted = true` with the serialized `DeletedState`), so the deletion
lifecycle (`load_any`, `load_deleted`, `modify_deleted`) behaves exactly as
in the event-sourced mode.

Note that events are in the `save` signature **from day one**, even with no
projection or outbox registered. They are the hook point — but they live
only for the duration of the transaction.

## In-transaction projections

State-stored read models are built by projection handlers registered on the
concrete store and invoked *inside* the save transaction, with the batch of
events and the backend's transaction handle:

```rust
#[async_trait]
pub trait StateProjection<Ctx: Send>: Send + Sync {
    fn name(&self) -> &str;
    fn filter(&self) -> EventFilter;   // defaults to EventFilter::All
    async fn project(&self, ctx: &mut Ctx, events: &[EventEnvelope]) -> Result<()>;
}

struct OrderTotals;

#[async_trait]
impl StateProjection<sqlx::PgConnection> for OrderTotals {
    fn name(&self) -> &str { "OrderTotals" }

    fn filter(&self) -> EventFilter {
        EventFilter::by_event::<ItemAddedEvent>()
    }

    async fn project(
        &self,
        conn: &mut sqlx::PgConnection,
        events: &[EventEnvelope],
    ) -> Result<()> {
        // UPDATE/INSERT against your read-model table on the save transaction's
        // connection.
        Ok(())
    }
}

let store = PostgresStateStore::builder()
    .pool(pool)
    .with_projection(Arc::new(OrderTotals))
    .build()?;
```

The semantics are deliberately strong:

- **Exactly-once.** The projection runs once per save, in the same
  transaction as the state write. No checkpoints, no replay, no dedup.
- **Read-your-writes.** When `repo.save()` returns, the read model already
  reflects the change. There is no eventual-consistency window to paper over
  in your UI.
- **A failing projection rolls back the command.** The state write, claims,
  and every registered projection commit or roll back together. Your read
  model can never drift from your write model — at the price that a buggy
  projection blocks writes. Keep handlers small and total.
- **DB-local only.** The handler receives a transaction handle into the
  *same database* as the state rows. It cannot — and must not — call
  external services; anything that leaves the database belongs in the
  outbox.

Registration happens on the concrete store's builder, not on the `StateStore`
trait, because transaction handle types differ per backend
(`sqlx::PgConnection` for Postgres, `InMemoryProjectionContext` for the
memory store). The core trait stays primitive. Projections whose filter
matches none of a commit's events are skipped entirely.

### Why there are no after-commit callbacks

A tempting third channel — "just call me after the commit succeeds" — does
not exist in event-sauce, **by design**. An after-commit callback is
at-most-once delivery: if the process crashes between the commit and the
callback (or the callback fails), the effect is lost, and in a state-stored
system there is no log to notice the gap or replay it from. The failure is
silent and unrepairable. Event-sourced systems can afford loosely-coupled
consumers because the log makes redelivery possible; state-stored systems
cannot, so the library only offers the two channels with honest guarantees:
in-transaction projections (exactly-once, DB-local) and the outbox
(at-least-once, durable).

## Transactional outbox

Anything that must outlive the transaction — sending an email, calling a
payment provider, notifying another service, reacting in another aggregate —
goes through the outbox. It is the state-stored sibling of the event-sourced
policy outbox:

1. When the outbox is enabled on the store, `save` writes each event
   envelope into an outbox table **in the same transaction** as the state
   row. If the command rolls back, nothing is enqueued; if it commits, the
   intent is durably recorded.
2. A background dispatcher claims pending rows and invokes your handlers
   with the `EventEnvelope`, honoring a retry policy on failure.
3. Rows are pruned after successful dispatch, so the table stays small.

```rust
let store = PostgresStateStore::builder()
    .pool(pool)
    .with_outbox()
    .build()?;

struct SendReceipt;

#[async_trait]
impl StateOutboxHandler for SendReceipt {
    async fn handle(&self, envelope: &EventEnvelope) -> Result<()> {
        // external I/O here — at-least-once, so make it idempotent
        Ok(())
    }
}

let dispatcher = store
    .outbox()
    .dispatcher(Arc::new(SendReceipt))
    .with_batch_size(50);
loop {
    dispatcher.run_once().await?;
    tokio::time::sleep(std::time::Duration::from_secs(1)).await;
}
```

This is at-least-once delivery: a crash after your handler succeeds but
before the row is deleted redelivers the event. Handlers must be idempotent
(dedup on `event_id`, or make the operation naturally idempotent). That is
the standard outbox trade-off, and it is the *only* trade-off on offer —
see above for why fire-and-forget is not.

## Backfilling state-derived read models

In event-sourced mode, a new projection starts from `Position::start()` and
replays all of history. In state-stored mode there is no history, so adding
a read model after data already exists takes two steps:

1. **Register the projection** so every save from now on keeps it current.
2. **Backfill from current state**: iterate the existing state rows and seed
   the read-model table from each aggregate's present state.

```rust
// Illustrative — a backfill_from_states helper is planned; until it lands,
// hand-roll the seed query against the state table.
let rows = sqlx::query_as::<_, (uuid::Uuid, serde_json::Value)>(
    "SELECT aggregate_id, state FROM aggregate_states
     WHERE aggregate_type = 'Order' AND NOT is_deleted",
)
.fetch_all(&pool)
.await?;

for (id, state) in rows {
    let order: Order = serde_json::from_value(state)?;
    seed_order_totals(&pool, id, &order).await?;
}
```

Be aware of what a backfill can and cannot produce. It can derive anything
computable from *current* state (totals, listings, denormalized views). It
cannot derive anything that needed the discarded events — "orders that were
ever discounted", "average time between add-to-cart and checkout". If a read
model's definition depends on the past, state storage cannot build it, no
matter when you ask.

## Limitations

Stated plainly:

- **No retroactive replay.** Events are discarded at commit. Read models can
  be kept current and backfilled from present state, never rebuilt from
  history.
- **No audit log, no time travel, no causation tracing.** These are
  event-log features; the [audit log guide](audit-log.md) does not apply to
  state-stored aggregates.
- **Encryption parity is pending.** Encrypted aggregates
  (`A::is_encrypted()`) are rejected by the state-stored repository with a
  clear `Error::Encryption` until state-row encryption (and with it,
  crypto-shredding — simple in this mode, since state is one row) lands.
  This is enforced, not a silent gap: an encrypted aggregate will not
  quietly persist in plaintext.
- **In-memory transactionality is best-effort.** `InMemoryStateStore` applies
  state, claims, and projections under one lock and rolls back on projection
  failure, but it is a testing tool, not a transaction manager. Use it so
  app + projection code is testable without Postgres; measure guarantees
  against the Postgres store.
- **Projections are DB-local.** A read model in a different database or
  system is an external effect — feed it through the outbox and accept
  at-least-once semantics.

## See also

- [Architecture Overview](architecture.md) — where `Repository` and the two
  implementations sit in the crate structure
- [Projections](projections.md) — the checkpointed, replayable projection
  model on the event-sourced path
- [Policies](policies.md) — the event-sourced sibling of the outbox
- [Claims](claims.md) — uniqueness constraints, identical in both modes
