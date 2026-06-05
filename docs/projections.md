# Projections (Postgres-Backed, Transactional)

Projections are read models built from event streams. event-sauce ships **only
one** projection model: postgres-backed and transactional. The runner applies
each event and advances the subscription checkpoint **inside the same
database transaction**, so a crash mid-batch never leaves the projection
ahead of (or behind) its checkpoint.

## Why postgres-only?

The previous in-memory `Projection` trait left the projection's writes and
the checkpoint update in separate operations. If the handler succeeded but
the checkpoint save failed (or the process crashed in between), restart
re-applied events that had already been committed — breaking idempotency
for any non-idempotent projection (counters, list appends, totals).

The new model removes that gap by construction. There is no in-memory
projection trait to misuse: every projection writes through a transaction
the runner owns, and the checkpoint upsert rides along.

## The contract

```rust
use event_sauce_postgres::PostgresProjection;
use event_sauce_core::{EventEnvelope, EventFilter, Result};

#[async_trait::async_trait]
pub trait PostgresProjection: Send {
    const NAME: &'static str;

    fn handled_event_types() -> Option<Vec<&'static str>> { None }
    fn event_filter() -> EventFilter { /* derived from handled_event_types */ }

    async fn handle(
        &mut self,
        envelope: &EventEnvelope,
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    ) -> Result<()>;
}
```

- `NAME` doubles as the subscription name used for checkpointing.
- `handled_event_types` filters the stream so `handle` only sees relevant events.
- `handle` writes through the supplied transaction. Don't open your own — the
  runner already did, and your writes need to commit with the checkpoint.

## Defining a projection

```rust
use event_sauce_core::{EventEnvelope, Error, Result};
use event_sauce_postgres::PostgresProjection;

pub struct OrderTotals;

impl OrderTotals {
    /// User-owned migration — call once at startup.
    pub async fn migrate(pool: &sqlx::PgPool) -> Result<()> {
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS order_totals (
                order_id UUID PRIMARY KEY,
                total BIGINT NOT NULL
            )",
        )
        .execute(pool)
        .await
        .map_err(|e| Error::custom(e.to_string()))?;
        Ok(())
    }
}

#[async_trait::async_trait]
impl PostgresProjection for OrderTotals {
    const NAME: &'static str = "OrderTotals";

    fn handled_event_types() -> Option<Vec<&'static str>> {
        Some(vec!["Order.ItemAdded"])
    }

    async fn handle(
        &mut self,
        envelope: &EventEnvelope,
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    ) -> Result<()> {
        let delta: i64 = envelope.event_data.get("price")
            .and_then(|v| v.as_i64())
            .ok_or_else(|| Error::custom("missing price"))?;
        sqlx::query(
            "INSERT INTO order_totals (order_id, total) VALUES ($1, $2)
             ON CONFLICT (order_id) DO UPDATE SET total = order_totals.total + $2",
        )
        .bind(envelope.aggregate_id)
        .bind(delta)
        .execute(&mut **tx)
        .await
        .map_err(|e| Error::custom(e.to_string()))?;
        Ok(())
    }
}
```

## Running the projection

```rust
let backend = event_sauce_postgres::PostgresBackend::setup(
    "postgresql://localhost/events",
    "event_sauce",
).await?;

OrderTotals::migrate(backend.pool()).await?;

let mut projection = OrderTotals;
backend.run_postgres_projection(&mut projection).await?;
```

What the runner does for each matched event:

1. Open a transaction on the backend pool.
2. Call `projection.handle(&event, &mut tx)`.
3. Upsert the subscription checkpoint inside the same transaction.
4. Commit.

If any step returns an error (or the process dies), the transaction rolls
back — neither the projection rows nor the checkpoint advance — and the next
run replays the same event.

## Migrations

Projection table migrations are user-owned. event-sauce manages migrations
for the event log, snapshots, checkpoints, and crypto key store; *your*
projection schema is yours to evolve.

When a schema change (or a projection bug) requires re-deriving the read-model
from scratch, use `PostgresBackend::rebuild` rather than the old manual
procedure. See [Rebuilding a projection](#rebuilding-a-projection) below.

## Filtering

`handled_event_types` is the simple knob: list the event-type strings you
care about and the runner skips everything else without opening a
transaction. For finer-grained selection, override `event_filter` and return
any [`EventFilter`](../crates/event-sauce-core/src/event_filter.rs) directly.

## Resuming after restart

The checkpoint table (managed by `PostgresCheckpointStore`) tracks the last
committed position per `NAME`. On startup the runner loads the checkpoint
and streams from there — no extra wiring required.

The position is the event's real global id (the `events.id` BIGSERIAL), not a
count of processed events — the runner reads it straight off each
`EventLogEntry` rather than reconstructing it. Ids are strictly increasing but
may have gaps (a rolled-back append, e.g. a concurrency conflict, burns a
sequence value); the runner resumes from `WHERE id > checkpoint`, so gaps are
skipped harmlessly and an event is never re-applied because the checkpoint
lagged the id. A checkpoint written before this contract landed is reinterpreted
as an id threshold; in the rare case it sits inside a gap, a bounded tail of
already-seen events may be re-processed once on the next run — always the safe
direction (never a skip).

### Why `WHERE id > checkpoint` is safe: commit-order == id-order

Resuming from an id threshold only works because the event log guarantees:
**once an event with global id `N` is visible to a reader, every event with id
`< N` is already committed and visible.** Without that, a checkpoint reader
could observe a higher id, advance past it, and then never see a lower id whose
transaction was still in flight — a silent, permanent skip.

The `events.id` BIGSERIAL is allocated at `INSERT` but the row only becomes
visible at `COMMIT`. Under `READ COMMITTED`, two concurrent appends to
*different* streams could otherwise take ids `N` and `N+1` yet commit in the
opposite order. The Postgres backend prevents this by taking a
transaction-scoped advisory lock (keyed by the qualified events table) at the
start of every id-allocating `append`, before reading the version and inserting
— so insert order equals commit order. The lock auto-releases at commit/rollback
(`pg_advisory_xact_lock`). The in-memory backend gets the same property for free
by publishing the global position under its append write locks.

Trade-offs to know:

- **Appends serialize across all streams for the insert window only.** Readers
  stay fully concurrent — the advisory lock does not block `SELECT`. Tune the
  wait bound with `PostgresEventStore::builder().append_lock_timeout(..)`
  (default 5s); exceeding it surfaces as a backend error rather than a hang.
- **Claims-only / clear-only appends** (no events) allocate no ids and skip the
  lock entirely, so claim registration is never blocked by event traffic.
- **Ids stay sparse, not dense** — the lock orders *committed* events; it does
  not reclaim values burned by rolled-back appends. Gaps remain, and consumers
  are already gap-tolerant (`WHERE id > checkpoint`).

## Multiple projections

Each projection has its own `NAME` and therefore its own checkpoint row. Run
them as independent calls (the projections do not share transactions with
each other; only with their own checkpoint).

```rust
let mut totals = OrderTotals;
let mut summary = OrderSummary;

tokio::try_join!(
    backend.run_postgres_projection(&mut totals),
    backend.run_postgres_projection(&mut summary),
)?;
```

## Read-your-writes: waiting for a projection

Projections are **eventually** consistent: after you `commit` events, the
projection that builds a read model has not necessarily processed them yet, so
reading the read model immediately can return stale data. When a caller must
read its own write — e.g. return the freshly-updated total to the user who just
placed the order — block until the projection catches up with
`wait_for_checkpoint`.

The flow is *commit → get position → wait → read*:

```rust
use std::time::Duration;
use event_sauce_core::{wait_for_checkpoint, Position};

// 1. Commit produces events; obtain their store-issued position.
repository.commit(order).await?;
let write_position: Position = backend.event_store().max_position().await?;

// 2. Block until the projection's checkpoint reaches that position.
let caught_up = wait_for_checkpoint(
    backend.checkpoint_store(),
    OrderTotals::NAME, // the subscription name == projection NAME
    write_position,
    Duration::from_millis(10), // poll interval
    Duration::from_secs(5),    // timeout
)
.await?;

// 3. Now it is safe to read the read model.
if caught_up {
    let total = OrderTotals::read(backend.pool(), order_id).await?;
}
```

`wait_for_checkpoint` returns `Ok(true)` once the stored checkpoint is at or
past `target` within the timeout, or `Ok(false)` if the timeout elapses first (a
missing checkpoint counts as `Position::start`). It is **polling**-based, which
keeps it portable across every backend. On Postgres a `LISTEN`/`NOTIFY`-driven
variant could wake the poll for lower latency — a future enhancement; the
polling helper is correct on every backend today.

## Testing

Use a `testcontainers` postgres instance, append events through the event
store, and call `run_postgres_projection`. Assert two things: the projection
rows are correct **and** the checkpoint advanced. Then make `handle` fail on
a specific event and assert that neither the row nor the checkpoint moved
past the failure point.

See `crates/event-sauce-postgres/src/backend.rs` (the
`test_run_postgres_projection_*` tests) for working examples.

## Scaling out: standalone workers

`run_postgres_projection` is fine for in-process use, but two instances
calling it on the same projection will race on the checkpoint and
double-apply events. For multi-instance deployments — typically one
projection-worker binary running on N hosts — use
`run_leased_projection`:

```rust
use std::time::Duration;
use event_sauce_postgres::LeaseOutcome;

let worker_id = format!("{}-{}", hostname()?, std::process::id());
let outcome = backend
    .run_leased_projection(&mut OrderTotals, &worker_id, Duration::from_secs(30))
    .await?;
match outcome {
    LeaseOutcome::Completed => { /* this worker drained available events */ }
    LeaseOutcome::Busy => { /* another worker holds the lease; try again */ }
}
```

Three pieces make this safe:

1. **Lease on the checkpoint** — only the worker that holds the lease for
   `P::NAME` is the active processor. Other workers see `Busy` and back off.
   The lease is renewed automatically every `lease_duration / 3`; if the
   active worker dies, the lease expires and another worker takes over.
2. **Fenced checkpoint advance** — the per-event transaction advances the
   checkpoint with a write that is *fenced* on lease ownership and
   monotonicity: it only lands when the row is still leased by this worker,
   the lease has not expired, and the new position strictly advances the
   stored one. Lease renewal happens only *between* events, so if a worker
   stalls (GC pause, slow handle, network hiccup) past `leased_until` while a
   per-event transaction is in flight, another worker can take over and move
   the checkpoint forward. When the stalled worker resumes and tries to
   commit, the fence rejects its write, the runner **rolls the transaction
   back** and returns `Error::LeaseLost { subscription, worker_id }` instead
   of double-applying the event or regressing the checkpoint to a lower
   position. Treat `LeaseLost` as a benign hand-off: stop, drop the lease,
   and let the new owner continue.
3. **`LISTEN`/`NOTIFY` for low-latency wake-up** — between leased runs a
   worker waits on `PostgresEventStore::listen_for_events()` (which yields a
   new `Position` for every committed transaction) instead of polling. New
   events are picked up in milliseconds without burning CPU.

A complete worker loop, including graceful shutdown:

```rust
let listener = backend.event_store().listen_for_events().await?;
tokio::pin!(listener);

loop {
    match backend
        .run_leased_projection(&mut OrderTotals, &worker_id, Duration::from_secs(30))
        .await?
    {
        LeaseOutcome::Completed | LeaseOutcome::Busy => {}
    }

    tokio::select! {
        _ = shutdown.notified() => break,
        // Wake when any event commits.
        _ = futures::StreamExt::next(&mut listener) => {}
        // Belt-and-braces tick to recover from a missed notification.
        () = tokio::time::sleep(Duration::from_secs(1)) => {}
    }
}
```

A complete runnable example lives at
`crates/event-sauce/examples/projection-worker.rs` — it spins up two
competing workers. Under normal operation only the lease holder processes
events; a stalled worker that loses its lease mid-transaction has its write
fenced (rolled back, surfaced as `Error::LeaseLost`), so the checkpoint never
regresses and no event is double-applied across the hand-off.

## Rebuilding a projection

When you need to re-derive a read-model from scratch — a schema change, a fixed
projection bug, or recovering a corrupted table — call
`PostgresBackend::rebuild`:

```rust
use event_sauce_postgres::LeaseOutcome;
use std::time::Duration;

let mut projection = OrderTotals;
match backend.rebuild(&mut projection, &worker_id, Duration::from_secs(30)).await? {
    LeaseOutcome::Completed => { /* table wiped and re-derived from position 0 */ }
    LeaseOutcome::Busy => { /* another worker holds the lease; retry later */ }
}
```

`rebuild` is **lease-guarded** and **atomic**, unlike the old manual procedure
(drop the table by hand, `delete_checkpoint(P::NAME)`, re-run) which had no
concurrency protection and also wiped the lease columns:

1. **Acquire the lease** for `P::NAME`. If another worker is actively running
   against the live model, `rebuild` returns `LeaseOutcome::Busy` **without
   touching** the read-model or the checkpoint — resetting it under a live
   reader would corrupt that worker's view.
2. **Atomic reset.** In a single transaction it calls `reset(&mut tx)` to clear
   the read-model and rewinds the checkpoint to position 0. Both commit (or roll
   back) together, so a rebuild never leaves a wiped table paired with a stale
   checkpoint.
3. **Re-drain from genesis** using the same fenced per-event loop as
   `run_leased_projection`.
4. **Release the lease** on exit, including on error.

To support `rebuild`, a projection **must override** `reset` — the trait method
that clears its rows:

```rust
async fn reset(
    &mut self,
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> Result<()> {
    sqlx::query("TRUNCATE order_totals")
        .execute(&mut **tx)
        .await
        .map_err(|e| Error::custom(e.to_string()))?;
    Ok(())
}
```

The default `reset` returns an error, so calling `rebuild` on a projection that
has not opted in fails loudly rather than silently leaving the table intact
while the checkpoint rewinds.

The step-2 rewind to position 0 is a *backward* checkpoint move, which the
fenced save (`save_checkpoint_fenced_tx`) deliberately rejects to preserve
monotonicity. The rewind therefore uses the **unfenced** `save_checkpoint_tx` —
safe precisely because `rebuild` holds the lease and is the legitimate owner
performing an intentional rewind. The forward re-drain that follows uses the
fenced save as usual, so ordinary runs keep their no-regression guarantee.

## Choosing checkpoint vs. queue

Projections live in the **checkpoint** tier: ordering matters, replay
matters, and write amplification is low (one row per event, regardless of
consumer count). Stick with `PostgresProjection` + `run_leased_projection`.

For *side-effecting* work (sending email, calling external APIs,
fan-out webhooks) where per-event retry, parallel draining, and a DLQ
matter more than ordering, see [policies.md](policies.md) — the
`PostgresPolicyOutbox` is a queue-shaped layer derived from the same
event log, processed via `FOR UPDATE SKIP LOCKED`.
