# Feature Plan: Domain / Event-Sourcing Separation (State-Store Split)

## Goal

Make event-sauce usable in **non-event-sourced applications** with zero changes
to domain code. The only difference between an event-sourced app and a
state-stored (CRUD-style) app must be:

1. which store is constructed at the composition root, and
2. optionally, a cargo feature flag.

Aggregates, events, commands, `define_events!`, `command_handler!`,
`repo.load()` / `repo.modify()` / `repo.save()` — all byte-for-byte identical
in both worlds.

## Why this works today

`AggregateRoot::apply()` applies events **eagerly** to the entity and buffers
them in `pending_events`. The aggregate therefore always carries current
state, so:

- an **event-sourced** repository persists the buffered events (status quo);
- a **state-stored** repository persists the entity itself (`Entity` is
  already `Serialize + DeserializeOwned`), using `AggregateVersion` as a
  plain optimistic-lock row version. `ConcurrencyConflict` semantics carry
  over unchanged.

The existing `Snapshot` row shape (`aggregate_id`, `aggregate_type`,
`snapshot_data`, `is_deleted`, `snapshot_schema_version`) is already the
state-store row; `snapshot_schema_version` is the non-ES analog of upcasting.

## Locked design decisions

- **In-transaction projections** and the **transactional outbox** are two
  separate, explicitly opt-in channels on the state-stored path.
- **No after-commit callbacks anywhere** — at-most-once delivery is
  unrepairable without an event log; rejected.
- Projections: handlers registered on the store, invoked *inside* the save
  transaction with the batch of events + the transaction handle. Exactly-once,
  read-your-writes; a failing projection rolls back the command. DB-local only.
- Outbox: the only mechanism for effects that outlive the transaction
  (policies, external I/O). Events written to an outbox table in the same
  transaction; background dispatcher with retries; rows pruned after dispatch.
- Events are part of the `StateStore::save` signature **from day one**, so the
  hook point exists even before any handler is registered.
- Honest feature matrix to document: state-stored mode = projections of the
  *present* (plus state-derived backfill); audit log and retroactive replay
  remain ES-only.

## Target architecture

```
                    ┌──────────────────────────────────────┐
                    │            Application code           │
                    │  aggregates · events · commands ·     │
                    │  define_events! · command_handler! ·  │
                    │  R: Repository<A>                     │
                    └──────────────┬───────────────────────┘
                                   │  trait Repository<A>
                 ┌─────────────────┴──────────────────┐
                 │                                    │
  EventSourcedRepository<S: EventStore, A>   StateStoredRepository<S: StateStore, A>
                 │                                    │
     InMemoryEventStore / PostgresEventStore   InMemoryStateStore / PostgresStateStore
     (streams, snapshots, policies,            (state rows, in-tx projections,
      checkpoints, audit log, crypto)           outbox dispatcher)
```

### New core items

```rust
/// Persistence-style-agnostic aggregate persistence (the abstraction apps code against).
#[async_trait]
pub trait Repository<A: Aggregate>: Send + Sync {
    // primitives (implemented per persistence style)
    async fn load(&self, id: ...) -> Result<AggregateRoot<A>>;
    async fn load_any(&self, id: ...) -> Result<Loaded<A>>;
    async fn load_deleted(&self, id: ...) -> Result<DeletedAggregateRoot<A>>;
    async fn save(&self, aggregate: &mut AggregateRoot<A>) -> Result<()>;
    async fn save_all(&self, aggregates: &mut [&mut AggregateRoot<A>]) -> Result<()>;
    async fn save_deleted(&self, aggregate: &mut DeletedAggregateRoot<A>) -> Result<()>;
    async fn exists(&self, id: ...) -> Result<bool>;
    async fn get_version(&self, id: ...) -> Result<AggregateVersion>;
    // generic default implementations (per "generic implementation first")
    async fn modify(...) / modify_deleted(...)
    fn create_uninit() / create_uninit_with_id() / create_with() / create_with_id_and()
    fn create() / create_with_id()          // where A: DefaultEntity
}

/// One aggregate's state write (the state-path analog of StreamCommit).
pub struct StateCommit {
    pub state: StoredState,                  // row: id, type, json state, new version,
                                             // is_deleted, schema version
    pub expected_version: AggregateVersion,  // optimistic lock
    pub events: Vec<EventEnvelope>,          // feed projections + outbox; NOT stored as log
    pub claims: Vec<AggregateClaim>,
    pub clear_claims: bool,
}

/// Primitive current-state persistence.
#[async_trait]
pub trait StateStore: Send + Sync {
    async fn load(&self, id: StreamId) -> Result<Option<StoredState>>;
    async fn save(&self, commit: StateCommit) -> Result<()>;
    async fn save_batch(&self, commits: Vec<StateCommit>) -> Result<()> { /* default: loop */ }
    async fn get_version(&self, id: StreamId) -> Result<AggregateVersion> { /* via load */ }
    async fn exists(&self, id: StreamId) -> Result<bool> { /* via load */ }
}

/// In-transaction projection handler; Ctx is the backend's transaction handle.
#[async_trait]
pub trait StateProjection<Ctx: Send>: Send + Sync {
    fn name(&self) -> &str;
    fn filter(&self) -> EventFilter;
    async fn project(&self, ctx: &mut Ctx, events: &[EventEnvelope]) -> Result<()>;
}
```

Registration of projections and outbox activation happen on the concrete
store (builder), because transaction types differ per backend. The core trait
stays primitive.

## Phases

Each phase is TDD (RED → GREEN → REFACTOR), ends fully green
(tests, doc tests, examples, clippy `-D warnings`, fmt, coverage), and is
committed with jj before the next begins.

### Phase 1 — Extract the `Repository<A>` trait  `refactor(core)!`

The single biggest blocker: `Repository<S, A>` is a concrete struct
hard-bound to `S: EventStore` (`repository.rs:40-51`), so there is no
abstraction point to swap persistence styles behind.

1. Introduce `trait Repository<A: Aggregate>` in `repository.rs`
   (`#[async_trait]`, not object-safe — generic methods are fine).
   Trait bounds stay minimal; serde bounds live on impls.
   `modify`/`modify_deleted` and all `create_*` become **default methods**
   implemented via the primitives.
2. Rename the struct to `EventSourcedRepository<S, A>`; implement the trait.
   `EventStore::commit`/`commit_deleted` stay on the store as the ES-specific
   orchestration primitives (the repository delegates to them; ~100 existing
   test/example call sites keep working) — the `Repository` trait is the
   documented app-facing abstraction, and forcing every direct-store call
   site through a repository would be churn without behavior change.
   `EventStore::repository()` remains as the convenient ES constructor.
3. `count_events` is ES-specific → inherent method on
   `EventSourcedRepository` only, not on the trait.
4. Mechanical fallout: update `PolicyContext`, examples, benches, docs that
   call `store.commit(...)` or name the `Repository<S, A>` type.
   No dead code: the old paths are removed, not deprecated.

Exit criteria: workspace green; app-facing docs/examples compile against the
trait; behavior unchanged (pure refactor, breaking API rename).

### Phase 2 — State-store core types + `StateStoredRepository`  `feat(core)`

1. New module `state_store.rs`: `StoredState`, `StateCommit`,
   `trait StateStore` (as sketched above). Row shape mirrors `Snapshot` but is
   its own type — snapshots stay an ES cache; state rows are a source of truth.
2. `AggregateRoot::from_state(entity, version)` — persistence-neutral
   constructor (the existing snapshot path reuses it; `from_snapshot` folds
   into it or delegates).
3. New module `state_repository.rs`: `StateStoredRepository<S: StateStore, A>`
   implementing `Repository<A>`:
   - `load`: fetch row → deserialize entity → `AggregateRoot::from_state`;
     deleted rows load via `load_deleted`/`load_any` (`is_deleted` +
     `DeletedState` deserialization).
   - `save`: drain `pending_events` → envelopes (reusing the existing
     envelope-building logic factored out of `prepare_commit`), serialize
     entity, build `StateCommit` (expected = version - n pending), call
     `store.save`. Poisoned aggregates are rejected exactly like the ES path.
   - `save_all` → `save_batch`; `save_deleted` → tombstone row
     (`is_deleted = true`, serialized `DeletedState`).
   - Encrypted aggregates (`A::is_encrypted()`): rejected with a clear
     `Error::Encryption` until Phase 7 delivers parity. Documented limitation,
     enforced and tested — not a silent gap.
4. `StateProjection<Ctx>` trait in core (`state_projection.rs`), reusing
   `EventFilter` for routing.
5. Schema evolution: `StoredState.schema_version` written from
   `A::snapshot_version()`; version-mismatch handling mirrors the snapshot
   path's behavior.

### Phase 3 — `InMemoryStateStore`  `feat(memory)`

1. `InMemoryStateStore`: `RwLock<HashMap<StreamId, StoredState>>` +
   optimistic version check + claims map (same semantics as the memory event
   store's claim enforcement).
2. In-transaction projections: handlers registered at construction
   (`with_projection`), `Ctx = InMemoryProjectionContext` (a keyed
   `serde_json::Value` map living inside the store). Save applies state,
   claims, and projections under the store's write lock; a projection error
   fails the save and rolls back the in-memory mutation (apply to a clone,
   swap on success). Best-effort transactionality, documented as such — it
   exists so app + projection code is testable without Postgres.
3. Integration tests proving **the same aggregate + command code** runs
   against `InMemoryEventStore` and `InMemoryStateStore` behind
   `R: Repository<A>` (this test IS the feature's acceptance criterion).

### Phase 4 — `PostgresStateStore` + projections + outbox  `feat(postgres)`

1. Migration: `aggregate_states` table
   (`aggregate_type`, `aggregate_id`) PK, `state jsonb`, `version bigint`,
   `is_deleted bool`, `schema_version int`, timestamps — plus reuse of the
   existing claims table, and a `state_outbox` table
   (id, envelope jsonb, available_at, attempts, claimed_by/claimed_until).
2. `PostgresStateStore` (builder pattern like `PostgresBackend`):
   - `save`/`save_batch` in one transaction: version-checked UPSERT +
     claims + registered `StateProjection<PgTransaction>` handlers (filtered
     via `EventFilter`) + outbox INSERTs when outbox is enabled.
   - Projection failure → transaction rollback (test this explicitly).
3. Outbox dispatcher: background worker generalizing the
   `PostgresPolicyOutbox` claim/retry/prune machinery; handlers get the
   `EventEnvelope` + retry policy; rows deleted after successful dispatch.
   Reuses `RetryConfig`/`OnError` shapes from `policy.rs`.
4. Same-code integration test as Phase 3 against Postgres (uses the existing
   `TestDatabase` infra; DB-gated like current postgres tests).

### Phase 5 — Feature flags  `feat`

1. `event-sauce-core` features:
   - `event-sourcing` (default): gates `event_store`, `event_log`,
     `checkpoint`, `policy`, `snapshot_config`, `snapshot_strategy`,
     ES-only crypto helpers, and their re-exports + test fixtures.
   - `state-store` (default): gates `state_store`, `state_repository`,
     `state_projection`.
   - Always-on base: entity/aggregate/apply/lifecycle traits,
     `AggregateRoot` family, `Repository` trait, `EventEnvelope`,
     `DomainEvent`, `EventFilter`, versions, `StreamId`, errors, macros
     (envelopes stay in base — the macros and both paths need them; the
     heavy deps to shed in a state-only build are `base64` and the
     ES modules' machinery). `StreamId`/`Position` move out of
     `event_store.rs` into `types.rs` so the base compiles without ES.
2. Backend crates: `event-sauce-memory` and `event-sauce-postgres` make
   `event-sauce-crypto` optional (`crypto` feature, default on) and grow
   nothing else — each ships both its stores.
3. Facade: `state`/`event-sourcing` features forwarding to core; `full`
   includes both. CI matrix gains `--no-default-features --features state-store`
   and `--no-default-features --features event-sourcing` check jobs.

### Phase 6 — Docs + examples  `docs`

1. New `docs/state-storage.md`: choosing a persistence style, the feature
   matrix (present-vs-past projections table), in-tx projections, outbox,
   backfill guidance, limitations (encryption until Phase 7, no replay).
2. Update `docs/architecture.md` (layer diagram gains the two repository
   impls; also fix the stale `EventStore` sketch), `docs/getting-started.md`,
   `docs/projections.md`, `README.md`.
3. New example `state-stored-order.rs`: one aggregate + commands module used
   twice — once behind the event store, once behind the state store — to
   demonstrate the "only the store differs" property, with an in-tx
   projection and an outbox handler.

### Phase 7 — Follow-ups (separate features, not this branch)

- Encryption parity for the state path (encrypt `state_data` with the
  existing `CryptoProvider`/`CryptoKeyStore` + state AAD; crypto-shredding
  works since state is one row).
- `backfill_from_states` helper for retroactively populating state-derived
  projections.
- Purity pass: split `DomainEvent` into domain/stored halves; move
  `is_encrypted`/`snapshot_version` off `Aggregate`. Deliberately deferred —
  events staying serializable costs little and keeps `define_events!` intact.

## Out of scope

- Migration tooling between the two persistence styles.
- Changing macro syntax or any aggregate-facing API.
- Message-broker integrations (the outbox handler trait is the seam).

## Quality gates (every phase)

```
cargo test --workspace --all-features --all-targets
cargo test --workspace --all-features --doc
cargo build --workspace --all-features --examples
cargo clippy --workspace --all-features --all-targets -- -D warnings
cargo fmt --all -- --check
cargo llvm-cov --workspace --all-features --all-targets --summary-only   # 100%
cargo doc --no-deps --workspace --all-features
```

Work happens on the `state-store-split` bookmark
(`.worktree/state-store-split`), one jj commit per RED/GREEN/REFACTOR step or
coherent feature slice, rebased onto `master` before integration.
