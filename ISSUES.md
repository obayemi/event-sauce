# event-sauce — Known Issues & Fix Designs

**Audit date:** 2026-06-04
**Method:** event-sauce was compared against the opinionated design and explicitly-warned-about "spikes" of Python's [`eventsourcing`](https://eventsourcing.readthedocs.io/) library, across 8 themes (ordering, concurrency, serialization, snapshots, exactly-once, encryption, domain-model, projections). Candidate issues were **adversarially verified against the actual code** before inclusion: **52 candidates → 48 confirmed, 4 refuted.** Each issue below cites `file:line` evidence; each fix design is code-grounded (real signatures, SQL, migration versions) and TDD-oriented (the first test is the regression that fails today).

> **Status context.** As the README states, event-sauce is **not production-ready** and targets single-node use. The most severe findings here (the *Position / gaps-in-sequence* cluster) are latent: the test suite is green because it uses dense, single-threaded, gap-free fixtures. They surface in production under concurrent writers or after any rolled-back append.

---

## The dominant finding (one root cause, 5 criticals)

Python `eventsourcing` is built around one guarantee: the **notification log is a gap-free, commit-ordered, total sequence**, because everything downstream (projections, process applications, exactly-once) is just "read the log past my checkpoint." It pays for that guarantee with a `LOCK TABLE ... IN EXCLUSIVE MODE` around inserts (so insert-order == commit-order) plus atomic tracking records.

event-sauce adopts the notification-log model (`events.id BIGSERIAL` = global `Position`) **but does not reproduce the guarantee**, and — worse — its consumers don't track the real `id` at all. They track a **synthetic `+1` ordinal counter** fed back into a `WHERE id > $checkpoint` query. The result is silent, latent data-correctness bugs (C1–C4) that the gap-free test fixtures hide.

---

## Severity & kind legend

`critical` data loss/corruption · `high` correctness/security in a supported path · `medium` correctness under specific conditions or honesty gaps · `low` papercuts & completeness gaps.
`current-bug` wrong today · `potential-issue` wrong under specific conditions · `design-gap` missing capability/contract · `future-issue` will bite as data ages.

## Index

| ID | Sev | Kind | Theme | Summary | Fix cluster |
|----|-----|------|-------|---------|-------------|
| C1 | ✅ fixed | current-bug | ordering | `Position` has two incompatible meanings across backends; trait documents none | F1 |
| C2 | ✅ fixed | design-gap | ordering | `EventRow`/`EventEnvelope` drop the global `id`, so runners *can't* checkpoint the real position | F1 |
| C3 | ✅ fixed | current-bug | ordering/projections | Runners checkpoint a synthetic `+1` ordinal vs `BIGSERIAL id` → gap re-processing (projections double-apply; new policies re-process history) | F1 |
| C4 | ✅ fixed | current-bug | ordering | No commit-order serialization on `append` → late-committing lower id permanently skipped | F2 |
| H1 | ✅ fixed | current-bug | concurrency | UNIQUE-violation race surfaced as `Error::Backend`, not `ConcurrencyConflict`; documented retry never fires | F3 |
| H2 | ✅ fixed | design-gap | serialization | No upcasting hook — `event_version` written but never read on load; `@version` is write-only | F4 |
| H3 | ✅ fixed | current-bug | encryption | Snapshot encryption fails *open* — silently persists plaintext on key/encrypt failure | F5 |
| H4 | ✅ fixed | design-gap | encryption | Field-encrypted aggregates degrade silently after key deletion (no `KeyNotFound`) | F5 |
| H5 | ✅ fixed | current-bug | domain-model | `load()` on an empty-stream `#[aggregate(init)]` **panics** instead of `NotFound` | F7 |
| H6 | ✅ fixed | current-bug | projections | Projection lease has no fencing token; checkpoint write unconditional & non-monotonic | F6 |
| M1 | ✅ fixed | design-gap | exactly-once | Outbox delivery is at-least-once (effect + `mark_done` not in one tx) | F6 |
| M2 | ✅ fixed | current-bug | exactly-once | In-process `PolicyRunner` = consume-side dual-write; advances checkpoint unconditionally | F6 |
| M3 | ✅ fixed | design-gap | exactly-once | `PolicyContext::flush` has no surrounding tx → multi-aggregate reaction not atomic | F6 |
| M4 | ✅ fixed | design-gap | snapshots | Snapshots have no version/type check; deserialize failure is terminal (no replay fallback) | F4 |
| M5 | ✅ fixed | current-bug | snapshots | `EveryNEvents` snapshots never fire for multi-event commits | F7 |
| M6 | ✅ fixed | design-gap | serialization | One unknown/undeserializable event aborts the whole load; rename orphans history | F4 |
| M7 | ✅ fixed | design-gap | crypto×evolution | Encrypted aggregates can never be schema-migrated (no upcast + no rewrite path) | F4, F5 |
| M8 | ✅ fixed | design-gap | projections | Deprecated core `Subscription` API still public — non-atomic checkpoint, `Retry` that doesn't retry | F7 |
| M9 | ✅ fixed | design-gap | concurrency | No Postgres test for the concurrent-append / UNIQUE-violation path | F3 |
| L1 | ✅ fixed | design-gap | exactly-once | No atomic multi-aggregate save / unit-of-work | F6 |
| L2 | low | design-gap | projections | No read-your-writes `wait(position)` — and un-buildable until Position is fixed | F1 |
| L3 | ✅ fixed | design-gap | domain-model | No deterministic/namespaced (UUIDv5) aggregate IDs | F7 |
| L4 | ✅ fixed | design-gap | audit-log | `EventLogOrder::CreatedAt*` actually order by `id` — misleading names | F7 |
| L5 | ✅ fixed | design-gap | encryption | No AAD binds ciphertext to its event — intra-aggregate relocation/replay possible | F5 |
| L6 | ✅ fixed | future-issue | encryption | No per-key message cap/rotation — relies on the 96-bit random-nonce birthday bound | F5 |
| L7 | ✅ fixed | potential-issue | encryption | `is_encrypted` structural on the literal `"__encrypted"` key — collides with user data | F5 |
| L8 | ✅ fixed | design-gap | serialization | `#[derive(Event)]` applies one `event_version` to all variants (vs per-variant `define_events!`) | F4 |
| L9 | ✅ fixed | design-gap | domain-model | `apply()` purity is doc-only; `post_validate` leaves the entity mutated on failure | F7 |
| L10 | ✅ fixed | design-gap | exactly-once | Outbox `attempts` incremented at claim time → crash-reclaims prematurely DLQ | F6 |
| L11 | ✅ fixed | design-gap | projections | No built-in projection rebuild | F6 |

---

# Detailed findings

## 🔴 Critical — the Position / gaps-in-sequence cluster

These four share one root cause: `Position` is underspecified and the real global `id` is discarded at the read boundary, so consumers track an ordinal instead of the truth. Fixed by **F1** (plumb the real id + checkpoint by it) and **F2** (serialize commit order). Both are required — neither alone is sufficient.

> **✅ C1, C2, C3 fixed (F1).** `EventStore::stream_all` now yields `EventLogEntry { position, envelope }` and `fetch_events_batch` returns `Vec<EventLogEntry>` (postgres `EventRow` gained the BIGSERIAL `id`); all runners (`backend.rs` ×3, `subscription.rs`, `policy.rs`) checkpoint `entry.position` (the real id) instead of a synthetic `+1` counter; new `EventStore::max_position()` (generic default + postgres `MAX(id)` + memory overrides) replaces `resolve_checkpoint`'s row-count; `Position` is now documented as an opaque, store-issued, strictly-monotonic token, and the in-memory/mock stores assign real monotonic positions (id-threshold semantics, not `.skip(n)`). RED regression test: a counting projection no longer re-applies an event after an id gap.
>
> **✅ C4 fixed (F2).** `PostgresEventStore::append` now takes a transaction-scoped `pg_advisory_xact_lock` (keyed by a stable FNV-1a hash of the qualified events table — *not* `DefaultHasher`, whose per-process seed would defeat cross-process locking) before allocating ids, guarded by a configurable `append_lock_timeout` (default 5s, builder option). This serializes the id-assignment→commit window so **id order == commit order** (a reader can never observe id N+1 before N is committed); claims-only appends skip the lock; readers stay fully concurrent. The in-memory backend already had the property via its write locks. RED test: `append` did not block on a held advisory lock before the fix; does after. **The entire critical cluster C1–C4 is now resolved.**

### C1 — `Position` has two incompatible meanings; the trait documents none
- **Evidence:** core trait `crates/event-sauce-core/src/event_store.rs:240-244` (no semantics documented); memory `crates/event-sauce-memory/src/event_store.rs:464` uses `.skip(n)` (offset); postgres `crates/event-sauce-postgres/src/event_store.rs:278,1082` uses `WHERE id > $1` (id threshold). Both backends ship tests asserting *opposite* interpretations are "correct."
- **Impact:** the generic runners were written to the in-memory (dense-ordinal) model; on Postgres the same value means an id threshold. Invisible at the trait boundary; the green suite hides it.

### C2 — `EventRow`/`EventEnvelope` drop the global `id` (the enabling defect)
- **Evidence:** `EventRow` (`event_store.rs:1231-1243`) never SELECTs `id`; `stream_all`/`fetch_events_batch` SELECT lists (`1079-1080`, `275-276`) omit it though they filter/order by it. `EventEnvelope.id` is the *event UUID* (`crates/event-sauce-core/src/event_envelope.rs:257`). The read-only `EventLogRow` path *does* carry it (`event_log.rs:221`), proving the SELECT is cheap.
- **Impact:** runners *cannot* checkpoint the real position even if they wanted to — they can only count.

### C3 — Runners checkpoint a synthetic `+1` ordinal, queried against `BIGSERIAL id`
- **Evidence:** `backend.rs:196,356,417`; `subscription.rs:566,725`; `policy.rs:519-527,581`. The counter equals the real `id` only on a dense `1,2,3…` sequence. Any gap (a rolled-back append burns a sequence value — claim conflicts at `event_store.rs:1024`, partial multi-event appends, crashes) → `WHERE id > counter` **re-selects already-applied events**.
- **Impact:** non-idempotent projections (counters, balances, INSERTs) silently double-apply on the routine optimistic-retry path. `PolicyRunner::resolve_checkpoint` row-*counts* instead of `MAX(id)`, so a brand-new policy that should "skip all history" re-processes part of it. The outbox's `ON CONFLICT (policy_name, event_id) DO NOTHING` (`policy_outbox.rs:175`) shields *policy* delivery from the duplicates; projections have no such guard.

### C4 — No commit-order serialization on `append`
- **Evidence:** `append` runs at default READ COMMITTED with no `LOCK TABLE`/advisory lock (`event_store.rs:950`); `INSERT ... RETURNING id` (`992`). Id allocated at INSERT, visible only at COMMIT.
- **Impact:** txn A grabs `id=N`, B grabs `N+1`, B commits first; a reader sees `N+1`, advances past it; when A commits `N`, `WHERE id > checkpoint` **never returns to it → permanent silent loss**. This is exactly the spike Python's `EXCLUSIVE` lock prevents.

## 🟠 High

### H1 — UNIQUE-violation race surfaced as `Error::Backend`, not `ConcurrencyConflict`
- **Evidence:** under READ COMMITTED both appends pass the `SELECT MAX(stream_version)` precheck (`event_store.rs:962-976`); the loser's INSERT hits `UNIQUE(...stream_version)` and is mapped generically at `:1009`. The claims path *does* translate this (`:836`); the events path doesn't. `is_concurrency_conflict()` (`error.rs:330`) returns false. The documented retry loop (`docs/event-store.md:362`) is written against the in-memory backend (which *does* produce the typed error), so copied code silently never retries on Postgres.
- **✅ Fixed:** the events INSERT loop now matches `is_unique_violation()` and returns `Error::concurrency_conflict(expected, expected+1)` (the violation aborts the tx so the true version can't be re-read; the lower bound `expected+1` keeps `actual > expected`); all other insert errors stay `Error::Backend`. Refactored the precheck and the conflict path to share a `current_stream_version` helper. Regression test + negative guard (M9) added on the testcontainers harness; `docs/event-store.md` retry example retargeted to Postgres with `is_concurrency_conflict()` + jittered backoff + idempotency note.

### H2 — No upcasting: `event_version` is write-only
- **Evidence:** `from_envelope` branches solely on `event_type` then `serde_json::from_value` into the *current* struct (`macros.rs:1268-1280`; default trait `domain_event.rs:125-128`). `event_version` is written (`macros.rs:1227,1262`) and never read on load. The "Event Upcasting" docs (`docs/events.md:805-824`) show a manual helper that nothing calls. Any field-shape change serde-fails historical events.
- **✅ Fixed:** added a default-no-op `DomainEvent::upcast(event_type, from_version, &mut data)` hook (additive, non-breaking) called in **both** `from_envelope` paths (the default trait impl *and* the `define_events!`-generated one) with the **stored `event_version`**, before `serde_json::from_value` — so old payloads are migrated in place on load. `define_events!` gained an optional enum-level `@upcast |event_type, from_version, data| { … }` clause; manual `DomainEvent` impls can override `upcast` directly. `docs/events.md` rewritten to show the real auto-invoked hook (replacing the never-called manual helper); `examples/upcasting.rs` added. RED proven by reverting the wiring (a v1 payload missing a field fails to deserialize; migrates after).

### H3 — Snapshot encryption fails *open* to plaintext
- **Evidence:** `encrypt_snapshot_data` (`event_store.rs:703-724`) only encrypts on `if let Ok(Some(key))`; it drops the `Err` arm and on `encrypt_value` failure emits a `tracing::warn` and persists the **plaintext** full-state snapshot. The *event* path uses `?` and fails closed (`:448-455`). A plaintext snapshot is passed through silently on load and can never be crypto-shredded.
- **✅ Fixed:** `encrypt_snapshot_data` now returns `Result<()>` and fails closed — requires the key store/provider, propagates the `get_key` `Err`, returns `KeyNotFound` on a missing key, and `?`-propagates `encrypt_value` failures; `build_snapshot`/`build_deleted_snapshot` return `Result<Option<Snapshot>>` and the `?` fires before `clear_pending_events`/`flush_prepared`, so a crypto failure aborts the whole commit (no plaintext snapshot ever persisted). A serde *serialize* failure stays best-effort (events already committed). RED test isolates the snapshot path on a field-encrypted aggregate committing a non-encrypted-field event.

### H4 — Field-encrypted aggregates degrade silently after key deletion
- **Evidence:** full-encryption returns `Error::key_not_found` (`event_store.rs:803-811`); field-encryption treats the key as optional (`:812-818`) → `load` succeeds returning `{"__encrypted":…}` blobs, or fails the fully-encrypted snapshot with an opaque `Error::custom` (`:843`) — never `KeyNotFound`. Tests bake this in (`encrypted_aggregate.rs:280` asserts `is_key_not_found()`; `field_encryption.rs:260` asserts only `is_err()`). GDPR-erasure detection via `is_key_not_found()` is mode-dependent.
- **✅ Fixed:** `load_any`'s field-encryption branch now, when the key store is present and `get_key==None`, calls a `has_committed_data` helper (snapshot or any event in the stream); if data exists it returns `Error::key_not_found` (shredded out from under data), else `None` (no data yet → surfaces as `NotFound` post-F7a). Defense-in-depth: if a snapshot is still ciphertext after the decrypt pass, return `KeyNotFound` rather than an opaque serde error. `is_key_not_found()` is now reliable for both encryption modes; the overlapping shred test was tightened from `is_err()` to `is_key_not_found()`.

### H5 — `load()` on an empty-stream init aggregate panics
- **Evidence:** empty stream + no snapshot → `AggregateRoot::new_for_replay(id)` → `A::new(id)`, which for `#[aggregate(init)]` is `panic!` by design (`event_store.rs:876-880`, `entity.rs:82-89`). Reachable from `Repository::load`/`modify` with **any nonexistent/stale id** (no `DefaultEntity` bound on that impl block, `repository.rs:45-51`). `Error::NotFound` already exists. (`TODO.md` already lists "remove code that panics.")
- **✅ Fixed:** the empty-stream branch of `load_any` now returns `Err(Error::not_found(A::aggregate_type(), id))` for **all** aggregates (Rust can't runtime-branch on `A: DefaultEntity` in the generic path, and a synthetic default-state root for a nonexistent id is itself a foot-gun). `new_for_replay` is kept for the legacy non-empty-first-event branch (a non-init first event implies a `DefaultEntity` aggregate). Unified behavior: `load`/`modify` of a missing id → `NotFound`, never a panic, never a synthetic default. Blast radius was one renamed unit test + rustdoc/`docs/event-store.md` updates. RED test (`load_missing.rs`): loading a nonexistent init aggregate panicked before, returns `NotFound` after.

### H6 — Projection lease has no fencing token
- **Evidence:** `save_checkpoint_tx` is an unconditional `ON CONFLICT DO UPDATE SET position=$2` — no `worker_id`/`leased_until`/monotonicity guard (`checkpoint_store.rs:153-189`). Lease renewal happens only *between* events (`backend.rs:419`); a stalled worker past `leased_until` still commits its in-flight tx, **double-applying** and even moving the checkpoint **backward**. `docs/projections.md:236` claims "exactly-once-across-workers holds."
- **✅ Fixed:** new postgres-inherent `save_checkpoint_fenced_tx(tx, name, worker_id, position) -> Result<bool>` whose `DO UPDATE` is fenced `WHERE worker_id = $3 AND leased_until > NOW() AND EXCLUDED.position > position`; the two leased runners (`run_under_lease`, `dispatch_under_lease`) use it and, on `Ok(false)`, **roll back the per-event tx and return `Error::LeaseLost`** instead of committing — a stalled/expired-lease worker can no longer double-apply or regress the checkpoint. The unfenced `save_checkpoint_tx` is kept for the documented single-instance unleased runner (`CheckpointStore` trait + in-memory impl untouched). New `Error::LeaseLost`/`lease_lost()`/`is_lease_lost()`. RED proven by reverting the fence (both tests fail). `docs/projections.md` corrected. *(Note: `Error` is not `#[non_exhaustive]`; the variant was added pre-1.0 as prior variants were. `renew_lease` still signals loss via `Error::custom` — cosmetic, could unify later.)*

## 🟡 Medium

- **M1 — Outbox delivery is at-least-once.** Side effect and `mark_done` are separate statements (`policy_outbox.rs:253`), so a crash/lease-expiry double-fires — yet the outbox is sold for *non-idempotent* effects. **✅ Fixed (docs/contract):** the module docs + `docs/policies.md` now state the at-least-once contract loudly and require idempotent handlers (the previous "use the outbox for non-idempotent effects" claim was inverted and is corrected). A transactional drain (handler in the same tx as `mark_done`) is documented as future work — it's intractable to express generically pre-stable-async-closures; callers needing exactly-once DB effects should write in the same tx as the projection runner. **L10 fixed below** removes the premature-DLQ hazard.
- **M2 — In-process `PolicyRunner` is a consume-side dual-write.** Produced events and the checkpoint are separate writes; the checkpoint advances **unconditionally** including for `OnError::Skip` and non-matching events (`policy.rs:509-528,577-644`); saved only at loop end, so `OnError::Fail` on event N re-runs 1..N-1. **✅ Fixed:** the per-policy checkpoint is now persisted up to the **last fully-handled** event before returning on failure — on **both** the direct `OnError::Fail` arm and the `OnError::Retry` → exhausted → `Fail` path (extracted a shared `save_checkpoints` helper) — so a re-run resumes at the failing event and never replays already-flushed effects. `Skip`/non-matching still advance (documented: `Skip` permanently skips, `Fail` resumes at the event). The events+checkpoint dual-write remains by design (in-process runner is at-least-once on the consume side; handlers should be idempotent — `docs/policies.md` updated). RED: a 2-event batch where event 1 succeeds and event 2 fails (regression tests for both the `Fail` and the `Retry`-exhausted paths).
- **M3 — `PolicyContext::flush` has no surrounding transaction.** Multiple buffered aggregate commits applied one-by-one (`policy.rs:326-332`) → a cross-aggregate reaction ("move funds between two accounts") is not atomic. **✅ Fixed (with L1):** added a public `StreamCommit` and `EventStore::append_batch(Vec<StreamCommit>)` — generic default loops `append` (non-atomic, documented), postgres override commits **all** streams in one transaction (one advisory lock, per-stream version check + inserts + claims, single `NOTIFY`; first conflict rolls back the whole batch). `PolicyContext::flush` now drains all buffered commits and calls `append_batch` once → multi-aggregate reactions are atomic on postgres. `Repository::save_all` exposes the same. The postgres `append` body was factored into shared `&mut tx` helpers (`acquire_append_lock`/`write_commit_in_tx`/`notify_inserted`) preserving F1/F2/F3 — guarded by the full 112-test postgres suite. RED: flush of two aggregates where the second conflicts now rolls back the first (proven by revert).
- **M4 — Snapshots: no version/type check, no replay fallback.** Deserialized blind (`event_store.rs:823-848`); a struct-shape change silently corrupts or makes the aggregate **unloadable**; a deserialize failure is **terminal** — load never falls back to full replay. **✅ Fixed:** added `Aggregate::snapshot_version() -> u32` (default 0; `#[aggregate(snapshot_version = N)]` attr) and a `snapshot_schema_version` field on `Snapshot` (nullable BIGINT via new postgres migration #4, `NULL→0`). On load, a snapshot whose `aggregate_type` mismatches, whose schema version `!=` the aggregate's, **or** that fails to deserialize is treated as a **cache miss** → falls through to full event replay (self-healing; next commit writes a fresh snapshot). Decrypt-then-`KeyNotFound` for crypto-shredded snapshots is checked **before** the fall-through (F5 preserved). RED: an undeserializable snapshot self-heals via replay; a schema-version-mismatched snapshot (sentinel value replay never produces) is skipped.
- **M5 — `EveryNEvents` never fires for multi-event commits.** Exact-boundary check (`snapshot_strategy.rs:213-219`) is stepped over when a commit appends several events; default is `EveryNEvents(100)` → multi-event aggregates never snapshot → unbounded replay growth. **✅ Fixed** (non-breaking): added `SnapshotStrategy::should_snapshot_range(previous, current)` (defaults to `should_snapshot(current)`), overridden in `EveryNEvents` to fire on interval *crossing* (`current/N > previous/N`); both commit call sites now pass `(expected_version, current_version)`.
- **M6 — One bad event aborts the whole load.** A single unknown `event_type` (e.g. renamed variant) or one drifted-shape legacy event fails the entire load (`event_store.rs:850-913`) with no skip/remap — Python's `TopicError` outage, but harsher (no `register_topic`). **✅ Fixed:** added a per-variant `@aliases("Old.Name", …)` clause to `define_events!` — `from_envelope` now matches the canonical `EVENT_TYPE` **or** any alias, so renaming an aggregate/variant stays load-compatible by keeping the old wire name as an alias (the analogue of `register_topic`). Fail-fast on a genuinely-unknown `event_type` is preserved by design (no silent skip). Drifted-shape recovery is covered by the H2 upcasting hook. (For `#[derive(Event)]`, variant selection is by serde tag not `event_type`, so rename-safety there is via `#[serde(alias)]`; an `event_type` alias would be inert and was deliberately not added.)
- **M7 — Encryption × schema-evolution are mutually hostile.** No upcast hook *and* no decrypt→transform→re-encrypt primitive, so turning on encryption forfeits the ability to evolve the event schema; surfaces as an unrecoverable load long after the change ships. **✅ Fixed:** resolved by H2 — the load path decrypts **before** `from_envelope`, so the upcast hook runs on recovered plaintext (`decrypt → upcast → deserialize`); encrypted aggregates are read-time schema-migratable with **no** ciphertext rewrite needed. Verified by `encrypted_upcasting.rs` (encrypted v1 payload missing a field migrates on load; current payload untouched; **crypto-shredded stays `KeyNotFound`** — no erasure back door; proven meaningful by reverting the decrypt-before-upcast ordering). `docs/privacy.md` documents this + the shred caveat. The in-place ciphertext rewrite (re-key / physically drop a removed field's plaintext) mutates the immutable log and is documented as deliberate future maintenance work, not a casual API.
- **M8 — The deprecated core `Subscription` API still ships a foot-gun.** Still public/exported, non-atomic checkpoint, `Retry` that doesn't retry (`subscription.rs:541-779`). **✅ Fixed:** deleted `subscription.rs` (the `Subscription`/`SubscriptionBuilder`/`SubscriptionConfig`/`CheckpointStrategy`/`ErrorPolicy` types, `run()`/`into_stream()`/old `rebuild()`, and `EventStore`/`PostgresBackend::subscription_builder`). The load-bearing `EventFilter` and `CheckpointStore` were moved to their own modules and re-exported at the **same** public paths (downstream unaffected); the unused `async-stream` core dep was dropped. Dead subscription-only tests removed; the surviving primitives (`stream_all`, checkpoints, `PostgresProjection`) keep their coverage. Dangling-reference sweep is clean.
- **M9 — No Postgres test for the concurrent-append / UNIQUE-violation path** (only sequential, `event_store.rs:1453`) — that gap is why H1 shipped. **✅ Fixed** with H1: `test_unique_violation_race_is_concurrency_conflict` (deterministic interleave via a held-open seeding tx) + `test_non_unique_insert_failure_is_backend` (guards the `is_unique_violation` narrowing).

## ⚪ Low & completeness gaps

- **L1** No atomic multi-aggregate save / unit-of-work (Python `Application.save`) — `repository.rs:69,116`, `event_store.rs:224` are single-stream.
- **L2** No read-your-writes `wait(position)` — and un-buildable until C1/C2 unify the position space.
- **L3** No deterministic/namespaced (UUIDv5) aggregate IDs. **✅ Fixed:** added `EntityId::from_namespace(ns, name)` and `from_namespace_bytes` (UUIDv5; enabled the `uuid` `v5` feature) for idempotent creation / natural-key lookup; `EntityId::new()` stays random.
- **L4** `EventLogOrder::CreatedAtDesc/Asc` actually emit `ORDER BY id` (`event_log.rs:56-87`) — misleading; `from_date/to_date` filter real `created_at` (which is `Utc::now()`, non-monotonic). **✅ Fixed:** renamed the variants to `ByIdDesc`/`ByIdAsc` (the `order_sql()` was already `id`-ordered; `#[default]` kept on `ByIdDesc`); enum docs now state `id` (insertion order) is the canonical monotonic chronological ordering and `created_at` is informational/non-monotonic. All consumers + `docs/audit-log.md` updated.
- **L5** AES-GCM uses no AAD (`event-sauce-crypto/src/lib.rs:67-100`) — a ciphertext can be relocated/replayed onto another event row of the same aggregate and still authenticate. **✅ Fixed:** `CryptoProvider::encrypt/decrypt` (and `encrypt_value`/`decrypt_value`/field helpers) now take an `aad` bound to `aggregate_id || event_id` (events), `aggregate_id || "snap"` (snapshots), and `|| field_name` for field-level — so a relocated/replayed ciphertext fails authentication on load. A **versioned envelope** (`__enc_v`) keeps already-stored ciphertext readable: absent/`1` → empty-AAD legacy path, `2` → bound AAD (mixed-version datasets stay readable; only fresh writes adopt it). `is_encrypted` was relaxed to accept the `__enc_v` companion (F5 invariants preserved). RED proven by revert (relocation + legacy-version gating).
- **L6** One key per aggregate for life, no rotation/cap (`event_store.rs:730-746`) — relies on the 96-bit random-nonce birthday bound (~2³² messages). **✅ Fixed (docs):** `docs/privacy.md` now documents the per-key message ceiling (stay well under ~2³² encryptions/key), the deliberate no-rotation stance, the AAD/versioned-envelope contract, and the reserved `__encrypted`/`__enc_v` keys, and sketches the future direction (opt-in `rotate_key`/re-encryption, or adopting XChaCha20-Poly1305 / AES-GCM-SIV).
- **L7** `is_encrypted` just checks for a `"__encrypted"` key (`crypto.rs:116-127`); doesn't require it be the sole key or a string → collides with user data.
- **L8** `#[derive(Event)]` applies one `#[event(version=N)]` to all variants (`lib.rs:106-127`); `define_events!` is per-variant. **✅ Fixed:** `#[derive(Event)]` now reads an optional per-variant `#[event(version = N)]`, falling back to the container version — matching `define_events!`'s per-variant model (and the H2 upcasting story, which keys on the stored `from_version`).
- **L9** `apply()` purity is doc-only; `post_validate` runs after `apply` mutates the entity (`aggregate_root.rs:186-196`) → a rejected event leaves a reusable, inconsistent `AggregateRoot`. **✅ Fixed:** `AggregateRoot` (and `DeletedAggregateRoot`) now carry a `poisoned` flag set on any failed `apply`/`apply_with_actor`/`apply_with_metadata`; `prepare_commit`/`prepare_commit_deleted` return `Error::InvalidState` for a poisoned root, so a rejected event can no longer be silently committed — the caller must discard and reload. Hardened the `ApplyEvent::apply`/`AggregateRoot::apply*` rustdoc + `docs/validation.md`: `apply` must be pure/total/deterministic and a failed apply poisons the root. RED proven by revert.
- **L10** Outbox `attempts` incremented at *claim* time (`policy_outbox.rs:219`) → crash-reclaims count toward `max_attempts`, prematurely DLQ'ing never-failed work. **✅ Fixed:** added a `failures` column (migration); the DLQ decision now keys off `failures` (incremented only in `mark_failed`), while `attempts` stays a claim/delivery counter. RED: a row reclaimed 3× without any handler failure is no longer DLQ'd; it goes to `failed` only on the 2nd real failure with `max_attempts=2`.
- **L11** No built-in projection rebuild; the manual procedure drops table + checkpoint with no concurrency guard (`docs/projections.md:126-135`). **✅ Fixed:** added `PostgresProjection::reset(&mut tx)` (default = loud `invalid_state` error so a projection must opt in) and `PostgresBackend::rebuild` — acquires the lease (returns `Busy` without touching state if held elsewhere), in one tx calls `reset()` + rewinds the checkpoint to start (via the **unfenced** save, safe because it holds the lease — the H6 fence rejects backward moves), then re-drains from genesis through the fenced loop. RED proven by revert (removing reset+rewind fails the tests; the busy-test correctly stays green). `docs/projections.md` replaces the manual drop+`delete_checkpoint` procedure.

---

# Fix designs

Each cluster is a self-contained change set; the global remediation order is at the bottom. **All fix designs are TDD: the first test listed is the regression that fails today.**

## F1 — Plumb the real global id & checkpoint by it (C2, C3, new-policy half of C1, L2) · effort **L**

**Root cause.** Two conflated notions of position: the authoritative store-issued `events.id BIGSERIAL`, vs a client-side `+1` ordinal that runners reconstruct and feed back into `WHERE id > $1`. Correct only when ids are dense from 1.

**Approach.**
1. **Surface the id.** Change `EventStore::stream_all` and `PostgresEventStore::fetch_events_batch` to yield/return the **existing** `EventLogEntry { position, envelope }` (`event_log.rs:131`) instead of bare `EventEnvelope`. Deliberately reuse `EventLogEntry` rather than add a `position` field to `EventEnvelope` — `EventEnvelope` is the `append` *input* (position is unknown pre-INSERT), is serde-persisted, and adding a field breaks `new()`, all `From<EventRow>` mappers, and on-disk payloads. Add `id` to `EventRow` and both streaming SELECTs.
2. **Checkpoint the real id.** At all 6 runner sites, delete `current = Position::new(current.as_i64()+1)`; set `current = entry.position` after a successful handle/commit. Outbox enqueue uses `entry.position.as_i64()`.
3. **`max_position()` primitive.** Add `EventStore::max_position() -> Result<Position>` (generic default: stream once, take last; Postgres override: `SELECT COALESCE(MAX(id),0)`). Rewrite `resolve_checkpoint` to call it — no more row-counting.
4. **Contract + memory reconciliation.** Document `Position` as an opaque, monotonic, store-issued token; `stream_all(p)` yields entries with `position > p`. Make the in-memory store assign a real monotonic position (`AtomicI64`, inside the existing append write-lock) and filter `position > from` instead of `.skip(n)`; same for the `MockEventStore` in `test_fixtures.rs`.
5. Leave `load_stream` untouched (it orders by `stream_version`, yields `EventEnvelope`).

**Migration:** none — `events.id` already exists and is populated for all historical rows.
**Breaking:** `stream_all`/`fetch_events_batch` return-item type changes (`EventEnvelope` → `EventLogEntry`); callers use `entry.envelope` and gain `entry.position`. `max_position` is additive. `EventEnvelope` unchanged.
**Backward-compat:** old synthetic-ordinal checkpoints are interpreted as id thresholds post-upgrade; where gaps exist, the first run re-processes a *bounded* tail (the safe direction — never skips). Document that operators wanting zero re-processing set the checkpoint to a known `MAX(id)` or rebuild.
**RED test:** Postgres, gapped sequence — force a gap (an append that aborts after the sequence advanced, e.g. a `ConcurrencyConflict`, or insert+rollback to make ids `{1,2,5,6,7}`), run a projection that records every `envelope.id` it handles, drive to completion twice, assert each id handled **exactly once**. Fails today (post-gap tail re-runs).
**Ordering:** foundational; establishes the authoritative-position contract everything else depends on.

## F2 — Serialize commit order on append (C4) · effort **M**

**Root cause.** `append` does not serialize the id-assignment-to-commit window, so commit order can differ from id order; the `WHERE id > checkpoint` reader then skips a late-committing lower id.

**Approach.** Take a **transaction-scoped advisory lock** at the start of `append` (when `!events.is_empty()`), before the version SELECT: `SELECT pg_advisory_xact_lock($key)` where `$key` is a **stable** 64-bit FNV-1a hash of the qualified events-table name (per-schema isolation; *not* `DefaultHasher` — its seed is randomized per process, which would defeat the lock). Auto-releases on commit/rollback. Guard with a configurable `SET LOCAL lock_timeout` (new builder option `append_lock_timeout`, default 5s) so a stuck appender fails fast (`55P03`) rather than hanging.
- Chosen over `LOCK TABLE ... EXCLUSIVE` (heavier; touches the lock manager / conflicts with maintenance) and over a reader-side watermark (which is *blocked on F1* to even be expressible).
- **Documented guarantee:** "once event id N is visible to a reader, all events with id < N are committed and visible; id order == commit order." (Does *not* make ids dense — rolled-back appends still leave gaps, which is exactly why **F1 is still required**.)
- In-memory backend already has this (append holds both write locks); add a doc comment + parallel contract test.

**Migration:** none. **Breaking:** none to public signatures; additive builder option; under heavy contention `append` may now return a `lock_timeout` backend error (documented; recommend a retry).
**Trade-off:** serializes *all* appends across streams for the insert window — write throughput bounded by single-appender latency. This is the standard, correct event-sourcing trade (Python does the same); readers stay fully concurrent.
**RED test:** open two hand-rolled transactions on the same pool, INSERT event for stream A (id N) and stream B (id N+1), commit B then A; run a count-position reader and assert it collects **both** event ids. Fails today. Plus a 32-task concurrent-append test asserting no committed event is missing.
**Ordering:** can land first (stops C4 immediately with zero read-side changes); remains necessary after F1 (it's what makes "highest id seen" a safe high-water mark).

## F3 — Surface the UNIQUE-violation race as `ConcurrencyConflict` (H1, M9) · effort **M**

**Approach.** In the events INSERT loop, match the result instead of mapping all errors generically: on `Err(sqlx::Error::Database(db_err)) if db_err.is_unique_violation()` → re-SELECT committed `MAX(stream_version)` in the (about-to-roll-back) tx and return `Error::concurrency_conflict(expected, actual)`; keep generic `Error::backend` for everything else. The branch lives **inside** the loop (a multi-event batch can conflict on any event). Extract a `current_stream_version(tx, …)` helper shared by the precheck and the loser path (DRY). Mirrors the existing claims path at `event_store.rs:836`. No trait/schema change; memory backend already returns the typed error.
**Breaking:** behavioral only — Postgres now returns `ConcurrencyConflict` instead of `Backend` for genuine interleaved writers (brings it in line with the docs and the memory backend).
**RED test:** seed an uncommitted winner INSERT at `stream_version=0` in a held-open tx; concurrently call the real `append()` at `expected_version=initial()` so its precheck passes and its INSERT blocks on the unique index; commit the winner; assert `err.is_concurrency_conflict()`. Fails today (maps to `Backend`). Add a non-unique-violation negative test (still `Backend`) and update `docs/event-store.md` to demo against Postgres with jittered backoff + an idempotency note.
**Ordering:** standalone. (Note: F2's advisory lock makes this race largely unreachable via the precheck, but the translation remains correct and defensive for other gap sources.)

## F4 — Schema evolution: upcasting, snapshot versioning + replay fallback, rename safety (H2, M4, M6, M7, L8) · effort **L**

**Approach.**
- **(H2) Upcast hook.** Add `DomainEvent::upcast(event_type: &str, from: EventVersion, data: &mut serde_json::Value) {}` (default no-op); call it in **both** `from_envelope` sites (`domain_event.rs:125`, `macros.rs:1268-1280`) before `serde_json::from_value`. Type-directed (the enum owns its migrations), no new `EventStore` state. Because decryption already mutates `event_data` to plaintext before `from_envelope`, upcast works for encrypted aggregates too.
- **(M4) Snapshot fallback.** Add `Aggregate::snapshot_version() -> u32 { 0 }` and a `snapshot_schema_version` field on `Snapshot` (nullable BIGINT column, new Postgres migration `20_250_401_000_000`). On load, `aggregate_type` mismatch OR version mismatch OR a deserialize `Err` → `tracing::warn` + **fall through to full replay** instead of `?`-erroring. Self-healing; the next commit rewrites a fresh snapshot.
- **(M6) Rename safety.** Keep fail-fast as default. Add compile-time aliases — per-variant `@type_name("Old.Name")` in `define_events!` / `aliases("…")` in `#[event(...)]` — so `from_envelope` matches the canonical `EVENT_TYPE` *or* an alias. Chosen over a mutable runtime registry (compile-time, no `EventStore` state, symmetric with the existing `#[aggregate(type_name=…)]`).
- **(M7) Encrypted rewrite primitive.** Add an `EventStore::rewrite_stream_events` default method (decrypt → transform → re-encrypt → persist in place) as a maintenance tool; document that read-time upcast already covers encrypted streams while the key is live.
- **(L8) Reconcile versioning.** Make `#[derive(Event)]` accept per-variant `#[event(version=N)]` (additive; container default still works), matching `define_events!`.

**Breaking:** `Snapshot` gains a field — keep `new`/`new_deleted` defaulting it to 0 and add `new_with_schema_version`, so no source break. `DomainEvent::upcast` / `Aggregate::snapshot_version` are defaulted (additive). `EventEnvelope` unchanged.
**RED test:** store a v1 envelope (`event_version=1`) whose JSON lacks a field the current v2 struct requires; load → fails today at `from_envelope`; after the hook (which injects the field for `from==1`) load succeeds. Plus: a stale/undeserializable snapshot falls back to replay (today it errors).
**Ordering:** (1) upcast hook + RED, (2) snapshot version + fallback, (3) Postgres migration, (4) derive/define reconciliation (parallel), (5) aliases, (6) rewrite primitive. Update `docs/events.md` + `docs/privacy.md` in the same commits.

## F5 — Encryption: fail-closed snapshots, unified shred contract, AAD, nonce/rotation (H3, H4, L5, L6, L7) · effort **L**

**Approach (all fail-closed).**
- **(H3)** `encrypt_snapshot_data` → `Result<()>`; propagate key-store error, missing key, and `encrypt_value` failure with `?`. `build_snapshot`/`build_deleted_snapshot` → `Result<Option<Snapshot>>`. For an encrypted/field-encrypted aggregate, an un-encryptable snapshot is a **hard error** that aborts the commit — never plaintext. (A serde *serialize* failure stays warn+None; the events are already durable.)
- **(H4)** `require_key_if_shredded` helper: for field-encrypted aggregates, when the key store is present and `get_key()==None` **and committed data exists** (events or snapshot), return `Error::key_not_found`; only genuinely-no-data returns `None`. Also harden the encrypted-snapshot path to return `KeyNotFound` rather than a `from_value` `Error::custom`. Makes `is_key_not_found()` reliable in both modes.
- **(L5/L6) AAD + versioned envelope.** Add `aad: &[u8]` to `CryptoProvider::encrypt/decrypt` and `encrypt_value/decrypt_value`, binding `aggregate_id || event_id`. Because changing AAD breaks old ciphertext, version the marker: write `{"__encrypted":…,"__enc_v":2}`; on decrypt, legacy (no `__enc_v`) decrypts with empty AAD, v2 with the bound AAD. (stream_version is *not* in the AAD — unknown at encrypt time; the per-event UUID already defeats relocation.) Document the per-key message ceiling and sketch an opt-in `rotate_key` / XChaCha20-Poly1305 / AES-GCM-SIV path.
- **(L7)** Tighten `is_encrypted` to require an object whose sole data key is `__encrypted` (+ optional `__enc_v`) with a string value; reserve the marker at commit time.

**Migration:** none (versioning lives in the JSON marker).
**Breaking:** `CryptoProvider::encrypt/decrypt` and the `encrypt_value`/`decrypt_value`/`encrypt_fields` free fns gain `aad` (provide empty-AAD shims for trivial callers); `is_encrypted` tightens; `commit`/`load` can now return `KeyNotFound`/`Encryption` where they previously leaked plaintext / returned blobs. `EventEnvelope` unchanged.
**Backward-compat:** legacy ciphertext (no `__enc_v`) decrypts with empty AAD — still readable. Already-leaked plaintext snapshots still load (and should be deleted by operators to force re-encryption).
**RED tests:** failing-encrypt provider + `SnapshotConfig::always()` → commit returns `Err` and no plaintext snapshot exists (fails today); shredded field-encrypted aggregate → `is_key_not_found()` (fails today); relocated ciphertext fails auth with AAD (fails today).
**Ordering:** land AAD+version+`is_encrypted` first (so H3/H4 use final signatures), then fail-closed snapshots, then shred unification, then docs.

## F6 — Delivery semantics: lease fencing, atomic processing, multi-aggregate atomicity (H6, M1, M2, M3, L1, L10, L11) · effort **L**

**Approach.**
- **(H6) Fencing.** `PostgresCheckpointStore::save_checkpoint_tx` gains `worker_id: &str` and returns `Result<bool>`; the upsert fences: `… DO UPDATE SET position=EXCLUDED.position … WHERE checkpoints.worker_id = EXCLUDED.worker_id AND checkpoints.leased_until > NOW() AND EXCLUDED.position > checkpoints.position RETURNING position`. In both runner loops, a `false` return → **roll back** the tx and return a new `Error::LeaseLost` instead of committing. No migration (`worker_id`/`leased_until` already exist).
- **(L1/M3) Multi-aggregate atomicity.** Add `EventStore::append_batch(Vec<StreamCommit>)` (generic default loops `append`; Postgres override does all version checks + inserts + claims in one tx). Route `PolicyContext::flush` through it (one tx for all aggregates a handler touched); expose `Repository::save_all`.
- **(M2) In-process runner.** Stop the unconditional advance; advance only past non-matching / matched-Ok / explicitly-recorded-Skip events; in `OnError::Fail`, save checkpoints up to the last successfully-handled position before returning (so a re-run doesn't replay flushed effects).
- **(M1/L10) Outbox.** Document at-least-once loudly + require idempotent handlers; add an opt-in transactional `drain_batch_tx` where the handler runs in the same tx as `mark_done` (exactly-once for same-DB effects). Add a `failures` column (migration `20_260_605_000_001`); `mark_failed` increments/compares `failures`, not `attempts` (which stays a claim counter).
- **(L11) Rebuild.** `PostgresBackend::rebuild` acquires the lease, resets table + checkpoint in one fenced tx, drains from 0; add `PostgresProjection::reset(&mut tx)` (defaulted).

**Breaking:** `save_checkpoint_tx` signature; new `Error::LeaseLost` (add `#[non_exhaustive]` to `Error` if absent); `append_batch`/`StreamCommit`/`save_all`/`rebuild`/`reset` additive (defaults). `EventEnvelope` untouched.
**RED test:** worker-1 advances checkpoint to 50; force lease handover (expire `leased_until`); worker-2 takes lease, advances to 60; stalled worker-1 calls `save_checkpoint_tx("worker-1", 51)` → asserts `Ok(false)` and checkpoint stays 60 (fails today — no `worker_id` arg, unconditional write). Plus a two-account `flush` atomicity test (one side conflicts → neither persists).
**Ordering:** `LeaseLost` → fencing (+RED) → `append_batch` → `flush`/`save_all` → in-process runner → outbox `failures`+drain → rebuild → docs. Fix `docs/projections.md:236`'s false "exactly-once" claim.

## F7 — Panic-on-load, snapshot boundary, deprecated API, contract gaps (H5, M5, M8, L3, L4, L9) · effort **L**

**Approach (five independent commits).**
- **(H5)** In `load_any`, replace the empty-stream branch with `Err(Error::not_found(A::aggregate_type(), id))` for **all** aggregates (an empty stream means "doesn't exist"; the synthetic default-state root was always a foot-gun). Move `new_for_replay` into the `impl<A: Aggregate + DefaultEntity>` block; the non-init-first-event invariant keeps the legacy branch safe (add a `debug_assert`).
- **(M5)** Change `SnapshotStrategy::should_snapshot(previous, current)`; `EveryNEvents(n)` fires iff `current/n > previous/n` (crossing, not exact landing). Pass `previous = expected_version`, `current = aggregate.version()` at both call sites. Upgrade the swallowed `save_snapshot` failure from `warn` to `error` with stable metric fields (keep it non-fatal — a snapshot is an optimization).
- **(M8)** **Remove** the deprecated `Subscription`/`SubscriptionBuilder`/`SubscriptionConfig`/`CheckpointStrategy`/`ErrorPolicy` + `subscription_builder`, per the no-dead-code rule — but **preserve `EventFilter`** (move to `event_filter.rs`, re-export at the same path; it's required by `Policy::event_filter`, `policy.rs:97`) and `CheckpointStore`.
- **(L4)** Rename `EventLogOrder::CreatedAtDesc/Asc` → `ByIdDesc/ByIdAsc` (the SQL is already `id`-ordered); document that `id` is the canonical chronological order (`created_at` is non-monotonic).
- **(L9)** Add a `poisoned: bool` to `AggregateRoot`, set on any `apply()` `Err`; `apply*`/commit-prep then return `Error::InvalidState` instead of operating on a half-mutated entity. Strengthen the `apply`/`post_validate` rustdoc (must be pure/total/deterministic; discard the root on `Err`).
- **(L3)** Add `EntityId::from_namespace(ns, name)` (UUIDv5) — additive (enable `uuid`'s `v5` feature).

**Migration:** none.
**Breaking:** `SnapshotStrategy::should_snapshot` signature; removal of the `Subscription*` types + `subscription_builder`; `EventLogOrder` variant rename. All in core/facade; `EventEnvelope` untouched.
**RED tests:** load a nonexistent `#[aggregate(init)]` id → `Err(NotFound)` not panic (fails today); an init event + 100 more in commits that *cross* 100 without landing on it → a snapshot is written (fails today); a `post_validate`-rejected event then `commit` → `Err(InvalidState)` not a silent inconsistent commit (fails today).
**Ordering:** (a) panic→NotFound, (b) poison, (c) snapshot crossing, (f) deterministic ids in any order; do (M8) removal + (L4) rename last (widest doc/test fan-out). The multi-aggregate-save gap (L1) belongs to F6's `append_batch`, **not** here.

---

# Recommended remediation sequence

1. **F2** then **F1** — kill the critical data-loss/corruption cluster (C1–C4). F2 stops the late-commit skip immediately with no read-side change; F1 then fixes the gap-induced re-processing and unifies the position contract. Both are required.
2. **F3** (one-line correctness + the missing concurrency test) and **F7(a)** (panic→NotFound) — tiny, high-value.
3. **F5** — close the encryption confidentiality + GDPR-erasure gaps (H3/H4).
4. **F6** — make delivery semantics honest and safe (fencing, atomic flush, outbox).
5. **F4** + remaining **F7** — decide whether schema evolution is actually supported; if yes, ship the upcast hook + snapshot versioning + rename safety; if no, remove the misleading API/docs (dead-code rule).

---

# Verified NOT issues (refuted during the audit)

Recorded for credibility — these plausible claims were checked against the code and rejected:

1. **"`std::sync::Mutex` poisoning makes `commit()`/`flush()` panic."** Refuted: the lock is **never held across the handler or an `.await`** (each site is a statement-scoped `.lock().unwrap().push(...)`), and a fresh `PolicyContext` (fresh `Mutex`) is created per event — poison cannot propagate.
2. **"Field-encrypted aggregate permanently unloadable after shred = bug."** That is the **intended GDPR-erasure semantics**, consistent across snapshot/no-snapshot paths and asserted by existing tests. (The separate H4 ergonomics gap — wrong error *type* — is real and fixed by F5.)
3. **"NOTIFY + `stream_all` live in incompatible position spaces, so the documented pattern skips events."** Refuted: every example uses NOTIFY as a **wake-only signal and discards the position**. (The real adjacent bug is C3.)
