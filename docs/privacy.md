# Privacy & Crypto-Shredding

event-sauce supports GDPR-style "crypto-shredding" for encrypted aggregates. When an aggregate is marked as encrypted, its event data and snapshot data are encrypted at rest using per-aggregate encryption keys. Deleting a key renders that aggregate's history permanently unreadable, implementing the right to be forgotten.

## Example

See the complete runnable example: [`examples/crypto-shredding.rs`](../crates/event-sauce/examples/crypto-shredding.rs)

```bash
cargo run --example crypto-shredding
```

## Quick Start

```rust
use event_sauce::prelude::*;

// Mark an aggregate as encrypted with the `encrypted` flag
#[aggregate(event = "UserEvent", error = "UserError", encrypted)]
struct User {
    #[id]
    id: EntityId,
    name: String,
    email: String,
}
```

## How It Works

### Encryption Flow

When you `commit()` events for an encrypted aggregate:

1. A per-aggregate encryption key is generated (or retrieved if it already exists)
2. Each event's `event_data` is serialized to JSON, then encrypted with AES-256-GCM
3. The ciphertext is bound to its event via **associated data (AAD)** = `aggregate_id || event_id` (and `|| field_name` for field-level encryption), so a ciphertext can only be decrypted in the same context
4. The encrypted payload replaces the plaintext in the `EventEnvelope`
5. Snapshots are encrypted the same way (bound to `aggregate_id || "snap"`)
6. The backend stores the encrypted data — it never sees plaintext

### Decryption Flow

When you `load()` an encrypted aggregate:

1. The per-aggregate encryption key is fetched from the key store
2. If the key is missing (deleted), `Error::KeyNotFound` is returned
3. Encrypted event data and snapshot data are decrypted transparently
4. The aggregate is reconstructed from the decrypted events as usual

### Encrypted Format

Encrypted values are stored as a **versioned** JSON envelope:

```json
{"__encrypted": "base64(nonce_12_bytes || aes_gcm_ciphertext)", "__enc_v": 2}
```

The `__encrypted` key signals that the value needs decryption; `__enc_v` is the
envelope version. Both keys are **reserved** and must not be used as user field
names. Unencrypted values pass through unchanged, providing backward
compatibility.

**Envelope versions:**

| `__enc_v` | Meaning | Decryption AAD |
|-----------|---------|----------------|
| absent (or `1`) | Legacy row written before AAD binding | empty AAD |
| `2` | Ciphertext bound to its event/snapshot | `aggregate_id \|\| event_id` (events) or `aggregate_id \|\| "snap"` (snapshots) |

The version drives backward compatibility: rows written before AAD binding have
no `__enc_v` marker and are decrypted with an empty AAD, exactly as they were
written, so a mixed-version dataset stays fully readable. New rows are written as
version `2`. This means **changing the AAD scheme is non-breaking for existing
data** — only fresh writes adopt it.

#### Why AAD binding matters

The per-aggregate key is shared by every event of that aggregate, so AES-GCM
alone authenticates only that a blob was produced under the key — not *which*
event it belongs to. Without AAD, an attacker (or a buggy migration) with write
access could copy one event's encrypted payload onto a different event row of the
same aggregate, or restore an old encrypted value, and it would decrypt and
authenticate cleanly, silently corrupting replayed state. Binding the per-event
UUID into the AAD makes each ciphertext context-unique, so a relocated or replayed
ciphertext fails authentication on load. Field-level encryption additionally binds
the field name to prevent intra-event field swapping.

## Setup

A key store is always installed. With the `crypto` feature enabled
(`event-sauce = { version = "0.1", features = ["crypto"] }`), every store
builder also installs the AES-256-GCM provider automatically — no extra
setup needed beyond turning the feature on. Without it, no provider is
installed, and committing or loading an encrypted aggregate fails with
`Error::InvalidState`.

### In-Memory (Testing)

```rust
use event_sauce::memory::InMemoryEventStore;

// With the `crypto` feature: AES-256-GCM + InMemoryCryptoKeyStore, both automatic
let store = InMemoryEventStore::new();
```

### PostgreSQL (Production)

```rust
use event_sauce::postgres::PostgresEventStore;

// With the `crypto` feature: AES-256-GCM + PostgresCryptoKeyStore, both automatic
let store = PostgresEventStore::builder()
    .pool(pool)
    .build()?;

store.migrate().await?; // Creates events, snapshots, and crypto_keys tables
```

Or using `PostgresBackend`:

```rust
use event_sauce::postgres::PostgresBackend;

// Same automatic setup, with the `crypto` feature enabled
let backend = PostgresBackend::builder()
    .database_url("postgresql://localhost/events")
    .build()
    .await?;
```

### Custom Crypto Provider

You can override the default crypto provider and key store if needed:

```rust
use event_sauce::memory::{InMemoryEventStore, InMemoryCryptoKeyStore};
use event_sauce::crypto::Aes256GcmProvider;
use std::sync::Arc;

let store = InMemoryEventStore::builder()
    .crypto_key_store(Arc::new(InMemoryCryptoKeyStore::new()))
    .crypto_provider(Arc::new(Aes256GcmProvider))
    .build();
```

## Defining Encrypted Aggregates

### With the Proc Macro

```rust
#[aggregate(event = "PatientEvent", error = "PatientError", encrypted)]
struct Patient {
    #[id]
    id: EntityId,
    name: String,
    medical_record: String,
}
```

The `encrypted` flag generates `fn is_encrypted() -> bool { true }` in the `Aggregate` implementation.

### Manual Implementation

```rust
impl Aggregate for Patient {
    type Event = PatientEvent;
    type Error = PatientError;

    fn is_encrypted() -> bool {
        true
    }
}
```

## Crypto-Shredding (Right to Be Forgotten)

To permanently erase a user's data, delete their encryption key:

```rust
// Delete the encryption key for a specific aggregate
key_store.delete_key(user_id.as_uuid()).await?;

// Any subsequent load will fail with KeyNotFound
let result = repo.load(user_id).await;
assert!(result.unwrap_err().is_key_not_found());

// So does any further commit — shredding is permanent
let result = repo.save(&mut aggregate).await;
assert!(result.unwrap_err().is_key_not_found());
```

The encrypted event and snapshot data remains in the database but is permanently unreadable without the key. This satisfies GDPR Article 17 (right to erasure) without requiring actual deletion of event history.

Shredding is permanent, and it does not rest on a pre-check a caller could
race past: the `is_shredded` check a commit makes first is only a fast
path, and the `get_or_insert_key` call behind it refuses atomically for a
shredded aggregate
([`CryptoKeyStore::get_or_insert_key`](https://docs.rs/event-sauce/latest/event_sauce/trait.CryptoKeyStore.html#tymethod.get_or_insert_key)) —
the only way to reverse a shred is an explicit
[`upsert_key`](https://docs.rs/event-sauce/latest/event_sauce/trait.CryptoKeyStore.html#tymethod.upsert_key)
call, which always wins and clears the tombstone the delete left behind. The
Postgres key store persists that tombstone as a `shredded_at` timestamp on the
`crypto_keys` row.

## Field-Level Encryption

For aggregates where only some fields are sensitive, you can encrypt individual fields instead of the entire event. Non-sensitive fields remain queryable in plaintext, while encrypted fields are stored as `{"__encrypted": "..."}`.

### Syntax

Use `@encrypted_fields(field1, field2)` in `define_events!`:

```rust
use event_sauce::define_events;

define_events! {
    pub enum PatientEvent for Patient {
        Registered {
            name: String,
            diagnosis: String,
            visit_count: i32,
        }
        @encrypted_fields(name, diagnosis)
        => |patient, event| {
            patient.name = event.name.clone();
            patient.diagnosis = event.diagnosis.clone();
            patient.visit_count = event.visit_count;
        },

        VisitRecorded {
            visit_count: i32,
        }
        => |patient, event| {
            patient.visit_count = event.visit_count;
        },
    }
}
```

### Stored Format

The `Registered` event is stored as:

```json
{
  "name": {"__encrypted": "base64..."},
  "diagnosis": {"__encrypted": "base64..."},
  "visit_count": 42,
  "timestamp": "2026-03-01T12:00:00Z"
}
```

The `VisitRecorded` event is stored entirely in plaintext (no `@encrypted_fields` annotation).

### How It Works

- **Same key management**: Field-encrypted aggregates use the same per-aggregate `CryptoKeyStore` and `CryptoProvider` as fully encrypted ones
- **Snapshots are fully encrypted and fail closed**: When any event variant has `@encrypted_fields`, snapshots are encrypted with `encrypt_value()` to prevent plaintext leaks. If snapshot encryption cannot complete (missing key, missing provider, or an `encrypt` failure), `commit()` returns an error and refuses to persist a plaintext snapshot — confidentiality is never traded for an optimization
- **Auto-detection on load**: Encrypted fields are detected by scanning for the `{"__encrypted": ...}` marker — no field list is needed for decryption
- **Crypto-shredding works, unified with full encryption**: Deleting the key for an aggregate that already has committed data makes its encrypted fields unreadable, and `load()` returns `Error::KeyNotFound` — exactly like a fully encrypted aggregate, so `is_key_not_found()` reliably detects GDPR erasure in both modes. An aggregate with no committed data and no key surfaces as `Error::NotFound` (nothing to shred), not `KeyNotFound`
- **Not fully encrypted**: `Aggregate::is_encrypted()` remains `false` — the aggregate is not marked as fully encrypted

### When to Use

| Scenario | Approach |
|----------|----------|
| All data is sensitive (e.g., medical records) | `#[aggregate(encrypted)]` — full encryption |
| Only some fields are sensitive (e.g., PII in otherwise public data) | `@encrypted_fields(name, email)` — field-level |
| No sensitive data | Default — no encryption |

## Architecture

### Components

| Component | Description |
|-----------|-------------|
| `CryptoKeyStore` trait | Per-aggregate key lifecycle (get/upsert/delete/get_or_insert_key/is_shredded) |
| `CryptoProvider` trait | Pluggable encryption implementation |
| `Aes256GcmProvider` | Default AES-256-GCM implementation |
| `InMemoryCryptoKeyStore` | In-memory key store for testing |
| `PostgresCryptoKeyStore` | PostgreSQL-backed key store for production |

### Key Per Aggregate

Each encrypted aggregate instance gets its own unique encryption key. This means:

- Deleting one user's key doesn't affect other users
- Keys are generated automatically on first `commit()`, unless the aggregate was shredded
- Keys are stored in the configured `CryptoKeyStore`

### Non-Encrypted Aggregates Are Unaffected

Aggregates without `encrypted` (the default) continue to store plaintext data. The encryption system only activates when `Aggregate::is_encrypted()` returns `true`.

### Backward Compatibility

The system handles mixed encrypted/unencrypted data gracefully:

- `decrypt_value()` checks for the `__encrypted` marker
- Unencrypted JSON values pass through unchanged
- You can add `encrypted` to an existing aggregate — new events will be encrypted, old events remain readable
- Mixed envelope versions coexist permanently: legacy ciphertext (no `__enc_v`) is decrypted with an empty AAD, while new `__enc_v: 2` rows are decrypted with the bound AAD — no rewrite of stored ciphertext is required (see [Encrypted Format](#encrypted-format))

### Schema Evolution (Upcasting)

Encrypted aggregates support read-time schema evolution exactly like plaintext ones. On load the event data is **decrypted first**, then the [`DomainEvent::upcast`](events.md#event-upcasting) hook runs on the recovered plaintext, and only then is the event deserialized — i.e. `decrypt → upcast → deserialize`. An older-version encrypted payload (for example one missing a field added in a later version) is migrated by `upcast` before deserialization, with **no need to rewrite the stored ciphertext**. See the [Events guide — Event Upcasting](events.md#event-upcasting).

A crypto-**shredded** aggregate (its key deleted) is the opposite case and remains *intentionally* unrecoverable: load returns `Error::KeyNotFound` and no upcast can run, because the plaintext can never be recovered. That is the entire point of crypto-shredding.

> Rewriting the stored ciphertext itself (e.g. to physically drop a removed field's plaintext, or to re-key) is a deliberate maintenance operation that mutates the otherwise-immutable event log; it is intentionally **not** offered as a casual API. Read-time upcasting covers ordinary schema evolution.

## Error Handling

```rust
match repo.load(user_id).await {
    Ok(aggregate) => { /* success */ },
    Err(e) if e.is_key_not_found() => {
        // Key was deleted (crypto-shredded)
        // Handle gracefully — e.g., return "user deleted" response
    },
    Err(e) if e.is_encryption() => {
        // Encryption/decryption failure (corrupted data, wrong key, etc.)
    },
    Err(e) => { /* other errors */ },
}
```

## Security Considerations

- **Key storage**: In production, consider using a dedicated secrets manager or HSM for key storage instead of the same database
- **Ciphertext binding (AAD)**: Each ciphertext is bound to its context via AES-GCM associated data (`aggregate_id || event_id`, plus `|| field_name` for field-level; `aggregate_id || "snap"` for snapshots). This defeats relocation/replay of a ciphertext onto a different event of the same aggregate. The on-disk `__enc_v` envelope version keeps legacy (pre-binding) rows readable — see [Encrypted Format](#encrypted-format).
- **Nonce ceiling**: `Aes256GcmProvider` draws a fresh random 96-bit nonce per encryption. With random nonces under a single key, collision probability grows with the message count — stay well under **~2³² encryptions per key** for a < 2⁻³² collision bound. A nonce collision under a fixed GCM key is catastrophic (it can leak the authentication subkey). event-sauce uses **one key per aggregate**, so each key only encrypts that aggregate's events and snapshots — the ceiling is reached only by aggregates with on the order of billions of events. If you expect such volume per aggregate, prefer a nonce-misuse-resistant scheme (see below).
- **Key rotation**: The current implementation deliberately uses **one key per aggregate for its lifetime and does not rotate keys**. This keeps crypto-shredding simple (delete the one key, the aggregate is unrecoverable). If you need rotation — to bound the per-key message count above, or for periodic key hygiene — implement an opt-in migration that, per aggregate: reads the current key, decrypts every event and snapshot, generates a new key, re-encrypts under it (writing fresh `__enc_v: 2` envelopes bound to the same AAD), and finally upserts the new key. A future `CryptoKeyStore::rotate_key(aggregate_id)` + a re-encryption pass over the stream is the natural shape for this.
- **Alternative providers**: `CryptoProvider` is pluggable. For very high per-key message counts, a provider built on **XChaCha20-Poly1305** (192-bit nonce, making random-nonce collision negligible) or **AES-GCM-SIV** (nonce-misuse-resistant: a repeated nonce degrades to deterministic encryption rather than catastrophic failure) sidesteps the AES-GCM nonce ceiling. Such a provider can coexist with existing data by writing a new `__enc_v` version; older envelopes keep decrypting under the original provider.
- **Backup**: Ensure encryption keys are included in your backup strategy. Lost keys = lost data (by design for crypto-shredding, but accidental key loss is permanent)
- **Audit trail**: The encrypted events themselves remain in the event store, providing proof that data existed even after shredding
