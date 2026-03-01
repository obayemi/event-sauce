# Privacy & Crypto-Shredding

event-sauce supports GDPR-style "crypto-shredding" for private aggregates. When an aggregate is marked as private, its event data and snapshot data are encrypted at rest using per-aggregate encryption keys. Deleting a key renders that aggregate's history permanently unreadable, implementing the right to be forgotten.

## Quick Start

```rust
use event_sauce::prelude::*;

// Mark an aggregate as private with the `private` flag
#[aggregate(event = "UserEvent", error = "UserError", private)]
struct User {
    #[id]
    id: EntityId,
    name: String,
    email: String,
}
```

## How It Works

### Encryption Flow

When you `commit()` events for a private aggregate:

1. A per-aggregate encryption key is generated (or retrieved if it already exists)
2. Each event's `event_data` is serialized to JSON, then encrypted with AES-256-GCM
3. The encrypted payload replaces the plaintext in the `EventEnvelope`
4. Snapshots are encrypted the same way
5. The backend stores the encrypted data — it never sees plaintext

### Decryption Flow

When you `load()` a private aggregate:

1. The per-aggregate encryption key is fetched from the key store
2. If the key is missing (deleted), `Error::KeyNotFound` is returned
3. Encrypted event data and snapshot data are decrypted transparently
4. The aggregate is reconstructed from the decrypted events as usual

### Encrypted Format

Encrypted values are stored as JSON:

```json
{"__encrypted": "base64(nonce_12_bytes || aes_gcm_ciphertext)"}
```

The `__encrypted` key signals that the value needs decryption. Unencrypted values pass through unchanged, providing backward compatibility.

## Setup

### Dependencies

Enable the `crypto` feature in your `Cargo.toml`:

```toml
[dependencies]
event-sauce = { version = "0.1", features = ["crypto", "memory"] }
# Or for PostgreSQL:
event-sauce = { version = "0.1", features = ["crypto", "postgres"] }
```

### In-Memory (Testing)

```rust
use event_sauce_memory::{InMemoryEventStore, InMemoryCryptoKeyStore};
use event_sauce_crypto::Aes256GcmProvider;
use event_sauce_core::SnapshotConfig;
use std::sync::Arc;

let key_store = Arc::new(InMemoryCryptoKeyStore::new());
let provider = Arc::new(Aes256GcmProvider);

let store = InMemoryEventStore::builder()
    .snapshot_config(SnapshotConfig::disabled())
    .crypto_key_store(key_store.clone())
    .crypto_provider(provider)
    .build();
```

### PostgreSQL (Production)

```rust
use event_sauce_postgres::{PostgresEventStore, PostgresCryptoKeyStore};
use event_sauce_crypto::Aes256GcmProvider;
use std::sync::Arc;

let key_store = Arc::new(PostgresCryptoKeyStore::new(pool.clone()));
key_store.migrate().await?;

let provider = Arc::new(Aes256GcmProvider);

let store = PostgresEventStore::builder()
    .pool(pool)
    .crypto_key_store(key_store.clone())
    .crypto_provider(provider)
    .build()?;

store.migrate().await?;
```

Or using `PostgresBackend`:

```rust
use event_sauce_postgres::{PostgresBackend, PostgresCryptoKeyStore};
use event_sauce_crypto::Aes256GcmProvider;
use std::sync::Arc;

let key_store = Arc::new(PostgresCryptoKeyStore::new(pool.clone()));
key_store.migrate().await?;

let backend = PostgresBackend::builder()
    .database_url("postgresql://localhost/events")
    .crypto_key_store(key_store)
    .crypto_provider(Arc::new(Aes256GcmProvider))
    .build()
    .await?;
```

## Defining Private Aggregates

### With the Proc Macro

```rust
#[aggregate(event = "PatientEvent", error = "PatientError", private)]
struct Patient {
    #[id]
    id: EntityId,
    name: String,
    medical_record: String,
}
```

The `private` flag generates `fn is_private() -> bool { true }` in the `Aggregate` implementation.

### Manual Implementation

```rust
impl Aggregate for Patient {
    type Event = PatientEvent;
    type Error = PatientError;

    fn is_private() -> bool {
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
```

The encrypted event and snapshot data remains in the database but is permanently unreadable without the key. This satisfies GDPR Article 17 (right to erasure) without requiring actual deletion of event history.

## Architecture

### Components

| Component | Description |
|-----------|-------------|
| `CryptoKeyStore` trait | Per-aggregate key lifecycle (get/upsert/delete) |
| `CryptoProvider` trait | Pluggable encryption implementation |
| `Aes256GcmProvider` | Default AES-256-GCM implementation |
| `InMemoryCryptoKeyStore` | In-memory key store for testing |
| `PostgresCryptoKeyStore` | PostgreSQL-backed key store for production |

### Key Per Aggregate

Each private aggregate instance gets its own unique encryption key. This means:

- Deleting one user's key doesn't affect other users
- Keys are generated automatically on first `commit()`
- Keys are stored in the configured `CryptoKeyStore`

### Non-Private Aggregates Are Unaffected

Aggregates without `private` (the default) continue to store plaintext data. The encryption system only activates when `Aggregate::is_private()` returns `true`.

### Backward Compatibility

The system handles mixed encrypted/unencrypted data gracefully:

- `decrypt_value()` checks for the `__encrypted` marker
- Unencrypted JSON values pass through unchanged
- You can add `private` to an existing aggregate — new events will be encrypted, old events remain readable

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
- **Key rotation**: The current implementation does not support key rotation. If needed, implement a migration that decrypts with the old key and re-encrypts with a new one
- **Backup**: Ensure encryption keys are included in your backup strategy. Lost keys = lost data (by design for crypto-shredding, but accidental key loss is permanent)
- **Audit trail**: The encrypted events themselves remain in the event store, providing proof that data existed even after shredding
