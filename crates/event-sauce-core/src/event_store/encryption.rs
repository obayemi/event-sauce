//! Crypto helpers shared by the commit and load pipelines: resolving the
//! configured provider/key store, resolving or minting an aggregate's key,
//! and encrypting/decrypting envelopes and snapshots around them.

use uuid::Uuid;
use zeroize::Zeroizing;

use super::EventStore;
use crate::{Aggregate, AggregateVersion, DomainEvent, EventEnvelope, Result, StreamId};

/// Unwraps the crypto provider or returns an error.
pub(super) fn require_crypto_provider<S: EventStore + ?Sized>(
    store: &S,
) -> Result<&dyn crate::CryptoProvider> {
    store
        .crypto_provider()
        .ok_or_else(|| crate::Error::invalid_state("Encrypted aggregate requires crypto_provider"))
}

/// Unwraps the crypto key store or returns an error.
pub(super) fn require_crypto_key_store<S: EventStore + ?Sized>(
    store: &S,
) -> Result<&dyn crate::CryptoKeyStore> {
    store
        .crypto_key_store()
        .ok_or_else(|| crate::Error::invalid_state("Encrypted aggregate requires crypto_key_store"))
}

/// Decrypts event envelope data using either full-value or field-level decryption.
///
/// `aad` binds the ciphertext to its context (event or snapshot); see
/// [`event_aad`](crate::crypto::event_aad)/[`snapshot_aad`](crate::crypto::snapshot_aad).
/// Legacy (v1) rows ignore the AAD via the versioned envelope, so pre-existing
/// ciphertext stays readable.
pub(super) fn decrypt_event_data<S: EventStore + ?Sized>(
    store: &S,
    crypto_key: Option<&[u8]>,
    event_data: &mut serde_json::Value,
    aad: &[u8],
) -> Result<()> {
    if let Some(key) = crypto_key {
        if crate::crypto::is_encrypted(event_data) {
            let provider = require_crypto_provider(store)?;
            *event_data = crate::crypto::decrypt_value(provider, key, event_data, aad)?;
        } else if crate::crypto::has_encrypted_fields(event_data) {
            let provider = require_crypto_provider(store)?;
            crate::crypto::decrypt_encrypted_fields(provider, key, event_data, aad)?;
        }
    }
    Ok(())
}

/// Returns `true` if the aggregate has any committed data — a persisted
/// snapshot, or at least one event in its stream.
///
/// Used to distinguish a crypto-shredded aggregate (data exists but its key is
/// gone) from a genuinely never-committed one (nothing to read), so that a
/// missing field-encryption key only surfaces as `KeyNotFound` when there is
/// actually data that has become unreadable.
pub(super) async fn has_committed_data<S: EventStore + ?Sized>(
    store: &S,
    stream_id: &StreamId,
) -> Result<bool> {
    use futures::StreamExt;

    if store.snapshot_config().use_snapshots_on_load()
        && store.load_snapshot(stream_id.clone()).await?.is_some()
    {
        return Ok(true);
    }

    let event_stream = store
        .load_stream(stream_id.clone(), AggregateVersion::initial())
        .await?;
    futures::pin_mut!(event_stream);
    Ok(event_stream.next().await.is_some())
}

/// Resolves the crypto key to use when loading an aggregate, or `None` for a
/// plaintext one.
///
/// For a fully encrypted aggregate the key must exist: `Error::KeyNotFound`
/// otherwise. For field-level encryption the key may legitimately be absent
/// when nothing has been committed yet — but if data already exists and the
/// key is gone, it was crypto-shredded out from under it, so that also
/// surfaces as `KeyNotFound`, uniformly with the fully-encrypted case.
///
/// # Errors
///
/// Returns `Error::InvalidState` if a fully encrypted aggregate has no
/// configured [`CryptoKeyStore`](crate::CryptoKeyStore), or `Error::KeyNotFound`
/// if the aggregate was crypto-shredded.
pub(super) async fn resolve_crypto_key<S: EventStore + ?Sized, A: Aggregate>(
    store: &S,
    uuid: Uuid,
    stream_id: &StreamId,
) -> Result<Option<Zeroizing<Vec<u8>>>> {
    if A::is_encrypted() {
        let key_store = require_crypto_key_store(store)?;
        let key = key_store
            .get_key(uuid)
            .await?
            .ok_or_else(|| crate::Error::key_not_found(uuid))?;
        return Ok(Some(Zeroizing::new(key)));
    }

    if !A::Event::has_any_encrypted_fields() {
        return Ok(None);
    }
    let Some(key_store) = store.crypto_key_store() else {
        return Ok(None);
    };
    if let Some(key) = key_store.get_key(uuid).await? {
        return Ok(Some(Zeroizing::new(key)));
    }
    if has_committed_data(store, stream_id).await? {
        return Err(crate::Error::key_not_found(uuid));
    }
    Ok(None)
}

/// Gets or creates a crypto key for an aggregate.
///
/// If the key already exists, returns it. Otherwise, generates a candidate key
/// and atomically inserts it via
/// [`get_or_insert_key`](crate::CryptoKeyStore::get_or_insert_key), always
/// encrypting with whichever key won that race — never the locally generated
/// candidate blindly. This is what keeps two concurrent first commits of the
/// same aggregate from encrypting under two different keys.
///
/// The returned key is held in [`Zeroizing`] so the buffer is wiped when this
/// engine copy is dropped (after encryption), rather than lingering in freed
/// heap. [`CryptoKeyStore`](crate::CryptoKeyStore) still exchanges plain
/// `Vec<u8>`, so each call takes a short-lived copy bound straight to the
/// backend read/write.
pub(super) async fn ensure_crypto_key<S: EventStore + ?Sized>(
    store: &S,
    aggregate_id: Uuid,
) -> Result<Zeroizing<Vec<u8>>> {
    let key_store = require_crypto_key_store(store)?;
    let provider = require_crypto_provider(store)?;

    if let Some(existing) = key_store.get_key(aggregate_id).await? {
        return Ok(Zeroizing::new(existing));
    }

    let candidate = provider.generate_key();
    let winner = key_store.get_or_insert_key(aggregate_id, candidate).await?;
    Ok(Zeroizing::new(winner))
}

/// Encrypts event envelopes in place for an encrypted or field-encrypted
/// aggregate; a no-op for a plaintext one.
///
/// Full-value encryption replaces `event_data` outright; field-level
/// encryption only touches the event variant's declared `encrypted_fields()`.
/// Either way the ciphertext is bound to `aggregate_id || event_id` (see
/// [`crate::crypto::event_aad`]), so it cannot be relocated onto another event.
pub(super) async fn encrypt_envelopes<S: EventStore + ?Sized, A: Aggregate>(
    store: &S,
    aggregate_id: Uuid,
    envelopes: &mut [EventEnvelope],
    pending: &[crate::aggregate_root::PendingEvent<A::Event>],
) -> Result<()>
where
    A::Event: serde::Serialize,
{
    if !A::is_encrypted() && !A::Event::has_any_encrypted_fields() {
        return Ok(());
    }

    let crypto_key = ensure_crypto_key(store, aggregate_id).await?;
    let provider = require_crypto_provider(store)?;

    for (envelope, pe) in envelopes.iter_mut().zip(pending.iter()) {
        let aad = crate::crypto::event_aad(aggregate_id, envelope.id);
        if A::is_encrypted() {
            envelope.event_data =
                crate::crypto::encrypt_value(provider, &crypto_key, &envelope.event_data, &aad)?;
        } else {
            let fields = pe.event.encrypted_fields();
            if !fields.is_empty() {
                crate::crypto::encrypt_fields(
                    provider,
                    &crypto_key,
                    &mut envelope.event_data,
                    fields,
                    &aad,
                )?;
            }
        }
    }
    Ok(())
}

/// Encrypts snapshot data in-place, failing closed for an aggregate that
/// requires encryption.
///
/// Callers invoke this only when the aggregate is fully encrypted or has any
/// field-encrypted event variant, so a missing key store, a missing provider, a
/// key-store read error, a missing key, or an `encrypt_value` failure are all
/// HARD errors: the snapshot would otherwise be persisted in plaintext —
/// leaking full state and being un-shreddable. We never fall through to a
/// plaintext snapshot for an encryption-required aggregate.
///
/// # Errors
///
/// Returns `Error::InvalidState` if crypto configuration is missing,
/// `Error::KeyNotFound` if no key exists for the aggregate, or
/// `Error::Encryption` if encryption fails.
pub(super) async fn encrypt_snapshot_data<S: EventStore + ?Sized>(
    store: &S,
    aggregate_id: Uuid,
    snapshot_data: &mut serde_json::Value,
) -> Result<()> {
    let key_store = require_crypto_key_store(store)?;
    let provider = require_crypto_provider(store)?;

    let crypto_key = Zeroizing::new(
        key_store
            .get_key(aggregate_id)
            .await?
            .ok_or_else(|| crate::Error::key_not_found(aggregate_id))?,
    );

    let aad = crate::crypto::snapshot_aad(aggregate_id);
    *snapshot_data = crate::crypto::encrypt_value(provider, &crypto_key, snapshot_data, &aad)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event_store::test_support::*;
    use crate::{CryptoKeyStore, SnapshotConfig};

    #[tokio::test]
    async fn ensure_crypto_key_returns_the_existing_key_unchanged() {
        let (store, key_store) = crypto_test_store();
        let id = Uuid::new_v4();
        key_store.upsert_key(id, vec![9; 32]).await.unwrap();

        let key = ensure_crypto_key(&store, id).await.unwrap();

        assert_eq!(&*key, &[9; 32]);
    }

    #[tokio::test]
    async fn ensure_crypto_key_returns_whichever_key_won_the_race() {
        let winning_key = vec![7; 32];
        let store = CommitTestStore::new(SnapshotConfig::disabled()).with_crypto(
            std::sync::Arc::new(AlwaysWinsKeyStore {
                winning_key: winning_key.clone(),
            }),
            std::sync::Arc::new(crate::test_fixtures::AadCheckingCryptoProvider),
        );

        let key = ensure_crypto_key(&store, Uuid::new_v4()).await.unwrap();

        assert_eq!(
            key.to_vec(),
            winning_key,
            "the caller's own candidate must never override the race's winner"
        );
    }

    #[tokio::test]
    async fn ensure_crypto_key_refuses_a_shredded_aggregate() {
        let (store, key_store) = crypto_test_store();
        let id = Uuid::new_v4();
        key_store.upsert_key(id, vec![1; 32]).await.unwrap();
        key_store.delete_key(id).await.unwrap();

        let result = ensure_crypto_key(&store, id).await;

        assert!(result.unwrap_err().is_key_not_found());
    }
}
