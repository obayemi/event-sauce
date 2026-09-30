//! The commit pipeline: preparing pending events for persistence and
//! flushing them, shared between active and deleted aggregates.

use uuid::Uuid;
use zeroize::Zeroizing;

use super::{
    require_crypto_key_store, require_crypto_provider, snapshot_aad, EventStore, PreparedCommit,
    Snapshot, StreamCommit,
};
use crate::commit_source::CommitSource;
#[cfg(test)]
use crate::AggregateRoot;
use crate::{
    Aggregate, AggregateType, AggregateVersion, DomainEvent, EventEnvelope, Result, StreamId,
};

/// Encrypts event envelopes in place for an encrypted or field-encrypted
/// aggregate; a no-op for a plaintext one.
///
/// Full-value encryption replaces `event_data` outright; field-level
/// encryption only touches the event variant's declared `encrypted_fields()`.
/// Either way the ciphertext is bound to `aggregate_id || event_id` (see
/// [`crate::crypto::event_aad`]), so it cannot be relocated onto another event.
async fn encrypt_envelopes<S: EventStore + ?Sized, A: Aggregate>(
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

/// Prepares a commit without persisting it.
///
/// Extracts pending events from the aggregate root, serializes and encrypts
/// them and computes a snapshot if needed. Returns `None` if there are no
/// pending events.
///
/// This does **not** clear the aggregate's pending events — the caller must
/// do that only once the prepared commit has actually been persisted, so a
/// failed or cancelled write can be retried instead of silently losing the
/// events.
pub(crate) async fn prepare_commit<S: EventStore + ?Sized, A, R: CommitSource<A>>(
    store: &S,
    root: &R,
) -> Result<Option<PreparedCommit>>
where
    A: Aggregate,
    A::Event: serde::Serialize,
{
    root.ensure_not_poisoned("commit")?;

    let pending = root.pending_events_with_actors();
    if pending.is_empty() {
        return Ok(None);
    }

    let aggregate_id = root.entity_id().as_uuid();
    let aggregate_type = R::aggregate_type();
    let expected_version = root.committed_version();

    let mut envelopes =
        crate::aggregate_root::envelopes_from_pending::<A::Event>(pending, aggregate_id)?;
    encrypt_envelopes::<S, A>(store, aggregate_id, &mut envelopes, pending).await?;

    let config = store.snapshot_config();
    let strategy = config.strategy_for_type(aggregate_type.as_str());
    let current_version = root.version();

    let snapshot = if strategy.should_snapshot_range(expected_version, current_version) {
        build_snapshot::<S, A, R>(store, root, aggregate_id, &aggregate_type, current_version)
            .await?
    } else {
        None
    };

    let stream_id = StreamId::new(aggregate_type, aggregate_id);

    Ok(Some(PreparedCommit {
        commit: StreamCommit {
            stream_id,
            events: envelopes,
            expected_version,
            claims: root.claims(),
            clear_claims: R::IS_DELETED,
        },
        snapshot,
    }))
}

/// Flushes a prepared commit to the event store.
///
/// Appends events and saves the snapshot (best-effort for snapshot failures).
pub(crate) async fn flush_prepared<S: EventStore + ?Sized>(
    store: &S,
    prepared: PreparedCommit,
) -> Result<()> {
    let PreparedCommit { commit, snapshot } = prepared;
    store
        .append(
            commit.stream_id,
            commit.events,
            commit.expected_version,
            commit.claims,
            commit.clear_claims,
        )
        .await?;

    if let Some(snapshot) = snapshot {
        save_snapshot_best_effort(store, snapshot).await;
    }

    Ok(())
}

/// Flushes several prepared commits as one logical write.
///
/// Appends every commit's events through a single
/// [`append_batch`](EventStore::append_batch) call — so on backends with real
/// transactions (e.g. `PostgreSQL`) the whole set is atomic. Snapshots are then
/// saved best-effort, after the events are durable.
///
/// Returns `Ok(())` immediately for an empty batch.
pub(crate) async fn flush_prepared_batch<S: EventStore + ?Sized>(
    store: &S,
    prepared: Vec<PreparedCommit>,
) -> Result<()> {
    if prepared.is_empty() {
        return Ok(());
    }

    let (commits, snapshots): (Vec<StreamCommit>, Vec<Option<Snapshot>>) =
        prepared.into_iter().map(|p| (p.commit, p.snapshot)).unzip();
    store.append_batch(commits).await?;

    for snapshot in snapshots.into_iter().flatten() {
        save_snapshot_best_effort(store, snapshot).await;
    }

    Ok(())
}

/// Saves a snapshot, warning (rather than failing the caller) if it fails.
///
/// A snapshot is a cache of already-durably-committed events, so a failure to
/// save it is never fatal to the commit that triggered it.
async fn save_snapshot_best_effort<S: EventStore + ?Sized>(store: &S, snapshot: Snapshot) {
    let aggregate_type = snapshot.aggregate_type.clone();
    let aggregate_id = snapshot.aggregate_id;
    if let Err(e) = store.save_snapshot(snapshot).await {
        tracing::warn!(
            aggregate_type = %aggregate_type,
            aggregate_id = %aggregate_id,
            error = %e,
            "Failed to save snapshot"
        );
    }
}

/// Builds an aggregate's snapshot, encrypting if needed.
///
/// A serialization failure is best-effort (the events are already committed and
/// the snapshot is only an optimization) so it logs and yields `None`. A crypto
/// failure for an encryption-required aggregate is fatal: it propagates as an
/// error so the commit fails closed rather than persisting a plaintext snapshot.
///
/// # Errors
///
/// Returns an error if snapshot encryption fails for an encryption-required
/// aggregate (see [`encrypt_snapshot_data`]).
async fn build_snapshot<S: EventStore + ?Sized, A: Aggregate, R: CommitSource<A>>(
    store: &S,
    root: &R,
    aggregate_id: uuid::Uuid,
    aggregate_type: &AggregateType,
    current_version: AggregateVersion,
) -> Result<Option<Snapshot>> {
    match root.serialize_state() {
        Ok(mut snapshot_data) => {
            if A::is_encrypted() || A::Event::has_any_encrypted_fields() {
                encrypt_snapshot_data(store, aggregate_id, &mut snapshot_data).await?;
            }
            let new = if R::IS_DELETED {
                Snapshot::new_deleted_with_schema_version
            } else {
                Snapshot::new_with_schema_version
            };
            Ok(Some(new(
                aggregate_id,
                aggregate_type.clone(),
                current_version,
                snapshot_data,
                A::snapshot_version(),
            )))
        }
        Err(e) => {
            tracing::warn!(
                aggregate_type = %aggregate_type,
                aggregate_id = %aggregate_id,
                is_deleted = R::IS_DELETED,
                error = %e,
                "Failed to serialize state for snapshot"
            );
            Ok(None)
        }
    }
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
async fn encrypt_snapshot_data<S: EventStore + ?Sized>(
    store: &S,
    aggregate_id: uuid::Uuid,
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

    let aad = snapshot_aad(aggregate_id);
    *snapshot_data = crate::crypto::encrypt_value(provider, &crypto_key, snapshot_data, &aad)?;
    Ok(())
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
async fn ensure_crypto_key<S: EventStore + ?Sized>(
    store: &S,
    aggregate_id: Uuid,
) -> Result<Zeroizing<Vec<u8>>> {
    let key_store = require_crypto_key_store(store)?;
    let provider = require_crypto_provider(store)?;

    if let Some(existing) = key_store.get_key(aggregate_id).await? {
        return Ok(Zeroizing::new(existing));
    }

    if key_store.is_shredded(aggregate_id).await? {
        return Err(crate::Error::key_not_found(aggregate_id));
    }

    let candidate = provider.generate_key();
    let winner = key_store.get_or_insert_key(aggregate_id, candidate).await?;
    Ok(Zeroizing::new(winner))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event_store::test_support::*;
    use crate::test_fixtures::{SimpleTestDelete, SimpleTestEntity, SimpleTestEvent};
    use crate::{CryptoKeyStore, SnapshotConfig};

    // -- commit() tests --

    #[tokio::test]
    async fn test_commit_with_pending_events() {
        let store = CommitTestStore::new(SnapshotConfig::disabled());
        let id = crate::EntityId::new();
        let mut agg = AggregateRoot::<SimpleTestEntity>::new(id);

        agg.apply(SimpleTestEvent::Created { value: 42 }).unwrap();
        agg.apply(SimpleTestEvent::Updated { value: 99 }).unwrap();

        assert_eq!(agg.pending_events().len(), 2);
        assert_eq!(agg.version(), AggregateVersion::new(2));

        store.commit(&mut agg).await.unwrap();

        assert_eq!(
            agg.pending_events().len(),
            0,
            "Pending events should be cleared after commit"
        );
        assert_eq!(
            store.append_count(),
            1,
            "Append should be called exactly once"
        );

        // Verify the events were stored
        let stream_id = StreamId::new("SimpleTestEntity", id.as_uuid());
        let streams = store.streams.lock().unwrap();
        let stored = streams.get(&stream_id).unwrap();
        assert_eq!(stored.len(), 2, "Two events should be stored");
    }

    #[tokio::test]
    async fn test_commit_with_no_pending_events() {
        let store = CommitTestStore::new(SnapshotConfig::disabled());
        let id = crate::EntityId::new();
        let mut agg = AggregateRoot::<SimpleTestEntity>::new(id);

        // No events applied, so commit should be a no-op
        store.commit(&mut agg).await.unwrap();

        assert_eq!(
            store.append_count(),
            0,
            "Append should not be called when there are no pending events"
        );
    }

    #[tokio::test]
    async fn test_commit_triggers_snapshot_when_strategy_says_yes() {
        let store = CommitTestStore::new(SnapshotConfig::always());
        let id = crate::EntityId::new();
        let mut agg = AggregateRoot::<SimpleTestEntity>::new(id);

        agg.apply(SimpleTestEvent::Created { value: 10 }).unwrap();

        store.commit(&mut agg).await.unwrap();

        assert_eq!(
            store.save_snapshot_count(),
            1,
            "save_snapshot should be called when strategy says yes"
        );

        // Verify snapshot was stored
        let stream_id = StreamId::new("SimpleTestEntity", id.as_uuid());
        let snapshots = store.snapshots.lock().unwrap();
        let snapshot = snapshots.get(&stream_id).expect("Snapshot should exist");
        assert_eq!(snapshot.snapshot_version, AggregateVersion::new(1));
        assert_eq!(snapshot.aggregate_type, "SimpleTestEntity");
    }

    #[tokio::test]
    async fn test_commit_does_not_snapshot_when_strategy_says_no() {
        let store = CommitTestStore::new(SnapshotConfig::disabled());
        let id = crate::EntityId::new();
        let mut agg = AggregateRoot::<SimpleTestEntity>::new(id);

        agg.apply(SimpleTestEvent::Created { value: 10 }).unwrap();

        store.commit(&mut agg).await.unwrap();

        assert_eq!(
            store.save_snapshot_count(),
            0,
            "save_snapshot should not be called when strategy says no"
        );
    }

    #[tokio::test]
    async fn test_commit_handles_snapshot_save_failure_gracefully() {
        let store = CommitTestStore::new(SnapshotConfig::always()).with_fail_save_snapshot();
        let id = crate::EntityId::new();
        let mut agg = AggregateRoot::<SimpleTestEntity>::new(id);

        agg.apply(SimpleTestEvent::Created { value: 10 }).unwrap();

        // commit should still succeed even when save_snapshot fails
        let result = store.commit(&mut agg).await;
        assert!(
            result.is_ok(),
            "Commit should succeed even if snapshot save fails"
        );

        assert_eq!(store.append_count(), 1, "Events should still be appended");
        assert_eq!(
            store.save_snapshot_count(),
            1,
            "save_snapshot should have been attempted"
        );
        assert!(
            agg.pending_events().is_empty(),
            "Pending events should still be cleared"
        );
    }

    #[tokio::test]
    async fn test_flush_prepared_batch_saves_a_snapshot_per_aggregate() {
        let store = CommitTestStore::new(SnapshotConfig::always());

        let id_a = crate::EntityId::new();
        let mut agg_a = AggregateRoot::<SimpleTestEntity>::new(id_a);
        agg_a.apply(SimpleTestEvent::Created { value: 1 }).unwrap();

        let id_b = crate::EntityId::new();
        let mut agg_b = AggregateRoot::<SimpleTestEntity>::new(id_b);
        agg_b.apply(SimpleTestEvent::Created { value: 2 }).unwrap();

        let prepared_a = prepare_commit(&store, &agg_a).await.unwrap().unwrap();
        let prepared_b = prepare_commit(&store, &agg_b).await.unwrap().unwrap();

        flush_prepared_batch(&store, vec![prepared_a, prepared_b])
            .await
            .unwrap();

        assert_eq!(
            store.save_snapshot_count(),
            2,
            "each dirty aggregate in the batch should get its own snapshot"
        );

        for id in [id_a, id_b] {
            let stream_id = StreamId::new("SimpleTestEntity", id.as_uuid());
            let snapshots = store.snapshots.lock().unwrap();
            let snapshot = snapshots
                .get(&stream_id)
                .expect("snapshot should exist for this stream");
            assert_eq!(snapshot.snapshot_version, AggregateVersion::new(1));
        }
    }

    #[tokio::test]
    async fn test_flush_prepared_batch_survives_snapshot_save_failure() {
        let store = CommitTestStore::new(SnapshotConfig::always()).with_fail_save_snapshot();

        let id = crate::EntityId::new();
        let mut agg = AggregateRoot::<SimpleTestEntity>::new(id);
        agg.apply(SimpleTestEvent::Created { value: 1 }).unwrap();

        let prepared = prepare_commit(&store, &agg).await.unwrap().unwrap();

        let result = flush_prepared_batch(&store, vec![prepared]).await;
        assert!(
            result.is_ok(),
            "batch should succeed even if a snapshot save fails"
        );
        assert_eq!(store.append_count(), 1, "events should still be appended");
        assert_eq!(
            store.save_snapshot_count(),
            1,
            "save_snapshot should have been attempted"
        );

        agg.clear_pending_events();
        assert!(
            agg.pending_events().is_empty(),
            "pending events should still be cleared once the batch is durable"
        );
    }

    #[tokio::test]
    async fn test_commit_clears_pending_events_after_success() {
        let store = CommitTestStore::new(SnapshotConfig::disabled());
        let id = crate::EntityId::new();
        let mut agg = AggregateRoot::<SimpleTestEntity>::new(id);

        agg.apply(SimpleTestEvent::Created { value: 1 }).unwrap();
        agg.apply(SimpleTestEvent::Updated { value: 2 }).unwrap();
        agg.apply(SimpleTestEvent::Updated { value: 3 }).unwrap();

        assert_eq!(agg.pending_events().len(), 3);

        store.commit(&mut agg).await.unwrap();

        assert!(
            agg.pending_events().is_empty(),
            "All pending events should be cleared after successful commit"
        );
        // AggregateVersion should be preserved
        assert_eq!(agg.version(), AggregateVersion::new(3));
    }

    #[tokio::test]
    async fn test_commit_propagates_actor_id_to_created_by() {
        let store = CommitTestStore::new(SnapshotConfig::disabled());
        let id = crate::EntityId::new();
        let actor_id = crate::EntityId::new();
        let mut agg = AggregateRoot::<SimpleTestEntity>::new(id);

        // First event without actor
        agg.apply(SimpleTestEvent::Created { value: 1 }).unwrap();
        // Second event with actor
        agg.apply_with_actor(SimpleTestEvent::Updated { value: 2 }, actor_id)
            .unwrap();

        store.commit(&mut agg).await.unwrap();

        let stream_id = StreamId::new("SimpleTestEntity", id.as_uuid());
        let streams = store.streams.lock().unwrap();
        let stored = streams.get(&stream_id).unwrap();

        assert_eq!(stored.len(), 2);
        assert!(
            stored[0].created_by.is_none(),
            "First event should not have created_by"
        );
        assert_eq!(
            stored[1].created_by,
            Some(actor_id.as_uuid()),
            "Second event should have actor's UUID as created_by"
        );
    }

    #[tokio::test]
    async fn prepare_commit_carries_the_active_root_claims() {
        let store = CommitTestStore::new(SnapshotConfig::disabled());
        let id = crate::EntityId::new();
        let mut agg = AggregateRoot::<DeletableEntity>::new(id);
        agg.apply(DeletableEvent::Created { value: 7 }).unwrap();

        let prepared = prepare_commit(&store, &agg).await.unwrap().unwrap();
        let expected = agg.entity().claims();

        assert_eq!(prepared.commit.claims.len(), expected.len());
        assert_eq!(prepared.commit.claims[0].claim_type, expected[0].claim_type);
        assert_eq!(prepared.commit.claims[0].claim_key, expected[0].claim_key);
        assert!(!prepared.commit.clear_claims);
    }

    #[tokio::test]
    async fn prepare_commit_deleted_clears_claims() {
        let store = CommitTestStore::new(SnapshotConfig::disabled());
        let id = crate::EntityId::new();
        let mut agg = AggregateRoot::<DeletableEntity>::new(id);
        agg.apply(DeletableEvent::Created { value: 7 }).unwrap();
        store.commit(&mut agg).await.unwrap();

        let deleted = agg
            .apply_delete(DeletableEvent::Deleted {
                reason: "gone".to_string(),
            })
            .unwrap();

        let prepared = prepare_commit(&store, &deleted).await.unwrap().unwrap();

        assert!(prepared.commit.claims.is_empty());
        assert!(prepared.commit.clear_claims);
    }

    #[tokio::test]
    async fn test_commit_deleted_persists_events() {
        let store = CommitTestStore::new(SnapshotConfig::disabled());
        let id = crate::EntityId::new();

        let mut agg = AggregateRoot::<DeletableEntity>::new(id);
        agg.apply(DeletableEvent::Created { value: 10 }).unwrap();

        let mut deleted = agg
            .apply_delete(DeletableEvent::Deleted {
                reason: "test".to_string(),
            })
            .unwrap();

        assert_eq!(deleted.pending_events().len(), 2);

        store.commit_deleted(&mut deleted).await.unwrap();

        assert!(
            deleted.pending_events().is_empty(),
            "Pending events should be cleared after commit"
        );

        // Verify events were stored
        let stream_id = StreamId::new("DeletableEntity", id.as_uuid());
        let streams = store.streams.lock().unwrap();
        let stored = streams.get(&stream_id).unwrap();
        assert_eq!(stored.len(), 2, "Both create and delete events stored");
    }

    #[tokio::test]
    async fn test_commit_deleted_with_no_pending_events_is_noop() {
        let store = CommitTestStore::new(SnapshotConfig::disabled());
        let id = crate::EntityId::new();

        let mut agg = AggregateRoot::<DeletableEntity>::new(id);
        agg.apply(DeletableEvent::Created { value: 10 }).unwrap();

        let mut deleted = agg
            .apply_delete(DeletableEvent::Deleted {
                reason: "test".to_string(),
            })
            .unwrap();

        // Clear pending to simulate already-committed
        deleted.clear_pending_events();
        store.commit_deleted(&mut deleted).await.unwrap();

        assert_eq!(store.append_count(), 0, "No append for empty pending");
    }

    #[tokio::test]
    async fn test_commit_deleted_saves_snapshot() {
        let store = CommitTestStore::new(SnapshotConfig::always());
        let id = crate::EntityId::new();

        let mut agg = AggregateRoot::<DeletableEntity>::new(id);
        agg.apply(DeletableEvent::Created { value: 10 }).unwrap();

        let mut deleted = agg
            .apply_delete(DeletableEvent::Deleted {
                reason: "test".to_string(),
            })
            .unwrap();

        store.commit_deleted(&mut deleted).await.unwrap();

        assert_eq!(store.save_snapshot_count(), 1);

        let stream_id = StreamId::new("DeletableEntity", id.as_uuid());
        let snapshots = store.snapshots.lock().unwrap();
        let snapshot = snapshots.get(&stream_id).expect("Snapshot should exist");
        assert!(snapshot.is_deleted);
        assert_eq!(snapshot.snapshot_version, AggregateVersion::new(2));
    }

    #[tokio::test]
    async fn test_commit_deleted_propagates_actor_id() {
        let store = CommitTestStore::new(SnapshotConfig::disabled());
        let id = crate::EntityId::new();
        let actor_id = crate::EntityId::new();

        let mut agg = AggregateRoot::<DeletableEntity>::new(id);
        agg.apply(DeletableEvent::Created { value: 10 }).unwrap();

        let mut deleted = agg
            .apply_delete_with_actor(
                DeletableEvent::Deleted {
                    reason: "test".to_string(),
                },
                actor_id,
            )
            .unwrap();

        store.commit_deleted(&mut deleted).await.unwrap();

        let stream_id = StreamId::new("DeletableEntity", id.as_uuid());
        let streams = store.streams.lock().unwrap();
        let stored = streams.get(&stream_id).unwrap();
        // The delete event (last one) should have the actor ID
        let last = stored.last().unwrap();
        assert_eq!(last.created_by, Some(actor_id.as_uuid()));
    }

    #[tokio::test]
    async fn commit_keeps_pending_events_when_append_fails() {
        let store = CommitTestStore::new(SnapshotConfig::disabled()).with_fail_append_times(1);
        let id = crate::EntityId::new();
        let mut agg = AggregateRoot::<DeletableEntity>::new(id);
        agg.apply(DeletableEvent::Created { value: 1 }).unwrap();

        let result = store.commit(&mut agg).await;

        assert!(result.is_err());
        assert_eq!(
            agg.pending_events().len(),
            1,
            "a failed commit must not drop the pending events"
        );

        store.commit(&mut agg).await.unwrap();
        assert_eq!(
            store.append_count(),
            2,
            "retrying must actually append again, not silently no-op"
        );
        assert!(agg.pending_events().is_empty());
    }

    #[tokio::test]
    async fn commit_deleted_keeps_pending_events_when_append_fails() {
        let store = CommitTestStore::new(SnapshotConfig::disabled()).with_fail_append_times(1);
        let id = crate::EntityId::new();
        let mut agg = AggregateRoot::<DeletableEntity>::new(id);
        agg.apply(DeletableEvent::Created { value: 1 }).unwrap();
        let mut deleted = agg
            .apply_delete(DeletableEvent::Deleted {
                reason: "done".to_string(),
            })
            .unwrap();

        let result = store.commit_deleted(&mut deleted).await;

        assert!(result.is_err());
        assert_eq!(
            deleted.pending_events().len(),
            2,
            "a failed commit_deleted must not drop the pending events"
        );

        store.commit_deleted(&mut deleted).await.unwrap();
        assert!(deleted.pending_events().is_empty());
    }

    #[tokio::test]
    async fn commit_keeps_pending_events_when_the_save_future_is_dropped() {
        let store = CommitTestStore::new(SnapshotConfig::disabled()).with_hang_append();
        let id = crate::EntityId::new();
        let mut agg = AggregateRoot::<DeletableEntity>::new(id);
        agg.apply(DeletableEvent::Created { value: 1 }).unwrap();

        let timed_out =
            tokio::time::timeout(std::time::Duration::from_millis(20), store.commit(&mut agg))
                .await
                .is_err();
        assert!(
            timed_out,
            "append must hang until the timeout, simulating a cancelled request"
        );

        assert_eq!(
            agg.pending_events().len(),
            1,
            "a dropped commit future must not drop the pending events"
        );
    }

    #[tokio::test]
    async fn commit_of_a_fully_encrypted_aggregate_replaces_event_data() {
        let (store, _key_store) = crypto_test_store();
        let id = crate::EntityId::new();
        let mut agg = AggregateRoot::<SecretThing>::new(id);
        agg.apply(SecretCreated {
            email: "alice@example.com".into(),
        })
        .unwrap();

        store.commit(&mut agg).await.unwrap();

        let stream_id = StreamId::new(SecretThing::aggregate_type(), id.as_uuid());
        let stored = store.streams.lock().unwrap().get(&stream_id).unwrap()[0].clone();
        assert!(
            crate::crypto::is_encrypted(&stored.event_data),
            "event_data must be replaced by its ciphertext envelope"
        );
    }

    #[tokio::test]
    async fn commit_of_a_field_encrypted_aggregate_only_touches_declared_fields() {
        let (store, _key_store) = crypto_test_store();
        let id = crate::EntityId::new();
        let mut agg = AggregateRoot::<PartialSecretThing>::new(id);
        agg.apply(PartialSecretCreated {
            public: "visible".into(),
            secret: "hidden".into(),
        })
        .unwrap();

        store.commit(&mut agg).await.unwrap();

        let stream_id = StreamId::new(PartialSecretThing::aggregate_type(), id.as_uuid());
        let stored = store.streams.lock().unwrap().get(&stream_id).unwrap()[0].clone();
        let obj = stored.event_data.as_object().unwrap();
        assert_eq!(obj.get("public").unwrap(), "visible");
        assert!(
            crate::crypto::is_encrypted(obj.get("secret").unwrap()),
            "the declared field must be encrypted"
        );
    }

    #[tokio::test]
    async fn a_snapshot_of_an_encrypted_aggregate_is_produced_encrypted() {
        let (key_store, provider) = (
            std::sync::Arc::new(MockCryptoKeyStore::default()),
            std::sync::Arc::new(AadCheckingCryptoProvider),
        );
        let store = CommitTestStore::new(SnapshotConfig::always()).with_crypto(key_store, provider);
        let id = crate::EntityId::new();
        let mut agg = AggregateRoot::<SecretThing>::new(id);
        agg.apply(SecretCreated {
            email: "shielded@example.com".into(),
        })
        .unwrap();

        store.commit(&mut agg).await.unwrap();

        let stream_id = StreamId::new(SecretThing::aggregate_type(), id.as_uuid());
        let snapshot = store
            .snapshots
            .lock()
            .unwrap()
            .get(&stream_id)
            .cloned()
            .expect("snapshot must have been saved");
        assert!(
            crate::crypto::is_encrypted(&snapshot.snapshot_data),
            "an encrypted aggregate's snapshot must be encrypted at rest"
        );
    }

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
            std::sync::Arc::new(AadCheckingCryptoProvider),
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

    #[tokio::test]
    async fn commit_refuses_a_poisoned_aggregate() {
        let store = CommitTestStore::new(SnapshotConfig::disabled());
        let id = crate::EntityId::new();
        let mut agg = AggregateRoot::<SimpleTestEntity>::new(id);
        agg.apply(SimpleTestEvent::Created { value: 1 }).unwrap();
        assert!(agg.apply(SimpleTestEvent::Rejected).is_err());

        let err = store.commit(&mut agg).await.unwrap_err();

        assert!(matches!(err, crate::Error::InvalidState(_)));
        assert_eq!(store.append_count(), 0);
    }

    #[tokio::test]
    async fn commit_deleted_refuses_a_poisoned_aggregate() {
        let store = CommitTestStore::new(SnapshotConfig::disabled());
        let id = crate::EntityId::new();
        let mut agg = AggregateRoot::<SimpleTestEntity>::new(id);
        agg.apply(SimpleTestEvent::Created { value: 1 }).unwrap();
        assert!(agg.apply(SimpleTestEvent::Rejected).is_err());
        let mut deleted = agg.apply_delete(SimpleTestDelete).unwrap();

        let err = store.commit_deleted(&mut deleted).await.unwrap_err();

        assert!(matches!(err, crate::Error::InvalidState(_)));
        assert_eq!(store.append_count(), 0);
    }
}
