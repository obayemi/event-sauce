//! Loading and replaying an aggregate from its event stream, optionally
//! resuming from a snapshot.

use uuid::Uuid;

use futures::Stream;

use super::encryption::{decrypt_event_data, resolve_crypto_key};
use super::{EventStore, Snapshot};
use crate::{
    Aggregate, AggregateRoot, AggregateVersion, DeletedAggregateRoot, DomainEvent, EntityId,
    EventEnvelope, Loaded, Result, StreamId,
};

/// Decrypts (if needed) and deserializes one event envelope.
fn decode_event<S: EventStore + ?Sized, A: Aggregate>(
    store: &S,
    key: Option<&[u8]>,
    aggregate_id: Uuid,
    mut envelope: EventEnvelope,
) -> Result<A::Event>
where
    A::Event: serde::de::DeserializeOwned,
{
    let aad = crate::crypto::event_aad(aggregate_id, envelope.id);
    decrypt_event_data(store, key, &mut envelope.event_data, &aad)?;
    A::Event::from_envelope(&envelope)
}

/// Replays every remaining item of an event stream onto an in-progress
/// aggregate, stopping early (as [`Loaded::Deleted`]) if a delete event is
/// found — shared by [`try_load_from_snapshot`] (replaying the events after a
/// snapshot) and [`load_any`] (replaying the events after the first one).
async fn replay_onto<S, A, St>(
    store: &S,
    mut stream: St,
    key: Option<&[u8]>,
    mut aggregate: AggregateRoot<A>,
) -> Result<Loaded<A>>
where
    S: EventStore + ?Sized,
    A: Aggregate,
    A::Event: serde::de::DeserializeOwned,
    St: Stream<Item = Result<EventEnvelope>> + Unpin,
{
    use crate::EventApplicator;
    use futures::StreamExt;

    let aggregate_id = aggregate.entity_id().as_uuid();

    while let Some(envelope) = stream.next().await {
        let event = decode_event::<S, A>(store, key, aggregate_id, envelope?)?;
        if EventApplicator::is_delete(&event) {
            return Ok(Loaded::Deleted(aggregate.apply_delete_unchecked(&event)));
        }
        aggregate.apply_unchecked(&event);
    }

    Ok(Loaded::Active(aggregate))
}

/// Attempts to reconstruct an aggregate from a (decrypted) snapshot plus its
/// post-snapshot events.
///
/// Returns `Ok(Some(loaded))` when the snapshot is a usable cache entry, and
/// `Ok(None)` when it is a CACHE MISS — i.e. the snapshot's type tag or schema
/// version no longer matches `A`, or its data can no longer be deserialized
/// into `A` / `A::DeletedState`. On a miss the caller falls through to full
/// event replay (snapshot is a cache, never the source of truth) and a fresh
/// snapshot is written at the current version on the next commit.
///
/// The `snapshot.snapshot_data` is expected to already be decrypted (the
/// crypto-shredding `KeyNotFound` case is handled by the caller before this is
/// invoked).
///
/// # Errors
///
/// Returns an error only if loading the post-snapshot event stream or
/// deserializing an event fails. A snapshot deserialization failure is a cache
/// miss (`Ok(None)`), not an error.
async fn try_load_from_snapshot<S, A>(
    store: &S,
    stream_id: StreamId,
    crypto_key: Option<&[u8]>,
    snapshot: Snapshot,
) -> Result<Option<Loaded<A>>>
where
    S: EventStore,
    A: Aggregate + serde::de::DeserializeOwned,
    A::DeletedState: serde::de::DeserializeOwned,
    A::Event: serde::de::DeserializeOwned,
{
    // (i) Type-tag mismatch: a different aggregate's snapshot was stored under
    // this id, or the type name changed. Discard and replay.
    if snapshot.aggregate_type != A::aggregate_type() {
        tracing::warn!(
            aggregate_type = %A::aggregate_type(),
            snapshot_aggregate_type = %snapshot.aggregate_type,
            aggregate_id = %snapshot.aggregate_id,
            "Snapshot aggregate-type mismatch; discarding stale snapshot and replaying events"
        );
        return Ok(None);
    }

    // (ii) Schema-version mismatch: the aggregate's serialized shape changed
    // (its `snapshot_version()` was bumped). Discard and replay.
    if snapshot.snapshot_schema_version != A::snapshot_version() {
        tracing::warn!(
            aggregate_type = %A::aggregate_type(),
            aggregate_id = %snapshot.aggregate_id,
            snapshot_schema_version = snapshot.snapshot_schema_version,
            expected_schema_version = A::snapshot_version(),
            "Snapshot schema-version mismatch; discarding stale snapshot and replaying events"
        );
        return Ok(None);
    }

    let snapshot_version = snapshot.snapshot_version;
    let aggregate_id = snapshot.aggregate_id;

    // Deleted snapshot: deserialize as DeletedState and return immediately.
    if snapshot.is_deleted {
        let Ok(deleted_state) = serde_json::from_value::<A::DeletedState>(snapshot.snapshot_data)
        else {
            // (iii) Shape mismatch on the deleted state: cache miss, replay.
            tracing::warn!(
                aggregate_type = %A::aggregate_type(),
                aggregate_id = %aggregate_id,
                "Deleted snapshot no longer deserializes into current shape; \
                 discarding stale snapshot and replaying events"
            );
            return Ok(None);
        };
        let entity_id = EntityId::from(aggregate_id);
        return Ok(Some(Loaded::Deleted(DeletedAggregateRoot::restore(
            deleted_state,
            entity_id,
            snapshot_version,
        ))));
    }

    // (iii) Active snapshot: a deserialization failure is a cache miss, NOT a
    // hard error — the full correct state is still derivable from events.
    let Ok(entity) = serde_json::from_value::<A>(snapshot.snapshot_data) else {
        tracing::warn!(
            aggregate_type = %A::aggregate_type(),
            aggregate_id = %aggregate_id,
            "Snapshot no longer deserializes into current shape; \
             discarding stale snapshot and replaying events"
        );
        return Ok(None);
    };

    let aggregate = AggregateRoot::restore(snapshot_version, entity);

    let event_stream = store.load_stream(stream_id, snapshot_version).await?;
    futures::pin_mut!(event_stream);

    replay_onto(store, event_stream, crypto_key, aggregate)
        .await
        .map(Some)
}

/// Loads and reconstructs an aggregate from its snapshot, if snapshots are
/// enabled and one exists.
///
/// Returns `Ok(None)` when snapshots are disabled, none is stored, or
/// [`try_load_from_snapshot`] treats the stored one as a cache miss — the
/// caller then falls through to full event replay.
///
/// # Errors
///
/// Returns `Error::KeyNotFound` if the snapshot is still ciphertext after
/// decryption (the aggregate was crypto-shredded), or an error from loading
/// or replaying its post-snapshot events.
async fn load_from_snapshot<S, A>(
    store: &S,
    stream_id: &StreamId,
    uuid: Uuid,
    key: Option<&[u8]>,
) -> Result<Option<Loaded<A>>>
where
    S: EventStore,
    A: Aggregate + serde::de::DeserializeOwned,
    A::DeletedState: serde::de::DeserializeOwned,
    A::Event: serde::de::DeserializeOwned,
{
    if !store.snapshot_config().use_snapshots_on_load() {
        return Ok(None);
    }
    let Some(mut snapshot) = store.load_snapshot(stream_id.clone()).await? else {
        return Ok(None);
    };

    let snap_aad = crate::crypto::snapshot_aad(uuid);
    decrypt_event_data(store, key, &mut snapshot.snapshot_data, &snap_aad)?;

    if crate::crypto::is_encrypted(&snapshot.snapshot_data) {
        return Err(crate::Error::key_not_found(uuid));
    }

    try_load_from_snapshot::<S, A>(store, stream_id.clone(), key, snapshot).await
}

/// Loads an aggregate from the event store, returning its lifecycle state.
///
/// Returns `Loaded::Active(AggregateRoot<A>)` for active aggregates, or
/// `Loaded::Deleted(DeletedAggregateRoot<A>)` for deleted ones.
///
/// This function handles `DefaultEntity` aggregates (legacy), init-event
/// aggregates, and delete events. It detects which pattern is used by
/// checking `is_init()` and `is_delete()` on events during replay.
///
/// A snapshot is a CACHE, never the source of truth — a stale or
/// incompatible one (see [`try_load_from_snapshot`]) self-heals by falling
/// through to full event replay below rather than corrupting state or
/// hard-failing.
///
/// # Errors
///
/// Returns an error if events cannot be deserialized or replay fails.
pub(crate) async fn load_any<S, A>(store: &S, id: EntityId) -> Result<Loaded<A>>
where
    S: EventStore,
    A: Aggregate + serde::de::DeserializeOwned,
    A::DeletedState: serde::de::DeserializeOwned,
    A::Event: serde::de::DeserializeOwned,
{
    use crate::{EventApplicator, UninitAggregateRoot};
    use futures::StreamExt;

    let uuid = id.as_uuid();
    let aggregate_type = A::aggregate_type();
    let stream_id = StreamId::new(aggregate_type, uuid);

    let crypto_key = resolve_crypto_key::<S, A>(store, uuid, &stream_id).await?;
    let key = crypto_key.as_deref().map(Vec::as_slice);

    if let Some(loaded) = load_from_snapshot::<S, A>(store, &stream_id, uuid, key).await? {
        return Ok(loaded);
    }

    // No snapshot (or a snapshot that missed): load all events
    let event_stream = store
        .load_stream(stream_id, AggregateVersion::initial())
        .await?;
    futures::pin_mut!(event_stream);

    let Some(first_envelope) = event_stream.next().await else {
        return Err(crate::Error::not_found(
            A::aggregate_type().as_str(),
            uuid.to_string(),
        ));
    };
    let first_event = decode_event::<S, A>(store, key, uuid, first_envelope?)?;

    // Detect init vs legacy from first event
    let aggregate = if EventApplicator::is_init(&first_event) {
        // Init-event aggregate: type-state transition
        UninitAggregateRoot::<A>::new(id).apply_init_unchecked(&first_event)
    } else {
        // Legacy aggregate: Entity::new(id) + apply first event
        let mut agg = AggregateRoot::<A>::restore(AggregateVersion::initial(), A::new(id));
        agg.apply_unchecked(&first_event);
        agg
    };

    // Replay remaining events
    replay_onto(store, event_stream, key, aggregate).await
}

/// Loads an aggregate from the event store by its ID.
///
/// Returns an `AggregateRoot<A>` reconstructed by replaying events,
/// optionally using a snapshot for optimization.
///
/// If the aggregate has been deleted, returns `Error::AggregateDeleted`.
/// Use [`load_any()`] to handle both active and deleted aggregates.
///
/// # Errors
///
/// Returns `Error::NotFound` if the aggregate does not exist (no events and no
/// snapshot). This is uniform for both legacy and `@init` aggregates: `load`
/// never synthesizes an empty default-state aggregate.
/// Returns `Error::AggregateDeleted` if the aggregate has been deleted.
/// Returns an error if events cannot be deserialized or replay fails.
pub(crate) async fn load<S, A>(store: &S, id: EntityId) -> Result<AggregateRoot<A>>
where
    S: EventStore,
    A: Aggregate + serde::de::DeserializeOwned,
    A::DeletedState: serde::de::DeserializeOwned,
    A::Event: serde::de::DeserializeOwned,
{
    load_any(store, id).await?.into_active()
}

/// Loads a deleted aggregate from the event store by its ID.
///
/// Returns `DeletedAggregateRoot<A>` if the aggregate has been deleted.
/// Returns `Error::InvalidState` if the aggregate is still active.
///
/// # Errors
///
/// Returns `Error::InvalidState` if the aggregate is not deleted.
/// Returns an error if events cannot be deserialized or replay fails.
pub(crate) async fn load_deleted<S, A>(store: &S, id: EntityId) -> Result<DeletedAggregateRoot<A>>
where
    S: EventStore,
    A: Aggregate + serde::de::DeserializeOwned,
    A::DeletedState: serde::de::DeserializeOwned,
    A::Event: serde::de::DeserializeOwned,
{
    load_any(store, id).await?.into_deleted()
}

/// Counts the number of events in a stream.
///
/// # Errors
///
/// Returns an error if the event store fails to load the stream.
pub(crate) async fn count_events<S>(store: &S, stream_id: StreamId) -> Result<usize>
where
    S: EventStore,
{
    use futures::StreamExt;

    let event_stream = store
        .load_stream(stream_id, AggregateVersion::initial())
        .await?;
    futures::pin_mut!(event_stream);

    let mut count = 0;
    while let Some(result) = event_stream.next().await {
        result?;
        count += 1;
    }

    Ok(count)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event_store::test_support::*;
    use crate::test_fixtures::{SimpleTestEntity, SimpleTestEvent};
    use crate::{CryptoKeyStore, SnapshotConfig};

    // -- load() tests --

    #[tokio::test]
    async fn test_load_with_no_snapshot_replays_from_beginning() {
        let store = CommitTestStore::new(SnapshotConfig::disabled());
        let id = crate::EntityId::new();

        // First, commit some events
        let mut agg = AggregateRoot::<SimpleTestEntity>::new(id);
        agg.apply(SimpleTestEvent::Created { value: 10 }).unwrap();
        agg.apply(SimpleTestEvent::Updated { value: 20 }).unwrap();
        agg.apply(SimpleTestEvent::Updated { value: 30 }).unwrap();
        store.commit(&mut agg).await.unwrap();

        // Load from store
        let loaded: AggregateRoot<SimpleTestEntity> = load(&store, id).await.unwrap();

        assert_eq!(
            loaded.value, 30,
            "Entity state should reflect all replayed events"
        );
        assert_eq!(
            loaded.version(),
            AggregateVersion::new(3),
            "AggregateVersion should match number of events"
        );
        assert!(
            loaded.pending_events().is_empty(),
            "Loaded aggregate should have no pending events"
        );
    }

    #[tokio::test]
    async fn test_load_with_snapshot_resumes_from_snapshot_version() {
        let config = SnapshotConfig::always();
        let store = CommitTestStore::new(config);
        let id = crate::EntityId::new();

        // Pre-save a snapshot at version 2 with value=20
        let snapshot_entity = SimpleTestEntity { id, value: 20 };
        let snapshot_data = serde_json::to_value(&snapshot_entity).unwrap();
        let stream_id = StreamId::new("SimpleTestEntity", id.as_uuid());
        let snapshot = Snapshot::new(
            id.as_uuid(),
            "SimpleTestEntity".to_string(),
            AggregateVersion::new(2),
            snapshot_data,
        );
        store
            .snapshots
            .lock()
            .unwrap()
            .insert(stream_id.clone(), snapshot);

        // Add all events (skip-based filtering requires full stream).
        // Events at index 0 and 1 are pre-snapshot, 2 and 3 are post-snapshot.
        let envelope1 = EventEnvelope::new(
            Uuid::new_v4(),
            id.as_uuid(),
            "SimpleTestEntity".to_string(),
            "SimpleTestCreated".to_string(),
            crate::EventVersion::new(1),
            serde_json::to_value(&SimpleTestEvent::Created { value: 10 }).unwrap(),
        );
        let envelope2 = EventEnvelope::new(
            Uuid::new_v4(),
            id.as_uuid(),
            "SimpleTestEntity".to_string(),
            "SimpleTestUpdated".to_string(),
            crate::EventVersion::new(1),
            serde_json::to_value(&SimpleTestEvent::Updated { value: 20 }).unwrap(),
        );
        let envelope3 = EventEnvelope::new(
            Uuid::new_v4(),
            id.as_uuid(),
            "SimpleTestEntity".to_string(),
            "SimpleTestUpdated".to_string(),
            crate::EventVersion::new(1),
            serde_json::to_value(&SimpleTestEvent::Updated { value: 30 }).unwrap(),
        );
        let envelope4 = EventEnvelope::new(
            Uuid::new_v4(),
            id.as_uuid(),
            "SimpleTestEntity".to_string(),
            "SimpleTestUpdated".to_string(),
            crate::EventVersion::new(1),
            serde_json::to_value(&SimpleTestEvent::Updated { value: 40 }).unwrap(),
        );
        store
            .streams
            .lock()
            .unwrap()
            .insert(stream_id, vec![envelope1, envelope2, envelope3, envelope4]);

        // Load from store — should start from snapshot and replay events 3 and 4
        let loaded: AggregateRoot<SimpleTestEntity> = load(&store, id).await.unwrap();

        assert_eq!(
            loaded.value, 40,
            "Entity should reflect snapshot + replayed events"
        );
        // AggregateVersion should be snapshot (2) + replayed events (2) = 4
        assert_eq!(loaded.version(), AggregateVersion::new(4));
    }

    #[tokio::test]
    async fn test_load_with_snapshots_disabled_ignores_snapshot() {
        let config = SnapshotConfig::builder()
            .default_strategy(crate::AlwaysSnapshot)
            .use_snapshots_on_load(false)
            .build();
        let store = CommitTestStore::new(config);
        let id = crate::EntityId::new();

        // Save a snapshot that should be ignored
        let snapshot_entity = SimpleTestEntity { id, value: 999 };
        let snapshot_data = serde_json::to_value(&snapshot_entity).unwrap();
        let stream_id = StreamId::new("SimpleTestEntity", id.as_uuid());
        let snapshot = Snapshot::new(
            id.as_uuid(),
            "SimpleTestEntity".to_string(),
            AggregateVersion::new(5),
            snapshot_data,
        );
        store
            .snapshots
            .lock()
            .unwrap()
            .insert(stream_id.clone(), snapshot);

        // Add events from the beginning
        let envelope1 = EventEnvelope::new(
            Uuid::new_v4(),
            id.as_uuid(),
            "SimpleTestEntity".to_string(),
            "SimpleTestCreated".to_string(),
            crate::EventVersion::new(1),
            serde_json::to_value(&SimpleTestEvent::Created { value: 10 }).unwrap(),
        );
        let envelope2 = EventEnvelope::new(
            Uuid::new_v4(),
            id.as_uuid(),
            "SimpleTestEntity".to_string(),
            "SimpleTestUpdated".to_string(),
            crate::EventVersion::new(1),
            serde_json::to_value(&SimpleTestEvent::Updated { value: 20 }).unwrap(),
        );
        store
            .streams
            .lock()
            .unwrap()
            .insert(stream_id, vec![envelope1, envelope2]);

        // Load — should ignore snapshot and replay all events from beginning
        let loaded: AggregateRoot<SimpleTestEntity> = load(&store, id).await.unwrap();

        assert_eq!(
            loaded.value, 20,
            "Should reflect full replay, not snapshot value of 999"
        );
        assert_eq!(loaded.version(), AggregateVersion::new(2));
    }

    // -- M4: snapshot self-heals on an undeserializable / stale snapshot --

    #[tokio::test]
    async fn test_load_falls_through_to_replay_when_snapshot_does_not_deserialize() {
        // M4 (TERMINAL -> REPLAY): a persisted snapshot whose `snapshot_data`
        // can no longer be deserialized into the current aggregate shape (here:
        // the required `value` field is absent — `SimpleTestEntity` has no
        // `#[serde(default)]`) must be treated as a CACHE MISS. The store should
        // fall through to full-event replay and rebuild the correct state,
        // NOT return an opaque "Failed to deserialize snapshot entity" error.
        //
        // TODAY: `serde_json::from_value::<SimpleTestEntity>` fails and
        // `load_any` propagates `Error::custom("Failed to deserialize snapshot
        // entity: ...")`, taking the aggregate offline even though the full
        // correct state is derivable from the events.
        let config = SnapshotConfig::always();
        let store = CommitTestStore::new(config);
        let id = crate::EntityId::new();
        let stream_id = StreamId::new("SimpleTestEntity", id.as_uuid());

        // Seed an incompatible snapshot at version 1: object lacks `value`.
        let bad_snapshot = Snapshot::new(
            id.as_uuid(),
            "SimpleTestEntity".to_string(),
            AggregateVersion::new(1),
            serde_json::json!({ "id": id }), // missing required `value`
        );
        store
            .snapshots
            .lock()
            .unwrap()
            .insert(stream_id.clone(), bad_snapshot);

        // Seed the full event stream that produces the real state (value = 30).
        let make_env = |event: &SimpleTestEvent| {
            EventEnvelope::new(
                Uuid::new_v4(),
                id.as_uuid(),
                "SimpleTestEntity".to_string(),
                "SimpleTestEntity.Updated".to_string(),
                crate::EventVersion::new(1),
                serde_json::to_value(event).unwrap(),
            )
        };
        store.streams.lock().unwrap().insert(
            stream_id,
            vec![
                make_env(&SimpleTestEvent::Created { value: 10 }),
                make_env(&SimpleTestEvent::Updated { value: 20 }),
                make_env(&SimpleTestEvent::Updated { value: 30 }),
            ],
        );

        let result: Result<AggregateRoot<SimpleTestEntity>> = load(&store, id).await;

        let loaded = result.expect(
            "an undeserializable snapshot must self-heal via replay, not hard-fail \
             (snapshot is a cache, never the source of truth)",
        );
        assert_eq!(
            loaded.value, 30,
            "state must be rebuilt from full event replay"
        );
        assert_eq!(
            loaded.version(),
            AggregateVersion::new(3),
            "version must reflect all replayed events, not the stale snapshot"
        );
    }

    #[tokio::test]
    async fn test_load_uses_matching_snapshot_on_happy_path() {
        // M4 GUARD (must stay green): a correctly-shaped snapshot is still USED
        // (not replayed). We prove the snapshot path is taken by giving the
        // snapshot a DIFFERENT state than replaying from version 0 would
        // produce: the snapshot is at version 2 with value=20, and only the
        // post-snapshot events (index >= 2) are applied on top. If replay were
        // (wrongly) used instead, the pre-snapshot Created/Updated events would
        // re-run and the resulting version would not match.
        let config = SnapshotConfig::always();
        let store = CommitTestStore::new(config);
        let id = crate::EntityId::new();
        let stream_id = StreamId::new("SimpleTestEntity", id.as_uuid());

        let snapshot_entity = SimpleTestEntity { id, value: 20 };
        let snapshot = Snapshot::new(
            id.as_uuid(),
            "SimpleTestEntity".to_string(),
            AggregateVersion::new(2),
            serde_json::to_value(&snapshot_entity).unwrap(),
        );
        store
            .snapshots
            .lock()
            .unwrap()
            .insert(stream_id.clone(), snapshot);

        let make_env = |event: &SimpleTestEvent| {
            EventEnvelope::new(
                Uuid::new_v4(),
                id.as_uuid(),
                "SimpleTestEntity".to_string(),
                "SimpleTestEntity.Updated".to_string(),
                crate::EventVersion::new(1),
                serde_json::to_value(event).unwrap(),
            )
        };
        // Four events; snapshot is at version 2 so only events at index 2,3 apply.
        store.streams.lock().unwrap().insert(
            stream_id,
            vec![
                make_env(&SimpleTestEvent::Created { value: 10 }),
                make_env(&SimpleTestEvent::Updated { value: 20 }),
                make_env(&SimpleTestEvent::Updated { value: 30 }),
                make_env(&SimpleTestEvent::Updated { value: 40 }),
            ],
        );

        let loaded: AggregateRoot<SimpleTestEntity> = load(&store, id).await.unwrap();

        assert_eq!(loaded.value, 40, "snapshot + post-snapshot replay");
        assert_eq!(
            loaded.version(),
            AggregateVersion::new(4),
            "snapshot version (2) + 2 replayed events; proves snapshot path was taken"
        );
    }

    /// Aggregate whose snapshot schema version is `1` (state shape was
    /// "changed" relative to the default `0`), used to exercise the M4
    /// version-mismatch -> replay path without disturbing the shared
    /// `SimpleTestEntity` fixture.
    #[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
    struct VersionedTestEntity {
        id: EntityId,
        value: i32,
    }

    impl crate::Entity for VersionedTestEntity {
        fn new(id: EntityId) -> Self {
            Self { id, value: 0 }
        }
        fn entity_id(&self) -> EntityId {
            self.id
        }
    }

    impl crate::DefaultEntity for VersionedTestEntity {}

    #[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
    enum VersionedTestEvent {
        Created { value: i32 },
        Updated { value: i32 },
    }

    impl crate::DomainEvent for VersionedTestEvent {
        type Aggregate = VersionedTestEntity;
        fn event_type(&self) -> &'static str {
            match self {
                Self::Created { .. } => "VersionedTestEntity.Created",
                Self::Updated { .. } => "VersionedTestEntity.Updated",
            }
        }
        fn event_version(&self) -> crate::EventVersion {
            crate::EventVersion::new(1)
        }
        fn occurred_at(&self) -> chrono::DateTime<chrono::Utc> {
            chrono::Utc::now()
        }
    }

    impl crate::EventApplicator<VersionedTestEntity> for VersionedTestEvent {
        fn dispatch(
            &self,
            entity: &mut VersionedTestEntity,
        ) -> std::result::Result<(), crate::test_fixtures::SimpleTestError> {
            self.dispatch_unchecked(entity);
            Ok(())
        }
        fn dispatch_unchecked(&self, entity: &mut VersionedTestEntity) {
            match self {
                Self::Created { value } | Self::Updated { value } => entity.value = *value,
            }
        }
    }

    impl crate::Aggregate for VersionedTestEntity {
        type Event = VersionedTestEvent;
        type Error = crate::test_fixtures::SimpleTestError;
        type DeletedState = Self;

        fn snapshot_version() -> u32 {
            1
        }
    }

    #[tokio::test]
    async fn test_load_falls_through_to_replay_on_snapshot_schema_version_mismatch() {
        // M4 (VERSION-MISMATCH -> REPLAY): a snapshot stamped at schema version 0
        // is stale for an aggregate whose `snapshot_version()` is now 1. Even
        // though the snapshot data deserializes cleanly, its schema stamp no
        // longer matches, so it must be discarded as a cache miss and the state
        // rebuilt from full event replay. A subsequent commit must then persist a
        // fresh snapshot stamped at the current version (1).
        assert_eq!(VersionedTestEntity::snapshot_version(), 1);

        let store = CommitTestStore::new(SnapshotConfig::always());
        let id = crate::EntityId::new();
        let stream_id = StreamId::new("VersionedTestEntity", id.as_uuid());

        // Seed a structurally-valid but schema-version-0 snapshot at version 2
        // (the full stream length) carrying a sentinel value (999) that replay
        // never produces. The version (2) is deliberately >= the event count:
        // `load_stream` skips `from_version` events, so if the stale snapshot
        // were (wrongly) USED, no post-snapshot events would replay over it and
        // the loaded value would stay 999 — whereas a correct cache-miss replays
        // from genesis and yields 20. The assertion on 20 therefore FAILS if the
        // schema-version check is removed (the snapshot would be used → 999).
        let stale_entity = VersionedTestEntity { id, value: 999 };
        let stale_snapshot = Snapshot::new_with_schema_version(
            id.as_uuid(),
            "VersionedTestEntity".to_string(),
            AggregateVersion::new(2),
            serde_json::to_value(&stale_entity).unwrap(),
            0, // old schema version
        );
        store
            .snapshots
            .lock()
            .unwrap()
            .insert(stream_id.clone(), stale_snapshot);

        let make_env = |event: &VersionedTestEvent| {
            EventEnvelope::new(
                Uuid::new_v4(),
                id.as_uuid(),
                "VersionedTestEntity".to_string(),
                "VersionedTestEntity.Updated".to_string(),
                crate::EventVersion::new(1),
                serde_json::to_value(event).unwrap(),
            )
        };
        store.streams.lock().unwrap().insert(
            stream_id.clone(),
            vec![
                make_env(&VersionedTestEvent::Created { value: 10 }),
                make_env(&VersionedTestEvent::Updated { value: 20 }),
            ],
        );

        let loaded: AggregateRoot<VersionedTestEntity> = load(&store, id).await.unwrap();
        assert_eq!(
            loaded.value, 20,
            "stale-version snapshot must be skipped; state rebuilt from replay"
        );
        assert_eq!(loaded.version(), AggregateVersion::new(2));

        // A subsequent commit must write a fresh snapshot stamped at version 1.
        let mut agg = loaded;
        agg.apply(VersionedTestEvent::Updated { value: 30 })
            .unwrap();
        store.commit(&mut agg).await.unwrap();

        let snapshots = store.snapshots.lock().unwrap();
        let fresh = snapshots.get(&stream_id).expect("fresh snapshot written");
        assert_eq!(
            fresh.snapshot_schema_version, 1,
            "fresh snapshot must be stamped at the current schema version"
        );
    }

    #[tokio::test]
    async fn test_load_with_no_events_returns_not_found() {
        let store = CommitTestStore::new(SnapshotConfig::disabled());
        let id = crate::EntityId::new();

        // Load an entity that has no events at all: an empty stream means the
        // aggregate does not exist, so `load` must return `NotFound` rather than
        // a synthetic default-state root (unified behavior across all aggregate
        // kinds — see the empty-stream branch of `load_any`).
        let result: Result<AggregateRoot<SimpleTestEntity>> = load(&store, id).await;

        let err = result.expect_err("loading an aggregate with no events must be NotFound");
        assert!(err.is_not_found(), "expected NotFound, got: {err:?}");
    }

    #[tokio::test]
    async fn test_count_events_returns_correct_count() {
        let stream_id = StreamId::new("Order", Uuid::new_v4());
        let store = MockEventStoreWithStreams::new().with_stream(stream_id.clone(), 10);

        let count = count_events(&store, stream_id).await.unwrap();
        assert_eq!(count, 10, "Should count all events in stream");
    }

    #[tokio::test]
    async fn test_count_events_returns_zero_for_empty_stream() {
        let stream_id = StreamId::new("Order", Uuid::new_v4());
        let store = MockEventStoreWithStreams::new().with_stream(stream_id.clone(), 0);

        let count = count_events(&store, stream_id).await.unwrap();
        assert_eq!(count, 0, "Empty stream should have zero events");
    }

    #[tokio::test]
    async fn test_count_events_returns_zero_for_nonexistent_stream() {
        let store = MockEventStoreWithStreams::new();
        let stream_id = StreamId::new("Order", Uuid::new_v4());

        let count = count_events(&store, stream_id).await.unwrap();
        assert_eq!(count, 0, "Nonexistent stream should have zero events");
    }

    #[tokio::test]
    async fn test_load_any_returns_active_for_regular_aggregate() {
        let store = CommitTestStore::new(SnapshotConfig::disabled());
        let id = crate::EntityId::new();

        let mut agg = AggregateRoot::<DeletableEntity>::new(id);
        agg.apply(DeletableEvent::Created { value: 10 }).unwrap();
        agg.apply(DeletableEvent::Updated { value: 20 }).unwrap();
        store.commit(&mut agg).await.unwrap();

        let loaded = load_any::<_, DeletableEntity>(&store, id).await.unwrap();
        assert!(loaded.is_active());
        let active = loaded.into_active().unwrap();
        assert_eq!(active.value, 20);
    }

    #[tokio::test]
    async fn test_load_any_returns_deleted_when_delete_event_present() {
        let store = CommitTestStore::new(SnapshotConfig::disabled());
        let id = crate::EntityId::new();

        // Commit regular events
        let mut agg = AggregateRoot::<DeletableEntity>::new(id);
        agg.apply(DeletableEvent::Created { value: 10 }).unwrap();
        store.commit(&mut agg).await.unwrap();

        // Now commit a delete event via manual envelope
        let delete_event = DeletableEvent::Deleted {
            reason: "test".to_string(),
        };
        let envelope = EventEnvelope::new(
            Uuid::new_v4(),
            id.as_uuid(),
            "DeletableEntity".to_string(),
            "DeletableEntity.Deleted".to_string(),
            crate::EventVersion::new(1),
            serde_json::to_value(&delete_event).unwrap(),
        );
        store
            .streams
            .lock()
            .unwrap()
            .get_mut(&StreamId::new("DeletableEntity", id.as_uuid()))
            .unwrap()
            .push(envelope);

        let loaded = load_any::<_, DeletableEntity>(&store, id).await.unwrap();
        assert!(loaded.is_deleted());
        let deleted = loaded.into_deleted().unwrap();
        assert_eq!(deleted.state().value, -1);
        assert_eq!(deleted.entity_id(), id);
    }

    #[tokio::test]
    async fn test_load_errors_on_deleted_aggregate() {
        let store = CommitTestStore::new(SnapshotConfig::disabled());
        let id = crate::EntityId::new();

        // Commit regular events + delete event
        let mut agg = AggregateRoot::<DeletableEntity>::new(id);
        agg.apply(DeletableEvent::Created { value: 10 }).unwrap();
        store.commit(&mut agg).await.unwrap();

        let delete_event = DeletableEvent::Deleted {
            reason: "gone".to_string(),
        };
        let envelope = EventEnvelope::new(
            Uuid::new_v4(),
            id.as_uuid(),
            "DeletableEntity".to_string(),
            "DeletableEntity.Deleted".to_string(),
            crate::EventVersion::new(1),
            serde_json::to_value(&delete_event).unwrap(),
        );
        store
            .streams
            .lock()
            .unwrap()
            .get_mut(&StreamId::new("DeletableEntity", id.as_uuid()))
            .unwrap()
            .push(envelope);

        let result = load::<_, DeletableEntity>(&store, id).await;
        assert!(result.is_err());
        assert!(result.unwrap_err().is_aggregate_deleted());
    }

    #[tokio::test]
    async fn test_load_deleted_succeeds_for_deleted_aggregate() {
        let store = CommitTestStore::new(SnapshotConfig::disabled());
        let id = crate::EntityId::new();

        let mut agg = AggregateRoot::<DeletableEntity>::new(id);
        agg.apply(DeletableEvent::Created { value: 42 }).unwrap();
        store.commit(&mut agg).await.unwrap();

        let delete_event = DeletableEvent::Deleted {
            reason: "bye".to_string(),
        };
        let envelope = EventEnvelope::new(
            Uuid::new_v4(),
            id.as_uuid(),
            "DeletableEntity".to_string(),
            "DeletableEntity.Deleted".to_string(),
            crate::EventVersion::new(1),
            serde_json::to_value(&delete_event).unwrap(),
        );
        store
            .streams
            .lock()
            .unwrap()
            .get_mut(&StreamId::new("DeletableEntity", id.as_uuid()))
            .unwrap()
            .push(envelope);

        let deleted = load_deleted::<_, DeletableEntity>(&store, id)
            .await
            .unwrap();
        assert_eq!(deleted.state().value, -1);
        assert_eq!(deleted.version(), AggregateVersion::new(2));
    }

    #[tokio::test]
    async fn test_load_deleted_errors_for_active_aggregate() {
        let store = CommitTestStore::new(SnapshotConfig::disabled());
        let id = crate::EntityId::new();

        let mut agg = AggregateRoot::<DeletableEntity>::new(id);
        agg.apply(DeletableEvent::Created { value: 10 }).unwrap();
        store.commit(&mut agg).await.unwrap();

        let result = load_deleted::<_, DeletableEntity>(&store, id).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_load_any_active_then_commit_delete_then_load_any_deleted() {
        let store = CommitTestStore::new(SnapshotConfig::disabled());
        let id = crate::EntityId::new();

        // Create and save
        let mut agg = AggregateRoot::<DeletableEntity>::new(id);
        agg.apply(DeletableEvent::Created { value: 42 }).unwrap();
        store.commit(&mut agg).await.unwrap();

        // Load — should be active
        let loaded = load_any::<_, DeletableEntity>(&store, id).await.unwrap();
        assert!(loaded.is_active());
        let agg = loaded.into_active().unwrap();

        // Delete and save
        let mut deleted = agg
            .apply_delete(DeletableEvent::Deleted {
                reason: "done".to_string(),
            })
            .unwrap();
        store.commit_deleted(&mut deleted).await.unwrap();

        // Load again — should be deleted
        let loaded = load_any::<_, DeletableEntity>(&store, id).await.unwrap();
        assert!(loaded.is_deleted());
        let deleted = loaded.into_deleted().unwrap();
        assert_eq!(deleted.state().value, -1);
        assert_eq!(deleted.version(), AggregateVersion::new(2));
    }

    #[tokio::test]
    async fn test_load_any_returns_deleted_from_deleted_snapshot() {
        let store = CommitTestStore::new(SnapshotConfig::always());
        let id = crate::EntityId::new();

        // Pre-save a deleted snapshot
        let deleted_entity = DeletableEntity { id, value: -1 };
        let snapshot_data = serde_json::to_value(&deleted_entity).unwrap();
        let stream_id = StreamId::new("DeletableEntity", id.as_uuid());
        let snapshot = Snapshot::new_deleted(
            id.as_uuid(),
            "DeletableEntity".to_string(),
            AggregateVersion::new(3),
            snapshot_data,
        );
        store.snapshots.lock().unwrap().insert(stream_id, snapshot);

        let loaded = load_any::<_, DeletableEntity>(&store, id).await.unwrap();
        assert!(loaded.is_deleted());
        let deleted = loaded.into_deleted().unwrap();
        assert_eq!(deleted.state().value, -1);
        assert_eq!(deleted.entity_id(), id);
        assert_eq!(deleted.version(), AggregateVersion::new(3));
    }

    #[tokio::test]
    async fn test_load_any_deleted_snapshot_round_trip() {
        let store = CommitTestStore::new(SnapshotConfig::always());
        let id = crate::EntityId::new();

        // Create, then delete
        let mut agg = AggregateRoot::<DeletableEntity>::new(id);
        agg.apply(DeletableEvent::Created { value: 42 }).unwrap();

        let mut deleted = agg
            .apply_delete(DeletableEvent::Deleted {
                reason: "done".to_string(),
            })
            .unwrap();

        store.commit_deleted(&mut deleted).await.unwrap();

        // Verify snapshot was saved as deleted
        let stream_id = StreamId::new("DeletableEntity", id.as_uuid());
        {
            let snapshots = store.snapshots.lock().unwrap();
            let snapshot = snapshots.get(&stream_id).expect("Snapshot should exist");
            assert!(snapshot.is_deleted);
        }

        // Load — should come from deleted snapshot
        let loaded = load_any::<_, DeletableEntity>(&store, id).await.unwrap();
        assert!(loaded.is_deleted());
        let loaded_deleted = loaded.into_deleted().unwrap();
        assert_eq!(loaded_deleted.state().value, -1);
        assert_eq!(loaded_deleted.version(), AggregateVersion::new(2));
    }

    #[tokio::test]
    async fn load_of_a_fully_encrypted_aggregate_decrypts_back_to_the_original() {
        let (store, _key_store) = crypto_test_store();
        let id = crate::EntityId::new();
        let mut agg = AggregateRoot::<SecretThing>::new(id);
        agg.apply(SecretCreated {
            email: "alice@example.com".into(),
        })
        .unwrap();
        store.commit(&mut agg).await.unwrap();

        let loaded = load_any::<CommitTestStore, SecretThing>(&store, id)
            .await
            .unwrap()
            .into_active()
            .unwrap();

        assert_eq!(loaded.entity().email, "alice@example.com");
    }

    #[tokio::test]
    async fn load_any_of_a_fully_encrypted_aggregate_needs_a_key_store() {
        let store = CommitTestStore::new(SnapshotConfig::disabled());

        let result = load_any::<CommitTestStore, SecretThing>(&store, crate::EntityId::new()).await;

        assert!(result.unwrap_err().is_invalid_state());
    }

    #[tokio::test]
    async fn load_any_of_a_fully_encrypted_aggregate_with_no_key_is_key_not_found() {
        let (store, _key_store) = crypto_test_store();

        let result = load_any::<CommitTestStore, SecretThing>(&store, crate::EntityId::new()).await;

        assert!(result.unwrap_err().is_key_not_found());
    }

    #[tokio::test]
    async fn load_any_of_a_field_encrypted_aggregate_with_no_store_is_not_found() {
        let store = CommitTestStore::new(SnapshotConfig::disabled());

        let result =
            load_any::<CommitTestStore, PartialSecretThing>(&store, crate::EntityId::new()).await;

        assert!(
            result.unwrap_err().is_not_found(),
            "no key store and no committed data must fall through to NotFound, \
             not surface a crypto error"
        );
    }

    #[tokio::test]
    async fn load_any_of_a_field_encrypted_aggregate_with_no_key_and_no_data_is_not_found() {
        let (store, _key_store) = crypto_test_store();

        let result =
            load_any::<CommitTestStore, PartialSecretThing>(&store, crate::EntityId::new()).await;

        assert!(result.unwrap_err().is_not_found());
    }

    #[tokio::test]
    async fn load_any_of_a_field_encrypted_aggregate_with_committed_data_and_no_key_is_key_not_found(
    ) {
        let (store, key_store) = crypto_test_store();
        let id = crate::EntityId::new();
        let mut agg = AggregateRoot::<PartialSecretThing>::new(id);
        agg.apply(PartialSecretCreated {
            public: "visible".into(),
            secret: "hidden".into(),
        })
        .unwrap();
        store.commit(&mut agg).await.unwrap();
        key_store.delete_key(id.as_uuid()).await.unwrap();

        let result = load_any::<CommitTestStore, PartialSecretThing>(&store, id).await;

        assert!(result.unwrap_err().is_key_not_found());
    }
}
