//! In-memory state store implementation.
//!
//! Provides a fast, thread-safe in-memory implementation of `StateStore`
//! suitable for testing and development, including in-transaction projections
//! running against an [`InMemoryProjectionContext`].

use async_trait::async_trait;
use event_sauce_core::{
    AggregateClaim, AggregateVersion, Error, EventEnvelope, Result, StateCommit, StateProjection,
    StateStore, StoredState, StreamId,
};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use uuid::Uuid;

/// Keyed JSON read-model state handed to in-transaction projections.
///
/// This is the `Ctx` type of [`StateProjection`] handlers registered on an
/// [`InMemoryStateStore`]: a simple map from projection-chosen keys to
/// [`serde_json::Value`]s, living inside the store and mutated only inside
/// [`StateStore::save`] / [`StateStore::save_batch`]. After a successful save,
/// tests and applications read it back through
/// [`InMemoryStateStore::projection_state`].
///
/// # Examples
///
/// ```
/// use event_sauce_memory::InMemoryProjectionContext;
/// use serde_json::json;
///
/// let mut ctx = InMemoryProjectionContext::default();
/// ctx.insert("total", json!(42));
/// assert_eq!(ctx.get("total"), Some(&json!(42)));
/// assert_eq!(ctx.remove("total"), Some(json!(42)));
/// assert_eq!(ctx.get("total"), None);
/// ```
#[derive(Debug, Clone, Default)]
pub struct InMemoryProjectionContext {
    values: HashMap<String, serde_json::Value>,
}

impl InMemoryProjectionContext {
    /// Returns the value stored under `key`, if any.
    #[must_use]
    pub fn get(&self, key: &str) -> Option<&serde_json::Value> {
        self.values.get(key)
    }

    /// Stores `value` under `key`, replacing any previous value.
    pub fn insert(&mut self, key: impl Into<String>, value: serde_json::Value) {
        self.values.insert(key.into(), value);
    }

    /// Removes and returns the value stored under `key`, if any.
    pub fn remove(&mut self, key: &str) -> Option<serde_json::Value> {
        self.values.remove(key)
    }
}

#[derive(Debug, Clone, Default)]
struct ClaimsData {
    holders: HashMap<(String, String), Uuid>,
    by_aggregate: HashMap<Uuid, HashMap<String, String>>,
}

impl ClaimsData {
    fn check(&self, aggregate_id: Uuid, claims: &[AggregateClaim]) -> Result<()> {
        for claim in claims {
            let key = (claim.claim_type.to_string(), claim.claim_key.to_string());
            if let Some(holder) = self.holders.get(&key) {
                if *holder != aggregate_id {
                    return Err(Error::claim_conflict(
                        claim.claim_type,
                        claim.claim_key.clone(),
                        Some(*holder),
                    ));
                }
            }
        }
        Ok(())
    }

    fn upsert(&mut self, aggregate_id: Uuid, claims: &[AggregateClaim]) {
        let held = self.by_aggregate.entry(aggregate_id).or_default();
        let stale: Vec<String> = held
            .keys()
            .filter(|claim_type| !claims.iter().any(|c| c.claim_type == claim_type.as_str()))
            .cloned()
            .collect();
        for claim_type in stale {
            if let Some(old_key) = held.remove(&claim_type) {
                self.holders.remove(&(claim_type, old_key));
            }
        }
        for claim in claims {
            let claim_type = claim.claim_type.to_string();
            let claim_key = claim.claim_key.to_string();
            if let Some(old_key) = held.insert(claim_type.clone(), claim_key.clone()) {
                if old_key != claim_key {
                    self.holders.remove(&(claim_type.clone(), old_key));
                }
            }
            self.holders.insert((claim_type, claim_key), aggregate_id);
        }
    }

    fn clear(&mut self, aggregate_id: Uuid) {
        if let Some(held) = self.by_aggregate.remove(&aggregate_id) {
            for entry in held {
                self.holders.remove(&entry);
            }
        }
    }
}

#[derive(Debug, Clone, Default)]
struct StateData {
    states: HashMap<StreamId, StoredState>,
    claims: ClaimsData,
    projection_context: InMemoryProjectionContext,
}

impl StateData {
    fn stage_commit(&mut self, commit: &StateCommit) -> Result<()> {
        let key = commit.state.stream_id();
        let current = self
            .states
            .get(&key)
            .map_or_else(AggregateVersion::initial, |state| state.version);
        if current != commit.expected_version {
            return Err(Error::concurrency_conflict(
                commit.expected_version,
                current,
            ));
        }

        let aggregate_id = commit.state.aggregate_id;
        if !commit.claims.is_empty() {
            self.claims.check(aggregate_id, &commit.claims)?;
            self.claims.upsert(aggregate_id, &commit.claims);
        }
        if commit.clear_claims {
            self.claims.clear(aggregate_id);
        }

        self.states.insert(key, commit.state.clone());
        Ok(())
    }
}

struct InMemoryStateStoreInner {
    data: RwLock<StateData>,
    projections: Vec<Arc<dyn StateProjection<InMemoryProjectionContext>>>,
}

/// In-memory state store for testing and development.
///
/// Persists each aggregate as one versioned [`StoredState`] row with
/// optimistic concurrency control, enforces [`AggregateClaim`] uniqueness
/// across aggregates, and runs registered [`StateProjection`] handlers
/// against an [`InMemoryProjectionContext`] as part of each save.
///
/// # Transactionality
///
/// Every save stages its effects on a copy of the store's data (state rows,
/// claims, and projection context) under the store's single write lock and
/// swaps the copy in only after the version check, the claim checks, and
/// every projection succeed. A failing projection therefore leaves rows,
/// claims, and the projection context unchanged — best-effort
/// transactionality that mirrors the real rollback semantics of
/// transactional backends closely enough to test application and projection
/// code without a database.
///
/// Unlike the event-store default that loops per stream,
/// [`save_batch`](StateStore::save_batch) applies the whole batch atomically:
/// everything happens under the one lock, so a failing commit leaves every
/// other commit in the batch unapplied too.
///
/// # Thread Safety
///
/// The store is thread-safe and can be cloned cheaply (uses `Arc` internally).
///
/// # Examples
///
/// ```
/// use event_sauce_core::{StateStore, StreamId};
/// use event_sauce_memory::InMemoryStateStore;
/// use uuid::Uuid;
///
/// #[tokio::main]
/// async fn main() -> Result<(), Box<dyn std::error::Error>> {
///     let store = InMemoryStateStore::new();
///     let stream_id = StreamId::new("User", Uuid::new_v4());
///     assert!(store.load(stream_id).await?.is_none());
///     Ok(())
/// }
/// ```
#[derive(Clone)]
pub struct InMemoryStateStore {
    inner: Arc<InMemoryStateStoreInner>,
}

impl InMemoryStateStore {
    /// Creates a new empty in-memory state store with no projections.
    ///
    /// Equivalent to `InMemoryStateStore::builder().build()`.
    ///
    /// # Examples
    ///
    /// ```
    /// use event_sauce_memory::InMemoryStateStore;
    ///
    /// let store = InMemoryStateStore::new();
    /// ```
    #[must_use]
    pub fn new() -> Self {
        Self::builder().build()
    }

    /// Creates a builder for configuring the state store.
    ///
    /// This is the recommended way to create an `InMemoryStateStore` when you
    /// need to register in-transaction projections.
    ///
    /// # Examples
    ///
    /// ```
    /// use event_sauce_memory::InMemoryStateStore;
    ///
    /// let store = InMemoryStateStore::builder().build();
    /// ```
    #[must_use]
    pub fn builder() -> InMemoryStateStoreBuilder {
        InMemoryStateStoreBuilder::new()
    }

    /// Returns the projection-context value stored under `key`, if any.
    ///
    /// This is the read side of the in-transaction projections: handlers
    /// write into the store's [`InMemoryProjectionContext`] during
    /// [`save`](StateStore::save), and tests or applications read the
    /// committed result back here.
    ///
    /// # Examples
    ///
    /// ```
    /// use event_sauce_memory::InMemoryStateStore;
    ///
    /// #[tokio::main]
    /// async fn main() {
    ///     let store = InMemoryStateStore::new();
    ///     assert!(store.projection_state("order_totals").await.is_none());
    /// }
    /// ```
    pub async fn projection_state(&self, key: &str) -> Option<serde_json::Value> {
        self.inner
            .data
            .read()
            .await
            .projection_context
            .get(key)
            .cloned()
    }

    async fn commit_all(&self, commits: Vec<StateCommit>) -> Result<()> {
        let mut data = self.inner.data.write().await;
        let mut staged = data.clone();
        for commit in &commits {
            staged.stage_commit(commit)?;
            for projection in &self.inner.projections {
                let filter = projection.filter();
                let matching: Vec<EventEnvelope> = commit
                    .events
                    .iter()
                    .filter(|event| filter.matches(event))
                    .cloned()
                    .collect();
                if !matching.is_empty() {
                    projection
                        .project(&mut staged.projection_context, &matching)
                        .await?;
                }
            }
        }
        *data = staged;
        Ok(())
    }
}

impl Default for InMemoryStateStore {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl StateStore for InMemoryStateStore {
    async fn load(&self, stream_id: StreamId) -> Result<Option<StoredState>> {
        Ok(self.inner.data.read().await.states.get(&stream_id).cloned())
    }

    /// Persists one state commit; see the [`InMemoryStateStore`]
    /// transactionality notes.
    ///
    /// # Errors
    ///
    /// Returns `Error::ConcurrencyConflict` on a stale
    /// [`expected_version`](StateCommit::expected_version),
    /// `Error::ClaimConflict` when a claim is held by another aggregate, and
    /// any error a registered projection returns. On error nothing is
    /// applied.
    async fn save(&self, commit: StateCommit) -> Result<()> {
        self.commit_all(vec![commit]).await
    }

    /// Persists several state commits atomically.
    ///
    /// Unlike the trait's looping default, this store applies the whole batch
    /// all-or-nothing: every commit is staged and projected under the single
    /// write lock and swapped in together, so a failure in any commit leaves
    /// all of them unapplied.
    ///
    /// # Errors
    ///
    /// Returns the first commit failure (version conflict, claim conflict, or
    /// projection error); on error no commit in the batch is applied.
    async fn save_batch(&self, commits: Vec<StateCommit>) -> Result<()> {
        self.commit_all(commits).await
    }
}

/// Builder for configuring `InMemoryStateStore`.
///
/// # Examples
///
/// ```
/// use std::sync::Arc;
/// use async_trait::async_trait;
/// use event_sauce_core::{EventEnvelope, Result, StateProjection};
/// use event_sauce_memory::{InMemoryProjectionContext, InMemoryStateStore};
///
/// struct EventCount;
///
/// #[async_trait]
/// impl StateProjection<InMemoryProjectionContext> for EventCount {
///     fn name(&self) -> &str {
///         "event_count"
///     }
///
///     async fn project(
///         &self,
///         ctx: &mut InMemoryProjectionContext,
///         events: &[EventEnvelope],
///     ) -> Result<()> {
///         let seen = ctx.get("seen").and_then(serde_json::Value::as_u64).unwrap_or(0);
///         ctx.insert("seen", (seen + events.len() as u64).into());
///         Ok(())
///     }
/// }
///
/// let store = InMemoryStateStore::builder()
///     .with_projection(Arc::new(EventCount))
///     .build();
/// ```
#[derive(Clone, Default)]
pub struct InMemoryStateStoreBuilder {
    projections: Vec<Arc<dyn StateProjection<InMemoryProjectionContext>>>,
}

impl InMemoryStateStoreBuilder {
    /// Creates a new builder with no projections registered.
    #[must_use]
    pub fn new() -> Self {
        Self {
            projections: Vec::new(),
        }
    }

    /// Registers an in-transaction projection.
    ///
    /// The projection runs inside every [`save`](StateStore::save) /
    /// [`save_batch`](StateStore::save_batch) with the commit's events
    /// matching its [`filter`](StateProjection::filter) (skipped entirely when
    /// none match). Projections run in registration order; an error from any
    /// of them fails the save and rolls back state rows, claims, and the
    /// projection context.
    #[must_use]
    pub fn with_projection(
        mut self,
        projection: Arc<dyn StateProjection<InMemoryProjectionContext>>,
    ) -> Self {
        self.projections.push(projection);
        self
    }

    /// Builds the `InMemoryStateStore` with the configured settings.
    #[must_use]
    pub fn build(self) -> InMemoryStateStore {
        InMemoryStateStore {
            inner: Arc::new(InMemoryStateStoreInner {
                data: RwLock::new(StateData::default()),
                projections: self.projections,
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{InMemoryProjectionContext, InMemoryStateStore, InMemoryStateStoreBuilder};
    use event_sauce_core::{
        AggregateClaim, AggregateVersion, StateCommit, StateStore, StoredState, StreamId,
    };
    use serde_json::json;
    use uuid::Uuid;

    fn commit_for(aggregate_id: Uuid, version: i64, claims: Vec<AggregateClaim>) -> StateCommit {
        StateCommit {
            state: StoredState {
                aggregate_id,
                aggregate_type: "Test".into(),
                state_data: json!({"version": version}),
                version: AggregateVersion::new(version),
                is_deleted: false,
                schema_version: 1,
            },
            expected_version: AggregateVersion::new(version - 1),
            events: vec![],
            claims,
            clear_claims: false,
        }
    }

    #[tokio::test]
    async fn test_load_missing_stream_returns_none() {
        let store = InMemoryStateStore::new();
        let stream_id = StreamId::new("Test", Uuid::new_v4());
        assert!(store.load(stream_id).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_save_and_load_roundtrip() {
        let store = InMemoryStateStore::new();
        let aggregate_id = Uuid::new_v4();

        store
            .save(commit_for(aggregate_id, 1, vec![]))
            .await
            .unwrap();

        let stream_id = StreamId::new("Test", aggregate_id);
        let loaded = store.load(stream_id.clone()).await.unwrap().unwrap();
        assert_eq!(loaded.version, AggregateVersion::new(1));
        assert_eq!(
            store.get_version(stream_id.clone()).await.unwrap().as_i64(),
            1
        );
        assert!(store.exists(stream_id).await.unwrap());
    }

    #[tokio::test]
    async fn test_stale_expected_version_conflicts() {
        let store = InMemoryStateStore::new();
        let aggregate_id = Uuid::new_v4();

        store
            .save(commit_for(aggregate_id, 1, vec![]))
            .await
            .unwrap();
        let err = store
            .save(commit_for(aggregate_id, 1, vec![]))
            .await
            .unwrap_err();
        assert!(err.is_concurrency_conflict());
    }

    #[tokio::test]
    async fn test_changing_a_claim_releases_the_old_value() {
        let store = InMemoryStateStore::new();
        let first = Uuid::new_v4();
        let second = Uuid::new_v4();

        store
            .save(commit_for(
                first,
                1,
                vec![AggregateClaim::new("Test.name", json!("original"))],
            ))
            .await
            .unwrap();
        store
            .save(commit_for(
                first,
                2,
                vec![AggregateClaim::new("Test.name", json!("renamed"))],
            ))
            .await
            .unwrap();

        store
            .save(commit_for(
                second,
                1,
                vec![AggregateClaim::new("Test.name", json!("original"))],
            ))
            .await
            .unwrap();

        let err = store
            .save(commit_for(
                second,
                2,
                vec![AggregateClaim::new("Test.name", json!("renamed"))],
            ))
            .await
            .unwrap_err();
        assert!(err.is_claim_conflict());
    }

    #[tokio::test]
    async fn test_dropping_a_claim_type_releases_it() {
        let store = InMemoryStateStore::new();
        let first = Uuid::new_v4();
        let second = Uuid::new_v4();

        store
            .save(commit_for(
                first,
                1,
                vec![
                    AggregateClaim::new("Test.name", json!("kept")),
                    AggregateClaim::new("Test.alias", json!("dropped")),
                ],
            ))
            .await
            .unwrap();
        store
            .save(commit_for(
                first,
                2,
                vec![AggregateClaim::new("Test.name", json!("kept"))],
            ))
            .await
            .unwrap();

        store
            .save(commit_for(
                second,
                1,
                vec![AggregateClaim::new("Test.alias", json!("dropped"))],
            ))
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_empty_claims_preserve_existing_claims() {
        let store = InMemoryStateStore::new();
        let first = Uuid::new_v4();
        let second = Uuid::new_v4();

        store
            .save(commit_for(
                first,
                1,
                vec![AggregateClaim::new("Test.name", json!("held"))],
            ))
            .await
            .unwrap();
        store.save(commit_for(first, 2, vec![])).await.unwrap();

        let err = store
            .save(commit_for(
                second,
                1,
                vec![AggregateClaim::new("Test.name", json!("held"))],
            ))
            .await
            .unwrap_err();
        assert!(err.is_claim_conflict());
    }

    #[tokio::test]
    async fn test_clear_claims_releases_everything() {
        let store = InMemoryStateStore::new();
        let first = Uuid::new_v4();
        let second = Uuid::new_v4();

        store
            .save(commit_for(
                first,
                1,
                vec![AggregateClaim::new("Test.name", json!("held"))],
            ))
            .await
            .unwrap();

        let mut tombstone = commit_for(first, 2, vec![]);
        tombstone.state.is_deleted = true;
        tombstone.clear_claims = true;
        store.save(tombstone).await.unwrap();

        store
            .save(commit_for(
                second,
                1,
                vec![AggregateClaim::new("Test.name", json!("held"))],
            ))
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_store_is_cloneable_and_shares_data() {
        let store = InMemoryStateStore::new();
        let clone = store.clone();
        let aggregate_id = Uuid::new_v4();

        store
            .save(commit_for(aggregate_id, 1, vec![]))
            .await
            .unwrap();

        let stream_id = StreamId::new("Test", aggregate_id);
        assert!(clone.exists(stream_id).await.unwrap());
    }

    #[tokio::test]
    async fn test_default_equals_new() {
        let store = InMemoryStateStore::default();
        let stream_id = StreamId::new("Test", Uuid::new_v4());
        assert!(!store.exists(stream_id).await.unwrap());
    }

    #[tokio::test]
    async fn test_builder_new_equals_default() {
        let store1 = InMemoryStateStoreBuilder::new().build();
        let store2 = InMemoryStateStoreBuilder::default().build();
        let stream_id = StreamId::new("Test", Uuid::new_v4());
        assert!(!store1.exists(stream_id.clone()).await.unwrap());
        assert!(!store2.exists(stream_id).await.unwrap());
    }

    #[test]
    fn test_projection_context_accessors() {
        let mut ctx = InMemoryProjectionContext::default();
        assert!(ctx.get("missing").is_none());
        ctx.insert("key", json!(1));
        assert_eq!(ctx.get("key"), Some(&json!(1)));
        ctx.insert("key", json!(2));
        assert_eq!(ctx.remove("key"), Some(json!(2)));
        assert!(ctx.remove("key").is_none());
    }
}
