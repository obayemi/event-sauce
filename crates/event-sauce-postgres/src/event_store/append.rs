//! Helpers for [`PostgresEventStore::append`]/[`PostgresEventStore::append_batch`]:
//! version-chain validation, the append-serialization lock, and the batch
//! insert.

use event_sauce_core::{AggregateVersion, Error, Position, Result, StreamId};

use super::PostgresEventStore;

/// Computes the stable advisory-lock key for an event log: the schema-
/// qualified events table name (e.g. `"public.events"`), hashed by
/// [`advisory_lock_key`](crate::migrations::advisory_lock_key), whose doc
/// carries the hash algorithm's stability contract. Keying on the qualified
/// table name isolates different schemas / test databases — they take
/// distinct keys and never serialize against one another.
///
/// [`PostgresEventStore::append`] takes a transaction-scoped advisory lock on
/// this key before allocating any global event ids, so that id order equals
/// commit order across all streams sharing the table (see
/// [`PostgresEventStore::append`] for the resulting guarantee).
pub(super) fn append_lock_key(qualified_events_table: &str) -> i64 {
    crate::migrations::advisory_lock_key(qualified_events_table)
}

/// A commit with nothing to persist: no events, no claims, and no claim
/// clearing. `append`/`append_batch` drop these rather than opening a
/// transaction for them.
pub(super) fn is_noop_commit(commit: &event_sauce_core::StreamCommit) -> bool {
    commit.events.is_empty() && commit.claims.is_empty() && !commit.clear_claims
}

/// Validates a whole batch's version chain in memory, with no I/O, so a
/// doomed batch is rejected before a transaction is even opened.
///
/// `stream_version` is dense and rows are never deleted, so a stream's
/// first commit in the batch is the only one whose expected version can
/// be checked against the database — that check is left to the caller,
/// via [`PostgresEventStore::precheck_version`]. Every later commit to the
/// same stream must supply the version the previous commit to that stream
/// leaves it at, or the batch is rejected as a [`Error::concurrency_conflict`].
///
/// Returns, in batch order, the indices of the commits whose stream is
/// seen here for the first time and therefore still need
/// [`PostgresEventStore::precheck_version`] against the database.
pub(super) fn plan_version_chain_checks(
    commits: &[event_sauce_core::StreamCommit],
) -> Result<Vec<usize>> {
    let mut next_required: std::collections::HashMap<&StreamId, AggregateVersion> =
        std::collections::HashMap::new();
    let mut needs_db_check = Vec::new();

    for (index, commit) in commits.iter().enumerate() {
        if commit.events.is_empty() {
            continue;
        }
        let event_count = i64::try_from(commit.events.len())
            .map_err(|_| Error::invalid_state("a single commit cannot carry this many events"))?;

        if let Some(&required) = next_required.get(&commit.stream_id) {
            if commit.expected_version != required {
                return Err(Error::concurrency_conflict(
                    commit.expected_version,
                    required,
                ));
            }
        } else {
            needs_db_check.push(index);
        }

        let leaves_at = AggregateVersion::new(commit.expected_version.as_i64() + event_count);
        next_required.insert(&commit.stream_id, leaves_at);
    }

    Ok(needs_db_check)
}

impl PostgresEventStore {
    /// Reads the current committed stream version through `executor`, which
    /// is either the pool (for a plain read, see [`Self::get_version`]) or an
    /// open transaction (so callers observe their own uncommitted writes and
    /// any committed concurrent writes under READ COMMITTED, see
    /// [`Self::precheck_version`]).
    ///
    /// Computes `MAX(stream_version) + 1` for the stream, treating an empty
    /// stream (`NULL`) as [`AggregateVersion::initial`].
    pub(super) async fn current_stream_version<'e, E>(
        executor: E,
        events_table: &str,
        stream_id: &StreamId,
    ) -> Result<AggregateVersion>
    where
        E: sqlx::Executor<'e, Database = sqlx::Postgres>,
    {
        let query = format!(
            "SELECT MAX(stream_version) FROM {events_table} WHERE aggregate_id = $1 AND aggregate_type = $2"
        );
        let current_version: Option<i64> = sqlx::query_scalar(&query)
            .bind(stream_id.aggregate_id())
            .bind(stream_id.aggregate_type().as_str())
            .fetch_one(executor)
            .await
            .map_err(|e| Error::backend("Failed to check version", e))?;

        Ok(AggregateVersion::new(current_version.unwrap_or(-1) + 1))
    }

    /// Takes the per-log append serialization advisory lock inside the given
    /// transaction.
    ///
    /// Serializes id-assignment-to-commit across ALL streams sharing this log.
    /// The `events.id` BIGSERIAL is allocated at INSERT but the row only becomes
    /// visible at COMMIT, so under READ COMMITTED two concurrent appends to
    /// different streams can take ids N and N+1 yet commit in the opposite order.
    /// A checkpoint reader scanning `WHERE id > checkpoint ORDER BY id ASC` could
    /// then observe N+1, advance its checkpoint past it, and never see N once it
    /// commits — silent, permanent event loss. Taking a transaction-scoped
    /// advisory lock here, before allocating any id, forces insert order to equal
    /// commit order. `pg_advisory_xact_lock` auto-releases at commit/rollback (no
    /// manual unlock). Readers stay fully concurrent: the advisory lock does not
    /// block SELECT. Only id-allocating appends take it — claims-only /
    /// clear-only writes allocate no ids and must not serialize on it.
    pub(super) async fn acquire_append_lock(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        events_table: &str,
    ) -> Result<()> {
        #[allow(clippy::cast_possible_truncation)]
        let lock_timeout_ms = self.append_lock_timeout.as_millis() as i64;
        sqlx::query(&format!("SET LOCAL lock_timeout = {lock_timeout_ms}"))
            .execute(&mut **tx)
            .await
            .map_err(|e| Error::backend("Failed to set append lock timeout", e))?;
        sqlx::query("SELECT pg_advisory_xact_lock($1)")
            .bind(append_lock_key(events_table))
            .execute(&mut **tx)
            .await
            .map_err(|e| Error::backend("Failed to acquire append serialization lock", e))?;
        Ok(())
    }

    /// Verifies `expected_version` against `stream_id`'s currently committed
    /// version. A fast-fail optimization only, run for a stream's first
    /// commit in a batch (see [`plan_version_chain_checks`]) before
    /// [`Self::acquire_append_lock`] so a doomed commit never contends for
    /// the lock. It is not authoritative: a commit that passes here can
    /// still lose a race for the lock to another writer of the same stream,
    /// and the `UNIQUE(aggregate_id, aggregate_type, stream_version)` index
    /// is what catches that at insert time.
    ///
    /// Callers only invoke this for a commit with events: an empty commit
    /// has no version to verify.
    pub(super) async fn precheck_version(
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        events_table: &str,
        stream_id: &StreamId,
        expected_version: AggregateVersion,
    ) -> Result<()> {
        let current_version =
            Self::current_stream_version(&mut **tx, events_table, stream_id).await?;
        if current_version != expected_version {
            return Err(Error::concurrency_conflict(
                expected_version,
                current_version,
            ));
        }
        Ok(())
    }

    /// Inserts every event of one commit in a single `INSERT ... SELECT FROM
    /// UNNEST(...)` statement, one round trip regardless of batch size.
    /// Persists inside an already-open transaction that already holds the
    /// append lock; claims are enforced separately, by
    /// [`Self::append_batch`], once that same lock is held. Its only caller
    /// filters out empty commits first, since an empty commit has nothing to
    /// insert.
    ///
    /// Trusts the version chain [`plan_version_chain_checks`] and
    /// [`Self::precheck_version`] already validated before the lock was
    /// taken, so it does not re-check the version itself.
    ///
    /// Returns the highest inserted [`Position`]. A unique-index violation (a
    /// concurrent writer took one of these `stream_version` slots first)
    /// aborts the whole multi-row insert atomically — nothing from this
    /// commit is left behind — and is translated to a typed
    /// `ConcurrencyConflict`. The violation aborts the transaction (any
    /// further query in it would fail with `25P02`), so the committed
    /// version cannot be re-read here; the reported `actual` is only the
    /// lower bound `expected + 1`, since the winner committed at
    /// `expected_version` and the true current version is at least one
    /// beyond it.
    ///
    /// Does NOT begin/commit the transaction or emit NOTIFY —
    /// [`Self::append_batch`], its only caller, owns the transaction and
    /// issues a single NOTIFY for the whole batch once every commit has been
    /// written.
    pub(super) async fn insert_events_batch(
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        events_table: &str,
        commit: &event_sauce_core::StreamCommit,
    ) -> Result<Option<Position>> {
        let stream_id = &commit.stream_id;
        let expected_version = commit.expected_version;
        let events = &commit.events;
        let aggregate_id = stream_id.aggregate_id();
        let aggregate_type = stream_id.aggregate_type().as_str().to_string();

        let event_ids: Vec<uuid::Uuid> = events.iter().map(|e| e.id).collect();
        let aggregate_ids = vec![aggregate_id; events.len()];
        let aggregate_types = vec![aggregate_type; events.len()];
        let event_types: Vec<String> = events.iter().map(|e| e.event_type.clone()).collect();
        let event_versions: Vec<i64> = events.iter().map(|e| e.event_version.as_i64()).collect();
        let event_datas: Vec<serde_json::Value> =
            events.iter().map(|e| e.event_data.clone()).collect();
        let stream_versions: Vec<i64> = (expected_version.as_i64()..).take(events.len()).collect();
        let created_bys: Vec<Option<uuid::Uuid>> = events.iter().map(|e| e.created_by).collect();
        let correlation_ids: Vec<Option<uuid::Uuid>> = events
            .iter()
            .map(|e| e.metadata.as_ref().and_then(|m| m.correlation_id))
            .collect();
        let causation_ids: Vec<Option<uuid::Uuid>> = events
            .iter()
            .map(|e| e.metadata.as_ref().and_then(|m| m.causation_id))
            .collect();
        let metadatas: Vec<Option<serde_json::Value>> = events
            .iter()
            .map(|e| e.metadata.as_ref().and_then(|m| m.additional.clone()))
            .collect();

        let insert_query = format!(
            "INSERT INTO {events_table} (
                event_id, aggregate_id, aggregate_type, event_type, event_version,
                event_data, stream_version, created_by, correlation_id, causation_id, metadata
            )
            SELECT * FROM UNNEST(
                $1::uuid[], $2::uuid[], $3::text[], $4::text[], $5::bigint[],
                $6::jsonb[], $7::bigint[], $8::uuid[], $9::uuid[], $10::uuid[], $11::jsonb[]
            )
            RETURNING id"
        );

        let insert_result = sqlx::query_scalar::<_, i64>(&insert_query)
            .bind(event_ids)
            .bind(aggregate_ids)
            .bind(aggregate_types)
            .bind(event_types)
            .bind(event_versions)
            .bind(event_datas)
            .bind(stream_versions)
            .bind(created_bys)
            .bind(correlation_ids)
            .bind(causation_ids)
            .bind(metadatas)
            .fetch_all(&mut **tx)
            .await;

        match insert_result {
            Ok(ids) => Ok(ids.into_iter().max().map(Position::new)),
            Err(sqlx::Error::Database(db_err)) if db_err.is_unique_violation() => {
                let actual = AggregateVersion::new(expected_version.as_i64() + 1);
                Err(Error::concurrency_conflict(expected_version, actual))
            }
            Err(e) => Err(Error::backend("Failed to insert events", e)),
        }
    }

    /// Emits a single NOTIFY carrying the highest inserted position for the
    /// transaction. Postgres holds notifications until commit, so this fires
    /// only if the transaction succeeds. `None` means no events were
    /// inserted (claims-only / clear-only) — nothing to notify.
    pub(super) async fn notify_inserted(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        last_inserted: Option<Position>,
    ) -> Result<()> {
        let Some(last_inserted) = last_inserted else {
            return Ok(());
        };
        let channel = self.notify_channel();
        sqlx::query("SELECT pg_notify($1, $2)")
            .bind(&channel)
            .bind(last_inserted.as_i64().to_string())
            .execute(&mut **tx)
            .await
            .map_err(|e| Error::backend("Failed to issue NOTIFY", e))?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use event_sauce_core::{EventEnvelope, EventVersion};
    use serde_json::json;
    use uuid::Uuid;

    fn stream_commit(
        stream_id: StreamId,
        expected_version: AggregateVersion,
        event_count: usize,
    ) -> event_sauce_core::StreamCommit {
        let aggregate_type = stream_id.aggregate_type().as_str().to_string();
        let aggregate_id = stream_id.aggregate_id();
        event_sauce_core::StreamCommit {
            events: (0..event_count)
                .map(|_| {
                    EventEnvelope::new(
                        Uuid::new_v4(),
                        aggregate_id,
                        aggregate_type.clone(),
                        "Written".to_string(),
                        EventVersion::new(1),
                        json!({"data": "test"}),
                    )
                })
                .collect(),
            stream_id,
            expected_version,
            claims: vec![],
            clear_claims: false,
        }
    }

    /// Unit test (no I/O) for the CONT-1/XN-6 in-memory chain check: two
    /// consecutive commits to the same stream, where the second's expected
    /// version equals the first's expected version plus its event count,
    /// must be accepted with only the first needing a database precheck.
    #[test]
    fn test_plan_version_chain_checks_accepts_consecutive_commits() {
        let stream = StreamId::new("Acct", Uuid::new_v4());
        let commits = vec![
            stream_commit(stream.clone(), AggregateVersion::new(0), 1),
            stream_commit(stream, AggregateVersion::new(1), 1),
        ];

        let needs_db_check =
            plan_version_chain_checks(&commits).expect("a consecutive chain must be accepted");

        assert_eq!(
            needs_db_check,
            vec![0],
            "only the stream's first commit needs a database precheck"
        );
    }

    /// A later commit to the same stream that skips a version (rather than
    /// extending the previous commit by exactly its event count) must be
    /// rejected in memory, without ever touching the database.
    #[test]
    fn test_plan_version_chain_checks_rejects_a_skipped_version() {
        let stream = StreamId::new("Acct", Uuid::new_v4());
        let commits = vec![
            stream_commit(stream.clone(), AggregateVersion::new(0), 1),
            stream_commit(stream, AggregateVersion::new(5), 1),
        ];

        let error = plan_version_chain_checks(&commits)
            .expect_err("a commit that skips a version must be rejected");

        assert!(
            matches!(error, Error::ConcurrencyConflict { .. }),
            "a skipped version must surface as a concurrency conflict, got {error:?}"
        );
    }

    /// Two streams interleaved in one batch must each be validated against
    /// their own chain, independently of the other's commits.
    #[test]
    fn test_plan_version_chain_checks_tracks_interleaved_streams_independently() {
        let stream_a = StreamId::new("Acct", Uuid::new_v4());
        let stream_b = StreamId::new("Acct", Uuid::new_v4());
        let commits = vec![
            stream_commit(stream_a.clone(), AggregateVersion::new(0), 1),
            stream_commit(stream_b.clone(), AggregateVersion::new(0), 2),
            stream_commit(stream_a, AggregateVersion::new(1), 1),
            stream_commit(stream_b, AggregateVersion::new(2), 1),
        ];

        let needs_db_check = plan_version_chain_checks(&commits)
            .expect("interleaved streams must each be validated against their own chain");

        assert_eq!(
            needs_db_check,
            vec![0, 1],
            "only each stream's first commit in the batch needs a database precheck"
        );
    }

    /// Pins the wire value of [`append_lock_key`]'s advisory-lock key for
    /// `public.events`, the table `append()` locks on: different processes —
    /// including ones on different library versions — must compute the SAME
    /// key to actually serialize against each other, so this forwarding
    /// wrapper's output has to stay pinned independently of
    /// `migrations::tests::advisory_lock_key_is_stable_and_input_distinct`,
    /// which only pins the underlying `advisory_lock_key` it forwards to.
    #[test]
    fn test_append_lock_key_pins_wire_value() {
        assert_eq!(
            append_lock_key("public.events"),
            -146_897_220_888_487_505,
            "append_lock_key must be a stable, known hash of the qualified events table"
        );
    }
}
