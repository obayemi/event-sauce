//! An aggregate's periodic state snapshot, used to speed up reconstruction.

use uuid::Uuid;

use crate::{AggregateType, AggregateVersion};

/// Snapshot of an aggregate's state.
///
/// Used to optimize aggregate reconstruction by storing periodic state snapshots.
/// For deleted aggregates, `is_deleted` is `true` and `snapshot_data` contains
/// the serialized `DeletedState` rather than the aggregate itself.
///
/// # Examples
///
/// ```
/// use event_sauce_core::{Snapshot, AggregateVersion};
/// use uuid::Uuid;
/// use serde_json::json;
///
/// let snapshot = Snapshot::new(
///     Uuid::new_v4(),
///     "User",
///     AggregateVersion::new(100),
///     json!({"email": "user@example.com", "status": "active"}),
/// );
/// assert!(!snapshot.is_deleted);
/// ```
#[derive(Debug, Clone)]
pub struct Snapshot {
    /// Aggregate ID.
    pub aggregate_id: Uuid,
    /// Aggregate type.
    pub aggregate_type: AggregateType,
    /// [`AggregateVersion`] at which snapshot was taken.
    pub snapshot_version: AggregateVersion,
    /// Serialized entity state.
    pub snapshot_data: serde_json::Value,
    /// Whether this snapshot represents a deleted aggregate.
    pub is_deleted: bool,
    /// Schema version of the serialized state, stamped from
    /// [`Aggregate::snapshot_version()`](crate::Aggregate::snapshot_version)
    /// at write time.
    ///
    /// On load, a snapshot whose stamp no longer matches the aggregate's
    /// current `snapshot_version()` is treated as a cache miss and discarded
    /// in favour of full event replay (snapshot is a cache, never the source
    /// of truth). Snapshots written before this field existed read back as `0`.
    pub snapshot_schema_version: u32,
}

impl Snapshot {
    /// Creates a new snapshot for an active aggregate at schema version `0`.
    ///
    /// Use [`Snapshot::new_with_schema_version`] to stamp a specific
    /// [`Aggregate::snapshot_version()`](crate::Aggregate::snapshot_version).
    #[must_use]
    pub fn new(
        aggregate_id: Uuid,
        aggregate_type: impl Into<AggregateType>,
        snapshot_version: AggregateVersion,
        snapshot_data: serde_json::Value,
    ) -> Self {
        Self::new_with_schema_version(
            aggregate_id,
            aggregate_type,
            snapshot_version,
            snapshot_data,
            0,
        )
    }

    /// Creates a new snapshot for an active aggregate at a specific schema version.
    #[must_use]
    pub fn new_with_schema_version(
        aggregate_id: Uuid,
        aggregate_type: impl Into<AggregateType>,
        snapshot_version: AggregateVersion,
        snapshot_data: serde_json::Value,
        snapshot_schema_version: u32,
    ) -> Self {
        Self {
            aggregate_id,
            aggregate_type: aggregate_type.into(),
            snapshot_version,
            snapshot_data,
            is_deleted: false,
            snapshot_schema_version,
        }
    }

    /// Creates a new snapshot for a deleted aggregate at schema version `0`.
    ///
    /// The `snapshot_data` should contain the serialized `A::DeletedState`.
    /// Use [`Snapshot::new_deleted_with_schema_version`] to stamp a specific
    /// [`Aggregate::snapshot_version()`](crate::Aggregate::snapshot_version).
    #[must_use]
    pub fn new_deleted(
        aggregate_id: Uuid,
        aggregate_type: impl Into<AggregateType>,
        snapshot_version: AggregateVersion,
        snapshot_data: serde_json::Value,
    ) -> Self {
        Self::new_deleted_with_schema_version(
            aggregate_id,
            aggregate_type,
            snapshot_version,
            snapshot_data,
            0,
        )
    }

    /// Creates a new snapshot for a deleted aggregate at a specific schema version.
    ///
    /// The `snapshot_data` should contain the serialized `A::DeletedState`.
    #[must_use]
    pub fn new_deleted_with_schema_version(
        aggregate_id: Uuid,
        aggregate_type: impl Into<AggregateType>,
        snapshot_version: AggregateVersion,
        snapshot_data: serde_json::Value,
        snapshot_schema_version: u32,
    ) -> Self {
        Self {
            aggregate_id,
            aggregate_type: aggregate_type.into(),
            snapshot_version,
            snapshot_data,
            is_deleted: true,
            snapshot_schema_version,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_snapshot_new() {
        let aggregate_id = Uuid::new_v4();
        let snapshot_data = serde_json::json!({"value": 42});

        let snapshot = Snapshot::new(
            aggregate_id,
            "Counter".to_string(),
            AggregateVersion::new(10),
            snapshot_data.clone(),
        );

        assert_eq!(snapshot.aggregate_id, aggregate_id);
        assert_eq!(snapshot.aggregate_type, "Counter");
        assert_eq!(snapshot.snapshot_version, AggregateVersion::new(10));
        assert_eq!(snapshot.snapshot_data, snapshot_data);
        assert!(!snapshot.is_deleted);
        assert_eq!(
            snapshot.snapshot_schema_version, 0,
            "Snapshot::new defaults the schema version to 0"
        );
    }

    #[test]
    fn test_snapshot_new_with_schema_version() {
        let aggregate_id = Uuid::new_v4();
        let snapshot_data = serde_json::json!({"value": 42});

        let snapshot = Snapshot::new_with_schema_version(
            aggregate_id,
            "Counter".to_string(),
            AggregateVersion::new(10),
            snapshot_data.clone(),
            3,
        );

        assert!(!snapshot.is_deleted);
        assert_eq!(snapshot.snapshot_data, snapshot_data);
        assert_eq!(snapshot.snapshot_schema_version, 3);
    }

    #[test]
    fn test_snapshot_new_deleted_with_schema_version() {
        let aggregate_id = Uuid::new_v4();
        let snapshot_data = serde_json::json!({"archived": true});

        let snapshot = Snapshot::new_deleted_with_schema_version(
            aggregate_id,
            "Counter".to_string(),
            AggregateVersion::new(5),
            snapshot_data.clone(),
            7,
        );

        assert!(snapshot.is_deleted);
        assert_eq!(snapshot.snapshot_data, snapshot_data);
        assert_eq!(snapshot.snapshot_schema_version, 7);
    }

    #[test]
    fn test_snapshot_new_deleted() {
        let aggregate_id = Uuid::new_v4();
        let snapshot_data = serde_json::json!({"value": -1, "archived": true});

        let snapshot = Snapshot::new_deleted(
            aggregate_id,
            "Counter".to_string(),
            AggregateVersion::new(5),
            snapshot_data.clone(),
        );

        assert_eq!(snapshot.aggregate_id, aggregate_id);
        assert_eq!(snapshot.aggregate_type, "Counter");
        assert_eq!(snapshot.snapshot_version, AggregateVersion::new(5));
        assert_eq!(snapshot.snapshot_data, snapshot_data);
        assert!(snapshot.is_deleted);
        assert_eq!(
            snapshot.snapshot_schema_version, 0,
            "Snapshot::new_deleted defaults the schema version to 0"
        );
    }
}
