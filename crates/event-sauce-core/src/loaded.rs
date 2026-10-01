//! `Loaded<A>` — result of loading an aggregate that may be deleted.
//!
//! When loading an aggregate from the event store, it may have been
//! deleted via a delete event. `Loaded<A>` encodes this as a sum type.

use crate::{Aggregate, AggregateRoot, AggregateVersion, DeletedAggregateRoot, EntityId};

/// Result of loading an aggregate that may be active or deleted.
///
/// Use [`load_any()`](crate::Repository::load_any) to get this type.
/// Use pattern matching or the convenience methods to extract the inner type.
///
/// # Examples
///
/// ```ignore
/// let loaded = repo.load_any(id).await?;
/// match loaded {
///     Loaded::Active(agg) => println!("Active: {:?}", agg.entity_id()),
///     Loaded::Deleted(del) => println!("Deleted: {:?}", del.entity_id()),
/// }
/// ```
#[derive(Debug)]
pub enum Loaded<A: Aggregate> {
    /// The aggregate is active (not deleted).
    Active(AggregateRoot<A>),
    /// The aggregate has been deleted.
    Deleted(DeletedAggregateRoot<A>),
}

impl<A: Aggregate> Loaded<A> {
    /// Returns true if the aggregate is active.
    #[must_use]
    pub fn is_active(&self) -> bool {
        matches!(self, Self::Active(_))
    }

    /// Returns true if the aggregate has been deleted.
    #[must_use]
    pub fn is_deleted(&self) -> bool {
        matches!(self, Self::Deleted(_))
    }

    /// Converts into an `AggregateRoot<A>`, returning an error if deleted.
    ///
    /// # Errors
    ///
    /// Returns `Error::AggregateDeleted` if the aggregate has been deleted.
    pub fn into_active(self) -> crate::Result<AggregateRoot<A>> {
        match self {
            Self::Active(agg) => Ok(agg),
            Self::Deleted(del) => Err(crate::Error::aggregate_deleted(
                A::aggregate_type().to_string(),
                del.entity_id().to_string(),
            )),
        }
    }

    /// Converts into a `DeletedAggregateRoot<A>`, returning an error if active.
    ///
    /// # Errors
    ///
    /// Returns `Error::InvalidState` if the aggregate is still active.
    pub fn into_deleted(self) -> crate::Result<DeletedAggregateRoot<A>> {
        match self {
            Self::Deleted(del) => Ok(del),
            Self::Active(_) => Err(crate::Error::invalid_state("Aggregate is not deleted")),
        }
    }

    /// Returns the entity ID regardless of state.
    #[must_use]
    pub fn entity_id(&self) -> EntityId {
        match self {
            Self::Active(agg) => agg.entity_id(),
            Self::Deleted(del) => del.entity_id(),
        }
    }

    /// Returns the version regardless of state.
    #[must_use]
    pub fn version(&self) -> AggregateVersion {
        match self {
            Self::Active(agg) => agg.version(),
            Self::Deleted(del) => del.version(),
        }
    }

    /// Rebuilds the root a store saved: its serialized state, whether it was
    /// deleted, its id and the version it was saved at.
    ///
    /// Shared by the state-store load and the event-store snapshot load.
    ///
    /// # Errors
    ///
    /// Returns `Error::InvalidState`, naming the aggregate type and id, if
    /// `version` is not a [`StoredVersion`](crate::StoredVersion), and
    /// `Error::Serialization` if
    /// `data` no longer deserializes into `A` or `A::DeletedState`.
    #[cfg(any(feature = "event-sourcing", feature = "state-store"))]
    pub(crate) fn restore(
        is_deleted: bool,
        data: serde_json::Value,
        aggregate_id: EntityId,
        version: AggregateVersion,
    ) -> crate::Result<Self>
    where
        A: serde::de::DeserializeOwned,
        A::DeletedState: serde::de::DeserializeOwned,
    {
        let version = crate::StoredVersion::try_from(version).map_err(|err| {
            crate::Error::invalid_state(format!(
                "stored state for {} {aggregate_id}: {err}",
                A::aggregate_type()
            ))
        })?;
        Ok(if is_deleted {
            let state: A::DeletedState = serde_json::from_value(data)?;
            let Ok(deleted) = DeletedAggregateRoot::restore(state, aggregate_id, version);
            Self::Deleted(deleted)
        } else {
            let entity: A = serde_json::from_value(data)?;
            let Ok(active) = AggregateRoot::restore(version, entity);
            Self::Active(active)
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_fixtures::{deleted_simple_entity, SimpleTestEntity};

    #[cfg(any(feature = "event-sourcing", feature = "state-store"))]
    #[test]
    fn test_loaded_restore_rebuilds_the_saved_lifecycle_state() {
        for is_deleted in [false, true] {
            let id = EntityId::new();
            let data = serde_json::to_value(SimpleTestEntity { id, value: 7 }).unwrap();

            let loaded =
                Loaded::<SimpleTestEntity>::restore(is_deleted, data, id, AggregateVersion::new(3))
                    .unwrap();

            assert_eq!(loaded.is_deleted(), is_deleted);
            assert_eq!(loaded.entity_id(), id);
            assert_eq!(loaded.version(), AggregateVersion::new(3));
        }
    }

    #[cfg(any(feature = "event-sourcing", feature = "state-store"))]
    #[test]
    fn test_loaded_restore_refuses_a_corrupt_version_or_state() {
        let id = EntityId::new();
        let valid = serde_json::to_value(SimpleTestEntity { id, value: 7 }).unwrap();

        let corrupt_version =
            Loaded::<SimpleTestEntity>::restore(false, valid, id, AggregateVersion::initial());
        let corrupt_state = Loaded::<SimpleTestEntity>::restore(
            true,
            serde_json::json!({ "id": id }),
            id,
            AggregateVersion::new(1),
        );

        assert!(matches!(
            corrupt_version,
            Err(crate::Error::InvalidState(_))
        ));
        assert!(matches!(corrupt_state, Err(crate::Error::Serialization(_))));
    }

    #[cfg(any(feature = "event-sourcing", feature = "state-store"))]
    #[test]
    fn test_loaded_restore_names_the_row_whose_version_is_corrupt() {
        let id = EntityId::new();
        let data = serde_json::to_value(SimpleTestEntity { id, value: 7 }).unwrap();

        let err = Loaded::<SimpleTestEntity>::restore(false, data, id, AggregateVersion::initial())
            .unwrap_err();

        let message = err.to_string();
        assert!(message.contains("SimpleTestEntity"), "{message}");
        assert!(message.contains(&id.to_string()), "{message}");
        assert!(message.contains("v0"), "{message}");
    }

    fn make_active() -> Loaded<SimpleTestEntity> {
        let id = EntityId::new();
        let agg = AggregateRoot::<SimpleTestEntity>::new(id);
        Loaded::Active(agg)
    }

    fn make_deleted() -> Loaded<SimpleTestEntity> {
        Loaded::Deleted(deleted_simple_entity(EntityId::new(), 42, 5))
    }

    #[test]
    fn test_loaded_active_is_active() {
        let loaded = make_active();
        assert!(loaded.is_active());
        assert!(!loaded.is_deleted());
    }

    #[test]
    fn test_loaded_deleted_is_deleted() {
        let loaded = make_deleted();
        assert!(loaded.is_deleted());
        assert!(!loaded.is_active());
    }

    #[test]
    fn test_loaded_into_active_succeeds() {
        let loaded = make_active();
        let result = loaded.into_active();
        assert!(result.is_ok());
    }

    #[test]
    fn test_loaded_into_active_fails_for_deleted() {
        let loaded = make_deleted();
        let result = loaded.into_active();
        assert!(result.is_err());
        assert!(result.unwrap_err().is_aggregate_deleted());
    }

    #[test]
    fn test_loaded_into_deleted_succeeds() {
        let loaded = make_deleted();
        let result = loaded.into_deleted();
        assert!(result.is_ok());
    }

    #[test]
    fn test_loaded_into_deleted_fails_for_active() {
        let loaded = make_active();
        let result = loaded.into_deleted();
        assert!(result.is_err());
    }

    #[test]
    fn test_loaded_entity_id_active() {
        let id = EntityId::new();
        let agg = AggregateRoot::<SimpleTestEntity>::new(id);
        let loaded = Loaded::Active(agg);
        assert_eq!(loaded.entity_id(), id);
    }

    #[test]
    fn test_loaded_entity_id_deleted() {
        let id = EntityId::new();
        let loaded = Loaded::Deleted(deleted_simple_entity(id, 0, 1));
        assert_eq!(loaded.entity_id(), id);
    }

    #[test]
    fn test_loaded_version_active() {
        let loaded = make_active();
        assert_eq!(loaded.version(), AggregateVersion::initial());
    }

    #[test]
    fn test_loaded_version_deleted() {
        let loaded = make_deleted();
        assert_eq!(loaded.version(), AggregateVersion::new(5));
    }

    #[test]
    fn test_loaded_debug() {
        let loaded = make_active();
        let debug = format!("{loaded:?}");
        assert!(debug.contains("Active"));

        let loaded = make_deleted();
        let debug = format!("{loaded:?}");
        assert!(debug.contains("Deleted"));
    }
}
