//! `Loaded<A>` — result of loading an aggregate that may be deleted.
//!
//! When loading an aggregate from the event store, it may have been
//! deleted via a delete event. `Loaded<A>` encodes this as a sum type.

use crate::{Aggregate, AggregateRoot, AggregateVersion, DeletedAggregateRoot, EntityId};

/// Result of loading an aggregate that may be active or deleted.
///
/// Use [`load_any()`](crate::EventStore::load_any) to get this type.
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_fixtures::SimpleTestEntity;

    fn make_active() -> Loaded<SimpleTestEntity> {
        let id = EntityId::new();
        let agg = AggregateRoot::<SimpleTestEntity>::new(id);
        Loaded::Active(agg)
    }

    fn make_deleted() -> Loaded<SimpleTestEntity> {
        let id = EntityId::new();
        let entity = SimpleTestEntity { id, value: 42 };
        let deleted =
            DeletedAggregateRoot::from_delete_replay(entity, id, AggregateVersion::new(5));
        Loaded::Deleted(deleted)
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
        let entity = SimpleTestEntity { id, value: 0 };
        let deleted =
            DeletedAggregateRoot::from_delete_replay(entity, id, AggregateVersion::new(1));
        let loaded = Loaded::<SimpleTestEntity>::Deleted(deleted);
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
