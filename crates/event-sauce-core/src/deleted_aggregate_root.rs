//! `DeletedAggregateRoot<A>` — terminal state for deleted aggregates.
//!
//! Represents the final state after a delete event has been applied.
//! No further events can be applied — this is enforced at compile time
//! by the absence of any `apply()` method.

use crate::aggregate_root::PendingEvent;
use crate::{Aggregate, AggregateVersion, EntityId};

/// Terminal state wrapper for a deleted aggregate.
///
/// Created by `AggregateRoot::apply_delete()`, this type holds the
/// post-deletion state (`A::DeletedState`) and any pending events.
/// It provides read-only access via `Deref` and cannot accept new events.
///
/// # Type-State Pattern
///
/// ```text
/// UninitAggregateRoot<A> ──apply_init()──> AggregateRoot<A> ──apply_delete()──> DeletedAggregateRoot<A>
/// ```
///
/// The lack of `apply()` or `apply_delete()` on `DeletedAggregateRoot`
/// makes it a compile-time error to apply events after deletion.
#[derive(Debug)]
pub struct DeletedAggregateRoot<A: Aggregate> {
    state: A::DeletedState,
    entity_id: EntityId,
    version: AggregateVersion,
    pending_events: Vec<PendingEvent<A::Event>>,
    /// Carried over from a poisoned [`AggregateRoot`](crate::AggregateRoot).
    ///
    /// If the source aggregate was poisoned by a failed `apply` before being
    /// deleted, its state is inconsistent. The commit path refuses a poisoned
    /// deleted root with [`crate::Error::InvalidState`] rather than persist it.
    poisoned: bool,
}

impl<A: Aggregate> std::ops::Deref for DeletedAggregateRoot<A> {
    type Target = A::DeletedState;

    fn deref(&self) -> &Self::Target {
        &self.state
    }
}

// No DerefMut — read-only access to deleted state

impl<A: Aggregate> DeletedAggregateRoot<A> {
    /// Returns the entity's unique identifier.
    #[must_use]
    pub fn entity_id(&self) -> EntityId {
        self.entity_id
    }

    /// Returns the current version of the aggregate.
    #[must_use]
    pub fn version(&self) -> AggregateVersion {
        self.version
    }

    /// Returns `true` if this deleted root was produced from a poisoned
    /// [`AggregateRoot`](crate::AggregateRoot).
    ///
    /// A poisoned deleted root holds inconsistent state and is refused by the
    /// commit path with [`crate::Error::InvalidState`].
    #[cfg(any(test, feature = "event-sourcing", feature = "state-store"))]
    pub(crate) fn is_poisoned(&self) -> bool {
        self.poisoned
    }

    /// Returns a reference to the deleted state.
    #[must_use]
    pub fn state(&self) -> &A::DeletedState {
        &self.state
    }

    /// Returns uncommitted events (without actor information).
    #[must_use]
    pub fn pending_events(&self) -> Vec<&A::Event> {
        self.pending_events.iter().map(|pe| &pe.event).collect()
    }

    /// Returns uncommitted events with actor information (for commit).
    #[cfg(any(test, feature = "event-sourcing", feature = "state-store"))]
    pub(crate) fn pending_events_with_actors(&self) -> &[PendingEvent<A::Event>] {
        &self.pending_events
    }

    /// Clears all pending events.
    ///
    /// Called after events have been successfully persisted.
    pub fn clear_pending_events(&mut self) {
        self.pending_events.clear();
    }

    /// Sets metadata on all pending events that don't already have metadata.
    ///
    /// Used by `PolicyContext::commit_deleted()` to inject causation tracking
    /// into pending events before delegating to the event store.
    #[cfg(any(test, feature = "event-sourcing"))]
    pub(crate) fn set_pending_metadata(&mut self, metadata: &crate::EventMetadata) {
        for pe in &mut self.pending_events {
            if pe.metadata.is_none() {
                pe.metadata = Some(metadata.clone());
            }
        }
    }

    /// Creates a deleted aggregate root from a delete operation with pending events.
    ///
    /// Used by `AggregateRoot::apply_delete()` at command time, and with no
    /// pending events by `apply_delete_unchecked()` on replay. `poisoned`
    /// carries the poison flag of the source aggregate so the commit path can
    /// refuse an inconsistent deleted root.
    pub(crate) fn from_delete_with_pending(
        state: A::DeletedState,
        entity_id: EntityId,
        version: AggregateVersion,
        pending_events: Vec<PendingEvent<A::Event>>,
        poisoned: bool,
    ) -> Self {
        Self {
            state,
            entity_id,
            version,
            pending_events,
            poisoned,
        }
    }

    /// Converts what a store read back into the deleted state and wraps it
    /// in a deleted root (no pending events).
    ///
    /// Used by `load_any()` when what came back is in a deleted state.
    /// `stored` converts into `A::DeletedState` through `TryFrom`, exactly
    /// like [`AggregateRoot::restore`](crate::AggregateRoot::restore):
    /// passing the deleted state itself can never fail.
    #[cfg(any(test, feature = "event-sourcing", feature = "state-store"))]
    pub(crate) fn restore<R>(
        stored: R,
        entity_id: EntityId,
        version: crate::StoredVersion,
    ) -> Result<Self, <A::DeletedState as TryFrom<R>>::Error>
    where
        A::DeletedState: TryFrom<R>,
    {
        Ok(Self::from_delete_with_pending(
            A::DeletedState::try_from(stored)?,
            entity_id,
            version.into(),
            Vec::new(),
            false,
        ))
    }

    /// Returns the aggregate type name.
    #[must_use]
    pub fn aggregate_type() -> crate::AggregateType {
        A::aggregate_type()
    }
}

impl<A: Aggregate> serde::Serialize for DeletedAggregateRoot<A>
where
    A::DeletedState: serde::Serialize,
    A::Event: serde::Serialize,
{
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeStruct;
        let mut state = serializer.serialize_struct("DeletedAggregateRoot", 3)?;
        state.serialize_field("state", &self.state)?;
        state.serialize_field("entity_id", &self.entity_id)?;
        state.serialize_field("version", &self.version)?;
        state.end()
    }
}

impl<A: Aggregate> Clone for DeletedAggregateRoot<A>
where
    A::DeletedState: Clone,
    A::Event: Clone,
{
    fn clone(&self) -> Self {
        Self {
            state: self.state.clone(),
            entity_id: self.entity_id,
            version: self.version,
            pending_events: self.pending_events.clone(),
            poisoned: self.poisoned,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_fixtures::{deleted_simple_entity, SimpleTestEntity, SimpleTestEvent};

    #[test]
    fn test_deleted_aggregate_root_deref() {
        let deleted = deleted_simple_entity(EntityId::new(), 42, 5);
        assert_eq!(deleted.value, 42);
    }

    #[test]
    fn test_deleted_aggregate_root_entity_id() {
        let id = EntityId::new();
        let deleted = deleted_simple_entity(id, 0, 1);
        assert_eq!(deleted.entity_id(), id);
    }

    #[test]
    fn test_deleted_aggregate_root_version() {
        let deleted = deleted_simple_entity(EntityId::new(), 0, 10);
        assert_eq!(deleted.version(), AggregateVersion::new(10));
    }

    #[test]
    fn test_deleted_aggregate_root_state() {
        let deleted = deleted_simple_entity(EntityId::new(), 99, 1);
        assert_eq!(deleted.state().value, 99);
    }

    #[test]
    fn test_deleted_aggregate_root_pending_events_from_delete() {
        let entity = SimpleTestEntity {
            id: EntityId::new(),
            value: 0,
        };
        let pending = vec![PendingEvent {
            event: SimpleTestEvent::Updated { value: 0 },
            actor_id: None,
            metadata: None,
        }];
        let deleted = DeletedAggregateRoot::<SimpleTestEntity>::from_delete_with_pending(
            entity,
            EntityId::new(),
            AggregateVersion::new(2),
            pending,
            false,
        );
        assert_eq!(deleted.pending_events().len(), 1);
    }

    #[test]
    fn test_deleted_aggregate_root_carries_poison() {
        let entity = SimpleTestEntity {
            id: EntityId::new(),
            value: 0,
        };
        let deleted = DeletedAggregateRoot::<SimpleTestEntity>::from_delete_with_pending(
            entity,
            EntityId::new(),
            AggregateVersion::new(2),
            Vec::new(),
            true,
        );
        assert!(deleted.is_poisoned());
    }

    #[test]
    fn test_deleted_aggregate_root_clear_pending_events() {
        let entity = SimpleTestEntity {
            id: EntityId::new(),
            value: 0,
        };
        let pending = vec![PendingEvent {
            event: SimpleTestEvent::Updated { value: 0 },
            actor_id: None,
            metadata: None,
        }];
        let mut deleted = DeletedAggregateRoot::<SimpleTestEntity>::from_delete_with_pending(
            entity,
            EntityId::new(),
            AggregateVersion::new(2),
            pending,
            false,
        );
        deleted.clear_pending_events();
        assert!(deleted.pending_events().is_empty());
    }

    #[test]
    fn test_deleted_aggregate_root_pending_events_with_actors() {
        let entity = SimpleTestEntity {
            id: EntityId::new(),
            value: 0,
        };
        let actor_id = EntityId::new();
        let pending = vec![PendingEvent {
            event: SimpleTestEvent::Updated { value: 0 },
            actor_id: Some(actor_id),
            metadata: None,
        }];
        let deleted = DeletedAggregateRoot::<SimpleTestEntity>::from_delete_with_pending(
            entity,
            EntityId::new(),
            AggregateVersion::new(2),
            pending,
            false,
        );
        let with_actors = deleted.pending_events_with_actors();
        assert_eq!(with_actors[0].actor_id, Some(actor_id));
    }

    #[test]
    fn test_deleted_aggregate_root_aggregate_type() {
        let type_name = DeletedAggregateRoot::<SimpleTestEntity>::aggregate_type();
        assert_eq!(type_name, "SimpleTestEntity");
    }

    #[test]
    fn test_deleted_aggregate_root_serialize() {
        let deleted = deleted_simple_entity(EntityId::new(), 42, 5);
        let json = serde_json::to_value(&deleted).unwrap();
        assert!(json.get("state").is_some());
        assert!(json.get("entity_id").is_some());
        assert!(json.get("version").is_some());
        assert_eq!(json["state"]["value"], 42);
        assert_eq!(json["version"], 5);
    }

    #[test]
    fn test_deleted_aggregate_root_clone() {
        let deleted = deleted_simple_entity(EntityId::new(), 42, 5);
        let cloned = deleted.clone();
        assert_eq!(cloned.entity_id(), deleted.entity_id());
        assert_eq!(cloned.version(), deleted.version());
        assert_eq!(cloned.state().value, deleted.state().value);
    }

    #[test]
    fn test_deleted_aggregate_root_restore() {
        let id = EntityId::new();
        let deleted = deleted_simple_entity(id, 77, 10);
        assert_eq!(deleted.entity_id(), id);
        assert_eq!(deleted.version(), AggregateVersion::new(10));
        assert_eq!(deleted.state().value, 77);
        assert!(deleted.pending_events().is_empty());
        assert!(!deleted.is_poisoned());
    }

    #[test]
    fn test_deleted_aggregate_root_debug() {
        let deleted = deleted_simple_entity(EntityId::new(), 0, 1);
        let debug = format!("{deleted:?}");
        assert!(debug.contains("DeletedAggregateRoot"));
    }

    #[test]
    fn test_deleted_aggregate_root_set_pending_metadata() {
        let entity = SimpleTestEntity {
            id: EntityId::new(),
            value: 0,
        };
        let pending = vec![
            PendingEvent {
                event: SimpleTestEvent::Updated { value: 1 },
                actor_id: None,
                metadata: None,
            },
            PendingEvent {
                event: SimpleTestEvent::Updated { value: 2 },
                actor_id: None,
                metadata: None,
            },
        ];
        let mut deleted = DeletedAggregateRoot::<SimpleTestEntity>::from_delete_with_pending(
            entity,
            EntityId::new(),
            AggregateVersion::new(2),
            pending,
            false,
        );

        let metadata = crate::EventMetadata::new().with_correlation_id(uuid::Uuid::new_v4());
        deleted.set_pending_metadata(&metadata);

        let with_actors = deleted.pending_events_with_actors();
        assert_eq!(with_actors[0].metadata, Some(metadata.clone()));
        assert_eq!(with_actors[1].metadata, Some(metadata));
    }

    #[test]
    fn test_deleted_aggregate_root_set_pending_metadata_does_not_overwrite() {
        let entity = SimpleTestEntity {
            id: EntityId::new(),
            value: 0,
        };
        let existing = crate::EventMetadata::new().with_causation_id(uuid::Uuid::new_v4());
        let pending = vec![
            PendingEvent {
                event: SimpleTestEvent::Updated { value: 1 },
                actor_id: None,
                metadata: Some(existing.clone()),
            },
            PendingEvent {
                event: SimpleTestEvent::Updated { value: 2 },
                actor_id: None,
                metadata: None,
            },
        ];
        let mut deleted = DeletedAggregateRoot::<SimpleTestEntity>::from_delete_with_pending(
            entity,
            EntityId::new(),
            AggregateVersion::new(2),
            pending,
            false,
        );

        let new_metadata = crate::EventMetadata::new().with_correlation_id(uuid::Uuid::new_v4());
        deleted.set_pending_metadata(&new_metadata);

        let with_actors = deleted.pending_events_with_actors();
        // First event keeps its existing metadata
        assert_eq!(with_actors[0].metadata, Some(existing));
        // Second event gets the new metadata
        assert_eq!(with_actors[1].metadata, Some(new_metadata));
    }
}
