use super::*;
use crate::test_fixtures::{deleted_simple_entity, SimpleTestEntity, SimpleTestEvent};
use crate::AggregateVersion;

fn active_root() -> Root<SimpleTestEntity> {
    Root::Active(AggregateRoot::new(EntityId::new()))
}

fn missing_root() -> Root<SimpleTestEntity> {
    Root::Missing(UninitAggregateRoot::new(EntityId::new()))
}

fn deleted_root() -> Root<SimpleTestEntity> {
    Root::Deleted(deleted_simple_entity(EntityId::new(), 0, 1))
}

#[test]
fn lifecycle_reports_each_variant() {
    assert_eq!(active_root().lifecycle(), Lifecycle::Active);
    assert_eq!(missing_root().lifecycle(), Lifecycle::Missing);
    assert_eq!(deleted_root().lifecycle(), Lifecycle::Deleted);
}

#[test]
fn entity_id_is_shared_across_variants() {
    let id = EntityId::new();
    let root: Root<SimpleTestEntity> = AggregateRoot::<SimpleTestEntity>::new(id).into();
    assert_eq!(root.entity_id(), id);

    let root: Root<SimpleTestEntity> = UninitAggregateRoot::<SimpleTestEntity>::new(id).into();
    assert_eq!(root.entity_id(), id);
}

#[test]
fn active_narrows_an_active_root() {
    let root = active_root();
    assert!(root.active().is_ok());
}

#[test]
fn active_refuses_a_missing_root() {
    let err = missing_root().active().unwrap_err();
    assert!(matches!(
        err,
        CommandError::Lifecycle {
            expected: Lifecycle::Active,
            found: Lifecycle::Missing
        }
    ));
}

#[test]
fn active_refuses_a_deleted_root() {
    let err = deleted_root().active().unwrap_err();
    assert!(matches!(
        err,
        CommandError::Lifecycle {
            expected: Lifecycle::Active,
            found: Lifecycle::Deleted
        }
    ));
}

#[test]
fn missing_narrows_a_missing_root() {
    assert!(missing_root().missing().is_ok());
}

#[test]
fn missing_refuses_an_active_root() {
    let err = active_root().missing().unwrap_err();
    assert!(matches!(
        err,
        CommandError::Lifecycle {
            expected: Lifecycle::Missing,
            found: Lifecycle::Active
        }
    ));
}

#[test]
fn missing_refuses_a_deleted_root() {
    let err = deleted_root().missing().unwrap_err();
    assert!(matches!(
        err,
        CommandError::Lifecycle {
            expected: Lifecycle::Missing,
            found: Lifecycle::Deleted
        }
    ));
}

#[test]
fn deleted_narrows_a_deleted_root() {
    assert!(deleted_root().deleted().is_ok());
}

#[test]
fn deleted_refuses_an_active_root() {
    let err = active_root().deleted().unwrap_err();
    assert!(matches!(
        err,
        CommandError::Lifecycle {
            expected: Lifecycle::Deleted,
            found: Lifecycle::Active
        }
    ));
}

#[test]
fn deleted_refuses_a_missing_root() {
    let err = missing_root().deleted().unwrap_err();
    assert!(matches!(
        err,
        CommandError::Lifecycle {
            expected: Lifecycle::Deleted,
            found: Lifecycle::Missing
        }
    ));
}

#[test]
fn from_aggregate_root_produces_the_active_variant() {
    let root: Root<SimpleTestEntity> =
        AggregateRoot::<SimpleTestEntity>::new(EntityId::new()).into();
    assert_eq!(root.lifecycle(), Lifecycle::Active);
}

#[test]
fn from_uninit_aggregate_root_produces_the_missing_variant() {
    let root: Root<SimpleTestEntity> =
        UninitAggregateRoot::<SimpleTestEntity>::new(EntityId::new()).into();
    assert_eq!(root.lifecycle(), Lifecycle::Missing);
}

#[test]
fn from_deleted_aggregate_root_produces_the_deleted_variant() {
    let Root::Deleted(_) = deleted_root() else {
        panic!("expected a deleted root");
    };
}

#[test]
fn into_default_activates_without_an_init_event() {
    let id = EntityId::new();
    let active = UninitAggregateRoot::<SimpleTestEntity>::new(id).into_default();
    assert_eq!(active.entity_id(), id);
    assert_eq!(active.value, 0);
    assert_eq!(active.version(), AggregateVersion::initial());
}

#[test]
fn active_root_can_apply_events_after_narrowing() {
    let mut active = active_root().active().unwrap();
    active.apply(SimpleTestEvent::Updated { value: 9 }).unwrap();
    assert_eq!(active.value, 9);
}

#[test]
fn loaded_active_converts_to_an_active_root() {
    let id = EntityId::new();
    let root: Root<SimpleTestEntity> = crate::Loaded::Active(AggregateRoot::new(id)).into();
    assert_eq!(root.lifecycle(), Lifecycle::Active);
    assert_eq!(root.entity_id(), id);
}

#[test]
fn loaded_deleted_converts_to_a_deleted_root() {
    let id = EntityId::new();
    let root: Root<SimpleTestEntity> =
        crate::Loaded::Deleted(deleted_simple_entity(id, 0, 1)).into();
    assert_eq!(root.lifecycle(), Lifecycle::Deleted);
    assert_eq!(root.entity_id(), id);
}
