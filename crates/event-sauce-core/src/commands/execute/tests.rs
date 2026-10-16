use super::*;
use crate::test_fixtures::SimpleTestError;
use crate::AggregateVersion;

#[test]
fn domain_variant_displays_the_inner_error() {
    let err = ExecuteError::Domain(SimpleTestError);
    assert_eq!(err.to_string(), "domain error: test error");
}

#[test]
fn lifecycle_variant_displays_expected_and_found() {
    let err = ExecuteError::<SimpleTestError>::Lifecycle {
        expected: Lifecycle::Active,
        found: Lifecycle::Missing,
    };
    assert_eq!(
        err.to_string(),
        "expected aggregate to be active, found missing"
    );
}

#[test]
fn load_variant_displays_the_inner_error() {
    let err = ExecuteError::<SimpleTestError>::Load(crate::Error::custom("store unavailable"));
    assert!(err.to_string().contains("load failed"));
    assert!(err.to_string().contains("store unavailable"));
}

#[test]
fn save_variant_displays_the_inner_error() {
    let err = ExecuteError::<SimpleTestError>::Save(crate::Error::custom("write timeout"));
    assert!(err.to_string().contains("save failed"));
    assert!(err.to_string().contains("write timeout"));
}

#[test]
fn is_concurrency_conflict_true_only_for_a_conflicting_save() {
    let conflict = ExecuteError::<SimpleTestError>::Save(crate::Error::concurrency_conflict(
        AggregateVersion::new(1),
        AggregateVersion::new(2),
    ));
    assert!(conflict.is_concurrency_conflict());

    let other_save = ExecuteError::<SimpleTestError>::Save(crate::Error::custom("disk full"));
    assert!(!other_save.is_concurrency_conflict());

    let domain = ExecuteError::Domain(SimpleTestError);
    assert!(!domain.is_concurrency_conflict());

    let load = ExecuteError::<SimpleTestError>::Load(crate::Error::custom("boom"));
    assert!(!load.is_concurrency_conflict());

    let lifecycle = ExecuteError::<SimpleTestError>::Lifecycle {
        expected: Lifecycle::Active,
        found: Lifecycle::Deleted,
    };
    assert!(!lifecycle.is_concurrency_conflict());
}

#[test]
fn domain_error_exposes_its_source() {
    use std::error::Error as _;
    let err = ExecuteError::Domain(SimpleTestError);
    assert!(err.source().is_some());
}

#[test]
fn executed_carries_the_root_and_attempt_count() {
    use crate::test_fixtures::SimpleTestEntity;
    use crate::{AggregateRoot, EntityId};
    use std::num::NonZeroU32;

    let id = EntityId::new();
    let executed = Executed {
        root: Root::Active(AggregateRoot::<SimpleTestEntity>::new(id)),
        attempts: NonZeroU32::new(2).unwrap(),
    };

    assert_eq!(executed.attempts.get(), 2);
    assert_eq!(executed.root.entity_id(), id);
    assert_eq!(executed.root.lifecycle(), Lifecycle::Active);
}

#[test]
fn executed_debug_mentions_attempts() {
    use crate::test_fixtures::SimpleTestEntity;
    use crate::{AggregateRoot, EntityId};
    use std::num::NonZeroU32;

    let executed = Executed {
        root: Root::Active(AggregateRoot::<SimpleTestEntity>::new(EntityId::new())),
        attempts: NonZeroU32::new(1).unwrap(),
    };
    assert!(format!("{executed:?}").contains("attempts"));
}
