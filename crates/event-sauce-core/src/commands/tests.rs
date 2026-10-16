use super::*;
use crate::test_fixtures::{SimpleTestEntity, SimpleTestError};

#[test]
fn lifecycle_display_matches_its_name() {
    assert_eq!(Lifecycle::Missing.to_string(), "missing");
    assert_eq!(Lifecycle::Active.to_string(), "active");
    assert_eq!(Lifecycle::Deleted.to_string(), "deleted");
}

#[test]
fn lifecycle_is_copy_and_comparable() {
    let a = Lifecycle::Active;
    let b = a;
    assert_eq!(a, b);
    assert_ne!(Lifecycle::Active, Lifecycle::Deleted);
}

struct DoubleCommand;

impl Command<SimpleTestEntity> for DoubleCommand {
    fn handle(
        &self,
        root: Root<SimpleTestEntity>,
        _ctx: &(),
    ) -> Result<Root<SimpleTestEntity>, CommandError<SimpleTestError>> {
        let mut root = root.active()?;
        root.apply(crate::test_fixtures::SimpleTestEvent::Updated { value: 2 })
            .map_err(CommandError::from)?;
        Ok(root.into())
    }
}

#[test]
fn a_type_implementing_command_can_be_invoked_through_the_trait() {
    let root = Root::Active(crate::AggregateRoot::<SimpleTestEntity>::new(
        crate::EntityId::new(),
    ));

    let result = DoubleCommand.handle(root, &()).unwrap();
    let active = result.active().unwrap();
    assert_eq!(active.value, 2);
}

#[test]
fn a_closure_implements_command_through_the_blanket_impl() {
    let handler = |root: Root<SimpleTestEntity>,
                   _ctx: &()|
     -> Result<Root<SimpleTestEntity>, CommandError<SimpleTestError>> {
        let mut root = root.active()?;
        root.apply(crate::test_fixtures::SimpleTestEvent::Updated { value: 5 })
            .map_err(CommandError::from)?;
        Ok(root.into())
    };

    let root = Root::Active(crate::AggregateRoot::<SimpleTestEntity>::new(
        crate::EntityId::new(),
    ));

    let result = Command::handle(&handler, root, &()).unwrap();
    assert_eq!(result.active().unwrap().value, 5);
}
