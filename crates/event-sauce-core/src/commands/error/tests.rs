use super::*;
use crate::test_fixtures::SimpleTestError;

#[test]
fn domain_variant_displays_the_inner_error() {
    let err = CommandError::Domain(SimpleTestError);
    assert_eq!(err.to_string(), "domain error: test error");
}

#[test]
fn lifecycle_variant_displays_expected_and_found() {
    let err = CommandError::<SimpleTestError>::Lifecycle {
        expected: Lifecycle::Active,
        found: Lifecycle::Missing,
    };
    assert_eq!(
        err.to_string(),
        "expected aggregate to be active, found missing"
    );
}

#[test]
fn a_domain_error_converts_into_a_command_error_via_from() {
    let err: CommandError<SimpleTestError> = SimpleTestError.into();
    assert!(matches!(err, CommandError::Domain(SimpleTestError)));
}

#[test]
fn question_mark_propagates_a_domain_error() {
    fn fails() -> Result<(), SimpleTestError> {
        Err(SimpleTestError)
    }
    fn handler() -> Result<(), CommandError<SimpleTestError>> {
        fails()?;
        Ok(())
    }
    assert!(matches!(
        handler(),
        Err(CommandError::Domain(SimpleTestError))
    ));
}

#[test]
fn domain_error_exposes_its_source() {
    use std::error::Error as _;
    let err = CommandError::Domain(SimpleTestError);
    assert!(err.source().is_some());
}
