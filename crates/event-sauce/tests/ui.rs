//! Compile-fail tests for event-sauce's public API surface.
//!
//! These tests verify that internal implementation details are not
//! reachable through the facade's public API.

#[test]
fn ui_tests() {
    let t = trybuild::TestCases::new();
    t.compile_fail("tests/ui/*.rs");
}
