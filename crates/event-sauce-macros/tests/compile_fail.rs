//! Compile-fail tests for event-sauce-macros.
//!
//! These tests verify that the macros produce helpful error messages
//! when used incorrectly.

#[test]
fn ui_tests() {
    let t = trybuild::TestCases::new();
    t.compile_fail("tests/ui/*.rs");
}
