//! What the typestate design refuses to compile: a session user built from an
//! aggregate nobody authenticated, and an application whose credentials can
//! hold a second factor but whose route does not send users to it.

#[test]
fn ui() {
    let cases = trybuild::TestCases::new();
    cases.compile_fail("tests/ui/*.rs");
}
