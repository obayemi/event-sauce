//! What the typestate design refuses to compile: a session opened from
//! anything but an `Authenticated<U>`, every way to name or reach the sealed
//! backend or its session, and an application whose credentials can hold a
//! second factor but whose route does not send users to it.

#[test]
fn ui() {
    let cases = trybuild::TestCases::new();
    cases.compile_fail("tests/ui/*.rs");
}
