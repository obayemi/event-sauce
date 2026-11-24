//! What the typestate design refuses to compile: a session opened from
//! anything but an `Authenticated<U>`, every way to name or reach the sealed
//! backend or its session, an application whose credentials can hold a
//! second factor but whose route does not send users to it, and a session
//! cookie protected twice.

#[test]
fn ui() {
    let cases = trybuild::TestCases::new();
    cases.compile_fail("tests/ui/*.rs");
}
