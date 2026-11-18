//! A session needs an `Authenticated`, and a login only yields one from a
//! complete state: these programs must not compile.

#[test]
fn ui_tests() {
    let t = trybuild::TestCases::new();
    t.compile_fail("tests/ui/*.rs");
}
