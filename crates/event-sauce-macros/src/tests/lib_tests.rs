use super::{core_path, imported_crate};

#[test]
fn a_dependency_resolves_to_its_import_identifier() {
    assert_eq!(
        imported_crate("event-sauce-auth").as_deref(),
        Some("event_sauce_auth")
    );
}

#[test]
fn the_crate_itself_resolves_to_its_own_identifier() {
    assert_eq!(
        imported_crate("event-sauce-macros").as_deref(),
        Some("event_sauce_macros")
    );
}

#[test]
fn an_unknown_package_resolves_to_nothing() {
    assert_eq!(imported_crate("event-sauce-unknown"), None);
}

#[test]
fn the_core_path_falls_back_to_the_core_crate() {
    assert_eq!(core_path().to_string(), ":: event_sauce_core");
}
