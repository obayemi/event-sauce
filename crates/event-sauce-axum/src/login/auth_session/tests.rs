use axum_login::tower_sessions::session_store;
use event_sauce_core::Error;

use super::SessionError;
use crate::login::backend::Backend;
use crate::login::test_support::Member;

fn session_failure() -> axum_login::tower_sessions::session::Error {
    session_store::Error::Backend("down".to_owned()).into()
}

#[test]
fn a_session_store_failure_maps_to_the_session_variant() {
    let mapped = super::session_error(axum_login::Error::<Backend<Member>>::Session(
        session_failure(),
    ));

    assert!(matches!(mapped, SessionError::Session(_)));
}

#[test]
fn a_backend_failure_maps_to_the_store_variant() {
    let mapped = super::session_error(axum_login::Error::<Backend<Member>>::Backend(
        Error::encryption("no key"),
    ));

    assert!(matches!(mapped, SessionError::Store(_)));
}

#[test]
fn the_error_displays_the_failure_it_wraps() {
    assert_eq!(SessionError::from(session_failure()).to_string(), "down");
    assert_eq!(
        SessionError::from(Error::encryption("no key")).to_string(),
        Error::encryption("no key").to_string()
    );
}
