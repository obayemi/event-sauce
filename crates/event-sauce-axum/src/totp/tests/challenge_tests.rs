use axum::http::StatusCode;
use axum_login::tower_sessions::session_store;

use super::*;

fn session_failure() -> session::Error {
    session_store::Error::Backend("session store down".to_owned()).into()
}

#[test]
fn a_session_failure_while_opening_is_an_outage() {
    let error = OpenError::from(session_failure());

    assert!(matches!(error, OpenError::Session(_)));
    assert_eq!(
        error.into_response().status(),
        StatusCode::INTERNAL_SERVER_ERROR
    );
}

#[test]
fn a_refused_login_while_opening_is_refused_as_every_login_is() {
    let error = OpenError::from(LoginError::refused());

    assert_eq!(error.into_response().status(), StatusCode::UNAUTHORIZED);
}
