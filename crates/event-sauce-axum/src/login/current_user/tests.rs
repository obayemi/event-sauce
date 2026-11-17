use axum::response::IntoResponse;
use http::StatusCode;

use super::AuthRejection;

#[test]
fn nobody_logged_in_is_unauthorized() {
    assert_eq!(
        AuthRejection::Unauthenticated.status(),
        StatusCode::UNAUTHORIZED
    );
}

#[test]
fn an_anonymous_user_is_forbidden() {
    assert_eq!(AuthRejection::Forbidden.status(), StatusCode::FORBIDDEN);
}

#[test]
fn a_missing_layer_is_a_server_error() {
    assert_eq!(
        AuthRejection::MissingLayer.status(),
        StatusCode::INTERNAL_SERVER_ERROR
    );
}

#[test]
fn the_response_carries_the_status() {
    for rejection in [
        AuthRejection::Unauthenticated,
        AuthRejection::Forbidden,
        AuthRejection::MissingLayer,
    ] {
        assert_eq!(rejection.into_response().status(), rejection.status());
    }
}
