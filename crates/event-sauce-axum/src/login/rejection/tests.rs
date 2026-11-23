use axum::response::IntoResponse;
use http::{header::LOCATION, HeaderValue, StatusCode};

use super::AuthRejection;

fn redirect() -> AuthRejection {
    AuthRejection::Redirect {
        location: HeaderValue::from_static("/login?return_to=%2Fa"),
    }
}

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
fn a_redirect_is_see_other() {
    assert_eq!(redirect().status(), StatusCode::SEE_OTHER);
}

#[test]
fn the_response_carries_the_status() {
    for rejection in [
        AuthRejection::Unauthenticated,
        AuthRejection::Forbidden,
        AuthRejection::MissingLayer,
        redirect(),
    ] {
        assert_eq!(
            rejection.clone().into_response().status(),
            rejection.status()
        );
    }
}

#[test]
fn a_redirect_answers_with_its_location() {
    let response = redirect().into_response();

    assert_eq!(response.headers()[LOCATION], "/login?return_to=%2Fa");
}

#[test]
fn the_other_rejections_answer_without_a_location() {
    let response = AuthRejection::Unauthenticated.into_response();

    assert!(response.headers().get(LOCATION).is_none());
}
