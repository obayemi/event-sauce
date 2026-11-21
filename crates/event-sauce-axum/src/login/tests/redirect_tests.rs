use axum::extract::OriginalUri;
use http::{request::Parts, HeaderValue, Request, Uri};

use super::LoginRedirect;
use crate::{AuthRejection, ReturnTo};

const LOGIN: ReturnTo = ReturnTo::from_static("/login");

fn parts(uri: &str, original: Option<&str>) -> Parts {
    let mut request = Request::builder().uri(uri);
    if let Some(original) = original {
        request = request.extension(OriginalUri(original.parse::<Uri>().unwrap()));
    }
    request.body(()).unwrap().into_parts().0
}

fn location(value: &'static str) -> AuthRejection {
    AuthRejection::Redirect {
        location: HeaderValue::from_static(value),
    }
}

#[test]
fn nobody_logged_in_is_sent_to_the_login_page_with_the_path_and_query() {
    let redirect = LoginRedirect::to(LOGIN);

    let rejection = redirect.refuse(&parts("/notes?x=1", None), AuthRejection::Unauthenticated);

    assert_eq!(rejection, location("/login?return_to=%2Fnotes%3Fx%3D1"));
}

#[test]
fn the_original_uri_wins_over_the_stripped_one() {
    let redirect = LoginRedirect::to(LOGIN);

    let rejection = redirect.refuse(
        &parts("/notes?x=1", Some("/app/notes?x=1")),
        AuthRejection::Unauthenticated,
    );

    assert_eq!(
        rejection,
        location("/login?return_to=%2Fapp%2Fnotes%3Fx%3D1")
    );
}

#[test]
fn non_ascii_bytes_and_reserved_characters_are_percent_encoded() {
    let redirect = LoginRedirect::to(LOGIN);

    let rejection = redirect.refuse(
        &parts("/caf%C3%A9?a=b&c=d", Some("/caf%C3%A9?a=b&c=d")),
        AuthRejection::Unauthenticated,
    );

    assert_eq!(
        rejection,
        location("/login?return_to=%2Fcaf%25C3%25A9%3Fa%3Db%26c%3Dd")
    );
}

#[test]
fn a_custom_parameter_name_is_used() {
    let redirect = LoginRedirect::to(LOGIN).param("redirect_url");

    let rejection = redirect.refuse(&parts("/a", None), AuthRejection::Unauthenticated);

    assert_eq!(rejection, location("/login?redirect_url=%2Fa"));
}

#[test]
fn a_login_page_with_a_query_gets_the_parameter_appended() {
    let redirect = LoginRedirect::to(ReturnTo::from_static("/login?lang=fr"));

    let rejection = redirect.refuse(&parts("/a", None), AuthRejection::Unauthenticated);

    assert_eq!(rejection, location("/login?lang=fr&return_to=%2Fa"));
}

#[test]
fn an_anonymous_user_is_sent_to_the_registration_page_when_there_is_one() {
    let redirect = LoginRedirect::to(LOGIN).register_at(ReturnTo::from_static("/register"));

    let rejection = redirect.refuse(&parts("/a?b=1", None), AuthRejection::Forbidden);

    assert_eq!(rejection, location("/register?return_to=%2Fa%3Fb%3D1"));
}

#[test]
fn an_anonymous_user_stays_forbidden_without_a_registration_page() {
    let redirect = LoginRedirect::to(LOGIN);

    let rejection = redirect.refuse(&parts("/a", None), AuthRejection::Forbidden);

    assert_eq!(rejection, AuthRejection::Forbidden);
}

#[test]
fn a_missing_layer_is_never_redirected() {
    let redirect = LoginRedirect::to(LOGIN).register_at(ReturnTo::from_static("/register"));

    let rejection = redirect.refuse(&parts("/a", None), AuthRejection::MissingLayer);

    assert_eq!(rejection, AuthRejection::MissingLayer);
}

#[test]
fn a_rejection_that_is_already_a_redirect_is_kept() {
    let redirect = LoginRedirect::to(LOGIN);

    let rejection = redirect.refuse(&parts("/a", None), location("/elsewhere"));

    assert_eq!(rejection, location("/elsewhere"));
}

#[test]
fn without_the_layer_a_rejection_is_kept() {
    let request = parts("/a", None);

    assert_eq!(
        super::refuse(&request, AuthRejection::Unauthenticated),
        AuthRejection::Unauthenticated
    );
}

#[test]
fn the_layer_installed_in_the_request_decides() {
    let mut request = parts("/a", None);
    request.extensions.insert(LoginRedirect::to(LOGIN));

    assert_eq!(
        super::refuse(&request, AuthRejection::Unauthenticated),
        location("/login?return_to=%2Fa")
    );
}

#[test]
#[should_panic(expected = "unreserved")]
fn a_parameter_name_with_a_control_character_is_refused() {
    let _ = LoginRedirect::to(LOGIN).param("next\n");
}

#[test]
#[should_panic(expected = "unreserved")]
fn a_parameter_name_with_a_reserved_character_is_refused() {
    let _ = LoginRedirect::to(LOGIN).param("a&b");
}

#[test]
#[should_panic(expected = "unreserved")]
fn an_empty_parameter_name_is_refused() {
    let _ = LoginRedirect::to(LOGIN).param("");
}
