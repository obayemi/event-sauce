use std::sync::Arc;

use http::request::Parts;
use http::{Request, StatusCode};

use super::admit;
use crate::login::test_support::{anonymous, registered, Member};
use crate::{AuthRejection, LoginRedirect, ReturnTo};

fn bare() -> Parts {
    Request::builder()
        .uri("/notes/1")
        .body(())
        .unwrap()
        .into_parts()
        .0
}

fn redirecting() -> Parts {
    let mut parts = bare();
    parts.extensions.insert(
        LoginRedirect::to(ReturnTo::from_static("/login"))
            .register_at(ReturnTo::from_static("/register")),
    );
    parts
}

#[test]
fn nobody_logged_in_is_refused_before_anything_else() {
    let refusal = admit::<Member>(&bare(), None, true).unwrap_err();

    assert!(matches!(refusal, AuthRejection::Unauthenticated));
    assert_eq!(refusal.status(), StatusCode::UNAUTHORIZED);
}

#[test]
fn an_anonymous_user_is_admitted_unless_a_registered_one_is_wanted() {
    let user = Arc::new(anonymous());

    assert!(admit::<Member>(&bare(), Some(Arc::clone(&user)), false).is_ok());
    let refusal = admit::<Member>(&bare(), Some(user), true).unwrap_err();
    assert!(matches!(refusal, AuthRejection::Forbidden));
    assert_eq!(refusal.status(), StatusCode::FORBIDDEN);
}

#[test]
fn a_registered_user_is_admitted_either_way() {
    let user = Arc::new(registered());

    assert!(admit::<Member>(&bare(), Some(Arc::clone(&user)), true).is_ok());
    assert!(admit::<Member>(&bare(), Some(user), false).is_ok());
}

#[test]
fn nobody_logged_in_is_redirected_under_a_login_redirect() {
    let refusal = admit::<Member>(&redirecting(), None, false).unwrap_err();

    assert_eq!(refusal.status(), StatusCode::SEE_OTHER);
    assert!(matches!(
        refusal,
        AuthRejection::Redirect { ref location } if location == "/login?return_to=%2Fnotes%2F1"
    ));
}

#[test]
fn an_anonymous_user_is_redirected_to_registration_under_a_login_redirect() {
    let refusal = admit::<Member>(&redirecting(), Some(Arc::new(anonymous())), true).unwrap_err();

    assert!(matches!(
        refusal,
        AuthRejection::Redirect { ref location } if location == "/register?return_to=%2Fnotes%2F1"
    ));
}
