use event_sauce_auth::{AuthError, HashError, LoginOutcome, PendingLoginError, PendingLoginId};

use super::{into_session_user, BackendError};
use crate::login::test_support::{registered, Member};

type Result = std::result::Result<LoginOutcome<Member>, AuthError<&'static str>>;

#[test]
fn an_authenticated_user_opens_a_session_user() {
    let root = registered();
    let id = root.entity_id();

    let user = into_session_user(Result::Ok(LoginOutcome::Authenticated(root)))
        .unwrap()
        .unwrap();

    assert_eq!(user.entity_id(), id);
}

#[test]
fn a_refused_login_is_no_user_rather_than_an_error() {
    for refusal in [
        AuthError::InvalidCredentials,
        AuthError::Inactive,
        AuthError::Unknown { attempt: "ada" },
    ] {
        assert!(into_session_user(Result::Err(refusal)).unwrap().is_none());
    }
}

#[test]
fn a_required_second_factor_is_an_error_carrying_the_pending_id() {
    let pending = PendingLoginId::new();

    let error =
        into_session_user(Result::Ok(LoginOutcome::SecondFactorRequired(pending))).unwrap_err();

    assert!(matches!(error, BackendError::SecondFactorRequired(id) if id == pending));
}

#[test]
fn infrastructure_failures_stay_errors() {
    let store = event_sauce_core::Error::cascade_depth_exceeded(1, 1);
    let hash = HashError("broken".to_owned());

    assert!(matches!(
        into_session_user(Result::Err(AuthError::Store(store))),
        Err(BackendError::Store(_))
    ));
    assert!(matches!(
        into_session_user(Result::Err(AuthError::Hash(hash))),
        Err(BackendError::Hash(_))
    ));
    let malformed = serde_json::from_str::<u8>("x").unwrap_err();
    assert!(matches!(
        into_session_user(Result::Err(AuthError::Pending(
            PendingLoginError::Encoding(malformed)
        ))),
        Err(BackendError::Pending(_))
    ));
}
