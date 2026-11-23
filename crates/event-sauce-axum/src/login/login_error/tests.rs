use std::error::Error as _;

use axum::response::{IntoResponse, Response};
use axum_login::tower_sessions::session_store;
use event_sauce_auth::{
    AuthError, Email, FinishError, HashError, LoginId, LoginStoreError, PendingLoginError,
    Rejected, ResumeError,
};
use event_sauce_core::Error;
use http::{header::SET_COOKIE, StatusCode};

use super::LoginError;
use crate::login::SessionError;
use crate::PendingLogin;

fn store() -> Error {
    Error::custom("user store down")
}

fn hash() -> HashError {
    HashError("hasher down".to_owned())
}

fn login_store() -> LoginStoreError {
    LoginStoreError::backend("taking", Error::custom("login store down"))
}

fn session() -> SessionError {
    SessionError::from(axum_login::tower_sessions::session::Error::from(
        session_store::Error::Backend("session store down".to_owned()),
    ))
}

fn unavailable() -> Box<dyn std::error::Error + Send + Sync> {
    Box::new(Error::custom("mailer down"))
}

fn refusals() -> Vec<LoginError> {
    vec![
        AuthError::<()>::InvalidCredentials.into(),
        AuthError::<()>::Inactive.into(),
        AuthError::Unknown { attempt: () }.into(),
        Rejected::Mismatch.into(),
        FinishError::Inactive.into(),
        FinishError::CredentialsChanged.into(),
        ResumeError::NotInProgress.into(),
        ResumeError::Inactive.into(),
        ResumeError::CredentialsChanged.into(),
        Email::parse("not-an-email").unwrap_err().into(),
        LoginError::refused(),
    ]
}

fn outages() -> Vec<LoginError> {
    vec![
        AuthError::<()>::Store(store()).into(),
        AuthError::<()>::Hash(hash()).into(),
        AuthError::<()>::LoginStore(login_store()).into(),
        AuthError::<()>::Unavailable(unavailable()).into(),
        Rejected::Hash(hash()).into(),
        Rejected::LoginStore(login_store()).into(),
        Rejected::Unavailable(unavailable()).into(),
        FinishError::Store(store()).into(),
        ResumeError::Store(store()).into(),
        ResumeError::Pending(PendingLoginError::Store(login_store())).into(),
        PendingLoginError::Encoding(serde_json::from_str::<u8>("x").unwrap_err()).into(),
        session().into(),
    ]
}

async fn snapshot(response: Response) -> (StatusCode, Vec<String>, String) {
    let (parts, body) = response.into_parts();
    let cookies = parts
        .headers
        .get_all(SET_COOKIE)
        .iter()
        .map(|value| value.to_str().unwrap().to_owned())
        .collect();
    let bytes = axum::body::to_bytes(body, usize::MAX).await.unwrap();
    (
        parts.status,
        cookies,
        String::from_utf8(bytes.to_vec()).unwrap(),
    )
}

#[test]
fn a_refusal_is_refused_and_an_outage_is_not() {
    assert!(refusals().iter().all(LoginError::is_refused));
    assert!(outages().iter().all(|error| !error.is_refused()));
}

#[tokio::test]
async fn every_refusal_answers_the_same_bytes_and_clears_the_pending_login() {
    let expected = (
        StatusCode::UNAUTHORIZED,
        vec![PendingLogin::clear_cookie()],
        "login refused".to_owned(),
    );

    for refusal in refusals() {
        assert_eq!(snapshot(refusal.into_response()).await, expected);
    }
}

#[tokio::test]
async fn every_outage_answers_a_server_error_and_leaves_the_pending_login_alone() {
    for outage in outages() {
        let (status, cookies, body) = snapshot(outage.into_response()).await;

        assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
        assert!(cookies.is_empty());
        assert_eq!(body, "internal error");
    }
}

#[tokio::test]
async fn a_session_failure_answers_a_server_error_with_no_detail() {
    let (status, cookies, body) = snapshot(session().into_response()).await;

    assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
    assert!(cookies.is_empty());
    assert_eq!(body, "internal error");
}

#[test]
fn an_outage_exposes_its_cause_and_a_refusal_has_none() {
    let outage = LoginError::from(AuthError::<()>::Store(store()));

    assert_eq!(outage.source().unwrap().to_string(), store().to_string());
    assert!(LoginError::refused().source().is_none());
}
