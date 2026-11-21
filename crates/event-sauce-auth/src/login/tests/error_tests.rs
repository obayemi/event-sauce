use super::*;
use crate::login::users::Stale;

fn store_failure() -> LoginStoreError {
    LoginStoreError::backend("fake", std::io::Error::other("broken"))
}

#[test]
fn a_mismatch_is_invalid_credentials() {
    let error: AuthError<()> = Rejected::Mismatch.into();

    assert!(matches!(error, AuthError::InvalidCredentials));
}

#[test]
fn a_hash_failure_stays_a_hash_error() {
    let error: AuthError<()> = Rejected::Hash(HashError("broken".into())).into();

    assert!(matches!(error, AuthError::Hash(HashError(ref cause)) if cause == "broken"));
}

#[test]
fn a_login_store_failure_stays_a_login_store_error() {
    let error: AuthError<()> = Rejected::LoginStore(store_failure()).into();

    assert!(matches!(error, AuthError::LoginStore(_)));
}

#[test]
fn an_unavailable_verifier_keeps_its_cause() {
    let error: AuthError<()> = Rejected::unavailable(std::io::Error::other("mailer down")).into();

    assert!(
        matches!(error, AuthError::Unavailable(ref cause) if cause.to_string() == "mailer down")
    );
}

#[test]
fn a_pending_login_error_converts_into_a_resume_error() {
    let error = ResumeError::from(PendingLoginError::from(store_failure()));

    assert!(matches!(
        error,
        ResumeError::Pending(PendingLoginError::Store(_))
    ));
}

#[test]
fn every_failure_says_what_went_wrong() {
    assert_eq!(FinishError::Inactive.to_string(), "account inactive");
    assert_eq!(
        FinishError::CredentialsChanged.to_string(),
        "credentials changed during the login"
    );
    assert_eq!(
        ResumeError::NotInProgress.to_string(),
        "no login is in progress"
    );
    assert_eq!(SignUpError::NotNew.to_string(), "the user is not new");
}

#[test]
fn a_stale_login_converts_into_the_matching_finish_error() {
    assert!(matches!(
        FinishError::from(Stale::Inactive),
        FinishError::Inactive
    ));
    assert!(matches!(
        FinishError::from(Stale::CredentialsChanged),
        FinishError::CredentialsChanged
    ));
}

#[test]
fn a_stale_login_converts_into_the_matching_resume_error() {
    assert!(matches!(
        ResumeError::from(Stale::Inactive),
        ResumeError::Inactive
    ));
    assert!(matches!(
        ResumeError::from(Stale::CredentialsChanged),
        ResumeError::CredentialsChanged
    ));
}
