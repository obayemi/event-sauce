use super::*;
use crate::LoginStoreError;

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
fn a_login_store_failure_in_a_method_is_a_pending_login_error() {
    let error: AuthError<()> = Rejected::LoginStore(store_failure()).into();

    assert!(matches!(
        error,
        AuthError::Pending(PendingLoginError::Store(_))
    ));
}

#[test]
fn a_mismatching_response_is_an_invalid_response() {
    let error = CompleteError::from(Rejected::Mismatch);

    assert!(matches!(error, CompleteError::InvalidResponse));
}

#[test]
fn a_hash_failure_while_completing_stays_a_hash_error() {
    let error = CompleteError::from(Rejected::Hash(HashError("broken".into())));

    assert!(matches!(error, CompleteError::Hash(HashError(ref cause)) if cause == "broken"));
}

#[test]
fn a_login_store_failure_while_completing_is_a_pending_login_error() {
    let error = CompleteError::from(Rejected::LoginStore(store_failure()));

    assert!(matches!(
        error,
        CompleteError::Pending(PendingLoginError::Store(_))
    ));
}
