use super::*;

#[test]
fn a_hash_failure_converts_into_a_rejection_that_keeps_its_cause() {
    let rejected = Rejected::from(HashError("broken".into()));
    assert!(matches!(rejected, Rejected::Hash(HashError(ref cause)) if cause == "broken"));
}

#[test]
fn a_mismatch_says_credentials_do_not_match() {
    assert_eq!(Rejected::Mismatch.to_string(), "credentials do not match");
}

#[test]
fn a_login_store_failure_converts_into_a_rejection_that_keeps_its_cause() {
    let rejected = Rejected::from(LoginStoreError::backend(
        "fake store",
        std::io::Error::other("broken"),
    ));

    assert!(matches!(
        rejected,
        Rejected::LoginStore(LoginStoreError::Backend { .. })
    ));
}
