use super::*;

#[test]
fn a_hash_failure_converts_into_a_rejection_that_keeps_its_cause() {
    let rejected = Rejected::from(HashError("broken".into()));
    assert_eq!(rejected, Rejected::Hash(HashError("broken".into())));
}

#[test]
fn a_mismatch_says_credentials_do_not_match() {
    assert_eq!(Rejected::Mismatch.to_string(), "credentials do not match");
}
