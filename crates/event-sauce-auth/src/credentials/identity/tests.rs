use event_sauce_core::AggregateClaim;
use serde::{Deserialize, Serialize};

use super::*;
use crate::Fingerprint;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct Fake {
    login: String,
    secret: String,
}

impl Fake {
    fn new(login: &str, secret: &str) -> Self {
        Self {
            login: login.into(),
            secret: secret.into(),
        }
    }
}

impl Credentials for Fake {
    fn claims(&self) -> Vec<AggregateClaim> {
        vec![AggregateClaim::new("fake.login", self.login.clone().into())]
    }

    fn fingerprint(&self, fingerprint: &mut Fingerprint) {
        fingerprint.str_field(&self.login).str_field(&self.secret);
    }
}

fn registered(login: &str, secret: &str) -> Identity<Fake> {
    Identity::Registered(Fake::new(login, secret))
}

#[test]
fn anonymous_has_no_credentials() {
    assert_eq!(Identity::<Fake>::Anonymous.credentials(), None);
}

#[test]
fn register_moves_anonymous_to_registered() {
    let mut identity = Identity::Anonymous;
    identity.register(Fake::new("ada", "s1")).unwrap();
    assert_eq!(identity, registered("ada", "s1"));
    assert_eq!(identity.credentials(), Some(&Fake::new("ada", "s1")));
}

#[test]
fn register_twice_is_refused_and_keeps_the_first_credentials() {
    let mut identity = registered("ada", "s1");
    let error = identity.register(Fake::new("eve", "s2")).unwrap_err();
    assert_eq!(error, IdentityError::AlreadyRegistered);
    assert_eq!(identity, registered("ada", "s1"));
}

#[test]
fn fingerprint_is_stable_for_equal_identities() {
    assert_eq!(
        registered("ada", "s1").session_fingerprint(),
        registered("ada", "s1").session_fingerprint()
    );
}

#[test]
fn fingerprint_changes_with_each_credential_change() {
    let base = registered("ada", "s1").session_fingerprint();
    assert_ne!(base, registered("ada", "s2").session_fingerprint());
    assert_ne!(base, registered("eve", "s1").session_fingerprint());
}

#[test]
fn fingerprint_changes_when_registering() {
    assert_ne!(
        Identity::<Fake>::Anonymous.session_fingerprint(),
        registered("ada", "s1").session_fingerprint()
    );
}

#[test]
fn fingerprint_is_the_tagged_credentials_fingerprint() {
    let mut expected = Fingerprint::new();
    expected.str_field("registered");
    Fake::new("ada", "s1").fingerprint(&mut expected);
    assert_eq!(
        registered("ada", "s1").session_fingerprint(),
        expected.finish()
    );
}

#[test]
fn identity_round_trips_through_json() {
    for identity in [Identity::Anonymous, registered("ada", "s1")] {
        let json = serde_json::to_value(&identity).unwrap();
        assert_eq!(
            serde_json::from_value::<Identity<Fake>>(json).unwrap(),
            identity
        );
    }
}
