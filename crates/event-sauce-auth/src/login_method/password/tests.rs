use event_sauce_core::{AggregateClaim, EntityId};
use serde::{Deserialize, Serialize};

use super::*;
use crate::test_support::{email, fake_phc, secret, FakeHasher, FakeLocator};
use crate::{
    Credentials, Digest, Email, FederatedId, LoginId, PasswordOrFederated, Provider, Sha256,
    Username,
};

fn registered(hasher: &FakeHasher, login: &str, password: &str) -> Identity<Password<Email>> {
    let hash = hasher.hash(&secret(password)).unwrap();
    Identity::Registered(Password::new(email(login), hash))
}

fn attempt(login: &str, password: &str) -> PasswordAttempt<Email> {
    PasswordAttempt::new(email(login), secret(password))
}

#[tokio::test]
async fn locate_resolves_the_claim_of_the_login() {
    let owner = EntityId::new();
    let locator = FakeLocator::holding(&Password::claim_for(&email("ada@example.com")), owner);
    let hasher = FakeHasher::default();
    let method = PasswordLogin::new(hasher.clone());

    let found = LoginMethod::<Password<Email>>::locate(
        &method,
        &locator,
        &attempt("ADA@example.com", "pw"),
    )
    .await
    .unwrap();

    assert_eq!(found, Some(owner));
}

#[tokio::test]
async fn locate_finds_nobody_for_an_unclaimed_login() {
    let locator = FakeLocator::default();
    let hasher = FakeHasher::default();
    let method = PasswordLogin::new(hasher.clone());

    let found = LoginMethod::<Password<Email>>::locate(
        &method,
        &locator,
        &attempt("ada@example.com", "pw"),
    )
    .await
    .unwrap();

    assert_eq!(found, None);
}

#[tokio::test]
async fn verify_accepts_the_stored_password() {
    let hasher = FakeHasher::default();
    let method = PasswordLogin::new(hasher.clone());
    let identity = registered(&method.hasher, "ada@example.com", "correct-horse");

    let result = method
        .verify(&identity, &attempt("ada@example.com", "correct-horse"))
        .await;

    assert!(result.is_ok());
}

#[tokio::test]
async fn verify_rejects_another_password() {
    let hasher = FakeHasher::default();
    let method = PasswordLogin::new(hasher.clone());
    let identity = registered(&method.hasher, "ada@example.com", "correct-horse");

    let result = method
        .verify(&identity, &attempt("ada@example.com", "battery-staple"))
        .await;

    assert!(matches!(result, Err(Rejected::Mismatch)));
}

#[tokio::test]
async fn verify_rejects_an_anonymous_user_after_a_dummy_verification() {
    let hasher = FakeHasher::default();
    let method = PasswordLogin::new(hasher.clone());

    let result = method
        .verify(
            &Identity::<Password<Email>>::Anonymous,
            &attempt("ada@example.com", "pw"),
        )
        .await;

    assert!(matches!(result, Err(Rejected::Mismatch)));
    assert_eq!(hasher.verified(), 1);
}

#[tokio::test]
async fn verify_rejects_a_user_without_a_password_after_a_dummy_verification() {
    let hasher = FakeHasher::default();
    let method = PasswordLogin::new(hasher.clone());
    let identity = Identity::Registered(PasswordOrFederated::<Email>::Federated(
        crate::Federated::new(FederatedId::new(Provider::GOOGLE, "g-1")),
    ));

    let result = method
        .verify(&identity, &attempt("ada@example.com", "pw"))
        .await;

    assert!(matches!(result, Err(Rejected::Mismatch)));
    assert_eq!(hasher.verified(), 1);
}

#[tokio::test]
async fn verify_surfaces_a_hasher_failure() {
    let hasher = FakeHasher::broken();
    let method = PasswordLogin::new(hasher.clone());
    let hash = PasswordHash::new(fake_phc(&secret("correct-horse"))).unwrap();
    let identity = Identity::Registered(Password::new(email("ada@example.com"), hash));

    let result = method
        .verify(&identity, &attempt("ada@example.com", "correct-horse"))
        .await;

    assert!(matches!(result, Err(Rejected::Hash(HashError(ref cause))) if cause == "broken"));
}

#[tokio::test]
async fn on_unknown_verifies_once_against_a_dummy_hash() {
    let hasher = FakeHasher::default();
    let method = PasswordLogin::new(hasher.clone());

    LoginMethod::<Password<Email>>::on_unknown(&method, &attempt("ada@example.com", "pw")).await;

    assert_eq!(hasher.verified(), 1);
}

#[tokio::test]
async fn the_dummy_hash_is_made_once_and_reused() {
    let hasher = FakeHasher::default();
    let method = PasswordLogin::new(hasher.clone());

    for _ in 0..3 {
        LoginMethod::<Password<Email>>::on_unknown(&method, &attempt("ada@example.com", "pw"))
            .await;
    }

    assert_eq!(hasher.hashed(), 1);
    assert_eq!(hasher.verified(), 3);
}

#[tokio::test]
async fn on_unknown_survives_a_hasher_failure() {
    let hasher = FakeHasher::broken();
    let method = PasswordLogin::new(hasher.clone());

    LoginMethod::<Password<Email>>::on_unknown(&method, &attempt("ada@example.com", "pw")).await;

    assert_eq!(hasher.verified(), 0);
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct Staff {
    password: Password<Username>,
    badge: u32,
}

impl Credentials for Staff {
    fn claims(&self) -> Vec<AggregateClaim> {
        self.password.claims()
    }

    fn fingerprint(&self, digest: &mut Sha256) {
        self.password.fingerprint(digest);
        digest.update(self.badge.to_be_bytes());
    }
}

impl HasPassword for Staff {
    type Login = Username;

    fn password(&self) -> Option<&Password<Username>> {
        Some(&self.password)
    }
}

#[tokio::test]
async fn an_app_defined_credentials_type_logs_in_through_password_login() {
    let hasher = FakeHasher::default();
    let method = PasswordLogin::new(hasher.clone());
    let hash = hasher.hash(&secret("correct-horse")).unwrap();
    let identity = Identity::Registered(Staff {
        password: Password::new(Username::parse("ada").unwrap(), hash),
        badge: 7,
    });
    let attempt = PasswordAttempt::new(Username::parse("ADA").unwrap(), secret("correct-horse"));

    assert!(method.verify(&identity, &attempt).await.is_ok());
}
