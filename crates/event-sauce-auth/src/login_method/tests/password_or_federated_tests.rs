use event_sauce_core::EntityId;

use super::*;
use crate::test_support::{email, secret, FakeHasher, FakeLocator};
use crate::{
    Email, Federated, FederatedId, Password, PasswordHasher, PasswordOrFederated, Provider,
};

async fn both(hasher: &FakeHasher) -> Identity<PasswordOrFederated<Email>> {
    let hash = hasher.hash(&secret("correct-horse")).await.unwrap();
    Identity::Registered(PasswordOrFederated::Both(
        Password::new(email("ada@example.com"), hash),
        Federated::new(FederatedId::new(Provider::GOOGLE, "g-1")),
    ))
}

fn password(login: &str, password: &str) -> PasswordOrFederatedAttempt<Email> {
    PasswordOrFederatedAttempt::Password(PasswordAttempt::new(email(login), secret(password)))
}

fn federated(provider: Provider, subject: &str) -> PasswordOrFederatedAttempt<Email> {
    PasswordOrFederatedAttempt::Federated(VerifiedFederated {
        id: FederatedId::new(provider, subject),
        email: None,
    })
}

#[tokio::test]
async fn a_password_attempt_is_located_by_its_login_claim() {
    let owner = EntityId::new();
    let locator = FakeLocator::holding(&Password::claim_for(&email("ada@example.com")), owner);
    let hasher = FakeHasher::default();
    let method = PasswordOrFederatedLogin::new(hasher.clone());

    let found = LoginMethod::<PasswordOrFederated<Email>>::locate(
        &method,
        &locator,
        &password("ada@example.com", "pw"),
    )
    .await
    .unwrap();

    assert_eq!(found, Some(owner));
}

#[tokio::test]
async fn a_federated_attempt_is_located_by_its_federated_claim() {
    let owner = EntityId::new();
    let locator = FakeLocator::holding(&FederatedId::new(Provider::GOOGLE, "g-1").claim(), owner);
    let hasher = FakeHasher::default();
    let method = PasswordOrFederatedLogin::new(hasher.clone());

    let found = LoginMethod::<PasswordOrFederated<Email>>::locate(
        &method,
        &locator,
        &federated(Provider::GOOGLE, "g-1"),
    )
    .await
    .unwrap();

    assert_eq!(found, Some(owner));
}

#[tokio::test]
async fn a_user_with_both_logs_in_with_either() {
    let hasher = FakeHasher::default();
    let method = PasswordOrFederatedLogin::new(hasher.clone());
    let identity = both(&hasher).await;

    let with_password = method
        .verify(&identity, &password("ada@example.com", "correct-horse"))
        .await;
    let with_google = method
        .verify(&identity, &federated(Provider::GOOGLE, "g-1"))
        .await;

    assert!(with_password.is_ok() && with_google.is_ok());
}

#[tokio::test]
async fn each_attempt_is_checked_by_its_own_method() {
    let hasher = FakeHasher::default();
    let method = PasswordOrFederatedLogin::new(hasher.clone());
    let identity = both(&hasher).await;

    let wrong_password = method
        .verify(&identity, &password("ada@example.com", "battery-staple"))
        .await;
    let wrong_subject = method
        .verify(&identity, &federated(Provider::GOOGLE, "g-2"))
        .await;

    assert!(matches!(wrong_password, Err(Rejected::Mismatch)));
    assert!(matches!(wrong_subject, Err(Rejected::Mismatch)));
}

#[tokio::test]
async fn a_federated_only_user_cannot_log_in_with_a_password() {
    let hasher = FakeHasher::default();
    let method = PasswordOrFederatedLogin::new(hasher.clone());
    let identity = Identity::Registered(PasswordOrFederated::<Email>::Federated(Federated::new(
        FederatedId::new(Provider::GOOGLE, "g-1"),
    )));

    let result = method
        .verify(&identity, &password("ada@example.com", "pw"))
        .await;

    assert!(matches!(result, Err(Rejected::Mismatch)));
}

#[tokio::test]
async fn only_an_unknown_password_attempt_pays_the_dummy_verification() {
    let hasher = FakeHasher::default();
    let method = PasswordOrFederatedLogin::new(hasher.clone());

    LoginMethod::<PasswordOrFederated<Email>>::on_unknown(
        &method,
        &federated(Provider::GOOGLE, "g-1"),
    )
    .await;
    assert_eq!(hasher.verified(), 0);

    LoginMethod::<PasswordOrFederated<Email>>::on_unknown(
        &method,
        &password("ada@example.com", "pw"),
    )
    .await;
    assert_eq!(hasher.verified(), 1);
}
