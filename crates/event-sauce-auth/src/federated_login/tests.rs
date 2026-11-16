use event_sauce_core::EntityId;

use super::*;
use crate::test_support::{fake_phc, secret, FakeLocator};
use crate::{Email, Federated, LoginId, Password, PasswordHash, Provider};

fn google(subject: &str) -> FederatedId {
    FederatedId::new(Provider::GOOGLE, subject)
}

fn attempt(id: FederatedId) -> VerifiedFederated {
    VerifiedFederated { id, email: None }
}

fn linked(ids: &[FederatedId]) -> Identity<Federated> {
    let mut federated = Federated::new(ids[0].clone());
    for id in &ids[1..] {
        federated.link(id.clone()).unwrap();
    }
    Identity::Registered(federated)
}

#[tokio::test]
async fn locate_resolves_the_claim_of_the_identity() {
    let owner = EntityId::new();
    let locator = FakeLocator::holding(&google("g-1").claim(), owner);

    let found =
        LoginMethod::<Federated>::locate(&FederatedLogin, &locator, &attempt(google("g-1")))
            .await
            .unwrap();

    assert_eq!(found, Some(owner));
}

#[tokio::test]
async fn locate_finds_nobody_for_an_unlinked_subject() {
    let owner = EntityId::new();
    let locator = FakeLocator::holding(&google("g-1").claim(), owner);

    let found =
        LoginMethod::<Federated>::locate(&FederatedLogin, &locator, &attempt(google("g-2")))
            .await
            .unwrap();

    assert_eq!(found, None);
}

#[tokio::test]
async fn verify_accepts_a_linked_subject() {
    let identity = linked(&[google("g-1"), FederatedId::new(Provider::GITHUB, "h-1")]);

    let result = FederatedLogin
        .verify(
            &identity,
            &attempt(FederatedId::new(Provider::GITHUB, "h-1")),
        )
        .await;

    assert_eq!(result, Ok(()));
}

#[tokio::test]
async fn verify_rejects_a_subject_that_is_not_linked() {
    let result = FederatedLogin
        .verify(&linked(&[google("g-1")]), &attempt(google("g-2")))
        .await;

    assert_eq!(result, Err(Rejected::Mismatch));
}

#[tokio::test]
async fn verify_rejects_the_same_subject_at_another_provider() {
    let result = FederatedLogin
        .verify(
            &linked(&[google("g-1")]),
            &attempt(FederatedId::new(Provider::GITHUB, "g-1")),
        )
        .await;

    assert_eq!(result, Err(Rejected::Mismatch));
}

#[tokio::test]
async fn verify_rejects_an_anonymous_user() {
    let result = FederatedLogin
        .verify(&Identity::<Federated>::Anonymous, &attempt(google("g-1")))
        .await;

    assert_eq!(result, Err(Rejected::Mismatch));
}

#[tokio::test]
async fn verify_rejects_a_user_with_no_federated_links() {
    let hash = PasswordHash::new(fake_phc(&secret("pw"))).unwrap();
    let identity = Identity::Registered(crate::PasswordOrFederated::Password(Password::new(
        Email::parse("ada@example.com").unwrap(),
        hash,
    )));

    let result = FederatedLogin
        .verify(&identity, &attempt(google("g-1")))
        .await;

    assert_eq!(result, Err(Rejected::Mismatch));
}
