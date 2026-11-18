use std::sync::Mutex;

use chrono::{DateTime, Utc};

use super::*;
use crate::{
    test_support::{FakeLoginStore, SecondFactorCredentials},
    FixedClock, LoginKey, LoginTtl,
};

type Creds = SecondFactorCredentials;

#[derive(Default)]
struct FakeFactor {
    calls: Mutex<Vec<(String, String)>>,
}

impl FakeFactor {
    fn calls(&self) -> Vec<(String, String)> {
        self.calls.lock().unwrap().clone()
    }
}

#[async_trait]
impl SecondFactor<Creds> for Arc<FakeFactor> {
    type Challenge = String;
    type Response = String;

    fn challenge(&self, _: &Creds) -> String {
        "nonce".into()
    }

    async fn verify(
        &self,
        _: &Creds,
        challenge: &String,
        response: &String,
        store: &dyn LoginStore,
    ) -> Result<(), Rejected> {
        self.calls
            .lock()
            .unwrap()
            .push((challenge.clone(), response.clone()));
        store
            .put(&LoginKey::new("fake", "seen"), b"", LoginTtl::ONE_MINUTE)
            .await
            .map_err(Rejected::from)?;
        if response == "right" {
            Ok(())
        } else {
            Err(Rejected::Mismatch)
        }
    }
}

fn instant() -> DateTime<Utc> {
    DateTime::from_timestamp(1_700_000_000, 0).unwrap()
}

fn setup() -> (
    Arc<FakeLoginStore>,
    Arc<FakeFactor>,
    TwoFactor<Arc<FakeFactor>>,
) {
    let store = Arc::new(FakeLoginStore::default());
    let factor = Arc::new(FakeFactor::default());
    let two_factor =
        TwoFactor::new(factor.clone(), store.clone()).with_clock(Arc::new(FixedClock(instant())));
    (store, factor, two_factor)
}

fn registered(second_factor: bool) -> Identity<Creds> {
    Identity::Registered(Creds { second_factor })
}

#[tokio::test]
async fn no_second_factor_never_holds_a_login_back() {
    let begun = NoSecondFactor
        .begin(EntityId::new(), &registered(true))
        .await
        .unwrap();

    assert!(begun.is_none());
}

#[tokio::test]
async fn an_anonymous_identity_needs_no_second_factor() {
    let (store, _, two_factor) = setup();

    let begun = two_factor
        .begin(EntityId::new(), &Identity::<Creds>::Anonymous)
        .await
        .unwrap();

    assert!(begun.is_none());
    assert_eq!(store.len(), 0);
}

#[tokio::test]
async fn credentials_that_do_not_require_one_are_not_held_back() {
    let (store, _, two_factor) = setup();

    let begun = two_factor
        .begin(EntityId::new(), &registered(false))
        .await
        .unwrap();

    assert!(begun.is_none());
    assert_eq!(store.len(), 0);
}

#[tokio::test]
async fn a_required_second_factor_stores_a_pending_login() {
    let (store, _, two_factor) = setup();
    let user = EntityId::new();

    let id = two_factor
        .begin(user, &registered(true))
        .await
        .unwrap()
        .unwrap();

    let pending: PendingLogin<String> = two_factor.take(id).await.unwrap().unwrap();
    assert_eq!(
        pending,
        PendingLogin {
            user,
            challenge: "nonce".into(),
            created_at: instant()
        }
    );
    assert_eq!(store.len(), 0);
}

#[tokio::test]
async fn a_pending_login_lives_for_five_minutes_by_default() {
    let (store, _, two_factor) = setup();

    let id = two_factor
        .begin(EntityId::new(), &registered(true))
        .await
        .unwrap()
        .unwrap();

    assert_eq!(
        store.ttl_of(&LoginKey::new("pending", id)),
        Some(LoginTtl::FIVE_MINUTES)
    );
}

#[tokio::test]
async fn the_pending_ttl_is_configurable() {
    let (store, factor, _) = setup();
    let two_factor = TwoFactor::new(factor, store.clone()).with_pending_ttl(LoginTtl::ONE_HOUR);

    let id = two_factor
        .begin(EntityId::new(), &registered(true))
        .await
        .unwrap()
        .unwrap();

    assert_eq!(
        store.ttl_of(&LoginKey::new("pending", id)),
        Some(LoginTtl::ONE_HOUR)
    );
}

#[tokio::test]
async fn a_pending_login_is_single_use() {
    let (_, _, two_factor) = setup();
    let id = two_factor
        .begin(EntityId::new(), &registered(true))
        .await
        .unwrap()
        .unwrap();

    assert!(two_factor.take(id).await.unwrap().is_some());
    assert!(two_factor.take(id).await.unwrap().is_none());
}

#[tokio::test]
async fn a_failing_store_stops_the_login_from_starting() {
    let factor = Arc::new(FakeFactor::default());
    let two_factor = TwoFactor::new(factor, Arc::new(FakeLoginStore::broken()));

    let begun = two_factor.begin(EntityId::new(), &registered(true)).await;

    assert!(matches!(begun, Err(PendingLoginError::Store(_))));
}

fn pending() -> PendingLogin<String> {
    PendingLogin {
        user: EntityId::new(),
        challenge: "nonce".into(),
        created_at: instant(),
    }
}

#[tokio::test]
async fn verifying_runs_the_factor_with_the_stored_challenge_and_the_store() {
    let (store, factor, two_factor) = setup();

    let verified = two_factor
        .verify(&registered(true), &pending(), &"right".to_owned())
        .await;

    assert!(verified.is_ok());
    assert_eq!(factor.calls(), [("nonce".to_owned(), "right".to_owned())]);
    assert_eq!(store.len(), 1);
}

#[tokio::test]
async fn a_rejected_response_is_a_mismatch() {
    let (_, _, two_factor) = setup();

    let verified = two_factor
        .verify(&registered(true), &pending(), &"wrong".to_owned())
        .await;

    assert!(matches!(verified, Err(Rejected::Mismatch)));
}

#[tokio::test]
async fn an_identity_without_credentials_is_refused_without_asking_the_factor() {
    let (_, factor, two_factor) = setup();

    let verified = two_factor
        .verify(&Identity::Anonymous, &pending(), &"right".to_owned())
        .await;

    assert!(matches!(verified, Err(Rejected::Mismatch)));
    assert!(factor.calls().is_empty());
}
