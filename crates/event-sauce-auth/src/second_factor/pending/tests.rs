use super::*;
use crate::test_support::FakeLoginStore;

const TTL: LoginTtl = LoginTtl::FIVE_MINUTES;

fn pending(user: EntityId) -> PendingLogin<String> {
    PendingLogin {
        user,
        challenge: "nonce".into(),
        created_at: DateTime::from_timestamp(1_700_000_000, 0).unwrap(),
    }
}

fn pending_logins(store: &Arc<FakeLoginStore>) -> PendingLogins {
    PendingLogins::new(store.clone(), TTL)
}

#[test]
fn an_id_round_trips_through_its_text_form() {
    let id = PendingLoginId::new();

    assert_eq!(id.to_string().parse::<PendingLoginId>().unwrap(), id);
}

#[test]
fn an_id_that_is_not_a_uuid_does_not_parse() {
    assert!("not-an-id".parse::<PendingLoginId>().is_err());
}

#[test]
fn ids_are_unique() {
    assert_ne!(PendingLoginId::new(), PendingLoginId::new());
}

#[test]
fn an_id_serialises_as_a_bare_string() {
    let id = PendingLoginId::new();

    assert_eq!(serde_json::to_string(&id).unwrap(), format!("\"{id}\""));
}

#[tokio::test]
async fn a_started_login_is_taken_back_once() {
    let store = Arc::new(FakeLoginStore::default());
    let logins = pending_logins(&store);
    let user = EntityId::new();

    let id = logins.start(&pending(user)).await.unwrap();

    let taken: PendingLogin<String> = logins.take(id).await.unwrap().unwrap();
    assert_eq!(taken, pending(user));
    assert!(logins.take::<String>(id).await.unwrap().is_none());
}

#[tokio::test]
async fn a_login_is_stored_under_its_id_until_the_ttl_elapses() {
    let store = Arc::new(FakeLoginStore::default());
    let logins = pending_logins(&store);

    let id = logins.start(&pending(EntityId::new())).await.unwrap();

    assert_eq!(
        store.ttl_of(&LoginKey::new("pending", id)),
        Some(TTL),
        "the pending login lives under pending:<id> for the configured ttl"
    );
}

#[tokio::test]
async fn an_unknown_login_is_not_in_progress() {
    let store = Arc::new(FakeLoginStore::default());

    let taken = pending_logins(&store)
        .take::<String>(PendingLoginId::new())
        .await
        .unwrap();

    assert!(taken.is_none());
}

#[tokio::test]
async fn an_entry_that_is_not_a_pending_login_is_an_encoding_error() {
    let store = Arc::new(FakeLoginStore::default());
    let id = PendingLoginId::new();
    store
        .put(&LoginKey::new("pending", id), b"\xff garbage", TTL)
        .await
        .unwrap();

    let error = pending_logins(&store).take::<String>(id).await.unwrap_err();

    assert!(matches!(error, PendingLoginError::Encoding(_)));
}

#[tokio::test]
async fn a_failing_store_is_a_store_error() {
    let store = Arc::new(FakeLoginStore::broken());
    let logins = pending_logins(&store);

    let started = logins.start(&pending(EntityId::new())).await;
    let taken = logins.take::<String>(PendingLoginId::new()).await;

    assert!(matches!(started, Err(PendingLoginError::Store(_))));
    assert!(matches!(taken, Err(PendingLoginError::Store(_))));
}
