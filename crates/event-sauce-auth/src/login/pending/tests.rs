use chrono::DateTime;
use event_sauce_core::AggregateRoot;

use super::*;
use crate::{
    test_support::{credentials, restored_user, stored_user, FakeLoginStore, FakeUsers, TestUser},
    Done, LoginTtl, Logins,
};

#[derive(Debug, PartialEq, Serialize, Deserialize)]
struct Waiting {
    note: String,
}

impl LoginState for Waiting {
    const TAG: &'static str = "test.waiting";
}

#[derive(Debug, Serialize, Deserialize)]
struct Patient;

impl LoginState for Patient {
    const TAG: &'static str = "test.patient";
    const TTL: LoginTtl = LoginTtl::ONE_HOUR;
}

struct Fixture {
    user: Arc<AggregateRoot<TestUser>>,
    users: FakeUsers,
    store: Arc<FakeLoginStore>,
    pending: PendingLogins<TestUser>,
}

impl Fixture {
    fn new() -> Self {
        let user = stored_user(credentials("ada@example.com", "pw"), true);
        Self::with_store(user, FakeLoginStore::default())
    }

    fn with_store(user: Arc<AggregateRoot<TestUser>>, store: FakeLoginStore) -> Self {
        let users = FakeUsers::holding(&user);
        let store = Arc::new(store);
        let pending = Logins::over(Arc::new(users.clone())).pending(store.clone());
        Self {
            user,
            users,
            store,
            pending,
        }
    }

    fn login<S>(&self, state: S) -> Login<TestUser, S> {
        Login::new(
            self.user.clone(),
            DateTime::from_timestamp(1_700_000_000, 0).unwrap(),
            Arc::new(self.users.clone()),
            state,
        )
    }

    fn waiting(&self) -> Login<TestUser, Waiting> {
        self.login(Waiting {
            note: "hello".into(),
        })
    }
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
async fn a_saved_login_resumes_with_everything_it_held() {
    let fixture = Fixture::new();
    let id = fixture.pending.save(fixture.waiting()).await.unwrap();

    let login = fixture.pending.resume::<Waiting>(id).await.unwrap();

    assert_eq!(
        login.state(),
        &Waiting {
            note: "hello".into()
        }
    );
    assert_eq!(login.user().entity_id(), fixture.user.entity_id());
    assert_eq!(login.started_at(), fixture.waiting().started_at());
    assert_eq!(
        login.fingerprint(),
        fixture.user.identity().session_fingerprint()
    );
}

#[tokio::test]
async fn a_saved_login_lives_under_its_id_for_the_ttl_of_its_state() {
    let fixture = Fixture::new();

    let short = fixture.pending.save(fixture.waiting()).await.unwrap();
    let long = fixture.pending.save(fixture.login(Patient)).await.unwrap();

    assert_eq!(
        fixture.store.ttl_of(&LoginKey::new("pending", short)),
        Some(LoginTtl::FIVE_MINUTES)
    );
    assert_eq!(
        fixture.store.ttl_of(&LoginKey::new("pending", long)),
        Some(LoginTtl::ONE_HOUR)
    );
}

#[tokio::test]
async fn an_unknown_id_is_not_in_progress() {
    let fixture = Fixture::new();

    let error = fixture
        .pending
        .resume::<Waiting>(PendingLoginId::new())
        .await
        .unwrap_err();

    assert!(matches!(error, ResumeError::NotInProgress));
}

#[tokio::test]
async fn a_saved_login_resumes_once() {
    let fixture = Fixture::new();
    let id = fixture.pending.save(fixture.waiting()).await.unwrap();
    fixture.pending.resume::<Waiting>(id).await.unwrap();

    let error = fixture.pending.resume::<Waiting>(id).await.unwrap_err();

    assert!(matches!(error, ResumeError::NotInProgress));
}

#[tokio::test]
async fn a_login_does_not_resume_in_another_state_and_is_used_up() {
    let fixture = Fixture::new();
    let id = fixture.pending.save(fixture.waiting()).await.unwrap();

    let wrong = fixture.pending.resume::<Done>(id).await.unwrap_err();
    let right = fixture.pending.resume::<Waiting>(id).await.unwrap_err();

    assert!(matches!(wrong, ResumeError::NotInProgress));
    assert!(matches!(right, ResumeError::NotInProgress));
}

#[tokio::test]
async fn a_user_deactivated_since_cannot_resume() {
    let fixture = Fixture::new();
    let id = fixture.pending.save(fixture.waiting()).await.unwrap();
    fixture.users.replace(restored_user(
        fixture.user.entity_id(),
        credentials("ada@example.com", "pw"),
        false,
    ));

    let error = fixture.pending.resume::<Waiting>(id).await.unwrap_err();

    assert!(matches!(error, ResumeError::Inactive));
}

#[tokio::test]
async fn a_user_removed_since_cannot_resume() {
    let fixture = Fixture::new();
    let id = fixture.pending.save(fixture.waiting()).await.unwrap();
    fixture.users.remove(fixture.user.entity_id());

    let error = fixture.pending.resume::<Waiting>(id).await.unwrap_err();

    assert!(matches!(error, ResumeError::Inactive));
}

#[tokio::test]
async fn credentials_changed_since_cannot_resume() {
    let fixture = Fixture::new();
    let id = fixture.pending.save(fixture.waiting()).await.unwrap();
    fixture.users.replace(restored_user(
        fixture.user.entity_id(),
        credentials("ada@example.com", "new-password"),
        true,
    ));

    let error = fixture.pending.resume::<Waiting>(id).await.unwrap_err();

    assert!(matches!(error, ResumeError::CredentialsChanged));
}

#[tokio::test]
async fn a_failing_login_store_fails_both_directions() {
    let user = stored_user(credentials("ada@example.com", "pw"), true);
    let fixture = Fixture::with_store(user, FakeLoginStore::broken());

    let saved = fixture.pending.save(fixture.waiting()).await;
    let resumed = fixture
        .pending
        .resume::<Waiting>(PendingLoginId::new())
        .await;

    assert!(matches!(saved, Err(PendingLoginError::Store(_))));
    assert!(matches!(
        resumed,
        Err(ResumeError::Pending(PendingLoginError::Store(_)))
    ));
}

#[tokio::test]
async fn an_entry_that_is_not_a_pending_login_is_malformed() {
    let fixture = Fixture::new();
    let id = PendingLoginId::new();
    fixture
        .store
        .put(
            &LoginKey::new("pending", id),
            b"not json",
            LoginTtl::ONE_MINUTE,
        )
        .await
        .unwrap();

    let error = fixture.pending.resume::<Waiting>(id).await.unwrap_err();

    assert!(matches!(
        error,
        ResumeError::Pending(PendingLoginError::Encoding(_))
    ));
}

#[tokio::test]
async fn a_state_that_does_not_decode_is_malformed() {
    let fixture = Fixture::new();
    let id = fixture.pending.save(fixture.login(Patient)).await.unwrap();
    let key = LoginKey::new("pending", id);
    let bytes = fixture.store.take(&key).await.unwrap().unwrap();
    let retagged = String::from_utf8(bytes)
        .unwrap()
        .replace("test.patient", "test.waiting");
    fixture
        .store
        .put(&key, retagged.as_bytes(), LoginTtl::ONE_MINUTE)
        .await
        .unwrap();

    let error = fixture.pending.resume::<Waiting>(id).await.unwrap_err();

    assert!(matches!(
        error,
        ResumeError::Pending(PendingLoginError::Encoding(_))
    ));
}
