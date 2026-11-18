//! A password login that demands a TOTP code: `ClaimAuthenticator` holds the
//! login back, then `complete` turns it into an authenticated user.

use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use argon2::Params;
use async_trait::async_trait;
use chrono::DateTime;
use event_sauce_auth::{
    Argon2Hasher, AuthError, Authenticator, ClaimAuthenticator, CompleteError, Credentials, Email,
    Fingerprint, FixedClock, HasPassword, HasSecondFactor, HasTotp, Identity, LoginId, LoginKey,
    LoginOutcome, LoginStore, LoginStoreError, LoginTtl, Password, PasswordAttempt, PasswordHasher,
    PasswordLogin, PendingLoginId, Totp, TotpSecret, TwoFactor, UserAggregate,
};
use event_sauce_core::{
    aggregate_command, define_events, Aggregate, AggregateClaim, AggregateError, AggregateRoot,
    Entity, EntityId, EventStore, Repository,
};
use event_sauce_memory::InMemoryEventStore;
use secrecy::SecretString;
use serde::{Deserialize, Serialize};

const NOW: i64 = 1_700_000_010;
const STEP: i64 = 30;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct Secured {
    password: Password<Email>,
    totp: Option<TotpSecret>,
}

impl Credentials for Secured {
    fn claims(&self) -> Vec<AggregateClaim> {
        self.password.claims()
    }

    fn fingerprint(&self, fingerprint: &mut Fingerprint) {
        self.password.fingerprint(fingerprint);
        self.feed_totp_fingerprint(fingerprint);
    }
}

impl HasPassword for Secured {
    type Login = Email;

    fn password(&self) -> Option<&Password<Email>> {
        Some(&self.password)
    }
}

impl HasSecondFactor for Secured {
    fn requires_second_factor(&self) -> bool {
        self.totp.is_some()
    }
}

impl HasTotp for Secured {
    fn totp_secret(&self) -> Option<&TotpSecret> {
        self.totp.as_ref()
    }
}

#[derive(Debug, thiserror::Error)]
enum UserError {}

impl AggregateError for UserError {}

#[derive(Debug, Serialize, Deserialize)]
struct User {
    id: EntityId,
    identity: Identity<Secured>,
    active: bool,
}

impl Entity for User {
    fn entity_id(&self) -> EntityId {
        self.id
    }
}

impl Aggregate for User {
    type Event = UserEvent;
    type Error = UserError;
    type DeletedState = Self;

    fn claims(&self) -> Vec<AggregateClaim> {
        self.identity
            .credentials()
            .map(Credentials::claims)
            .unwrap_or_default()
    }
}

impl UserAggregate for User {
    type Credentials = Secured;

    fn identity(&self) -> &Identity<Secured> {
        &self.identity
    }

    fn is_active(&self) -> bool {
        self.active
    }
}

define_events! {
    enum UserEvent for User {
        Registered {
            credentials: Secured,
        }
        @init
        => |id, event| {
            User {
                id,
                identity: Identity::Registered(event.credentials.clone()),
                active: true,
            }
        },

        Deactivated {}
        => |user, _event| {
            user.active = false;
        },
    }
}

aggregate_command! {
    impl User {
        @clock @init fn register(credentials: Secured) -> RegisteredEvent { credentials };
        @clock fn deactivate() -> DeactivatedEvent { };
    }
}

#[derive(Default)]
struct MemoryLoginStore {
    entries: Mutex<HashMap<String, Vec<u8>>>,
}

impl MemoryLoginStore {
    fn len(&self) -> usize {
        self.entries.lock().unwrap().len()
    }
}

#[async_trait]
impl LoginStore for MemoryLoginStore {
    async fn put(&self, key: &LoginKey, value: &[u8], _: LoginTtl) -> Result<(), LoginStoreError> {
        self.entries
            .lock()
            .unwrap()
            .insert(key.as_str().to_owned(), value.to_vec());
        Ok(())
    }

    async fn take(&self, key: &LoginKey) -> Result<Option<Vec<u8>>, LoginStoreError> {
        Ok(self.entries.lock().unwrap().remove(key.as_str()))
    }

    async fn mark_once(&self, key: &LoginKey, _: LoginTtl) -> Result<bool, LoginStoreError> {
        Ok(self
            .entries
            .lock()
            .unwrap()
            .insert(key.as_str().to_owned(), Vec::new())
            .is_none())
    }
}

type Authenticated2fa =
    ClaimAuthenticator<User, PasswordLogin<Argon2Hasher>, InMemoryEventStore, TwoFactor<Totp>>;

fn cheap_hasher() -> Argon2Hasher {
    Argon2Hasher::new(Params::new(8, 1, 1, None).unwrap())
}

fn secret(raw: &str) -> SecretString {
    SecretString::from(raw)
}

fn totp_secret() -> TotpSecret {
    TotpSecret::from([7_u8; 20])
}

fn code_at(seconds: i64) -> SecretString {
    let reference = totp_rs::Builder::new()
        .with_secret(totp_secret())
        .build()
        .unwrap();
    SecretString::from(
        reference
            .generate(u64::try_from(seconds).unwrap())
            .to_string(),
    )
}

struct World {
    events: Arc<InMemoryEventStore>,
    logins: Arc<MemoryLoginStore>,
    authenticator: Authenticated2fa,
}

impl World {
    fn new() -> Self {
        let events = Arc::new(InMemoryEventStore::new());
        let logins = Arc::new(MemoryLoginStore::default());
        let totp = Totp::new().with_clock(Arc::new(FixedClock(
            DateTime::from_timestamp(NOW, 0).unwrap(),
        )));
        let authenticator =
            ClaimAuthenticator::new(events.clone(), PasswordLogin::new(cheap_hasher()))
                .with_second_factor(TwoFactor::new(totp, logins.clone()));
        Self {
            events,
            logins,
            authenticator,
        }
    }

    async fn register(&self, login: &str, enrolled: bool) -> EntityId {
        let hash = cheap_hasher().hash(&secret("correct-horse")).unwrap();
        let credentials = Secured {
            password: Password::new(Email::parse(login).unwrap(), hash),
            totp: enrolled.then(totp_secret),
        };
        let mut user = User::register(credentials).unwrap();
        let id = user.entity_id();
        self.events
            .repository::<User>()
            .save(&mut user)
            .await
            .unwrap();
        id
    }

    async fn deactivate(&self, id: EntityId) {
        self.events
            .repository::<User>()
            .modify(id, |user| user.deactivate())
            .await
            .unwrap();
    }

    async fn first_factor(
        &self,
        login: &str,
        password: &str,
    ) -> Result<LoginOutcome<User>, AuthError<PasswordAttempt<Email>>> {
        self.authenticator
            .authenticate(PasswordAttempt::new(
                Email::parse(login).unwrap(),
                secret(password),
            ))
            .await
    }

    async fn pending(&self, login: &str) -> PendingLoginId {
        match self.first_factor(login, "correct-horse").await.unwrap() {
            LoginOutcome::SecondFactorRequired(id) => id,
            other => panic!("expected a second factor to be required, got {other:?}"),
        }
    }
}

fn user_of(outcome: LoginOutcome<User>) -> AggregateRoot<User> {
    match outcome {
        LoginOutcome::Authenticated(user) => user,
        other => panic!("expected an authenticated user, got {other:?}"),
    }
}

#[tokio::test]
async fn a_user_with_a_second_factor_is_held_back_after_the_password() {
    let world = World::new();
    world.register("ada@example.com", true).await;

    let outcome = world
        .first_factor("ada@example.com", "correct-horse")
        .await
        .unwrap();

    assert!(matches!(outcome, LoginOutcome::SecondFactorRequired(_)));
    assert_eq!(world.logins.len(), 1);
}

#[tokio::test]
async fn the_right_code_completes_the_login() {
    let world = World::new();
    let id = world.register("ada@example.com", true).await;
    let pending = world.pending("ada@example.com").await;

    let user = world
        .authenticator
        .complete(pending, &code_at(NOW))
        .await
        .unwrap();

    assert_eq!(user.entity_id(), id);
}

#[tokio::test]
async fn a_pending_login_completes_only_once() {
    let world = World::new();
    world.register("ada@example.com", true).await;
    let pending = world.pending("ada@example.com").await;
    world
        .authenticator
        .complete(pending, &code_at(NOW))
        .await
        .unwrap();

    let again = world
        .authenticator
        .complete(pending, &code_at(NOW + STEP))
        .await
        .unwrap_err();

    assert!(matches!(again, CompleteError::NotInProgress));
}

#[tokio::test]
async fn a_wrong_code_is_invalid_and_ends_the_pending_login() {
    let world = World::new();
    world.register("ada@example.com", true).await;
    let pending = world.pending("ada@example.com").await;

    let wrong = world
        .authenticator
        .complete(pending, &secret("000000"))
        .await
        .unwrap_err();
    let retry = world
        .authenticator
        .complete(pending, &code_at(NOW))
        .await
        .unwrap_err();

    assert!(matches!(wrong, CompleteError::InvalidResponse));
    assert!(matches!(retry, CompleteError::NotInProgress));
}

#[tokio::test]
async fn a_code_cannot_be_replayed_by_another_login() {
    let world = World::new();
    world.register("ada@example.com", true).await;
    let first = world.pending("ada@example.com").await;
    let second = world.pending("ada@example.com").await;
    world
        .authenticator
        .complete(first, &code_at(NOW))
        .await
        .unwrap();

    let replay = world
        .authenticator
        .complete(second, &code_at(NOW))
        .await
        .unwrap_err();

    assert!(matches!(replay, CompleteError::InvalidResponse));
}

#[tokio::test]
async fn an_unknown_pending_login_is_not_in_progress() {
    let world = World::new();

    let error = world
        .authenticator
        .complete(PendingLoginId::new(), &code_at(NOW))
        .await
        .unwrap_err();

    assert!(matches!(error, CompleteError::NotInProgress));
}

#[tokio::test]
async fn a_user_deactivated_in_between_cannot_complete() {
    let world = World::new();
    let id = world.register("ada@example.com", true).await;
    let pending = world.pending("ada@example.com").await;
    world.deactivate(id).await;

    let error = world
        .authenticator
        .complete(pending, &code_at(NOW))
        .await
        .unwrap_err();

    assert!(matches!(error, CompleteError::Inactive));
}

#[tokio::test]
async fn a_user_without_a_second_factor_is_authenticated_right_away() {
    let world = World::new();
    let id = world.register("ada@example.com", false).await;

    let outcome = world
        .first_factor("ada@example.com", "correct-horse")
        .await
        .unwrap();

    assert_eq!(user_of(outcome).entity_id(), id);
    assert_eq!(world.logins.len(), 0);
}

#[tokio::test]
async fn a_wrong_password_starts_no_pending_login() {
    let world = World::new();
    world.register("ada@example.com", true).await;

    let error = world
        .first_factor("ada@example.com", "battery-staple")
        .await
        .unwrap_err();

    assert!(matches!(error, AuthError::InvalidCredentials));
    assert_eq!(world.logins.len(), 0);
}
