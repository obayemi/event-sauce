//! The first factor of a login against the in-memory event store and a real
//! user aggregate whose claims come from its credentials.

use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};

use argon2::Params;
use async_trait::async_trait;
use event_sauce_auth::{
    AlwaysDone, Argon2Hasher, AuthError, ClaimLocator, Credentials, Done, Email, FederatedId,
    FederatedLogin, FirstFactor, HashError, Identity, LocateError, Login, LoginId, LoginMethod,
    Logins, Password, PasswordAttempt, PasswordHash, PasswordHasher, PasswordLogin,
    PasswordOrFederated, PasswordOrFederatedAttempt, PasswordOrFederatedLogin, Provider, Rejected,
    UserAggregate, VerifiedFederated,
};
use event_sauce_core::{
    aggregate_command, define_events, Aggregate, AggregateClaim, AggregateError, Entity, EntityId,
    EventStore, Repository,
};
use event_sauce_memory::InMemoryEventStore;
use secrecy::SecretString;
use serde::{Deserialize, Serialize};

type UserCredentials = PasswordOrFederated<Email>;

#[derive(Debug, thiserror::Error)]
enum UserError {}

impl AggregateError for UserError {}

#[derive(Debug, Serialize, Deserialize)]
struct User {
    id: EntityId,
    identity: Identity<UserCredentials>,
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
    type Credentials = UserCredentials;

    fn identity(&self) -> &Identity<UserCredentials> {
        &self.identity
    }

    fn is_active(&self) -> bool {
        self.active
    }
}

define_events! {
    enum UserEvent for User {
        Registered {
            credentials: UserCredentials,
        }
        @init
        => |id, event| {
            User {
                id,
                identity: Identity::Registered(event.credentials.clone()),
                active: true,
            }
        },

        Started {}
        @init
        => |id, _event| {
            User { id, identity: Identity::Anonymous, active: true }
        },

        Deactivated {}
        => |user, _event| {
            user.active = false;
        },
    }
}

aggregate_command! {
    impl User {
        @clock @init fn register(credentials: UserCredentials) -> RegisteredEvent { credentials };
        @clock @init fn start() -> StartedEvent { };
        @clock fn deactivate() -> DeactivatedEvent { };
    }
}

fn cheap_hasher() -> Argon2Hasher {
    Argon2Hasher::new(Params::new(8, 1, 1, None).unwrap())
}

fn email(raw: &str) -> Email {
    Email::parse(raw).unwrap()
}

fn secret(raw: &str) -> SecretString {
    SecretString::from(raw)
}

fn google(subject: &str) -> FederatedId {
    FederatedId::new(Provider::GOOGLE, subject)
}

async fn password_credentials(login: &str, password: &str) -> UserCredentials {
    let hash = cheap_hasher().hash(&secret(password)).await.unwrap();
    PasswordOrFederated::Password(Password::new(email(login), hash))
}

async fn both_credentials(login: &str, password: &str, subject: &str) -> UserCredentials {
    let mut credentials = password_credentials(login, password).await;
    credentials.link(google(subject)).unwrap();
    credentials
}

fn password_attempt(login: &str, password: &str) -> PasswordOrFederatedAttempt<Email> {
    PasswordOrFederatedAttempt::Password(PasswordAttempt::new(email(login), secret(password)))
}

fn federated_attempt(subject: &str) -> PasswordOrFederatedAttempt<Email> {
    PasswordOrFederatedAttempt::Federated(VerifiedFederated {
        id: google(subject),
        email: None,
    })
}

struct World {
    store: Arc<InMemoryEventStore>,
}

impl World {
    fn new() -> Self {
        Self {
            store: Arc::new(InMemoryEventStore::new()),
        }
    }

    async fn register(&self, credentials: UserCredentials) -> EntityId {
        let mut user = User::register(credentials).unwrap();
        let id = user.entity_id();
        self.store
            .repository::<User>()
            .save(&mut user)
            .await
            .unwrap();
        id
    }

    async fn start_anonymous(&self) -> EntityId {
        let mut user = User::start().unwrap();
        let id = user.entity_id();
        self.store
            .repository::<User>()
            .save(&mut user)
            .await
            .unwrap();
        id
    }

    async fn deactivate(&self, id: EntityId) {
        self.store
            .repository::<User>()
            .modify(id, |user| user.deactivate())
            .await
            .unwrap();
    }

    async fn begin<M>(
        &self,
        method: M,
        attempt: M::Attempt,
    ) -> Result<Login<User, Done>, AuthError<M::Attempt>>
    where
        M: LoginMethod<UserCredentials>,
    {
        Logins::<User>::new(self.store.clone())
            .begin(&FirstFactor::new(method, AlwaysDone), attempt)
            .await
    }

    async fn combined(
        &self,
        attempt: PasswordOrFederatedAttempt<Email>,
    ) -> Result<Login<User, Done>, AuthError<PasswordOrFederatedAttempt<Email>>> {
        self.begin(PasswordOrFederatedLogin::new(cheap_hasher()), attempt)
            .await
    }
}

#[tokio::test]
async fn the_right_password_logs_in_the_user_that_holds_the_login() {
    let world = World::new();
    let id = world
        .register(password_credentials("ada@example.com", "correct-horse").await)
        .await;
    world
        .register(password_credentials("eve@example.com", "battery-staple").await)
        .await;

    let login = world
        .combined(password_attempt("ADA@example.com", "correct-horse"))
        .await
        .unwrap();

    assert_eq!(login.user().entity_id(), id);
    assert_eq!(login.user().version().as_i64(), 1);
}

#[tokio::test]
async fn the_wrong_password_is_invalid_credentials() {
    let world = World::new();
    world
        .register(password_credentials("ada@example.com", "correct-horse").await)
        .await;

    let error = world
        .combined(password_attempt("ada@example.com", "battery-staple"))
        .await
        .unwrap_err();

    assert!(matches!(error, AuthError::InvalidCredentials));
}

#[tokio::test]
async fn an_unknown_login_hands_the_attempt_back() {
    let world = World::new();
    world
        .register(password_credentials("ada@example.com", "correct-horse").await)
        .await;

    let error = world
        .combined(password_attempt("nobody@example.com", "correct-horse"))
        .await
        .unwrap_err();

    match error {
        AuthError::Unknown {
            attempt: PasswordOrFederatedAttempt::Password(attempt),
        } => assert_eq!(attempt.login, email("nobody@example.com")),
        other => panic!("expected an unknown login, got {other:?}"),
    }
}

#[tokio::test]
async fn a_linked_federated_identity_logs_in_the_user() {
    let world = World::new();
    let id = world
        .register(both_credentials("ada@example.com", "correct-horse", "g-1").await)
        .await;

    let login = world.combined(federated_attempt("g-1")).await.unwrap();

    assert_eq!(login.user().entity_id(), id);
}

#[tokio::test]
async fn an_unlinked_federated_identity_is_unknown() {
    let world = World::new();
    world
        .register(both_credentials("ada@example.com", "correct-horse", "g-1").await)
        .await;

    let error = world.combined(federated_attempt("g-2")).await.unwrap_err();

    assert!(matches!(
        error,
        AuthError::Unknown {
            attempt: PasswordOrFederatedAttempt::Federated(_)
        }
    ));
}

#[tokio::test]
async fn a_password_only_app_logs_in_with_password_login_alone() {
    let world = World::new();
    let id = world
        .register(password_credentials("ada@example.com", "correct-horse").await)
        .await;

    let login = world
        .begin(
            PasswordLogin::new(cheap_hasher()),
            PasswordAttempt::new(email("ada@example.com"), secret("correct-horse")),
        )
        .await
        .unwrap();

    assert_eq!(login.user().entity_id(), id);
}

#[tokio::test]
async fn a_federated_only_app_logs_in_with_federated_login_alone() {
    let world = World::new();
    let id = world
        .register(both_credentials("ada@example.com", "pw", "g-1").await)
        .await;

    let login = world
        .begin(
            FederatedLogin,
            VerifiedFederated {
                id: google("g-1"),
                email: None,
            },
        )
        .await
        .unwrap();

    assert_eq!(login.user().entity_id(), id);
}

#[tokio::test]
async fn an_inactive_user_is_refused_by_every_method() {
    let world = World::new();
    let id = world
        .register(both_credentials("ada@example.com", "correct-horse", "g-1").await)
        .await;
    world.deactivate(id).await;
    let spy = Spy::locating(id).accepting_anyone();

    let by_password = world
        .combined(password_attempt("ada@example.com", "correct-horse"))
        .await
        .unwrap_err();
    let by_federation = world.combined(federated_attempt("g-1")).await.unwrap_err();
    let by_spy = world.begin(spy.clone(), ()).await.unwrap_err();

    assert!(matches!(by_password, AuthError::Inactive));
    assert!(matches!(by_federation, AuthError::Inactive));
    assert!(matches!(by_spy, AuthError::Inactive));
    assert_eq!(spy.verifications(), 0);
}

#[tokio::test]
async fn an_anonymous_user_logs_in_only_with_a_method_that_allows_it() {
    let world = World::new();
    let id = world.start_anonymous().await;

    let allowed = world
        .begin(Spy::locating(id).accepting_anyone(), ())
        .await
        .unwrap();
    let refused = world.begin(Spy::locating(id), ()).await.unwrap_err();

    assert_eq!(allowed.user().entity_id(), id);
    assert!(matches!(refused, AuthError::InvalidCredentials));
}

#[tokio::test]
async fn nobody_found_by_a_method_runs_its_unknown_hook_once() {
    let world = World::new();
    let spy = Spy::locating_nobody();

    let error = world.begin(spy.clone(), ()).await.unwrap_err();

    assert!(matches!(error, AuthError::Unknown { attempt: () }));
    assert_eq!(spy.unknowns(), 1);
    assert_eq!(spy.verifications(), 0);
}

#[tokio::test]
async fn a_located_user_that_does_not_exist_is_unknown() {
    let world = World::new();
    let spy = Spy::locating(EntityId::new());

    let error = world.begin(spy.clone(), ()).await.unwrap_err();

    assert!(matches!(error, AuthError::Unknown { attempt: () }));
    assert_eq!(spy.unknowns(), 1);
}

#[tokio::test]
async fn a_hasher_failure_is_a_hash_error() {
    let world = World::new();
    world
        .register(password_credentials("ada@example.com", "correct-horse").await)
        .await;

    let error = world
        .begin(
            PasswordLogin::new(BrokenHasher),
            PasswordAttempt::new(email("ada@example.com"), secret("correct-horse")),
        )
        .await
        .unwrap_err();

    assert!(matches!(error, AuthError::Hash(HashError(_))));
}

struct BrokenHasher;

#[async_trait]
impl PasswordHasher for BrokenHasher {
    async fn hash(&self, _: &SecretString) -> Result<PasswordHash, HashError> {
        Err(HashError("broken".into()))
    }

    async fn verify(&self, _: &SecretString, _: &PasswordHash) -> Result<bool, HashError> {
        Err(HashError("broken".into()))
    }
}

#[tokio::test]
async fn a_login_is_unique_across_users() {
    let world = World::new();
    world
        .register(password_credentials("ada@example.com", "correct-horse").await)
        .await;

    let mut duplicate =
        User::register(password_credentials("ADA@example.com", "other").await).unwrap();
    let error = world
        .store
        .repository::<User>()
        .save(&mut duplicate)
        .await
        .unwrap_err();

    assert!(error.is_claim_conflict());
}

#[tokio::test]
async fn the_store_resolves_the_claims_the_credentials_declare() {
    let world = World::new();
    let id = world
        .register(both_credentials("ada@example.com", "correct-horse", "g-1").await)
        .await;

    let by_login = world
        .store
        .locate_claim(&Password::claim_for(&email("ada@example.com")))
        .await
        .unwrap();
    let by_subject = world
        .store
        .locate_claim(&google("g-1").claim())
        .await
        .unwrap();
    let missing = world
        .store
        .locate_claim(&google("g-2").claim())
        .await
        .unwrap();

    assert_eq!((by_login, by_subject, missing), (Some(id), Some(id), None));
}

/// A delegated method: it finds its user on its own, without a claim.
#[derive(Clone)]
struct Spy {
    target: Option<EntityId>,
    accepts_anyone: bool,
    verified: Arc<AtomicUsize>,
    unknown: Arc<AtomicUsize>,
}

impl Spy {
    fn locating(target: EntityId) -> Self {
        Self {
            target: Some(target),
            accepts_anyone: false,
            verified: Arc::default(),
            unknown: Arc::default(),
        }
    }

    fn locating_nobody() -> Self {
        Self {
            target: None,
            ..Self::locating(EntityId::new())
        }
    }

    fn accepting_anyone(mut self) -> Self {
        self.accepts_anyone = true;
        self
    }

    fn verifications(&self) -> usize {
        self.verified.load(Ordering::SeqCst)
    }

    fn unknowns(&self) -> usize {
        self.unknown.load(Ordering::SeqCst)
    }
}

#[async_trait]
impl LoginMethod<UserCredentials> for Spy {
    type Attempt = ();

    async fn locate(&self, _: &dyn ClaimLocator, _: &()) -> Result<Option<EntityId>, LocateError> {
        Ok(self.target)
    }

    async fn verify(&self, _: &Identity<UserCredentials>, _: &()) -> Result<(), Rejected> {
        self.verified.fetch_add(1, Ordering::SeqCst);
        if self.accepts_anyone {
            Ok(())
        } else {
            Err(Rejected::Mismatch)
        }
    }

    async fn on_unknown(&self, _: &()) {
        self.unknown.fetch_add(1, Ordering::SeqCst);
    }
}

#[tokio::test]
async fn a_single_factor_login_finishes_into_an_authenticated_user() {
    let world = World::new();
    let id = world
        .register(password_credentials("ada@example.com", "correct-horse").await)
        .await;

    let authenticated = world
        .combined(password_attempt("ada@example.com", "correct-horse"))
        .await
        .unwrap()
        .finish()
        .await
        .unwrap();

    assert_eq!(authenticated.user().entity_id(), id);
}

#[cfg(feature = "moka")]
#[tokio::test]
async fn a_shared_cache_serves_the_user_a_login_begins_with() {
    use event_sauce_auth::{MokaUserCache, UserCache};
    use event_sauce_core::{AggregateRoot, StoredVersion};

    let world = World::new();
    let id = world
        .register(password_credentials("ada@example.com", "stored-password").await)
        .await;
    let cache = Arc::new(MokaUserCache::<User>::new(10));
    let cached_copy = User {
        id,
        identity: Identity::Registered(
            password_credentials("ada@example.com", "cached-password").await,
        ),
        active: true,
    };
    cache
        .put(Arc::new(
            AggregateRoot::restore(StoredVersion::FIRST, cached_copy).unwrap(),
        ))
        .await
        .unwrap();
    let logins = Logins::<User>::new(world.store.clone()).with_shared_cache(cache);
    let first = FirstFactor::new(PasswordOrFederatedLogin::new(cheap_hasher()), AlwaysDone);

    let served = logins
        .begin(
            &first,
            password_attempt("ada@example.com", "cached-password"),
        )
        .await;

    assert_eq!(served.unwrap().user().entity_id(), id);
}
