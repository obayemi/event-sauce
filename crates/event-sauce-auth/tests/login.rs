//! Typestate logins end to end against the in-memory event store: a
//! password then a TOTP code, an app-defined three-step flow, a branching
//! flow, and what the library refuses along the way.

use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use argon2::Params;
use async_trait::async_trait;
use chrono::DateTime;
use event_sauce_auth::{
    Advance, Argon2Hasher, AuthError, Authenticated, Credentials, Done, Email, Entered,
    Fingerprint, FinishError, FirstFactor, FixedClock, HasPassword, HasTotp, Identity, Login,
    LoginId, LoginKey, LoginState, LoginStore, LoginStoreError, LoginTtl, Logins, NeedTotp,
    Password, PasswordAttempt, PasswordHasher, PasswordLogin, PendingLoginId, PendingLogins,
    Rejected, ResumeError, Route, SignUpError, Step, Totp, TotpIfEnrolled, TotpOrDone, TotpSecret,
    UserAggregate,
};
use event_sauce_core::{
    aggregate_command, define_events, Aggregate, AggregateClaim, AggregateError, AggregateRoot,
    Entity, EntityId, EventStore, Repository,
};
use event_sauce_memory::InMemoryEventStore;
use secrecy::SecretString;
use serde::{Deserialize, Serialize};

const NOW: i64 = 1_700_000_010;
const MAIL_CODE: &str = "424242";
const RECOVERY_CODE: &str = "recover-me";
const ADMIN_CODE: &str = "open-sesame";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct Secured {
    password: Password<Email>,
    totp: Option<TotpSecret>,
    admin: bool,
}

impl Credentials for Secured {
    fn claims(&self) -> Vec<AggregateClaim> {
        self.password.claims()
    }

    fn fingerprint(&self, fingerprint: &mut Fingerprint) {
        self.password.fingerprint(fingerprint);
        self.feed_totp_fingerprint(fingerprint);
        fingerprint.field(&[u8::from(self.admin)]);
    }
}

impl HasPassword for Secured {
    type Login = Email;

    fn password(&self) -> Option<&Password<Email>> {
        Some(&self.password)
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

        Touched {}
        => |_user, _event| {},

        TotpEnrolled {
            secret: TotpSecret,
        }
        => |user, event| {
            if let Identity::Registered(credentials) = &mut user.identity {
                credentials.totp = Some(event.secret.clone());
            }
        },
    }
}

aggregate_command! {
    impl User {
        @clock @init fn register(credentials: Secured) -> RegisteredEvent { credentials };
        @clock fn deactivate() -> DeactivatedEvent { };
        @clock fn touch() -> TouchedEvent { };
        @clock fn enroll_totp(secret: TotpSecret) -> TotpEnrolledEvent { secret };
    }
}

#[derive(Default)]
struct MemoryLoginStore {
    entries: Mutex<HashMap<String, Vec<u8>>>,
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

fn cheap_hasher() -> Argon2Hasher {
    Argon2Hasher::new(Params::new(8, 1, 1, None).unwrap())
}

fn secret(raw: &str) -> SecretString {
    SecretString::from(raw)
}

fn totp_secret() -> TotpSecret {
    TotpSecret::from([7_u8; 20])
}

fn current_code() -> SecretString {
    let reference = totp_rs::Builder::new()
        .with_secret(totp_secret())
        .build()
        .unwrap();
    SecretString::from(reference.generate(u64::try_from(NOW).unwrap()).to_string())
}

async fn credentials(login: &str, totp: Option<TotpSecret>, admin: bool) -> Secured {
    let hash = cheap_hasher().hash(&secret("correct-horse")).await.unwrap();
    Secured {
        password: Password::new(Email::parse(login).unwrap(), hash),
        totp,
        admin,
    }
}

fn attempt(login: &str, password: &str) -> PasswordAttempt<Email> {
    PasswordAttempt::new(Email::parse(login).unwrap(), secret(password))
}

type Factor<R> = FirstFactor<PasswordLogin<Argon2Hasher>, R>;

fn password_then<R: Route<User>>(route: R) -> Factor<R> {
    FirstFactor::new(PasswordLogin::new(cheap_hasher()), route)
}

struct World {
    store: Arc<InMemoryEventStore>,
    login_store: Arc<MemoryLoginStore>,
    logins: Logins<User>,
    pending: PendingLogins<User>,
    totp: Totp,
}

impl World {
    fn new() -> Self {
        let store = Arc::new(InMemoryEventStore::new());
        let login_store = Arc::new(MemoryLoginStore::default());
        let logins = Logins::<User>::new(store.clone());
        let pending = logins.pending(login_store.clone());
        let clock = Arc::new(FixedClock(DateTime::from_timestamp(NOW, 0).unwrap()));
        Self {
            store,
            login_store: login_store.clone(),
            logins: logins.with_clock(clock.clone()),
            pending,
            totp: Totp::new(login_store).with_clock(clock),
        }
    }

    async fn register(&self, credentials: Secured) -> EntityId {
        let mut user = User::register(credentials).unwrap();
        let id = user.entity_id();
        self.store
            .repository::<User>()
            .save(&mut user)
            .await
            .unwrap();
        id
    }

    async fn modify<F>(&self, id: EntityId, change: F)
    where
        F: FnOnce(&mut AggregateRoot<User>) -> Result<(), UserError> + Send,
    {
        self.store
            .repository::<User>()
            .modify(id, change)
            .await
            .unwrap();
    }

    async fn done_login(&self) -> Login<User, Done> {
        match self
            .logins
            .begin(
                &password_then(TotpIfEnrolled),
                attempt("ada@example.com", "correct-horse"),
            )
            .await
            .unwrap()
        {
            TotpOrDone::Done(login) => login,
            TotpOrDone::Totp(_) => panic!("a user without a secret has nothing left to prove"),
        }
    }

    async fn ada(&self, totp: Option<TotpSecret>) -> EntityId {
        self.register(credentials("ada@example.com", totp, false).await)
            .await
    }
}

fn assert_logged_in(authenticated: &Authenticated<User>, id: EntityId) {
    assert_eq!(authenticated.user().entity_id(), id);
}

async fn totp_login(world: &World) -> Login<User, NeedTotp> {
    match world
        .logins
        .begin(
            &password_then(TotpIfEnrolled),
            attempt("ada@example.com", "correct-horse"),
        )
        .await
        .unwrap()
    {
        TotpOrDone::Totp(login) => login,
        TotpOrDone::Done(_) => panic!("an enrolled user must be asked for a code"),
    }
}

#[tokio::test]
async fn a_password_alone_logs_in_a_user_without_a_second_factor() {
    let world = World::new();
    let id = world.ada(None).await;

    let outcome = world
        .logins
        .begin(
            &password_then(TotpIfEnrolled),
            attempt("ada@example.com", "correct-horse"),
        )
        .await
        .unwrap();

    let TotpOrDone::Done(login) = outcome else {
        panic!("a user without a secret has nothing left to prove");
    };
    assert_logged_in(&login.finish().await.unwrap(), id);
}

#[tokio::test]
async fn a_user_with_a_totp_secret_is_held_at_the_code() {
    let world = World::new();
    let id = world.ada(Some(totp_secret())).await;

    let login = totp_login(&world).await;

    assert_eq!(login.user().entity_id(), id);
}

#[tokio::test]
async fn password_then_totp_logs_in_across_two_requests() {
    let world = World::new();
    let id = world.ada(Some(totp_secret())).await;
    let pending_id = world.pending.save(totp_login(&world).await).await.unwrap();

    let login = world.pending.resume::<NeedTotp>(pending_id).await.unwrap();
    let done = login.prove(&world.totp, current_code()).await.unwrap();

    assert_logged_in(&done.finish().await.unwrap(), id);
}

#[tokio::test]
async fn a_wrong_code_ends_the_login() {
    let world = World::new();
    world.ada(Some(totp_secret())).await;
    let pending_id = world.pending.save(totp_login(&world).await).await.unwrap();
    let login = world.pending.resume::<NeedTotp>(pending_id).await.unwrap();

    let rejected = login
        .prove(&world.totp, secret("000000"))
        .await
        .unwrap_err();
    let again = world.pending.resume::<NeedTotp>(pending_id).await;

    assert!(matches!(rejected, Rejected::Mismatch));
    assert!(matches!(again, Err(ResumeError::NotInProgress)));
}

#[tokio::test]
async fn a_code_cannot_be_replayed_by_another_login() {
    let world = World::new();
    world.ada(Some(totp_secret())).await;
    let first = totp_login(&world).await;
    let second = totp_login(&world).await;

    first.prove(&world.totp, current_code()).await.unwrap();
    let replay = second.prove(&world.totp, current_code()).await.unwrap_err();

    assert!(matches!(replay, Rejected::Mismatch));
}

struct AlwaysTotp;

impl Route<User> for AlwaysTotp {
    type Next = Login<User, NeedTotp>;

    fn route(&self, entered: Advance<User, Entered>) -> Login<User, NeedTotp> {
        entered.to(NeedTotp)
    }
}

#[tokio::test]
async fn a_totp_step_for_a_user_without_a_secret_rejects_every_code() {
    let world = World::new();
    world.ada(None).await;
    let login = world
        .logins
        .begin(
            &password_then(AlwaysTotp),
            attempt("ada@example.com", "correct-horse"),
        )
        .await
        .unwrap();

    let rejected = login.prove(&world.totp, current_code()).await.unwrap_err();

    assert!(matches!(rejected, Rejected::Mismatch));
}

#[tokio::test]
async fn a_wrong_password_starts_no_login() {
    let world = World::new();
    world.ada(Some(totp_secret())).await;

    let error = world
        .logins
        .begin(
            &password_then(TotpIfEnrolled),
            attempt("ada@example.com", "battery-staple"),
        )
        .await
        .unwrap_err();

    assert!(matches!(error, AuthError::InvalidCredentials));
    assert!(world.login_store.entries.lock().unwrap().is_empty());
}

#[tokio::test]
async fn a_pending_login_resumes_only_once() {
    let world = World::new();
    world.ada(Some(totp_secret())).await;
    let id = world.pending.save(totp_login(&world).await).await.unwrap();
    world.pending.resume::<NeedTotp>(id).await.unwrap();

    let again = world.pending.resume::<NeedTotp>(id).await;

    assert!(matches!(again, Err(ResumeError::NotInProgress)));
}

#[tokio::test]
async fn an_unknown_pending_login_is_not_in_progress() {
    let world = World::new();

    let missing = world
        .pending
        .resume::<NeedTotp>(PendingLoginId::new())
        .await;

    assert!(matches!(missing, Err(ResumeError::NotInProgress)));
}

#[derive(Debug, Serialize, Deserialize)]
struct NeedSecond;

impl LoginState for NeedSecond {
    const TAG: &'static str = "app.second";
}

#[derive(Debug, Serialize, Deserialize)]
struct NeedEmailCode {
    code: String,
}

impl LoginState for NeedEmailCode {
    const TAG: &'static str = "app.email_code";
    const TTL: LoginTtl = LoginTtl::ONE_HOUR;
}

enum SecondProof {
    Totp(SecretString),
    Recovery(String),
}

struct SecondVerifier {
    totp: Totp,
    mailbox: Mutex<Vec<String>>,
}

#[async_trait]
impl Step<User> for NeedSecond {
    type Proof = SecondProof;
    type Verifier = SecondVerifier;
    type Next = Login<User, NeedEmailCode>;

    async fn verify(
        advance: Advance<User, Self>,
        verifier: &SecondVerifier,
        proof: SecondProof,
    ) -> Result<Login<User, NeedEmailCode>, Rejected> {
        let credentials = advance
            .login()
            .user()
            .identity()
            .credentials()
            .ok_or(Rejected::Mismatch)?;
        match proof {
            SecondProof::Totp(code) => {
                let secret = credentials.totp.as_ref().ok_or(Rejected::Mismatch)?;
                verifier.totp.check(secret, &code).await?;
            }
            SecondProof::Recovery(code) if code == RECOVERY_CODE => {}
            SecondProof::Recovery(_) => return Err(Rejected::Mismatch),
        }
        verifier.mailbox.lock().unwrap().push(MAIL_CODE.to_owned());
        Ok(advance.to(NeedEmailCode {
            code: MAIL_CODE.to_owned(),
        }))
    }
}

#[async_trait]
impl Step<User> for NeedEmailCode {
    type Proof = String;
    type Verifier = ();
    type Next = Login<User, Done>;

    async fn verify(
        advance: Advance<User, Self>,
        (): &(),
        code: String,
    ) -> Result<Login<User, Done>, Rejected> {
        if code == advance.login().state().code {
            Ok(advance.to(Done))
        } else {
            Err(Rejected::Mismatch)
        }
    }
}

enum AfterPassword {
    Done(Login<User, Done>),
    Second(Login<User, NeedSecond>),
}

struct ByEnrolment;

impl Route<User> for ByEnrolment {
    type Next = AfterPassword;

    fn route(&self, entered: Advance<User, Entered>) -> AfterPassword {
        let enrolled = entered
            .login()
            .user()
            .identity()
            .credentials()
            .is_some_and(|credentials| credentials.totp.is_some());
        if enrolled {
            AfterPassword::Second(entered.to(NeedSecond))
        } else {
            AfterPassword::Done(entered.to(Done))
        }
    }
}

fn second_verifier(world: &World) -> SecondVerifier {
    SecondVerifier {
        totp: world.totp.clone(),
        mailbox: Mutex::default(),
    }
}

async fn three_step_login(
    world: &World,
    verifier: &SecondVerifier,
    proof: SecondProof,
    email_code: &str,
) -> Result<Authenticated<User>, Box<dyn std::error::Error>> {
    let AfterPassword::Second(login) = world
        .logins
        .begin(
            &password_then(ByEnrolment),
            attempt("ada@example.com", "correct-horse"),
        )
        .await?
    else {
        panic!("an enrolled user must be asked for a second factor");
    };
    let id = world.pending.save(login).await?;

    let second = world.pending.resume::<NeedSecond>(id).await?;
    let email = second.prove(verifier, proof).await?;
    let id = world.pending.save(email).await?;

    let email = world.pending.resume::<NeedEmailCode>(id).await?;
    let done = email.prove(&(), email_code.to_owned()).await?;
    Ok(done.finish().await?)
}

#[tokio::test]
async fn a_three_step_flow_logs_in_after_a_totp_code_and_an_email_code() {
    let world = World::new();
    let id = world.ada(Some(totp_secret())).await;
    let verifier = second_verifier(&world);

    let authenticated = three_step_login(
        &world,
        &verifier,
        SecondProof::Totp(current_code()),
        MAIL_CODE,
    )
    .await
    .unwrap();

    assert_logged_in(&authenticated, id);
    assert_eq!(*verifier.mailbox.lock().unwrap(), [MAIL_CODE]);
}

#[tokio::test]
async fn a_three_step_flow_accepts_a_recovery_code_as_the_second_factor() {
    let world = World::new();
    let id = world.ada(Some(totp_secret())).await;

    let authenticated = three_step_login(
        &world,
        &second_verifier(&world),
        SecondProof::Recovery(RECOVERY_CODE.to_owned()),
        MAIL_CODE,
    )
    .await
    .unwrap();

    assert_logged_in(&authenticated, id);
}

#[tokio::test]
async fn a_three_step_flow_does_not_finish_with_a_wrong_email_code() {
    let world = World::new();
    world.ada(Some(totp_secret())).await;

    let error = three_step_login(
        &world,
        &second_verifier(&world),
        SecondProof::Totp(current_code()),
        "000000",
    )
    .await
    .unwrap_err();

    assert!(error.downcast_ref::<Rejected>().is_some());
}

#[tokio::test]
async fn a_three_step_flow_sends_no_email_for_a_wrong_second_factor() {
    let world = World::new();
    world.ada(Some(totp_secret())).await;
    let verifier = second_verifier(&world);

    let error = three_step_login(
        &world,
        &verifier,
        SecondProof::Recovery("guess".into()),
        MAIL_CODE,
    )
    .await
    .unwrap_err();

    assert!(error.downcast_ref::<Rejected>().is_some());
    assert!(verifier.mailbox.lock().unwrap().is_empty());
}

#[tokio::test]
async fn a_three_step_flow_lets_a_user_without_a_second_factor_stop_after_the_password() {
    let world = World::new();
    let id = world.ada(None).await;

    let outcome = world
        .logins
        .begin(
            &password_then(ByEnrolment),
            attempt("ada@example.com", "correct-horse"),
        )
        .await
        .unwrap();

    let AfterPassword::Done(login) = outcome else {
        panic!("a user without a second factor is done after the password");
    };
    assert_logged_in(&login.finish().await.unwrap(), id);
}

#[tokio::test]
async fn a_login_saved_at_one_step_does_not_resume_at_another() {
    let world = World::new();
    world.ada(Some(totp_secret())).await;
    let AfterPassword::Second(login) = world
        .logins
        .begin(
            &password_then(ByEnrolment),
            attempt("ada@example.com", "correct-horse"),
        )
        .await
        .unwrap()
    else {
        panic!("an enrolled user must be asked for a second factor");
    };
    let id = world.pending.save(login).await.unwrap();

    let wrong_step = world.pending.resume::<NeedEmailCode>(id).await;
    let right_step = world.pending.resume::<NeedSecond>(id).await;

    assert!(matches!(wrong_step, Err(ResumeError::NotInProgress)));
    assert!(matches!(right_step, Err(ResumeError::NotInProgress)));
}

#[derive(Debug, Serialize, Deserialize)]
struct NeedAdminCode;

impl LoginState for NeedAdminCode {
    const TAG: &'static str = "app.admin_code";
}

#[async_trait]
impl Step<User> for NeedAdminCode {
    type Proof = String;
    type Verifier = ();
    type Next = Login<User, Done>;

    async fn verify(
        advance: Advance<User, Self>,
        (): &(),
        code: String,
    ) -> Result<Login<User, Done>, Rejected> {
        if code == ADMIN_CODE {
            Ok(advance.to(Done))
        } else {
            Err(Rejected::Mismatch)
        }
    }
}

enum Branch {
    Admin(Login<User, NeedAdminCode>),
    Totp(Login<User, NeedTotp>),
    Done(Login<User, Done>),
}

struct ByProfile;

impl Route<User> for ByProfile {
    type Next = Branch;

    fn route(&self, entered: Advance<User, Entered>) -> Branch {
        let credentials = entered.login().user().identity().credentials();
        if credentials.is_some_and(|credentials| credentials.admin) {
            Branch::Admin(entered.to(NeedAdminCode))
        } else if credentials.is_some_and(|credentials| credentials.totp.is_some()) {
            Branch::Totp(entered.to(NeedTotp))
        } else {
            Branch::Done(entered.to(Done))
        }
    }
}

async fn branch_of(world: &World, login: &str) -> Branch {
    world
        .logins
        .begin(&password_then(ByProfile), attempt(login, "correct-horse"))
        .await
        .unwrap()
}

#[tokio::test]
async fn a_branching_flow_sends_each_user_down_its_own_path() {
    let world = World::new();
    world
        .register(credentials("admin@example.com", None, true).await)
        .await;
    world
        .register(credentials("totp@example.com", Some(totp_secret()), false).await)
        .await;
    world
        .register(credentials("plain@example.com", None, false).await)
        .await;

    let Branch::Admin(admin) = branch_of(&world, "admin@example.com").await else {
        panic!("an admin is asked for the admin code");
    };
    let Branch::Totp(enrolled) = branch_of(&world, "totp@example.com").await else {
        panic!("an enrolled user is asked for a TOTP code");
    };
    let Branch::Done(plain) = branch_of(&world, "plain@example.com").await else {
        panic!("a plain user is done after the password");
    };

    assert_eq!(
        admin
            .user()
            .identity()
            .credentials()
            .unwrap()
            .password
            .login()
            .as_str(),
        "admin@example.com"
    );
    assert_eq!(
        enrolled
            .user()
            .identity()
            .credentials()
            .unwrap()
            .password
            .login()
            .as_str(),
        "totp@example.com"
    );
    assert!(plain.finish().await.is_ok());
}

#[tokio::test]
async fn each_branch_finishes_only_through_its_own_step() {
    let world = World::new();
    let admin_id = world
        .register(credentials("admin@example.com", None, true).await)
        .await;
    let Branch::Admin(login) = branch_of(&world, "admin@example.com").await else {
        panic!("an admin is asked for the admin code");
    };

    let Branch::Admin(wrong) = branch_of(&world, "admin@example.com").await else {
        panic!("an admin is asked for the admin code");
    };
    let wrong = wrong.prove(&(), "guess".into()).await.unwrap_err();
    let done = login.prove(&(), ADMIN_CODE.into()).await.unwrap();

    assert!(matches!(wrong, Rejected::Mismatch));
    assert_logged_in(&done.finish().await.unwrap(), admin_id);
}

#[tokio::test]
async fn a_user_deactivated_mid_login_fails_at_finish() {
    let world = World::new();
    let id = world.ada(Some(totp_secret())).await;
    let done = totp_login(&world)
        .await
        .prove(&world.totp, current_code())
        .await
        .unwrap();
    world.modify(id, |user| user.deactivate()).await;

    let finished = done.finish().await;

    assert!(matches!(finished, Err(FinishError::Inactive)));
}

#[tokio::test]
async fn a_user_deactivated_while_the_client_is_away_cannot_resume() {
    let world = World::new();
    let id = world.ada(Some(totp_secret())).await;
    let pending_id = world.pending.save(totp_login(&world).await).await.unwrap();
    world.modify(id, |user| user.deactivate()).await;

    let resumed = world.pending.resume::<NeedTotp>(pending_id).await;

    assert!(matches!(resumed, Err(ResumeError::Inactive)));
}

#[tokio::test]
async fn credentials_changed_mid_login_fail_at_finish() {
    let world = World::new();
    let id = world.ada(Some(totp_secret())).await;
    let done = totp_login(&world)
        .await
        .prove(&world.totp, current_code())
        .await
        .unwrap();
    world
        .modify(id, |user| user.enroll_totp(TotpSecret::from([9_u8; 20])))
        .await;

    let finished = done.finish().await;

    assert!(matches!(finished, Err(FinishError::CredentialsChanged)));
}

#[tokio::test]
async fn credentials_changed_while_the_client_is_away_cannot_resume() {
    let world = World::new();
    let id = world.ada(Some(totp_secret())).await;
    let pending_id = world.pending.save(totp_login(&world).await).await.unwrap();
    world
        .modify(id, |user| user.enroll_totp(TotpSecret::from([9_u8; 20])))
        .await;

    let resumed = world.pending.resume::<NeedTotp>(pending_id).await;

    assert!(matches!(resumed, Err(ResumeError::CredentialsChanged)));
}

#[tokio::test]
async fn a_change_unrelated_to_the_credentials_does_not_end_a_login() {
    let world = World::new();
    let id = world.ada(None).await;
    let login = world.done_login().await;
    world.modify(id, |user| user.touch()).await;

    let authenticated = login.finish().await.unwrap();

    assert_logged_in(&authenticated, id);
    assert_eq!(authenticated.user().version().as_i64(), 2);
}

#[tokio::test]
async fn signing_up_commits_the_user_and_logs_it_in() {
    let world = World::new();
    let user = User::register(credentials("eve@example.com", None, false).await).unwrap();
    let id = user.entity_id();

    let authenticated = world
        .logins
        .sign_up(user)
        .await
        .unwrap()
        .finish()
        .await
        .unwrap();

    assert_logged_in(&authenticated, id);
    assert!(world
        .store
        .repository::<User>()
        .exists_by_id(id)
        .await
        .unwrap());
}

#[tokio::test]
async fn signing_up_an_aggregate_the_store_already_holds_is_refused() {
    let world = World::new();
    let id = world.ada(None).await;
    let mut loaded = world
        .store
        .repository::<User>()
        .load_by_id(id)
        .await
        .unwrap();
    loaded.deactivate().unwrap();

    let error = world.logins.sign_up(loaded).await.unwrap_err();

    assert!(matches!(error, SignUpError::NotNew));
}

#[tokio::test]
async fn signing_up_with_a_login_somebody_holds_is_a_claim_conflict() {
    let world = World::new();
    world.ada(None).await;
    let duplicate = User::register(credentials("ada@example.com", None, false).await).unwrap();

    let error = world.logins.sign_up(duplicate).await.unwrap_err();

    match error {
        SignUpError::Store(error) => assert!(error.is_claim_conflict()),
        other => panic!("expected a claim conflict, got {other:?}"),
    }
}
