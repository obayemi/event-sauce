//! The password reset flow, through a real axum router, with tower-sessions'
//! memory store, the in-memory event store, an in-memory reset record and a
//! mailer that reports what it sends.

mod secured_user;
mod support;

use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};

use async_trait::async_trait;
use axum::{
    extract::{FromRef, Path, State},
    http::{header::LOCATION, StatusCode},
    response::{IntoResponse, Redirect, Response},
    routing::{get, post},
    Json, Router,
};
use chrono::{DateTime, TimeDelta, Utc};
use event_sauce_auth::{
    Advance, CachedUserLoader, Clock, Done, Email, Entered, FirstFactor, HashError, Login, LoginId,
    Logins, Password, PasswordAttempt, PasswordHash, PasswordHasher, PasswordLogin, ResetToken,
    ResetTokenHash, Route, TotpIfEnrolled, TotpOrDone, TotpSecret,
};
use event_sauce_axum::{
    AuthLayer, AuthSession, Backoff, ClientAddr, ClientIpSource, CurrentUser, Finished, LoginError,
    LoginThrottles, NewPasswordError, Pages, PasswordReset, PendingReset, PublicOrigin,
    ResetAccounts, ResetMailer, ResetPages, Throttled,
};
use event_sauce_core::{EntityId, EventStore};
use event_sauce_memory::InMemoryEventStore;
use secrecy::SecretString;
use secured_user::{
    change_password, cheap_hasher, disable_totp, enroll_totp, register, totp_of, User, ADA,
    PASSWORD,
};
use serde::Deserialize;
use serde_json::json;
use support::Client;
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};
use tower_sessions::{MemoryStore, SessionManagerLayer};

const ORIGIN: &str = "https://saxy.example";
const MAILBOX: &str = "ada@mail.example";
const NEW_PASSWORD: &str = "a-brand-new-password";

#[derive(Debug, Clone)]
struct StepClock(Arc<Mutex<DateTime<Utc>>>);

impl StepClock {
    fn new() -> Self {
        Self(Arc::new(Mutex::new(
            DateTime::from_timestamp(1_700_000_000, 0).unwrap(),
        )))
    }

    fn advance(&self, by: TimeDelta) {
        *self.0.lock().unwrap() += by;
    }
}

impl Clock for StepClock {
    fn now(&self) -> DateTime<Utc> {
        *self.0.lock().unwrap()
    }
}

#[derive(Debug, PartialEq, Eq)]
enum Step {
    Holder(String),
    Record(EntityId),
    Sent {
        draft: String,
        to: String,
        link: String,
    },
}

#[derive(Debug, thiserror::Error)]
enum AccountsError {
    #[error("refused")]
    Refused,
    #[error("down")]
    Down,
}

struct Accounts {
    events: Arc<InMemoryEventStore>,
    pending: Mutex<HashMap<EntityId, PendingReset>>,
    steps: UnboundedSender<Step>,
    refuses: Arc<AtomicBool>,
    down: Arc<AtomicBool>,
}

#[async_trait]
impl ResetAccounts for Accounts {
    type Error = AccountsError;

    async fn holder(&self, login: &str) -> Result<Option<EntityId>, AccountsError> {
        self.steps.send(Step::Holder(login.to_owned())).unwrap();
        if self.down.load(Ordering::SeqCst) {
            return Err(AccountsError::Down);
        }
        let Ok(email) = Email::parse(login) else {
            return Ok(None);
        };
        Ok(self
            .events
            .resolve_claim(&Password::<Email>::claim_for(&email))
            .await
            .unwrap())
    }

    async fn record(
        &self,
        user: EntityId,
        hash: ResetTokenHash,
        now: DateTime<Utc>,
    ) -> Result<Option<String>, AccountsError> {
        self.steps.send(Step::Record(user)).unwrap();
        if self.refuses.load(Ordering::SeqCst) {
            return Ok(None);
        }
        let expires_at = now + TimeDelta::hours(24);
        self.pending
            .lock()
            .unwrap()
            .insert(user, PendingReset { hash, expires_at });
        Ok(Some(MAILBOX.to_owned()))
    }

    async fn pending(&self, user: EntityId) -> Result<Option<PendingReset>, AccountsError> {
        if self.down.load(Ordering::SeqCst) {
            return Err(AccountsError::Down);
        }
        Ok(self.pending.lock().unwrap().get(&user).cloned())
    }

    async fn redeem(
        &self,
        user: EntityId,
        hash: ResetTokenHash,
        password: PasswordHash,
        now: DateTime<Utc>,
    ) -> Result<(), AccountsError> {
        let taken = {
            let mut pending = self.pending.lock().unwrap();
            let matches = pending
                .get(&user)
                .is_some_and(|held| held.hash == hash && now <= held.expires_at);
            matches.then(|| pending.remove(&user))
        };
        if taken.is_none() {
            return Err(AccountsError::Refused);
        }
        change_password(&self.events, user, &password).await;
        Ok(())
    }

    fn link_path(&self, user: EntityId, token: &ResetToken) -> String {
        format!("/reset/{user}/{}", token.as_str())
    }
}

struct Mailer {
    steps: UnboundedSender<Step>,
}

#[async_trait]
impl ResetMailer for Mailer {
    type Draft = String;

    async fn send(
        &self,
        draft: String,
        to: &str,
        link: &str,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.steps
            .send(Step::Sent {
                draft,
                to: to.to_owned(),
                link: link.to_owned(),
            })
            .unwrap();
        Ok(())
    }
}

#[derive(Clone)]
struct TestHasher {
    fails: Arc<AtomicBool>,
}

#[async_trait]
impl PasswordHasher for TestHasher {
    async fn hash(&self, password: &SecretString) -> Result<PasswordHash, HashError> {
        if self.fails.load(Ordering::SeqCst) {
            return Err(HashError("hasher down".to_owned()));
        }
        cheap_hasher().hash(password).await
    }

    async fn verify(
        &self,
        password: &SecretString,
        hash: &PasswordHash,
    ) -> Result<bool, HashError> {
        cheap_hasher().verify(password, hash).await
    }
}

#[derive(Clone, Copy)]
struct SignInAnyway;

impl Route<User> for SignInAnyway {
    type Next = Login<User, Done>;

    fn route(&self, entered: Advance<User, Entered>) -> Login<User, Done> {
        entered.to(Done)
    }
}

enum Failure {
    Login(LoginError),
    Throttled(Throttled),
    InvalidLink,
}

impl From<Throttled> for Failure {
    fn from(throttled: Throttled) -> Self {
        Self::Throttled(throttled)
    }
}

impl From<LoginError> for Failure {
    fn from(error: LoginError) -> Self {
        Self::Login(error)
    }
}

impl IntoResponse for Failure {
    fn into_response(self) -> Response {
        match self {
            Self::Login(error) => error.into_response(),
            Self::Throttled(throttled) => throttled.into_response(),
            Self::InvalidLink => (StatusCode::NOT_FOUND, "invalid link").into_response(),
        }
    }
}

struct Answers;

impl Pages for Answers {
    type Response = Response;
    type Error = Failure;
}

impl ResetPages for Answers {
    fn request_form(&self) -> Response {
        "request form".into_response()
    }

    fn request_sent(&self) -> Response {
        "link sent".into_response()
    }

    fn new_password_form(&self, error: Option<NewPasswordError>) -> Response {
        format!("new password form {error:?}").into_response()
    }

    fn invalid_link(&self) -> Failure {
        Failure::InvalidLink
    }

    fn signed_in(&self) -> Response {
        Redirect::to("/").into_response()
    }

    fn sign_in_again(&self) -> Response {
        Redirect::to("/login").into_response()
    }
}

type Reset<R> = PasswordReset<User, Accounts, Mailer, TestHasher, R>;
type PasswordFirst = FirstFactor<PasswordLogin<event_sauce_auth::Argon2Hasher>, TotpIfEnrolled>;

struct App<R> {
    reset: Reset<R>,
    logins: Logins<User>,
    first: Arc<PasswordFirst>,
    client_ip: ClientIpSource,
}

impl<R: Clone> Clone for App<R> {
    fn clone(&self) -> Self {
        Self {
            reset: self.reset.clone(),
            logins: self.logins.clone(),
            first: self.first.clone(),
            client_ip: self.client_ip.clone(),
        }
    }
}

impl<R> FromRef<App<R>> for ClientIpSource {
    fn from_ref(app: &App<R>) -> Self {
        app.client_ip.clone()
    }
}

#[derive(Deserialize)]
struct LoginForm {
    email: String,
    password: String,
}

async fn login<R>(
    mut auth: AuthSession<User>,
    State(app): State<App<R>>,
    Json(form): Json<LoginForm>,
) -> Result<&'static str, Failure> {
    let email = Email::parse(&form.email).map_err(|_| Failure::InvalidLink)?;
    let attempt = PasswordAttempt::new(email, SecretString::from(form.password));
    let begun = app
        .logins
        .begin(&app.first, attempt)
        .await
        .map_err(|error| Failure::Login(error.into()))?;
    match begun {
        TotpOrDone::Done(login) => {
            let _opened = auth.finish(login).await?;
            Ok("signed in")
        }
        TotpOrDone::Totp(_) => Ok("code needed"),
    }
}

async fn me(CurrentUser(user): CurrentUser<User>) -> String {
    user.entity_id().to_string()
}

async fn request_form<R>(State(app): State<App<R>>) -> Response
where
    R: Route<User> + Clone + Send + Sync + 'static,
    R::Next: Finished<User>,
{
    app.reset.form(&Answers)
}

#[derive(Deserialize)]
struct ForgotForm {
    email: String,
}

async fn ask<R>(
    State(app): State<App<R>>,
    client: ClientAddr,
    Json(form): Json<ForgotForm>,
) -> Result<Response, Failure>
where
    R: Route<User> + Clone + Send + Sync + 'static,
    R::Next: Finished<User>,
{
    let draft = format!("draft for {}", form.email);
    app.reset.request(&Answers, &client, form.email, draft)
}

async fn page<R>(
    State(app): State<App<R>>,
    Path((user, token)): Path<(EntityId, String)>,
) -> Result<Response, Failure>
where
    R: Route<User> + Clone + Send + Sync + 'static,
    R::Next: Finished<User>,
{
    app.reset.page(&Answers, user, &token).await
}

#[derive(Deserialize)]
struct NewPassword {
    password: String,
}

async fn redeem<R>(
    mut auth: AuthSession<User>,
    State(app): State<App<R>>,
    Path((user, token)): Path<(EntityId, String)>,
    Json(form): Json<NewPassword>,
) -> Result<Response, Failure>
where
    R: Route<User> + Clone + Send + Sync + 'static,
    R::Next: Finished<User>,
{
    app.reset
        .redeem(
            &Answers,
            &mut auth,
            user,
            &token,
            SecretString::from(form.password),
        )
        .await
}

fn stuck_after_one_request() -> Backoff {
    Backoff {
        free: 0,
        base: Duration::from_hours(1),
        cap: Duration::from_hours(1),
        forget_after: Duration::from_hours(1),
    }
}

struct World {
    events: Arc<InMemoryEventStore>,
    client: Client,
    steps: UnboundedReceiver<Step>,
    clock: StepClock,
    hasher_fails: Arc<AtomicBool>,
    record_refuses: Arc<AtomicBool>,
    accounts_down: Arc<AtomicBool>,
}

impl World {
    fn new() -> Self {
        Self::build(TotpIfEnrolled, LoginThrottles::default())
    }

    fn throttled(throttles: LoginThrottles) -> Self {
        Self::build(TotpIfEnrolled, throttles)
    }

    fn build<R>(route: R, throttles: LoginThrottles) -> Self
    where
        R: Route<User> + Clone + Send + Sync + 'static,
        R::Next: Finished<User>,
    {
        let events = Arc::new(InMemoryEventStore::new());
        let (steps_in, steps) = unbounded_channel();
        let clock = StepClock::new();
        let hasher_fails = Arc::new(AtomicBool::new(false));
        let record_refuses = Arc::new(AtomicBool::new(false));
        let accounts_down = Arc::new(AtomicBool::new(false));
        let logins = Logins::<User>::new(events.clone());
        let accounts = Accounts {
            events: events.clone(),
            pending: Mutex::default(),
            steps: steps_in.clone(),
            refuses: record_refuses.clone(),
            down: accounts_down.clone(),
        };
        let reset = PasswordReset::new(
            Arc::new(accounts),
            Arc::new(Mailer { steps: steps_in }),
            TestHasher {
                fails: hasher_fails.clone(),
            },
            PublicOrigin::parse(ORIGIN).unwrap(),
            logins.clone(),
            route,
            Arc::new(throttles),
        )
        .with_clock(Arc::new(clock.clone()));
        let app = App {
            reset,
            logins,
            first: Arc::new(FirstFactor::new(
                PasswordLogin::new(cheap_hasher()),
                TotpIfEnrolled,
            )),
            client_ip: ClientIpSource::default(),
        };
        let router = Router::new()
            .route("/login", post(login::<R>))
            .route("/me", get(me))
            .route("/forgot", get(request_form::<R>).post(ask::<R>))
            .route("/reset/{user}/{token}", get(page::<R>).post(redeem::<R>))
            .with_state(app);
        let router = AuthLayer::<User>::new(CachedUserLoader::new(events.clone()))
            .layer(router, SessionManagerLayer::new(MemoryStore::default()));
        Self {
            events,
            client: Client::new(router),
            steps,
            clock,
            hasher_fails,
            record_refuses,
            accounts_down,
        }
    }

    async fn register(&self, totp: Option<&TotpSecret>) -> EntityId {
        register(&self.events, totp).await
    }

    async fn settled(&mut self) -> Vec<Step> {
        for _ in 0..5 {
            tokio::task::yield_now().await;
        }
        let mut steps = Vec::new();
        while let Ok(step) = self.steps.try_recv() {
            steps.push(step);
        }
        steps
    }

    async fn ask_for_a_link(&mut self, email: &str) -> support::Reply {
        self.client
            .post("/forgot", Some(json!({ "email": email })))
            .await
    }

    async fn the_emailed_path(&mut self, email: &str) -> String {
        self.ask_for_a_link(email).await;
        let steps = self.settled().await;
        let Some(Step::Sent { link, .. }) = steps.into_iter().last() else {
            panic!("no link was sent");
        };
        link.strip_prefix(ORIGIN).unwrap().to_owned()
    }

    async fn redeem(&mut self, path: &str, password: &str) -> support::Reply {
        self.client
            .post(path, Some(json!({ "password": password })))
            .await
    }
}

fn location(reply: &support::Reply) -> &str {
    reply.headers[LOCATION].to_str().unwrap()
}

fn key() -> TotpSecret {
    TotpSecret::from([7_u8; 20])
}

#[tokio::test]
async fn the_request_form_is_the_apps() {
    let mut world = World::new();

    let reply = world.client.get("/forgot").await;

    assert_eq!(reply.status, StatusCode::OK);
    assert_eq!(reply.body, "request form");
}

#[tokio::test]
async fn a_known_login_gets_a_link_to_the_configured_origin_at_the_recorded_address() {
    let mut world = World::new();
    let id = world.register(None).await;

    let reply = world.ask_for_a_link(ADA).await;
    let steps = world.settled().await;

    assert_eq!(reply.status, StatusCode::OK);
    assert_eq!(reply.body, "link sent");
    let [Step::Holder(login), Step::Record(user), Step::Sent { draft, to, link }] = &steps[..]
    else {
        panic!("unexpected steps {steps:?}");
    };
    assert_eq!((login.as_str(), *user), (ADA, id));
    assert_eq!(draft, &format!("draft for {ADA}"));
    assert_eq!(to, MAILBOX);
    assert!(link.starts_with(&format!("{ORIGIN}/reset/{id}/")));
}

#[tokio::test]
async fn an_unknown_login_gets_the_same_answer_and_no_mail() {
    let mut world = World::new();
    world.register(None).await;

    let known = world.ask_for_a_link(ADA).await;
    world.settled().await;
    let unknown = world.ask_for_a_link("nobody@example.com").await;
    let steps = world.settled().await;

    assert_eq!(unknown.status, known.status);
    assert_eq!(unknown.body, known.body);
    assert_eq!(steps, [Step::Holder("nobody@example.com".to_owned())]);
}

#[tokio::test]
async fn a_refused_record_sends_nothing() {
    let mut world = World::new();
    let id = world.register(None).await;
    world.record_refuses.store(true, Ordering::SeqCst);

    let reply = world.ask_for_a_link(ADA).await;
    let steps = world.settled().await;

    assert_eq!(reply.body, "link sent");
    assert_eq!(steps, [Step::Holder(ADA.to_owned()), Step::Record(id)]);
}

#[tokio::test]
async fn the_link_resets_the_password_once_and_signs_in() {
    let mut world = World::new();
    let id = world.register(None).await;
    let path = world.the_emailed_path(ADA).await;

    let reply = world.redeem(&path, NEW_PASSWORD).await;

    assert_eq!(reply.status, StatusCode::SEE_OTHER);
    assert_eq!(location(&reply), "/");
    assert_eq!(world.client.get("/me").await.body, id.to_string());
    let mut other = world.client.fork();
    assert_eq!(other.log_in(ADA, NEW_PASSWORD).await.body, "signed in");
    assert_eq!(
        other.log_in(ADA, PASSWORD).await.status,
        StatusCode::UNAUTHORIZED
    );
    let again = world.redeem(&path, "yet-another-password").await;
    assert_eq!(again.status, StatusCode::NOT_FOUND);
    assert_eq!(again.body, "invalid link");
}

#[tokio::test]
async fn the_page_checks_the_link_without_using_it_up() {
    let mut world = World::new();
    world.register(None).await;
    let path = world.the_emailed_path(ADA).await;

    let first = world.client.get(&path).await;
    let second = world.client.get(&path).await;
    let reply = world.redeem(&path, NEW_PASSWORD).await;

    assert_eq!(first.body, "new password form None");
    assert_eq!(second.body, "new password form None");
    assert_eq!(reply.status, StatusCode::SEE_OTHER);
}

#[tokio::test]
async fn every_way_a_link_can_be_wrong_is_the_same_invalid_link() {
    let mut world = World::new();
    world.register(None).await;
    let path = world.the_emailed_path(ADA).await;
    let (prefix, _) = path.rsplit_once('/').unwrap();
    let (other_token, _) = ResetToken::issue();
    let wrong = format!("{prefix}/{}", other_token.as_str());
    let malformed = format!("{prefix}/nope");
    let stranger = format!("/reset/{}/{}", EntityId::new(), other_token.as_str());

    let mut replies = Vec::new();
    for link in [&wrong, &malformed, &stranger] {
        replies.push(world.client.get(link).await);
        replies.push(world.redeem(link, NEW_PASSWORD).await);
    }
    world.clock.advance(TimeDelta::hours(25));
    replies.push(world.client.get(&path).await);
    replies.push(world.redeem(&path, NEW_PASSWORD).await);

    for reply in replies {
        assert_eq!(reply.status, StatusCode::NOT_FOUND);
        assert_eq!(reply.body, "invalid link");
    }
    let mut other = world.client.fork();
    assert_eq!(other.log_in(ADA, PASSWORD).await.body, "signed in");
    assert_eq!(
        world.client.get("/me").await.status,
        StatusCode::UNAUTHORIZED
    );
}

#[tokio::test]
async fn accounts_that_cannot_be_read_answer_a_request_as_ever_and_a_link_as_invalid() {
    let mut world = World::new();
    world.register(None).await;
    let path = world.the_emailed_path(ADA).await;
    world.accounts_down.store(true, Ordering::SeqCst);

    let asked = world.ask_for_a_link(ADA).await;
    let steps = world.settled().await;
    let page = world.client.get(&path).await;

    assert_eq!(asked.body, "link sent");
    assert_eq!(steps, [Step::Holder(ADA.to_owned())]);
    assert_eq!(page.status, StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn a_wrong_link_does_not_use_up_the_right_one() {
    let mut world = World::new();
    world.register(None).await;
    let path = world.the_emailed_path(ADA).await;
    let (prefix, _) = path.rsplit_once('/').unwrap();
    let (other_token, _) = ResetToken::issue();

    let wrong = world
        .redeem(&format!("{prefix}/{}", other_token.as_str()), NEW_PASSWORD)
        .await;
    let right = world.redeem(&path, NEW_PASSWORD).await;

    assert_eq!(wrong.status, StatusCode::NOT_FOUND);
    assert_eq!(right.status, StatusCode::SEE_OTHER);
}

#[tokio::test]
async fn a_profile_with_a_second_factor_signs_in_again() {
    let mut world = World::new();
    world.register(Some(&key())).await;
    let path = world.the_emailed_path(ADA).await;

    let reply = world.redeem(&path, NEW_PASSWORD).await;

    assert_eq!(reply.status, StatusCode::SEE_OTHER);
    assert_eq!(location(&reply), "/login");
    assert_eq!(
        world.client.get("/me").await.status,
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        world.client.log_in(ADA, NEW_PASSWORD).await.body,
        "code needed"
    );
}

#[tokio::test]
async fn a_reset_keeps_the_second_factor_of_the_profile() {
    let mut world = World::new();
    let id = world.register(Some(&key())).await;
    let path = world.the_emailed_path(ADA).await;

    world.redeem(&path, NEW_PASSWORD).await;

    assert_eq!(totp_of(&world.events, id).await, Some(key()));
}

#[tokio::test]
async fn a_key_enrolled_after_the_request_is_asked_for_when_the_link_is_used() {
    let mut world = World::new();
    let id = world.register(None).await;
    let path = world.the_emailed_path(ADA).await;
    enroll_totp(&world.events, id, &key()).await;

    let reply = world.redeem(&path, NEW_PASSWORD).await;

    assert_eq!(location(&reply), "/login");
    assert_eq!(
        world.client.get("/me").await.status,
        StatusCode::UNAUTHORIZED
    );
}

#[tokio::test]
async fn a_key_turned_off_after_the_request_is_not_asked_for_when_the_link_is_used() {
    let mut world = World::new();
    let id = world.register(Some(&key())).await;
    let path = world.the_emailed_path(ADA).await;
    disable_totp(&world.events, id).await;

    let reply = world.redeem(&path, NEW_PASSWORD).await;

    assert_eq!(location(&reply), "/");
    assert_eq!(world.client.get("/me").await.body, id.to_string());
}

#[tokio::test]
async fn a_route_that_asks_nothing_signs_in_whatever_the_profile_holds() {
    let mut world = World::build(SignInAnyway, LoginThrottles::default());
    let id = world.register(Some(&key())).await;
    let path = world.the_emailed_path(ADA).await;

    let reply = world.redeem(&path, NEW_PASSWORD).await;

    assert_eq!(location(&reply), "/");
    assert_eq!(world.client.get("/me").await.body, id.to_string());
}

#[tokio::test]
async fn a_hash_failure_shows_the_form_again_without_using_the_link_up() {
    let mut world = World::new();
    world.register(None).await;
    let path = world.the_emailed_path(ADA).await;
    world.hasher_fails.store(true, Ordering::SeqCst);

    let failed = world.redeem(&path, NEW_PASSWORD).await;
    world.hasher_fails.store(false, Ordering::SeqCst);
    let retried = world.redeem(&path, NEW_PASSWORD).await;

    assert_eq!(failed.status, StatusCode::OK);
    assert_eq!(failed.body, "new password form Some(HashFailed)");
    assert_eq!(retried.status, StatusCode::SEE_OTHER);
}

#[tokio::test]
async fn a_client_asking_too_often_waits_whatever_the_login() {
    let mut world =
        World::throttled(LoginThrottles::default().with_reset_requests(stuck_after_one_request()));
    world.register(None).await;

    let first = world.ask_for_a_link(ADA).await;
    world.settled().await;
    let second = world.ask_for_a_link("nobody@example.com").await;
    let third = world.ask_for_a_link(ADA).await;
    let steps = world.settled().await;

    assert_eq!(first.status, StatusCode::OK);
    assert_eq!(second.status, StatusCode::TOO_MANY_REQUESTS);
    assert!(second.headers.contains_key("retry-after"));
    assert_eq!(third.status, StatusCode::TOO_MANY_REQUESTS);
    assert!(steps.is_empty());
}
