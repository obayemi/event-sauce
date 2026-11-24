//! The TOTP code step of a login and the code proof of settings, through a
//! real axum router, with tower-sessions' memory store and the in-memory
//! event store.

mod memory_login_store;
mod secured_user;
mod support;

use std::{sync::Arc, time::Duration};

use axum::{
    extract::{FromRef, Query, State},
    http::{header::LOCATION, StatusCode},
    response::{IntoResponse, Redirect, Response},
    routing::{get, post},
    Json, Router,
};
use event_sauce_auth::{
    Argon2Hasher, CachedUserLoader, Email, FirstFactor, FixedClock, LoginId, Logins,
    PasswordAttempt, PasswordHasher, PasswordLogin, Totp, TotpIfEnrolled, TotpSecret,
};
use event_sauce_axum::{
    code_page, AuthLayer, AuthSession, Backoff, ClientAddr, ClientIpSource, CodeRefusal,
    CurrentUser, Enrolment, LoginError, LoginThrottles, OpenError, Opened, Pages, Proof, ReturnTo,
    Throttled, TotpChallenge, TotpLoginPages, TotpProof,
};
use event_sauce_core::EntityId;
use event_sauce_memory::InMemoryEventStore;
use memory_login_store::MemoryLoginStore;
use percent_encoding::{utf8_percent_encode, NON_ALPHANUMERIC};
use secrecy::SecretString;
use secured_user::{
    change_password, cheap_hasher, disable_totp, enroll_totp, register, totp_of, User, ADA,
    PASSWORD,
};
use serde::Deserialize;
use serde_json::json;
use support::Client;
use tower_sessions::{MemoryStore, Session, SessionManagerLayer};

const NOW: i64 = 1_700_000_010;
const STEP: i64 = 30;

fn key() -> TotpSecret {
    TotpSecret::from([7_u8; 20])
}

fn fresh_key() -> TotpSecret {
    TotpSecret::from([9_u8; 20])
}

fn code_of(secret: &TotpSecret, seconds: i64) -> String {
    totp_rs::Builder::new()
        .with_secret(secret.clone())
        .build()
        .unwrap()
        .generate(u64::try_from(seconds).unwrap())
        .to_string()
}

fn current_code() -> String {
    code_of(&key(), NOW)
}

fn another_code() -> String {
    code_of(&key(), NOW + STEP)
}

fn one_more_code() -> String {
    code_of(&key(), NOW - STEP)
}

fn stuck_after_one_failure() -> Backoff {
    Backoff {
        free: 0,
        base: Duration::from_hours(1),
        cap: Duration::from_hours(1),
        forget_after: Duration::from_hours(1),
    }
}

enum Failure {
    Login(LoginError),
    Open(OpenError),
    Throttled(Throttled),
    Store,
}

impl From<Throttled> for Failure {
    fn from(throttled: Throttled) -> Self {
        Self::Throttled(throttled)
    }
}

impl IntoResponse for Failure {
    fn into_response(self) -> Response {
        match self {
            Self::Login(error) => error.into_response(),
            Self::Open(error) => error.into_response(),
            Self::Throttled(throttled) => throttled.into_response(),
            Self::Store => StatusCode::INTERNAL_SERVER_ERROR.into_response(),
        }
    }
}

struct Answers;

impl Pages for Answers {
    type Response = Response;
    type Error = Failure;
}

impl TotpLoginPages for Answers {
    fn code_form(&self, refusal: Option<CodeRefusal>, return_to: &ReturnTo) -> Response {
        format!("code form {refusal:?} {return_to}").into_response()
    }

    fn no_login_waiting(&self) -> Response {
        Redirect::to("/login").into_response()
    }

    fn signed_in(&self, return_to: ReturnTo) -> Response {
        return_to.into_response()
    }
}

type PasswordFirst = FirstFactor<PasswordLogin<Argon2Hasher>, TotpIfEnrolled>;

#[derive(Clone, FromRef)]
struct AppState {
    logins: Logins<User>,
    first: Arc<PasswordFirst>,
    challenge: TotpChallenge<User>,
    proof: TotpProof,
    events: Arc<InMemoryEventStore>,
    client_ip: ClientIpSource,
}

#[derive(Deserialize)]
struct LoginForm {
    email: String,
    password: String,
    #[serde(default)]
    redirect_url: ReturnTo,
}

async fn login(
    mut auth: AuthSession<User>,
    session: Session,
    State(logins): State<Logins<User>>,
    State(first): State<Arc<PasswordFirst>>,
    State(challenge): State<TotpChallenge<User>>,
    Json(form): Json<LoginForm>,
) -> Result<Response, Failure> {
    let email = Email::parse(&form.email).map_err(|_| Failure::Store)?;
    let attempt = PasswordAttempt::new(email, SecretString::from(form.password));
    let begun = logins
        .begin(&first, attempt)
        .await
        .map_err(|error| Failure::Login(error.into()))?;
    let opened = challenge
        .open(&mut auth, &session, begun, form.redirect_url)
        .await
        .map_err(Failure::Open)?;
    Ok(match opened {
        Opened::SignedIn(return_to) => return_to.into_response(),
        Opened::AwaitingCode(return_to) => {
            let target = utf8_percent_encode(return_to.as_str(), NON_ALPHANUMERIC);
            Redirect::to(&format!("/login/totp?redirect_url={target}")).into_response()
        }
    })
}

#[derive(Deserialize)]
struct CodeQuery {
    #[serde(default)]
    redirect_url: ReturnTo,
}

async fn show_code_page(session: Session, Query(query): Query<CodeQuery>) -> Response {
    code_page(&Answers, &session, &query.redirect_url).await
}

#[derive(Deserialize)]
struct CodeForm {
    code: String,
    #[serde(default)]
    redirect_url: ReturnTo,
}

async fn code_submit(
    mut auth: AuthSession<User>,
    session: Session,
    client: ClientAddr,
    State(challenge): State<TotpChallenge<User>>,
    Json(form): Json<CodeForm>,
) -> Result<Response, Failure> {
    challenge
        .prove(
            &Answers,
            &mut auth,
            &session,
            &client,
            SecretString::from(form.code),
            form.redirect_url,
        )
        .await
}

async fn me(CurrentUser(user): CurrentUser<User>) -> String {
    user.entity_id().to_string()
}

#[derive(Deserialize)]
struct EnrolForm {
    secret: String,
    code: String,
}

async fn enrol(
    mut auth: AuthSession<User>,
    State(proof): State<TotpProof>,
    State(events): State<Arc<InMemoryEventStore>>,
    Json(form): Json<EnrolForm>,
) -> Result<&'static str, Failure> {
    let id = auth.user().unwrap().entity_id();
    let enrolment = proof
        .enrol(&mut auth, &form.secret, &form.code, async |secret| {
            enroll_totp(&events, id, &secret).await;
            Ok::<(), Failure>(())
        })
        .await?;
    Ok(match enrolment {
        Enrolment::Enrolled => "enrolled",
        Enrolment::CodeRefused => "code refused",
    })
}

#[derive(Deserialize)]
struct ProofForm {
    code: String,
}

fn describe(proof: Proof) -> &'static str {
    match proof {
        Proof::Proved => "proved",
        Proof::NotEnrolled => "not enrolled",
        Proof::Refused => "refused",
    }
}

async fn disable(
    mut auth: AuthSession<User>,
    client: ClientAddr,
    State(proof): State<TotpProof>,
    State(events): State<Arc<InMemoryEventStore>>,
    Json(form): Json<ProofForm>,
) -> Result<&'static str, Failure> {
    let user = auth.user().cloned().unwrap();
    let id = user.entity_id();
    let proved = proof
        .change(&mut auth, &user, &client, &form.code, async || {
            disable_totp(&events, id).await;
            Ok::<(), Failure>(())
        })
        .await?;
    Ok(describe(proved))
}

async fn check(
    auth: AuthSession<User>,
    client: ClientAddr,
    State(proof): State<TotpProof>,
    Json(form): Json<ProofForm>,
) -> Result<&'static str, Failure> {
    let user = auth.user().unwrap();
    Ok(describe(proof.prove(user, &client, &form.code).await?))
}

struct World {
    events: Arc<InMemoryEventStore>,
    client: Client,
}

impl World {
    fn new() -> Self {
        Self::throttled(LoginThrottles::default())
    }

    fn throttled(throttles: LoginThrottles) -> Self {
        let events = Arc::new(InMemoryEventStore::new());
        let login_store = Arc::new(MemoryLoginStore::default());
        let logins = Logins::<User>::new(events.clone());
        let totp = Totp::new(login_store.clone()).with_clock(Arc::new(FixedClock(
            chrono::DateTime::from_timestamp(NOW, 0).unwrap(),
        )));
        let throttles = Arc::new(throttles);
        let state = AppState {
            challenge: TotpChallenge::new(
                logins.pending(login_store),
                totp.clone(),
                throttles.clone(),
            ),
            proof: TotpProof::new(totp, throttles),
            first: Arc::new(FirstFactor::new(
                PasswordLogin::new(cheap_hasher()),
                TotpIfEnrolled,
            )),
            logins,
            events: events.clone(),
            client_ip: ClientIpSource::default(),
        };
        let router = Router::new()
            .route("/login", post(login))
            .route("/login/totp", get(show_code_page).post(code_submit))
            .route("/me", get(me))
            .route("/totp/enrol", post(enrol))
            .route("/totp/disable", post(disable))
            .route("/totp/check", post(check))
            .with_state(state);
        let router = AuthLayer::<User>::new(CachedUserLoader::new(events.clone()))
            .layer(router, SessionManagerLayer::new(MemoryStore::default()));
        Self {
            events,
            client: Client::new(router),
        }
    }

    async fn register(&self, totp: Option<&TotpSecret>) -> EntityId {
        register(&self.events, totp).await
    }

    async fn log_in(client: &mut Client) -> support::Reply {
        client.log_in(ADA, PASSWORD).await
    }

    async fn send_code(client: &mut Client, code: &str) -> support::Reply {
        client
            .post("/login/totp", Some(json!({ "code": code })))
            .await
    }

    async fn sign_in_with(client: &mut Client, code: &str) {
        Self::log_in(client).await;
        Self::send_code(client, code).await;
    }

    async fn post_code(&mut self, uri: &str, code: &str) -> support::Reply {
        self.client.post(uri, Some(json!({ "code": code }))).await
    }

    async fn status(&mut self, uri: &str) -> StatusCode {
        self.client.get(uri).await.status
    }
}

fn location(reply: &support::Reply) -> &str {
    reply.headers[LOCATION].to_str().unwrap()
}

#[tokio::test]
async fn a_login_without_a_key_signs_in_at_once() {
    let mut world = World::new();
    let id = world.register(None).await;

    let reply = World::log_in(&mut world.client).await;

    assert_eq!(reply.status, StatusCode::SEE_OTHER);
    assert_eq!(location(&reply), "/");
    assert_eq!(world.client.get("/me").await.body, id.to_string());
}

#[tokio::test]
async fn a_login_with_a_key_waits_for_its_code_in_the_session_alone() {
    let mut world = World::new();
    world.register(Some(&key())).await;

    let reply = World::log_in(&mut world.client).await;

    assert_eq!(reply.status, StatusCode::SEE_OTHER);
    assert_eq!(location(&reply), "/login/totp?redirect_url=%2F");
    assert_eq!(world.status("/me").await, StatusCode::UNAUTHORIZED);
    assert_eq!(world.client.cookies.len(), 1);
}

#[tokio::test]
async fn the_code_of_the_key_opens_the_session_and_goes_where_the_login_was_going() {
    let mut world = World::new();
    let id = world.register(Some(&key())).await;
    world
        .client
        .post(
            "/login",
            Some(json!({
                "email": ADA,
                "password": PASSWORD,
                "redirect_url": "/notes?page=2",
            })),
        )
        .await;

    let reply = world
        .client
        .post(
            "/login/totp",
            Some(json!({ "code": current_code(), "redirect_url": "/notes?page=2" })),
        )
        .await;

    assert_eq!(reply.status, StatusCode::SEE_OTHER);
    assert_eq!(location(&reply), "/notes?page=2");
    assert_eq!(world.client.get("/me").await.body, id.to_string());
}

#[tokio::test]
async fn the_code_page_shows_the_form_while_a_login_waits() {
    let mut world = World::new();
    world.register(Some(&key())).await;
    World::log_in(&mut world.client).await;

    let reply = world.client.get("/login/totp?redirect_url=%2Fnotes").await;

    assert_eq!(reply.status, StatusCode::OK);
    assert_eq!(reply.body, "code form None /notes");
}

#[tokio::test]
async fn the_code_page_without_a_waiting_login_sends_back_to_the_login() {
    let mut world = World::new();
    world.register(Some(&key())).await;

    let reply = world.client.get("/login/totp").await;

    assert_eq!(reply.status, StatusCode::SEE_OTHER);
    assert_eq!(location(&reply), "/login");
}

#[tokio::test]
async fn a_code_without_a_waiting_login_shows_the_form_refused() {
    let mut world = World::new();
    world.register(Some(&key())).await;

    let reply = World::send_code(&mut world.client, &current_code()).await;

    assert_eq!(reply.body, "code form Some(Invalid) /");
    assert_eq!(world.status("/me").await, StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn a_wrong_code_uses_the_login_up() {
    let mut world = World::new();
    world.register(Some(&key())).await;
    World::log_in(&mut world.client).await;

    let wrong = World::send_code(&mut world.client, "000000").await;
    let right_after = World::send_code(&mut world.client, &current_code()).await;
    let page = world.client.get("/login/totp").await;

    assert_eq!(wrong.body, "code form Some(Invalid) /");
    assert_eq!(right_after.body, "code form Some(Invalid) /");
    assert_eq!(location(&page), "/login");
    assert_eq!(world.status("/me").await, StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn a_code_is_accepted_once() {
    let mut world = World::new();
    world.register(Some(&key())).await;
    World::sign_in_with(&mut world.client, &current_code()).await;
    let mut other = world.client.fork();

    World::log_in(&mut other).await;
    let replayed = World::send_code(&mut other, &current_code()).await;

    assert_eq!(replayed.body, "code form Some(Invalid) /");
    assert_eq!(other.get("/me").await.status, StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn credentials_changed_between_the_steps_refuse_the_code() {
    let mut world = World::new();
    let id = world.register(Some(&key())).await;
    World::log_in(&mut world.client).await;
    enroll_totp(&world.events, id, &fresh_key()).await;

    let reply = World::send_code(&mut world.client, &current_code()).await;

    assert_eq!(reply.body, "code form Some(Invalid) /");
    assert_eq!(world.status("/me").await, StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn a_password_changed_between_the_steps_refuses_the_code() {
    let mut world = World::new();
    let id = world.register(Some(&key())).await;
    World::log_in(&mut world.client).await;
    let hash = cheap_hasher()
        .hash(&SecretString::from("another-password"))
        .await
        .unwrap();
    change_password(&world.events, id, &hash).await;

    let reply = World::send_code(&mut world.client, &current_code()).await;

    assert_eq!(reply.body, "code form Some(Invalid) /");
    assert_eq!(world.status("/me").await, StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn a_throttled_code_step_keeps_the_login_waiting() {
    let mut world =
        World::throttled(LoginThrottles::default().with_second_factor(stuck_after_one_failure()));
    world.register(Some(&key())).await;
    World::log_in(&mut world.client).await;
    World::send_code(&mut world.client, "000000").await;
    World::log_in(&mut world.client).await;

    let throttled = World::send_code(&mut world.client, &current_code()).await;
    let page = world.client.get("/login/totp").await;

    assert_eq!(throttled.status, StatusCode::TOO_MANY_REQUESTS);
    assert!(throttled.headers.contains_key("retry-after"));
    assert_eq!(page.body, "code form None /");
    assert_eq!(world.status("/me").await, StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn a_wrong_enrolment_code_enrols_nothing() {
    let mut world = World::new();
    let id = world.register(None).await;
    World::log_in(&mut world.client).await;

    let reply = world
        .client
        .post(
            "/totp/enrol",
            Some(json!({ "secret": fresh_key().to_base32(), "code": "000000" })),
        )
        .await;

    assert_eq!(reply.body, "code refused");
    assert_eq!(totp_of(&world.events, id).await, None);
}

#[tokio::test]
async fn a_malformed_enrolment_secret_enrols_nothing() {
    let mut world = World::new();
    let id = world.register(None).await;
    World::log_in(&mut world.client).await;

    let reply = world
        .client
        .post(
            "/totp/enrol",
            Some(json!({ "secret": "not base32 !", "code": "000000" })),
        )
        .await;

    assert_eq!(reply.body, "code refused");
    assert_eq!(totp_of(&world.events, id).await, None);
}

#[tokio::test]
async fn a_right_enrolment_code_enrols_and_renews_the_acting_session_only() {
    let mut world = World::new();
    let id = world.register(None).await;
    World::log_in(&mut world.client).await;
    let mut other = world.client.fork();
    World::log_in(&mut other).await;

    let reply = world
        .client
        .post(
            "/totp/enrol",
            Some(json!({
                "secret": fresh_key().to_base32(),
                "code": code_of(&fresh_key(), NOW),
            })),
        )
        .await;

    assert_eq!(reply.body, "enrolled");
    assert_eq!(totp_of(&world.events, id).await, Some(fresh_key()));
    assert_eq!(world.client.get("/me").await.body, id.to_string());
    assert_eq!(other.get("/me").await.status, StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn turning_the_key_off_needs_a_current_code() {
    let mut world = World::new();
    let id = world.register(Some(&key())).await;
    World::sign_in_with(&mut world.client, &current_code()).await;

    let wrong = world.post_code("/totp/disable", "000000").await;

    assert_eq!(wrong.body, "refused");
    assert_eq!(totp_of(&world.events, id).await, Some(key()));
}

#[tokio::test]
async fn turning_the_key_off_renews_the_acting_session_and_ends_the_others() {
    let mut world = World::new();
    let id = world.register(Some(&key())).await;
    World::sign_in_with(&mut world.client, &current_code()).await;
    let mut other = world.client.fork();
    World::sign_in_with(&mut other, &one_more_code()).await;

    let reply = world.post_code("/totp/disable", &another_code()).await;

    assert_eq!(reply.body, "proved");
    assert_eq!(totp_of(&world.events, id).await, None);
    assert_eq!(world.client.get("/me").await.body, id.to_string());
    assert_eq!(other.get("/me").await.status, StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn a_code_while_not_enrolled_proves_nothing_and_counts_nothing() {
    let mut world =
        World::throttled(LoginThrottles::default().with_second_factor(stuck_after_one_failure()));
    world.register(None).await;
    World::log_in(&mut world.client).await;

    let first = world.post_code("/totp/check", "000000").await;
    let second = world.post_code("/totp/check", "000000").await;
    let disabling = world.post_code("/totp/disable", "000000").await;

    assert_eq!(first.body, "not enrolled");
    assert_eq!(second.body, "not enrolled");
    assert_eq!(disabling.body, "not enrolled");
}

#[tokio::test]
async fn a_current_code_proves_the_key_and_is_used_up() {
    let mut world = World::new();
    world.register(Some(&key())).await;
    World::sign_in_with(&mut world.client, &current_code()).await;

    let proved = world.post_code("/totp/check", &another_code()).await;
    let replayed = world.post_code("/totp/check", &another_code()).await;

    assert_eq!(proved.body, "proved");
    assert_eq!(replayed.body, "refused");
}

#[tokio::test]
async fn wrong_codes_of_a_user_make_it_wait() {
    let mut world =
        World::throttled(LoginThrottles::default().with_second_factor(stuck_after_one_failure()));
    world.register(Some(&key())).await;
    World::sign_in_with(&mut world.client, &current_code()).await;

    let wrong = world.post_code("/totp/check", "000000").await;
    let waiting = world.post_code("/totp/check", &another_code()).await;

    assert_eq!(wrong.body, "refused");
    assert_eq!(waiting.status, StatusCode::TOO_MANY_REQUESTS);
    assert!(waiting.headers.contains_key("retry-after"));
}
