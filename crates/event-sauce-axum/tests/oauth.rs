//! The OAuth sign-in, link and claim flows through a real axum router, with a
//! scripted provider, tower-sessions' memory store and the in-memory event
//! store.

mod linked_user;
mod memory_login_store;
mod support;

use std::sync::{
    atomic::{AtomicI64, AtomicUsize, Ordering},
    Arc, Mutex,
};

use async_trait::async_trait;
use axum::{
    extract::{FromRef, Query, State},
    http::{header::LOCATION, StatusCode},
    response::{IntoResponse, Redirect, Response},
    routing::{get, post},
    Json, Router,
};
use chrono::{DateTime, TimeDelta, Utc};
use event_sauce_auth::{
    Advance, Authorization, AuthorizationCode, CachedUserLoader, Clock, Entered, FederatedAccount,
    FederatedId, FixedClock, Logins, OAuthProvider, OAuthState, PkceVerifier, Provider, Route,
    Totp, TotpIfEnrolled, TotpOrDone, TotpSecret,
};
use event_sauce_axum::{
    AuthLayer, AuthSession, CallbackError, ClientAddr, ClientIpSource, CurrentUser, LinkOutcome,
    LinkProof, LoginThrottles, OAuthAccounts, OAuthPages, OAuthSignIn, Pages, ReturnTo, Returned,
    Throttled, TotpChallenge, TotpLoginPages,
};
use event_sauce_core::{AggregateRoot, EntityId};
use event_sauce_memory::InMemoryEventStore;
use linked_user::{deactivate, google, links_of, register, User};
use memory_login_store::MemoryLoginStore;
use secrecy::SecretString;
use serde::Deserialize;
use serde_json::json;
use support::Client;
use tower_sessions::{MemoryStore, Session, SessionManagerLayer};
use uuid::Uuid;

const NOW: i64 = 1_700_000_010;
const ACCOUNT: ReturnTo = ReturnTo::from_static("/account");

fn key() -> TotpSecret {
    TotpSecret::from([7_u8; 20])
}

fn current_code() -> String {
    totp_rs::Builder::new()
        .with_secret(key())
        .build()
        .unwrap()
        .generate(u64::try_from(NOW).unwrap())
        .to_string()
}

#[derive(Debug, thiserror::Error)]
#[error("the provider declined")]
struct Declined;

#[derive(Debug)]
struct Denied;

#[derive(Default)]
struct Scripted {
    issued: AtomicUsize,
    identified: Mutex<Vec<(String, String)>>,
}

#[async_trait]
impl OAuthProvider for Scripted {
    type Profile = String;
    type Error = Declined;

    fn authorize(&self) -> Authorization {
        let n = self.issued.fetch_add(1, Ordering::SeqCst);
        Authorization {
            url: format!("https://provider.test/consent?state=state-{n}"),
            state: OAuthState::new(format!("state-{n}")),
            pkce_verifier: PkceVerifier::new(format!("verifier-{n}")),
        }
    }

    async fn identify(
        &self,
        code: AuthorizationCode,
        verifier: PkceVerifier,
    ) -> Result<FederatedAccount<String>, Declined> {
        self.identified
            .lock()
            .unwrap()
            .push((code.as_str().to_owned(), verifier.as_str().to_owned()));
        if code.as_str() == "declined" {
            return Err(Declined);
        }
        Ok(FederatedAccount {
            id: google(code.as_str()),
            profile: format!("{}@example.com", code.as_str()),
        })
    }
}

struct Accounts {
    events: Arc<InMemoryEventStore>,
    reconciled: AtomicUsize,
    adopted: AtomicUsize,
    proofs: Mutex<Vec<LinkProof>>,
    needs_proof: bool,
}

#[async_trait]
impl OAuthAccounts for Accounts {
    type Profile = String;
    type Refusal = Denied;

    async fn reconcile(&self) {
        self.reconciled.fetch_add(1, Ordering::SeqCst);
    }

    async fn unknown(&self, account: &FederatedAccount<String>) -> Result<(), Denied> {
        self.adopted.fetch_add(1, Ordering::SeqCst);
        if account.profile.starts_with("banned") {
            return Err(Denied);
        }
        register(&self.events, account.id.clone(), None).await;
        Ok(())
    }

    async fn link(
        &self,
        user: EntityId,
        account: FederatedAccount<String>,
        proof: LinkProof,
    ) -> LinkOutcome {
        self.proofs.lock().unwrap().push(proof);
        if self.needs_proof && proof == LinkProof::Unproved {
            return LinkOutcome::NeedsCode;
        }
        if account.id.subject() == "taken" {
            return LinkOutcome::Failed;
        }
        linked_user::link(&self.events, user, account.id).await;
        LinkOutcome::Linked
    }

    async fn claim(&self, user: EntityId, account: FederatedAccount<String>) -> bool {
        if account.id.subject() == "taken" {
            return false;
        }
        linked_user::link(&self.events, user, account.id).await;
        true
    }
}

#[derive(Debug)]
struct AdjustableClock(AtomicI64);

impl AdjustableClock {
    fn advance(&self, by: TimeDelta) {
        self.0.fetch_add(by.num_seconds(), Ordering::SeqCst);
    }
}

impl Clock for AdjustableClock {
    fn now(&self) -> DateTime<Utc> {
        DateTime::from_timestamp(self.0.load(Ordering::SeqCst), 0).unwrap()
    }
}

#[derive(Clone, Copy)]
struct Ask(bool);

impl Route<User> for Ask {
    type Next = TotpOrDone<User>;

    fn route(&self, entered: Advance<User, Entered>) -> TotpOrDone<User> {
        if self.0 {
            TotpIfEnrolled.route(entered)
        } else {
            TotpOrDone::Done(entered.to(event_sauce_auth::Done))
        }
    }
}

struct Answers;

impl Pages for Answers {
    type Response = Response;
    type Error = Throttled;
}

impl OAuthPages<Declined, Denied> for Answers {
    fn failed(
        &self,
        error: CallbackError<Declined, Denied>,
        return_to: Option<&ReturnTo>,
    ) -> Response {
        let target = return_to.map_or("none", ReturnTo::as_str);
        (
            StatusCode::UNPROCESSABLE_ENTITY,
            format!("failed {error:?} {target}"),
        )
            .into_response()
    }

    fn not_session_user(&self) -> Response {
        Redirect::to("/profile").into_response()
    }

    fn link_needs_code(&self) -> Response {
        Redirect::to("/link-code").into_response()
    }

    fn returned(&self, return_to: Option<ReturnTo>) -> Response {
        return_to
            .unwrap_or(ReturnTo::from_static("/profile"))
            .into_response()
    }

    fn code_step(&self, return_to: &ReturnTo) -> Response {
        Redirect::to(&format!(
            "/login/totp?redirect_url={}",
            return_to.as_str().replace('/', "%2F")
        ))
        .into_response()
    }

    fn done(&self, return_to: ReturnTo) -> Response {
        return_to.into_response()
    }
}

impl TotpLoginPages for Answers {
    fn code_form(
        &self,
        refusal: Option<event_sauce_axum::CodeRefusal>,
        return_to: &ReturnTo,
    ) -> Response {
        format!("code form {refusal:?} {return_to}").into_response()
    }

    fn no_login_waiting(&self) -> Response {
        Redirect::to("/login").into_response()
    }

    fn signed_in(&self, return_to: ReturnTo) -> Response {
        return_to.into_response()
    }
}

type Engine = OAuthSignIn<User, Scripted, Accounts, Ask>;

#[derive(Clone, FromRef)]
struct AppState {
    engine: Arc<Engine>,
    logins: Logins<User>,
    challenge: TotpChallenge<User>,
    client_ip: ClientIpSource,
}

#[derive(Deserialize)]
struct StartQuery {
    #[serde(default)]
    redirect_url: Option<ReturnTo>,
}

async fn start(
    State(engine): State<Arc<Engine>>,
    session: Session,
    Query(query): Query<StartQuery>,
) -> Response {
    engine
        .start(&session, query.redirect_url.as_ref())
        .await
        .into_response()
}

async fn callback(
    State(engine): State<Arc<Engine>>,
    mut auth: AuthSession<User>,
    session: Session,
    Query(returned): Query<Returned>,
) -> Response {
    engine
        .callback(&Answers, &mut auth, &session, returned)
        .await
}

#[derive(Deserialize)]
struct LinkQuery {
    #[serde(default)]
    proved: bool,
    other: Option<EntityId>,
}

async fn link(
    State(engine): State<Arc<Engine>>,
    auth: AuthSession<User>,
    session: Session,
    Query(query): Query<LinkQuery>,
) -> Response {
    let user = query
        .other
        .unwrap_or_else(|| auth.user().unwrap().entity_id());
    let proof = if query.proved {
        LinkProof::Proved
    } else {
        LinkProof::Unproved
    };
    engine
        .start_link(&session, user, proof, &ACCOUNT)
        .await
        .into_response()
}

#[derive(Deserialize)]
struct ClaimQuery {
    other: Option<EntityId>,
}

async fn claim(
    State(engine): State<Arc<Engine>>,
    auth: AuthSession<User>,
    session: Session,
    Query(query): Query<ClaimQuery>,
) -> Response {
    let user = query
        .other
        .unwrap_or_else(|| auth.user().unwrap().entity_id());
    engine.mark_claim(&session, user).await;
    Redirect::to("/oauth/start?redirect_url=/profile").into_response()
}

async fn guest(
    mut auth: AuthSession<User>,
    State(logins): State<Logins<User>>,
) -> Result<String, StatusCode> {
    let id = FederatedId::new(Provider::new("guest"), EntityId::new().to_string());
    let credentials = linked_user::guest(id);
    let user: AggregateRoot<User> = User::register(credentials).unwrap();
    let login = logins
        .sign_up(user)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    let authenticated = login
        .finish()
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    let id = authenticated.user().entity_id();
    auth.login(authenticated)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    Ok(id.to_string())
}

async fn plant_state(session: Session) {
    session
        .insert("event_sauce.oauth.state", OAuthState::new("planted"))
        .await
        .unwrap();
}

#[derive(Deserialize)]
struct CodeForm {
    code: String,
}

async fn code_submit(
    mut auth: AuthSession<User>,
    session: Session,
    client: ClientAddr,
    State(challenge): State<TotpChallenge<User>>,
    Json(form): Json<CodeForm>,
) -> Result<Response, Throttled> {
    challenge
        .prove(
            &Answers,
            &mut auth,
            &session,
            &client,
            SecretString::from(form.code),
            ReturnTo::root(),
        )
        .await
}

async fn me(CurrentUser(user): CurrentUser<User>) -> String {
    user.entity_id().to_string()
}

struct World {
    events: Arc<InMemoryEventStore>,
    provider: Arc<Scripted>,
    accounts: Arc<Accounts>,
    clock: Arc<AdjustableClock>,
    engine: Arc<Engine>,
    client: Client,
}

impl World {
    fn new() -> Self {
        Self::build(true, false)
    }

    fn without_code_step() -> Self {
        Self::build(false, false)
    }

    fn proving_links() -> Self {
        Self::build(true, true)
    }

    fn build(ask: bool, needs_proof: bool) -> Self {
        let events = Arc::new(InMemoryEventStore::new());
        let login_store = Arc::new(MemoryLoginStore::default());
        let logins = Logins::<User>::new(events.clone());
        let totp = Totp::new(login_store.clone()).with_clock(Arc::new(FixedClock(
            DateTime::from_timestamp(NOW, 0).unwrap(),
        )));
        let challenge = TotpChallenge::new(
            logins.pending(login_store),
            totp,
            Arc::new(LoginThrottles::default()),
        );
        let provider = Arc::new(Scripted::default());
        let accounts = Arc::new(Accounts {
            events: events.clone(),
            reconciled: AtomicUsize::new(0),
            adopted: AtomicUsize::new(0),
            proofs: Mutex::new(Vec::new()),
            needs_proof,
        });
        let clock = Arc::new(AdjustableClock(AtomicI64::new(NOW)));
        let engine = OAuthSignIn::new(
            provider.clone(),
            accounts.clone(),
            logins.clone(),
            Ask(ask),
            challenge.clone(),
        )
        .with_clock(clock.clone());
        let engine = Arc::new(engine);
        let state = AppState {
            engine: engine.clone(),
            logins,
            challenge,
            client_ip: ClientIpSource::default(),
        };
        let router = Router::new()
            .route("/oauth/start", get(start))
            .route("/oauth/callback", get(callback))
            .route("/link", get(link))
            .route("/claim", get(claim))
            .route("/guest", post(guest))
            .route("/plant-state", post(plant_state))
            .route("/login/totp", post(code_submit))
            .route("/me", get(me))
            .with_state(state);
        let router = AuthLayer::<User>::new(CachedUserLoader::new(events.clone()))
            .layer(router, SessionManagerLayer::new(MemoryStore::default()));
        Self {
            events,
            provider,
            accounts,
            clock,
            engine,
            client: Client::new(router),
        }
    }

    async fn holder(&self, subject: &str) -> EntityId {
        register(&self.events, google(subject), None).await
    }

    async fn enrolled_holder(&self, subject: &str) -> EntityId {
        register(&self.events, google(subject), Some(&key())).await
    }

    async fn start(client: &mut Client, query: &str) -> String {
        let reply = client.get(&format!("/oauth/start{query}")).await;
        assert_eq!(reply.status, StatusCode::SEE_OTHER);
        location(&reply)
            .strip_prefix("https://provider.test/consent?state=")
            .unwrap()
            .to_owned()
    }

    async fn come_back(client: &mut Client, code: &str, state: &str) -> support::Reply {
        client
            .get(&format!("/oauth/callback?code={code}&state={state}"))
            .await
    }

    async fn sign_in(client: &mut Client, code: &str) -> support::Reply {
        let state = Self::start(client, "").await;
        Self::come_back(client, code, &state).await
    }

    async fn me(&mut self) -> (StatusCode, String) {
        let reply = self.client.get("/me").await;
        (reply.status, reply.body)
    }

    async fn guest(client: &mut Client) -> EntityId {
        id_of(&client.post("/guest", None).await.body)
    }

    fn proofs(&self) -> Vec<LinkProof> {
        self.accounts.proofs.lock().unwrap().clone()
    }
}

fn id_of(body: &str) -> EntityId {
    EntityId::from(Uuid::parse_str(body).unwrap())
}

fn location(reply: &support::Reply) -> &str {
    reply.headers[LOCATION].to_str().unwrap()
}

#[tokio::test]
async fn a_flow_starts_at_the_consent_page_of_the_provider() {
    let mut world = World::new();

    let reply = world.client.get("/oauth/start").await;

    assert_eq!(reply.status, StatusCode::SEE_OTHER);
    assert_eq!(
        location(&reply),
        "https://provider.test/consent?state=state-0"
    );
}

#[tokio::test]
async fn a_known_account_signs_in_and_goes_where_the_flow_was_going() {
    let mut world = World::new();
    let ada = world.holder("ada").await;
    let state = World::start(&mut world.client, "?redirect_url=/notes?page=2").await;

    let reply = World::come_back(&mut world.client, "ada", &state).await;

    assert_eq!(reply.status, StatusCode::SEE_OTHER);
    assert_eq!(location(&reply), "/notes?page=2");
    assert_eq!(world.me().await, (StatusCode::OK, ada.to_string()));
    assert_eq!(world.accounts.reconciled.load(Ordering::SeqCst), 0);
    assert_eq!(world.accounts.adopted.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn the_verifier_of_the_flow_goes_to_the_provider() {
    let mut world = World::new();
    world.holder("ada").await;
    World::start(&mut world.client, "").await;
    let state = World::start(&mut world.client, "").await;

    World::come_back(&mut world.client, "ada", &state).await;

    let identified = world.provider.identified.lock().unwrap().clone();
    assert_eq!(identified, [("ada".to_owned(), "verifier-1".to_owned())]);
}

#[tokio::test]
async fn an_enrolled_user_is_asked_for_the_code_before_the_session_opens() {
    let mut world = World::new();
    let ada = world.enrolled_holder("ada").await;

    let reply = World::sign_in(&mut world.client, "ada").await;
    assert_eq!(location(&reply), "/login/totp?redirect_url=%2F");
    assert_eq!(world.me().await.0, StatusCode::UNAUTHORIZED);

    let coded = world
        .client
        .post("/login/totp", Some(json!({ "code": current_code() })))
        .await;
    assert_eq!(location(&coded), "/");
    assert_eq!(world.me().await, (StatusCode::OK, ada.to_string()));
}

#[tokio::test]
async fn the_code_step_keeps_the_target_of_the_flow() {
    let mut world = World::new();
    world.enrolled_holder("ada").await;
    let state = World::start(&mut world.client, "?redirect_url=/notes").await;

    let reply = World::come_back(&mut world.client, "ada", &state).await;

    assert_eq!(location(&reply), "/login/totp?redirect_url=%2Fnotes");
}

#[tokio::test]
async fn a_route_that_does_not_ask_signs_an_enrolled_user_in_at_once() {
    let mut world = World::without_code_step();
    let ada = world.enrolled_holder("ada").await;

    let reply = World::sign_in(&mut world.client, "ada").await;

    assert_eq!(location(&reply), "/");
    assert_eq!(world.me().await, (StatusCode::OK, ada.to_string()));
}

#[tokio::test]
async fn an_unknown_account_is_settled_then_adopted_once_and_signed_in() {
    let mut world = World::new();

    let reply = World::sign_in(&mut world.client, "grace").await;

    assert_eq!(location(&reply), "/");
    let (status, body) = world.me().await;
    assert_eq!(status, StatusCode::OK);
    let user = id_of(&body);
    assert_eq!(links_of(&world.events, user).await, [google("grace")]);
    assert_eq!(world.accounts.reconciled.load(Ordering::SeqCst), 1);
    assert_eq!(world.accounts.adopted.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn the_refusal_of_the_hook_reaches_the_page() {
    let mut world = World::new();

    let reply = World::sign_in(&mut world.client, "banned").await;

    assert_eq!(reply.status, StatusCode::UNPROCESSABLE_ENTITY);
    assert_eq!(reply.body, "failed Refused(Some(Denied)) /");
    assert_eq!(world.me().await.0, StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn an_inactive_user_is_refused_without_the_hook() {
    let mut world = World::new();
    let ada = world.holder("ada").await;
    deactivate(&world.events, ada).await;

    let reply = World::sign_in(&mut world.client, "ada").await;

    assert_eq!(reply.body, "failed Refused(None) /");
    assert_eq!(world.accounts.adopted.load(Ordering::SeqCst), 0);
    assert_eq!(world.me().await.0, StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn a_callback_outside_a_flow_is_refused() {
    let mut world = World::new();
    world.holder("ada").await;

    let reply = World::come_back(&mut world.client, "ada", "state-0").await;

    assert_eq!(reply.body, "failed State none");
    assert_eq!(world.me().await.0, StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn another_state_is_refused_and_ends_the_flow() {
    let mut world = World::new();
    world.holder("ada").await;
    let state = World::start(&mut world.client, "").await;

    let forged = World::come_back(&mut world.client, "ada", "forged").await;
    let genuine = World::come_back(&mut world.client, "ada", &state).await;

    assert_eq!(forged.body, "failed State none");
    assert_eq!(genuine.body, "failed State none");
    assert_eq!(world.me().await.0, StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn a_replayed_callback_is_refused() {
    let mut world = World::new();
    world.holder("ada").await;
    let state = World::start(&mut world.client, "").await;
    World::come_back(&mut world.client, "ada", &state).await;

    let replayed = World::come_back(&mut world.client, "ada", &state).await;

    assert_eq!(replayed.body, "failed State none");
}

#[tokio::test]
async fn a_flow_that_lost_its_verifier_has_expired() {
    let mut world = World::new();
    world.client.post("/plant-state", None).await;

    let reply = World::come_back(&mut world.client, "ada", "planted").await;

    assert_eq!(reply.body, "failed Expired none");
}

#[tokio::test]
async fn a_provider_error_goes_to_the_page_with_the_target() {
    let mut world = World::new();
    let state = World::start(&mut world.client, "?redirect_url=/notes").await;

    let reply = World::come_back(&mut world.client, "declined", &state).await;

    assert_eq!(reply.body, "failed Provider(Declined) /notes");
    assert_eq!(world.me().await.0, StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn a_cancelled_consent_ends_the_flow() {
    let mut world = World::new();
    world.holder("ada").await;
    let state = World::start(&mut world.client, "?redirect_url=/notes").await;

    let cancelled = world
        .client
        .get(&format!(
            "/oauth/callback?error=access_denied&state={state}"
        ))
        .await;
    let after = World::come_back(&mut world.client, "ada", &state).await;

    assert_eq!(cancelled.body, "failed Cancelled /notes");
    assert_eq!(after.body, "failed State none");
    assert!(world.provider.identified.lock().unwrap().is_empty());
}

#[tokio::test]
async fn the_target_of_an_abandoned_flow_is_forgotten() {
    let mut world = World::new();
    world.holder("ada").await;
    World::start(&mut world.client, "?redirect_url=/notes").await;
    let state = World::start(&mut world.client, "").await;

    let reply = World::come_back(&mut world.client, "ada", &state).await;

    assert_eq!(location(&reply), "/");
}

async fn signed_in(world: &mut World, subject: &str) -> EntityId {
    let id = world.holder(subject).await;
    World::sign_in(&mut world.client, subject).await;
    id
}

#[tokio::test]
async fn a_link_adds_the_account_and_renews_only_the_acting_session() {
    let mut world = World::new();
    let ada = signed_in(&mut world, "ada").await;
    let mut other = world.client.fork();
    World::sign_in(&mut other, "ada").await;
    let state = location_state(world.client.get("/link").await);
    assert_eq!(state, "state-2");

    let reply = World::come_back(&mut world.client, "ada-work", &state).await;

    assert_eq!(location(&reply), "/account");
    assert_eq!(
        links_of(&world.events, ada).await,
        [google("ada"), google("ada-work")]
    );
    assert_eq!(world.proofs(), [LinkProof::Unproved]);
    assert_eq!(world.me().await, (StatusCode::OK, ada.to_string()));
    assert_eq!(other.get("/me").await.status, StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn a_proved_code_makes_a_proved_link() {
    let mut world = World::new();
    signed_in(&mut world, "ada").await;
    let state = location_state(world.client.get("/link?proved=true").await);

    World::come_back(&mut world.client, "ada-work", &state).await;

    assert_eq!(world.proofs(), [LinkProof::Proved]);
}

fn location_state(reply: support::Reply) -> String {
    location(&reply)
        .strip_prefix("https://provider.test/consent?state=")
        .unwrap()
        .to_owned()
}

#[tokio::test]
async fn a_proved_code_counts_for_ten_minutes() {
    let mut world = World::new();
    signed_in(&mut world, "ada").await;
    let on_time = location_state(world.client.get("/link?proved=true").await);
    world.clock.advance(TimeDelta::minutes(10));
    World::come_back(&mut world.client, "ada-one", &on_time).await;
    let late = location_state(world.client.get("/link?proved=true").await);
    world
        .clock
        .advance(TimeDelta::minutes(10) + TimeDelta::seconds(1));

    World::come_back(&mut world.client, "ada-two", &late).await;

    assert_eq!(world.proofs(), [LinkProof::Proved, LinkProof::Unproved]);
}

#[tokio::test]
async fn a_link_that_needs_a_code_sends_to_the_code_page() {
    let mut world = World::proving_links();
    let ada = signed_in(&mut world, "ada").await;
    let state = location_state(world.client.get("/link").await);

    let reply = World::come_back(&mut world.client, "ada-work", &state).await;

    assert_eq!(location(&reply), "/link-code");
    assert_eq!(links_of(&world.events, ada).await, [google("ada")]);
}

#[tokio::test]
async fn a_link_the_app_could_not_make_comes_back_to_its_page() {
    let mut world = World::new();
    let ada = signed_in(&mut world, "ada").await;
    let state = location_state(world.client.get("/link").await);

    let reply = World::come_back(&mut world.client, "taken", &state).await;

    assert_eq!(location(&reply), "/account");
    assert_eq!(links_of(&world.events, ada).await, [google("ada")]);
    assert_eq!(world.me().await, (StatusCode::OK, ada.to_string()));
}

#[tokio::test]
async fn a_link_belongs_to_the_flow_that_started_it() {
    let mut world = World::new();
    signed_in(&mut world, "ada").await;
    let bob = world.holder("bob").await;
    world.client.get("/link").await;
    let other_flow = World::start(&mut world.client, "").await;

    let reply = World::come_back(&mut world.client, "bob", &other_flow).await;

    assert_eq!(location(&reply), "/");
    assert_eq!(world.proofs(), []);
    assert_eq!(world.me().await, (StatusCode::OK, bob.to_string()));
}

#[tokio::test]
async fn a_link_is_used_once() {
    let mut world = World::new();
    let ada = signed_in(&mut world, "ada").await;
    let state = location_state(world.client.get("/link").await);
    World::come_back(&mut world.client, "ada-work", &state).await;

    World::sign_in(&mut world.client, "ada-other").await;

    assert_eq!(world.proofs().len(), 1);
    assert_eq!(
        links_of(&world.events, ada).await,
        [google("ada"), google("ada-work")]
    );
}

#[tokio::test]
async fn a_link_of_another_user_links_nothing() {
    let mut world = World::new();
    let ada = signed_in(&mut world, "ada").await;
    let bob = world.holder("bob").await;
    let state = location_state(world.client.get(&format!("/link?other={bob}")).await);

    let reply = World::come_back(&mut world.client, "ada-work", &state).await;

    assert_eq!(location(&reply), "/profile");
    assert_eq!(world.proofs(), []);
    assert_eq!(links_of(&world.events, bob).await, [google("bob")]);
    assert_eq!(links_of(&world.events, ada).await, [google("ada")]);
}

#[tokio::test]
async fn a_cancelled_consent_ends_the_link() {
    let mut world = World::new();
    signed_in(&mut world, "ada").await;
    let state = location_state(world.client.get("/link").await);
    let cancelled = world
        .client
        .get(&format!(
            "/oauth/callback?error=access_denied&state={state}"
        ))
        .await;

    World::sign_in(&mut world.client, "ada").await;

    assert_eq!(cancelled.body, "failed Cancelled /account");
    assert_eq!(world.proofs(), []);
}

#[tokio::test]
async fn a_guest_claims_the_account_and_keeps_its_session() {
    let mut world = World::new();
    let guest = World::guest(&mut world.client).await;
    let mut other = world.client.fork();

    let marked = world.client.get("/claim").await;
    assert_eq!(location(&marked), "/oauth/start?redirect_url=/profile");
    let state = World::start(&mut world.client, "?redirect_url=/profile").await;
    let reply = World::come_back(&mut world.client, "ada", &state).await;

    assert_eq!(location(&reply), "/profile");
    assert_eq!(
        links_of(&world.events, guest).await.last(),
        Some(&google("ada"))
    );
    assert_eq!(world.me().await, (StatusCode::OK, guest.to_string()));
    World::sign_in(&mut other, "ada").await;
    assert_eq!(other.get("/me").await.body, guest.to_string());
    assert_eq!(world.accounts.adopted.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn a_claim_is_used_once() {
    let mut world = World::new();
    let guest = World::guest(&mut world.client).await;
    world.client.get("/claim").await;
    let state = World::start(&mut world.client, "").await;
    World::come_back(&mut world.client, "ada", &state).await;

    World::sign_in(&mut world.client, "grace").await;

    assert_eq!(world.accounts.adopted.load(Ordering::SeqCst), 1);
    assert_ne!(world.me().await.1, guest.to_string());
}

#[tokio::test]
async fn a_cancelled_consent_ends_the_claim() {
    let mut world = World::new();
    let guest = World::guest(&mut world.client).await;
    world.client.get("/claim").await;
    let state = World::start(&mut world.client, "").await;
    world
        .client
        .get(&format!(
            "/oauth/callback?error=access_denied&state={state}"
        ))
        .await;

    World::sign_in(&mut world.client, "grace").await;

    assert_eq!(world.accounts.adopted.load(Ordering::SeqCst), 1);
    assert_ne!(world.me().await.1, guest.to_string());
}

#[tokio::test]
async fn a_link_ends_the_claim_marked_before_it() {
    let mut world = World::new();
    let guest = World::guest(&mut world.client).await;
    world.client.get("/claim").await;
    let state = location_state(world.client.get("/link").await);
    World::come_back(&mut world.client, "ada", &state).await;

    World::sign_in(&mut world.client, "grace").await;

    assert_eq!(world.accounts.adopted.load(Ordering::SeqCst), 1);
    assert_ne!(world.me().await.1, guest.to_string());
}

#[tokio::test]
async fn a_claim_of_another_user_claims_nothing() {
    let mut world = World::new();
    let guest = World::guest(&mut world.client).await;
    let bob = world.holder("bob").await;
    world.client.get(&format!("/claim?other={bob}")).await;
    let state = World::start(&mut world.client, "").await;

    let reply = World::come_back(&mut world.client, "ada", &state).await;

    assert_eq!(location(&reply), "/profile");
    assert_eq!(links_of(&world.events, bob).await, [google("bob")]);
    assert_eq!(links_of(&world.events, guest).await.len(), 1);
}

#[tokio::test]
async fn a_claim_the_app_refuses_comes_back_without_renewing() {
    let mut world = World::new();
    let guest = World::guest(&mut world.client).await;
    world.client.get("/claim").await;
    let state = World::start(&mut world.client, "?redirect_url=/profile").await;

    let reply = World::come_back(&mut world.client, "taken", &state).await;

    assert_eq!(location(&reply), "/profile");
    assert_eq!(links_of(&world.events, guest).await.len(), 1);
    assert_eq!(world.me().await, (StatusCode::OK, guest.to_string()));
}

#[tokio::test]
async fn a_flow_is_cheap_to_clone_and_prints_nothing_of_its_state() {
    let world = World::new();
    let engine = world.engine.as_ref().clone();

    assert_eq!(format!("{engine:?}"), "OAuthSignIn { .. }");
}
