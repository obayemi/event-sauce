//! The user aggregate, its credentials, the steps of its login and the
//! handlers that drive them, shared by the login and app integration tests.
//!
//! A user logs in with a password. One who enrolled a code is then asked for
//! it, then for the code mailed to them, and only then gets a session.

use std::sync::Arc;

use argon2::Params;
use async_trait::async_trait;
use axum::{extract::State, routing::post, Json, Router};
use event_sauce_auth::{
    Advance, Argon2Hasher, Credentials, Done, Email, Entered, Fingerprint, FirstFactor,
    HasPassword, Identity, Login, LoginId, LoginState, LoginTtl, Logins, Password, PasswordAttempt,
    PasswordHash, PasswordHasher, PasswordLogin, PendingLogins, Rejected, Route, Step,
    UserAggregate,
};
use event_sauce_axum::{AuthSession, LoginError, LoginStep, PendingLogin};
use event_sauce_core::{
    aggregate_command, define_events, Aggregate, AggregateClaim, AggregateError, Entity, EntityId,
    EventStore, Repository,
};
use secrecy::SecretString;
use serde::{Deserialize, Serialize};

pub const ADA: &str = "ada@example.com";
pub const PASSWORD: &str = "correct-horse";
pub const CODE: &str = "424242";
pub const MAIL_CODE: &str = "654321";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Account {
    pub password: Password<Email>,
    pub code: Option<String>,
}

impl Credentials for Account {
    fn claims(&self) -> Vec<AggregateClaim> {
        self.password.claims()
    }

    fn fingerprint(&self, fingerprint: &mut Fingerprint) {
        self.password.fingerprint(fingerprint);
        if let Some(code) = &self.code {
            fingerprint.str_field(code);
        }
    }
}

impl HasPassword for Account {
    type Login = Email;

    fn password(&self) -> Option<&Password<Email>> {
        Some(&self.password)
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct NeedCode;

impl LoginState for NeedCode {
    const TAG: &'static str = "fixture.code";
}

#[derive(Debug, Serialize, Deserialize)]
pub struct NeedMail {
    sent: String,
}

impl LoginState for NeedMail {
    const TAG: &'static str = "fixture.mail";
    const TTL: LoginTtl = LoginTtl::ONE_HOUR;
}

#[async_trait]
impl Step<User> for NeedCode {
    type Proof = String;
    type Verifier = ();
    type Next = Login<User, NeedMail>;

    async fn verify(
        advance: Advance<User, Self>,
        (): &(),
        code: String,
    ) -> Result<Login<User, NeedMail>, Rejected> {
        let enrolled = advance
            .login()
            .user()
            .identity()
            .credentials()
            .and_then(|account| account.code.as_ref());
        if enrolled == Some(&code) {
            Ok(advance.to(NeedMail {
                sent: MAIL_CODE.to_owned(),
            }))
        } else {
            Err(Rejected::Mismatch)
        }
    }
}

#[async_trait]
impl Step<User> for NeedMail {
    type Proof = String;
    type Verifier = ();
    type Next = Login<User, Done>;

    async fn verify(
        advance: Advance<User, Self>,
        (): &(),
        code: String,
    ) -> Result<Login<User, Done>, Rejected> {
        if code == advance.login().state().sent {
            Ok(advance.to(Done))
        } else {
            Err(Rejected::Mismatch)
        }
    }
}

pub enum AfterPassword {
    Done(Login<User, Done>),
    Code(Login<User, NeedCode>),
}

pub struct CodeIfEnrolled;

impl Route<User> for CodeIfEnrolled {
    type Next = AfterPassword;

    fn route(&self, entered: Advance<User, Entered>) -> AfterPassword {
        let enrolled = entered
            .login()
            .user()
            .identity()
            .credentials()
            .is_some_and(|account| account.code.is_some());
        if enrolled {
            AfterPassword::Code(entered.to(NeedCode))
        } else {
            AfterPassword::Done(entered.to(Done))
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum UserError {}

impl AggregateError for UserError {}

#[derive(Debug, Serialize, Deserialize)]
pub struct User {
    id: EntityId,
    identity: Identity<Account>,
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
    type Credentials = Account;

    fn identity(&self) -> &Identity<Account> {
        &self.identity
    }

    fn is_active(&self) -> bool {
        self.active
    }
}

define_events! {
    pub enum UserEvent for User {
        Registered {
            credentials: Account,
        }
        @init
        => |id, event| {
            User {
                id,
                identity: Identity::Registered(event.credentials.clone()),
                active: true,
            }
        },

        Anonymous {}
        @init
        => |id, _event| {
            User { id, identity: Identity::Anonymous, active: true }
        },

        PasswordChanged {
            hash: PasswordHash,
        }
        => |user, event| {
            if let Identity::Registered(account) = &mut user.identity {
                account.password.set_hash(event.hash.clone());
            }
        },

        SecondFactorEnrolled {
            code: String,
        }
        => |user, event| {
            if let Identity::Registered(account) = &mut user.identity {
                account.code = Some(event.code.clone());
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
        @clock @init fn register(credentials: Account) -> RegisteredEvent { credentials };
        @clock @init fn anonymous() -> AnonymousEvent { };
        @clock fn change_password(hash: PasswordHash) -> PasswordChangedEvent { hash };
        @clock fn enroll_second_factor(code: String) -> SecondFactorEnrolledEvent { code };
        @clock fn deactivate() -> DeactivatedEvent { };
    }
}

pub fn cheap_hasher() -> Argon2Hasher {
    Argon2Hasher::new(Params::new(8, 1, 1, None).unwrap())
}

pub async fn hash(password: &str) -> PasswordHash {
    cheap_hasher()
        .hash(&SecretString::from(password))
        .await
        .unwrap()
}

#[derive(Deserialize)]
pub struct LoginForm {
    pub email: String,
    pub password: String,
}

#[derive(Deserialize)]
pub struct CodeForm {
    pub code: String,
}

/// Saves Ada, with `code` as her second factor when there is one.
pub async fn register<S: EventStore + 'static>(store: &Arc<S>, code: Option<&str>) -> EntityId {
    let account = Account {
        password: Password::new(Email::parse(ADA).unwrap(), hash(PASSWORD).await),
        code: code.map(str::to_owned),
    };
    let mut user = User::register(account).unwrap();
    let id = user.entity_id();
    store.repository::<User>().save(&mut user).await.unwrap();
    id
}

/// Changes the password of the user `id` to `password`.
pub async fn change_password<S: EventStore + 'static>(
    store: &Arc<S>,
    id: EntityId,
    password: &str,
) {
    let hash = hash(password).await;
    store
        .repository::<User>()
        .modify(id, |user| user.change_password(hash.clone()))
        .await
        .unwrap();
}

pub type PasswordFactor = FirstFactor<PasswordLogin<Argon2Hasher>, CodeIfEnrolled>;

/// What the login handlers share: where logins start, where they wait, and
/// the factor they start with.
#[derive(Clone)]
pub struct Flow {
    pub logins: Logins<User>,
    pub pending: PendingLogins<User>,
    pub first: Arc<PasswordFactor>,
}

pub async fn login(
    mut auth: AuthSession<User>,
    State(flow): State<Flow>,
    Json(form): Json<LoginForm>,
) -> Result<LoginStep, LoginError> {
    let email = Email::parse(&form.email).map_err(|_| LoginError::refused())?;
    let attempt = PasswordAttempt::new(email, SecretString::from(form.password));
    match flow.logins.begin(&flow.first, attempt).await? {
        AfterPassword::Done(login) => auth.finish(login).await,
        AfterPassword::Code(login) => PendingLogin::hold(&flow.pending, login).await,
    }
}

pub async fn code(
    State(flow): State<Flow>,
    pending: PendingLogin,
    Json(form): Json<CodeForm>,
) -> Result<LoginStep, LoginError> {
    let login = flow.pending.resume::<NeedCode>(pending.id()).await?;
    let next = login.prove(&(), form.code).await?;
    PendingLogin::hold(&flow.pending, next).await
}

pub async fn mail(
    mut auth: AuthSession<User>,
    State(flow): State<Flow>,
    pending: PendingLogin,
    Json(form): Json<CodeForm>,
) -> Result<LoginStep, LoginError> {
    let login = flow.pending.resume::<NeedMail>(pending.id()).await?;
    let done = login.prove(&(), form.code).await?;
    auth.finish(done).await
}

pub fn login_routes() -> Router<Flow> {
    Router::new()
        .route("/login", post(login))
        .route("/login/code", post(code))
        .route("/login/mail", post(mail))
}
