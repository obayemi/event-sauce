# Proposal: pluggable auth and axum integration

Status: steps 1 to 23 of the build order (section 8) are **implemented**
on `feat/auth`. Steps 15 to 17 replaced the `ClaimAuthenticator`, `TwoFactor`
and second-factor builder stage that the earlier steps shipped with the
typestate logins of sections 3.4, 3.6, 4.2 and 4.4. Step 18 sealed the
session behind `AuthSession<U>`. Steps 19 to 23 added the five HTTP helpers
of section 4.5: error responses, login steps, path loading, the login
redirect and `Acting`. Where the code differs from the text, the text
describes the code. Supersedes the
"extract `event-sauce-auth` from saxy-p" plan in saxy-p's `PLAN.md`.

Two new optional crates:

- `event-sauce-auth`: a `user` marker on one aggregate, a typed login
  model, and typestate logins: a login is a `Login<U, S>` moving through
  states the app declares, and only a finished one yields the
  `Authenticated<U>` a session needs.
- `event-sauce-axum`: correlation-id middleware, a cached current-user
  extractor, an `AuthSession<U>` that opens sessions only from an
  `Authenticated<U>` over an `axum-login` backend the application cannot
  name, and state helpers.

Both are reached from the facade through the `auth` and `axum` features
and their sub-features (see section 5).

## 1. What saxy-p does today

The design is driven by what saxy-p, the first real consumer, does by hand.

| Concern | saxy-p today | Problem |
|---|---|---|
| Account state | `AuthState::{Anonymous, Registered{email: String, password_hash: Option<String>, google_sub: Option<String>}}` (`saxy-p-domain/src/profile/aggregate.rs:84`) | Email is an unvalidated `String`. A registered account can hold neither password nor Google link, which leaves it with no way to log in. Only one OAuth provider is supported. |
| Login lookup | Plaintext `projections.profile_auth(email, password_hash, …)` (`saxy-p-postgres/src/projections/profile_projection.rs:126`) | The password hash leaves the encrypted aggregate, so crypto-shredding a user leaves the hash behind. |
| Hashing | Hand-rolled `hash_password`/`verify_password` (`saxy-p-web/src/web/auth.rs:276`) | This is generic code living in app code. |
| Session invalidation | `session_auth_hash` = email bytes (`auth.rs:44`) | Changing the password does **not** log out other sessions. |
| Current user | axum-login `get_user` replays `Profile` (`auth.rs:118`), then `ActorGuard::resolve` replays it again (`saxy-p-application/src/service/actor.rs:20`) | At least two full replays per guarded request, with no cache. |
| Login call sites | `auth_session.login(&SessionUser{..})` is rebuilt by hand at 9 call sites | Duplication. |
| Correlation | `correlate` middleware (`saxy-p-web/src/web/request_id.rs:51`) | Generic code living in app code. |
| State | `AppState<S>` with about 19 `Arc` fields, `Clone` written by hand, no `FromRef` (`saxy-p-web/src/web/state.rs:15`) | Boilerplate. |

## 2. Goals and non-goals

Goals:

- Credentials live **inside** the user aggregate: encrypted, shreddable,
  event-sourced. They are never copied out in plaintext.
- Invalid account states cannot be represented in the types.
- The app chooses its login methods at the type level (password only,
  federated only, both, or its own) and is not locked into the
  library's choice. Every step of login can be swapped out: stored
  credentials, verification method, hasher, authenticator, federated
  linking policy.
- One user load per request, and none at all when the cache is hit.
- No session before the whole login process ends, however many steps or
  branches it has: a session needs an `Authenticated<U>`, and the types
  allow one only at the end of the process the app declared.

Non-goals:

- **Authorization roles.** `is_admin`, group membership and scopes stay in
  the app. The superseded plan put `is_admin`/`is_anonymous` into the
  library trait. That mixes authorization into authentication, and saxy-p
  already models authorization separately (`AccessScope`, 170 `@actor`
  validators).
- **Session storage.** That is tower-sessions' job.
- **Rate limiting and lockout.** Both can be added by wrapping a
  `LoginMethod`, or a step's verifier, in a decorator (see section 3.5).
  They are not part of v1.
- **Recording logins as events.** That would bloat the user stream with
  data that is neither domain state nor needed for replay. Login audit
  belongs in tracing or an audit sink.

## 3. `event-sauce-auth`

### 3.1 `Identity<C>`: pluggable credentials

The library does not choose how users log in. The aggregate's identity
field is generic over one **credentials type** `C`. `C` is a plain type
the app names: it is never assembled from combinators in the field.

```rust
/// How a user identifies and authenticates at login.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Identity<C: Credentials> {
    /// Holds a session, owns data, cannot log in with anything.
    Anonymous,
    /// Can log in with what `C` holds.
    Registered(C),
}
```

| App wants | Field type |
|---|---|
| password only | `Identity<Password<Email>>` |
| federated only | `Identity<Federated>` |
| password, federated, or both | `Identity<PasswordOrFederated<Email>>` |
| anything else (passkeys, magic links, password + passkeys…) | `Identity<MyCredentials>`, where the app defines `MyCredentials` |

Each credentials type carries its own identifier, because what you log
in *as* depends on the method: a password login names a `LoginId`, a
federated login names a provider subject. There is therefore no separate
login field that a federated-only app would have to fill with a dummy.

#### The `Credentials` trait

This is the only trait a credentials type must implement:

```rust
/// What a registered user stores to be able to log in.
pub trait Credentials:
    Clone + Eq + Debug + Serialize + DeserializeOwned + Send + Sync + 'static
{
    /// Uniqueness claims for the identifiers these credentials answer to.
    /// They make the identifiers unique and resolvable at login.
    fn claims(&self) -> Vec<AggregateClaim>;

    /// Feeds every secret and identifier that grants access, so that any
    /// change to them changes the session fingerprint.
    fn fingerprint(&self, fingerprint: &mut Fingerprint);
}
```

#### Capability traits

Login methods need to reach the password or the federated links of
whatever type the app picked. Each capability is one small accessor
trait, implemented by the types that have it:

```rust
/// Credentials that can hold a password login.
pub trait HasPassword: Credentials {
    type Login: LoginId;
    /// `None` when this user currently has no password.
    fn password(&self) -> Option<&Password<Self::Login>>;
}

/// Credentials that can hold federated links.
pub trait HasFederated: Credentials {
    /// `None` when this user currently has no linked provider.
    fn federated(&self) -> Option<&Federated>;
}
```

`PasswordLogin` requires `C: HasPassword`, and `FederatedLogin` requires
`C: HasFederated` (section 3.4). The consequences:

- Configuring a login method a type does not support fails to compile.
- An app-defined type that holds a password implements `HasPassword`
  with one method and gets `PasswordLogin` for free.
- A brand-new method, such as passkeys, comes with its own capability
  trait and `LoginMethod`, both defined in the app.

#### What the library ships

| Type | Shape | Claims | Capabilities |
|---|---|---|---|
| `Password<L: LoginId>` | `{ login: L, hash: PasswordHash }` | `"auth.login"` keyed by `L.as_ref()` | `HasPassword` (always `Some`), `SingleFactor` |
| `Federated` | `NonEmpty<FederatedId>` | one `"auth.federated"` per link, keyed by `[provider, subject]` | `HasFederated` (always `Some`), `SingleFactor` |
| `PasswordOrFederated<L>` | `enum { Password(Password<L>), Federated(Federated), Both(Password<L>, Federated) }` | those of what it holds | both, `SingleFactor` |

`SingleFactor` is a marker (section 3.6): these types hold nothing beyond
the first factor, so a login with them may end right after it. An app type
that holds a second factor does not implement it.

`PasswordOrFederated` is a concrete enum written out in the library, not
a generic combination. Its three variants make "registered with no way
to log in" unrepresentable.

The other value types:

- `FederatedId { provider: Provider, subject: String }`. `Provider` is a
  `Cow<'static, str>` newtype with constants such as `Provider::GOOGLE`,
  and replaces saxy-p's single `google_sub`.
- `PasswordHash`: a PHC string newtype. `Debug` is redacted, and the
  constructor only accepts PHC strings.
- `NonEmpty<T>`: an ordered, deduplicated set that is never empty
  (`nonempty` crate or a small local type).

`LoginId` is the identifier inside `Password`, and inside any app type
that needs one. The library ships `Email`, trimmed and lowercased with
its syntax checked, and `Username`:

```rust
pub trait LoginId:
    AsRef<str> + Clone + Eq + Debug + Serialize + DeserializeOwned + Send + Sync + 'static
{
    type ParseError: std::error::Error + Send + Sync + 'static;
    /// Parses raw user input, normalising it (trim, case-fold, …).
    fn parse(raw: &str) -> Result<Self, Self::ParseError>;
}
```

#### Rules

The rules are inherent methods on the concrete types. They are pure and
return `Result`, which keeps them unit testable. The app calls them from
its `define_events!` apply closures and `@validate` blocks.

| Method | Rule |
|---|---|
| `Identity::register(c)` | Moves `Anonymous` to `Registered`. Errors with `AlreadyRegistered` otherwise. |
| `Identity::credentials() -> Option<&C>` | `None` when anonymous. |
| `Identity::default()` | `Anonymous`, so a `user` aggregate without an init event starts anonymous. |
| `Identity::session_fingerprint() -> [u8; 32]` | SHA-256 over `C::fingerprint`. Any credential change invalidates other sessions. |
| `Password::set_hash(hash)`, `Password::change_login(l)` | Always allowed; the claim follows on commit. |
| `Federated::link(id)` | Errors with `AlreadyLinked`. |
| `Federated::unlink(&id)` | Removes exactly that link. Errors with `NotLinked` when it is not linked, and with `LastLoginMethod` when it is the only link. |
| `Federated::unlink_provider(&provider)` | Removes every link at that provider. Errors with `NotLinked` when none exists, and with `LastLoginMethod` when it would remove every link. |
| `PasswordOrFederated::set_password(p)` | Adds or replaces the password; `Federated` becomes `Both`. |
| `PasswordOrFederated::remove_password()` | Errors with `LastLoginMethod` unless federated links remain. |
| `PasswordOrFederated::link(id)` | Adds a link; `Password` becomes `Both`. |
| `PasswordOrFederated::unlink(&id)` / `unlink_provider(&provider)` | Same rules as on `Federated`; `LastLoginMethod` when removing would leave no way to log in, except that a `Both` user falls back to the password. |

The rules fail with `LoginMethodError`, whose variants are
`AlreadyLinked`, `NotLinked` and `LastLoginMethod`. `Identity::register`
fails with `IdentityError::AlreadyRegistered`.

Each type only exposes what makes sense for it. For example,
`Password<L>` has no `remove_password`, so a password-only app cannot
reach the "no way to log in" state at all.

An app-defined type writes its own rules in the same style. For example,
a password-plus-passkeys enum would mirror `PasswordOrFederated`. Those
few lines are the price of not composing types in the field.

For saxy-p, `AuthState` becomes `Identity<PasswordOrFederated<Email>>`.
The anonymous flow stays as it is (`AnonymousCreated`, then
`AccountClaimed`). A Google-only profile is
`Registered(PasswordOrFederated::Federated(..))`. Its contact email stays
in an app field, because it is not a login identifier until a password
is set.

What the library leaves to the app, whatever it chooses:

- **Normalisation.** For `Username`, `parse` defines trimming, case
  folding, the allowed character set and confusable rejection (`аdmin`
  with a Cyrillic `а`). Without it, two look-alike names both pass the
  uniqueness claim.
- **Federated linking policy.** Linking a new provider subject to an
  existing user by the provider's email is an app decision (section 3.4).
- **Reset delivery.** The `reset` module issues and verifies tokens but
  holds no contact address.

### 3.2 The `user` marker

```rust
#[aggregate(event = "ProfileEvent", error = "ProfileError",
            init, encrypted, claims, user, snapshots(every = 10))]
struct Profile {
    #[id] id: ProfileId,
    #[user(identity)] identity: Identity<PasswordOrFederated<Email>>,
    #[user(active)]  is_active: bool,     // optional: always active if absent
    display_name: String,
    // …
}
```

The `user` flag generates:

```rust
impl UserAggregate for Profile {
    type Credentials = PasswordOrFederated<Email>;
    fn identity(&self) -> &Identity<Self::Credentials> { &self.identity }
    fn is_active(&self) -> bool { self.is_active }
}
```

It also **adds `C::claims()`** to `claims()` while the user is
registered, next to the app's own claims from `aggregate_claims()`. So
uniqueness of logins and federated subjects comes for free, with
conflicts surfacing as today (`Error::as_claim_conflict`). saxy-p's
hand-built `Profile.email` claim (`aggregate.rs:175`) goes away.

Restrictions and implementation notes:

- `user` without `#[user(identity)]` is a compile error, and so is `user`
  on more than one field kind.
- The credential claim types (`"auth.login"`, `"auth.federated"`) are not
  prefixed by the aggregate type, so a store holds a single `user`
  aggregate. This is the case in every app considered.
- A field-less marker was rejected because it would leave the credential
  layout to convention.
- The attribute is parsed alongside `#[id]` in `parse_aggregate_attrs`
  (`event-sauce-macros/src/lib.rs:1338`) and stripped the same way
  (`:1377`).

### 3.3 New core primitive: `resolve_claim`

Login has to go from an identifier to an aggregate id without a plaintext
projection. Both backends already index claims as
`(claim_type, sha256(key)) -> aggregate_id` (postgres `aggregate_claims`,
memory claim index). Nothing exposes that index for reading yet.

```rust
// event-sauce-core, EventStore
/// Returns the aggregate currently holding `claim`, if any.
async fn resolve_claim(&self, claim: &AggregateClaim) -> Result<Option<EntityId>>;
```

Per the trait-design rules in `CLAUDE.md`, this has to be a new trait
method. It cannot be built on `load_stream`/`append`, because the index is
backend state. The memory and postgres backends implement it as one
indexed lookup.

The method is required, with no default body, so every store states what it
answers: a store that keeps no claim index returns `Ok(None)`, and a store
wrapping another one delegates it.

The lookup hashes the input, so no plaintext is stored. Shredding a user
leaves only hashes behind, as `docs/claims.md` already describes. The
`profile_auth` projection is no longer needed for login.

### 3.4 Login methods: verification, pluggable the same way

Storing credentials (`Credentials`) and checking a login attempt are
separate traits, so an app can swap how a method verifies (another hasher,
an LDAP bind) without touching what is stored.

```rust
#[async_trait]
pub trait LoginMethod<C: Credentials>: Send + Sync {
    /// What the client submits.
    type Attempt: Send + Sync;

    /// Finds the user this attempt targets. Credential methods resolve a
    /// claim through the `locator`; delegated methods (device links) ignore
    /// it and look the user up in their own store.
    async fn locate(&self, locator: &dyn ClaimLocator, attempt: &Self::Attempt)
        -> Result<Option<EntityId>, LocateError>;

    /// Checks the attempt against the located user's identity. The method
    /// decides whether `Identity::Anonymous` may log in this way: credential
    /// methods reject it, device links accept it.
    async fn verify(&self, identity: &Identity<C>, attempt: &Self::Attempt) -> Result<(), Rejected>;

    /// Runs when no user holds the claim. Defaults to doing nothing;
    /// password methods override it to equalise timing.
    async fn on_unknown(&self, _attempt: &Self::Attempt) {}
}
```

`ClaimLocator` is the one-method view of `EventStore::resolve_claim` that
`locate` needs:

```rust
#[async_trait]
pub trait ClaimLocator: Send + Sync {
    async fn locate_claim(&self, claim: &AggregateClaim) -> Result<Option<EntityId>, LocateError>;
}
// implemented for every `S: EventStore + ?Sized`
```

`LocateError` is an alias of `event_sauce_core::Error`. `Rejected` is
what every check of a login answers, a login method's `verify` as well as
a later step's (section 3.6). It has four variants: `Mismatch` (the proof
does not match what is stored), `Hash(HashError)`,
`LoginStore(LoginStoreError)`, and
`Unavailable(Box<dyn Error + Send + Sync>)` for an app verifier that could
not check the proof at all (a mailer or a device-link store that failed),
built with `Rejected::unavailable(error)`.

The library ships three:

- **`PasswordLogin<H: PasswordHasher>`** implements `LoginMethod<C>`
  for every `C: HasPassword`:
  - Its attempt is `PasswordAttempt<C::Login> { login, password: SecretString }`.
  - `verify` awaits `H::verify` against the stored hash.
  - `on_unknown` verifies against a fixed dummy hash, so the timing does
    not reveal which logins exist.
- **`FederatedLogin`** implements `LoginMethod<C>` for every
  `C: HasFederated`:
  - Its attempt is `VerifiedFederated { id, email: Option<Email> }`.
  - The app builds that attempt after its own OAuth callback has checked
    the provider's token. The name says the proof has already been done;
    the library runs no OAuth flow.
  - `verify` checks that the subject is linked.
- **`PasswordOrFederatedLogin<H>`** implements `LoginMethod<C>` for
  every `C: HasPassword + HasFederated`:
  - Its attempt is the concrete enum
    `PasswordOrFederatedAttempt { Password(PasswordAttempt), Federated(VerifiedFederated) }`.
  - It dispatches each attempt to the two methods above.
  - Like the credentials type, it is written out, not built from a
    generic `Either`.

An app with its own credentials type either reuses these, through the
capability traits, or implements `LoginMethod` for its own method.

saxy-p's QR device link is such an app method, `DeviceLinkLogin`
(written out in section 3.6):
- `locate` takes the confirmed link from saxy-p's `DeviceLinkStore`.
- `verify` accepts any identity, anonymous included, because the
  already-logged-in device vouched for it.

The QR code or number, the SSE and the confirmation between the two
devices stay app UX. The active check now applies to device links too,
which `get_profile_aggregate_unchecked` (`profile.rs:779`) skips today.

The hashing trait:

```rust
#[async_trait]
pub trait PasswordHasher: Send + Sync {
    async fn hash(&self, password: &SecretString) -> Result<PasswordHash, HashError>;
    async fn verify(
        &self,
        password: &SecretString,
        hash: &PasswordHash,
    ) -> Result<bool, HashError>;
}
// feature "argon2" (default): Argon2Hasher, with configurable params; it runs
// the Argon2 work on `tokio::task::spawn_blocking`.
```

A login method is only the **first step** of a login. It does not decide
whether the login is over: `FirstFactor::new(method, route)` pairs it with
a `Route` that picks, per user, the state the login moves to next, and
`Logins::begin` runs both (section 3.6):

1. `method.locate(&*store, &attempt)`: the event store is the locator.
2. If no user matches, run `on_unknown` and return
   `AuthError::Unknown { attempt }`. `Unknown` hands the attempt back so the
   app can apply its own policy:
   - for a federated attempt, link the subject to an existing user by
     email and call `begin` again, or create a profile and call `sign_up`
     (what saxy-p does in `auth.rs:161-202`);
   - for a password attempt, map it to `InvalidCredentials` before it
     reaches the client.
3. Load the user, which decrypts the aggregate. Snapshots bound the replay
   cost, and a configured user cache serves a version-checked hit.
   A located id whose user is gone or deleted is treated like no match.
4. `is_active()`, otherwise return `Inactive`. This check runs for every
   method.
5. `method.verify(identity, &attempt)`.
6. Pin the identity's session fingerprint and the start time, and hand the
   login to the route, which returns `Login<U, Done>` or a login in a
   further state. Nothing here is a session yet.

```rust
#[derive(Debug, thiserror::Error)]
pub enum AuthError<A> {
    #[error("invalid credentials")]    InvalidCredentials,
    #[error("account inactive")]       Inactive,
    #[error("no user for this login")] Unknown { attempt: A },
    #[error(transparent)]              Store(#[from] event_sauce_core::Error),
    #[error(transparent)]              Hash(#[from] HashError),
    #[error(transparent)]              LoginStore(#[from] LoginStoreError),
    #[error("the attempt could not be checked")]
                                       Unavailable(Box<dyn Error + Send + Sync>),
}
```

`Rejected` converts into `AuthError`: `Mismatch` becomes
`InvalidCredentials`, and the other three keep their name.

saxy-p's password-or-Google login becomes:

```rust
let password_or_google = FirstFactor::new(
    PasswordOrFederatedLogin::new(Argon2Hasher::default()),
    AlwaysDone,
);
let next: Login<Profile, Done> = logins.begin(&password_or_google, attempt).await?;
```

A password-only app passes `PasswordLogin` alone. Passing
`FederatedLogin` to a password-only `Profile` fails to compile, because
`Password<Email>` does not implement `HasFederated`. Routing a user whose
credentials can hold a second factor through `AlwaysDone` fails to compile
too (section 3.6).

### 3.5 Password reset tokens (optional `reset` module)

saxy-p's `pending_password_reset` generalises into
`ResetToken::issue() -> (ResetToken, ResetTokenHash)`:

- The token is 32 random bytes, encoded base64url, and shown to the user
  once.
- The hash is SHA-256 and is stored in an event, never the token itself.
- `ResetTokenHash::verify(&token, issued_at, now, ttl)`. The clock is a
  parameter, so the check is pure.

Lockout and rate limiting would be decorators of a `LoginMethod` or of a
step's verifier, for example `Throttled<M, T: AttemptStore>`, outside v1.

### 3.6 Typestate logins and the login store

A login can take more than one proof: a password then a TOTP code, a
TOTP code *or* a recovery code, a third factor for some users, or a step
another device performs for the user. Whatever the process, **no session
may open before it ends**. The library makes that a type error rather
than a convention: a session needs an `Authenticated<U>`, and the only way
to get one is to finish a login that reached a state the app marked
`Complete`.

**The types.**

```rust
/// A user who went through a whole login process. There is no public
/// constructor, no `Default`, no `Deserialize`: `Login::finish` is the only
/// way to get one.
pub struct Authenticated<U: UserAggregate> { /* private */ }

impl<U: UserAggregate> Authenticated<U> {
    pub fn user(&self) -> &Arc<AggregateRoot<U>>;
    pub fn into_user(self) -> Arc<AggregateRoot<U>>;
    /// The fingerprint the session is opened with (section 3.1).
    pub fn session_fingerprint(&self) -> [u8; 32];
}

/// A login in progress for one user, in the app-named state `S`.
///
/// It holds the loaded user, the session fingerprint pinned when the first
/// factor passed, the start time, and the state's data. There is no public
/// constructor, and it is neither `Clone` nor serialisable: a login moves
/// forward by value, once.
pub struct Login<U: UserAggregate, S> { /* private */ }

impl<U: UserAggregate, S> Login<U, S> {
    pub fn user(&self) -> &AggregateRoot<U>;
    pub fn state(&self) -> &S;
    pub fn started_at(&self) -> DateTime<Utc>;
}

impl<U: UserAggregate, S: Step<U>> Login<U, S> {
    /// Checks `proof` with `verifier` and moves the login to the state the
    /// step picks. The login is consumed: a wrong proof ends it.
    pub async fn prove(self, verifier: &S::Verifier, proof: S::Proof)
        -> Result<S::Next, Rejected>;
}

impl<U: UserAggregate, S: Complete> Login<U, S> {
    /// Re-reads the stored version. When it moved since the login loaded the
    /// user, reloads the user and checks it again: still there, still
    /// active, same fingerprint.
    pub async fn finish(self) -> Result<Authenticated<U>, FinishError>;
}
```

The app names its states. A state is plain data, serialised with the login
while the client is away:

```rust
/// A state a login can be in.
pub trait LoginState: Serialize + DeserializeOwned + Send + Sync + 'static {
    /// Unique among the app's states; checked when a saved login resumes.
    const TAG: &'static str;
    /// How long a login saved in this state waits for the client.
    const TTL: LoginTtl = LoginTtl::FIVE_MINUTES;
}

/// A state that ends the login process: `finish` exists only for these.
pub trait Complete: LoginState {}

/// The only way out of the state `Self`.
#[async_trait]
pub trait Step<U: UserAggregate>: LoginState {
    /// What the client sends: a code, an enum of alternatives, nothing.
    type Proof: Send;
    /// The service that checks it: `Totp`, a mailer, a device-link store.
    type Verifier: Send + Sync;
    /// Where the login may go: one `Login<U, T>`, or an app enum of them,
    /// picked per user.
    type Next: Send;

    async fn verify(advance: Advance<U, Self>, verifier: &Self::Verifier, proof: Self::Proof)
        -> Result<Self::Next, Rejected>;
}

/// The right to move one login out of the state `S`. Only the library
/// builds one: `Login::prove` hands it to the step of `S`, and
/// `Logins::begin` to the route after the first factor.
pub struct Advance<U: UserAggregate, S> { /* private */ }

impl<U: UserAggregate, S> Advance<U, S> {
    pub fn login(&self) -> &Login<U, S>;
    pub fn to<T: LoginState>(self, state: T) -> Login<U, T>;
}
```

The first step is a `LoginMethod` (section 3.4) paired with a `Route`.
Before it there is no user, so it is not a `Login` state:

```rust
/// Located and verified by the first factor, nowhere yet. It is not a
/// `LoginState`, so it can be neither saved nor finished.
pub struct Entered;

/// Picks, per user, where a login goes once the first factor passed.
pub trait Route<U: UserAggregate>: Send + Sync {
    type Next: Send;
    fn route(&self, entered: Advance<U, Entered>) -> Self::Next;
}

pub struct FirstFactor<M, R> { /* method, route */ }
impl<M, R> FirstFactor<M, R> {
    pub fn new(method: M, route: R) -> Self;
}
```

`Logins<U>` starts logins, and `PendingLogins<U>` keeps them between two
requests. Both are cheap to clone and carry no event store parameter: the
store sits behind a trait object, so `Login<U, S>` does not name it either.

```rust
impl<U: UserAggregate> Logins<U> {
    pub fn new<S: EventStore + 'static>(store: Arc<S>) -> Self
    where U::DeletedState: Serialize + DeserializeOwned;
    /// Loads users through the same version-checked cache as the backend.
    pub fn with_shared_cache(self, cache: Arc<dyn UserCache<U>>) -> Self;
    pub fn with_clock(self, clock: Arc<dyn Clock>) -> Self;

    /// Runs the first factor (section 3.4) and its route.
    pub async fn begin<M, R>(&self, first: &FirstFactor<M, R>, attempt: M::Attempt)
        -> Result<R::Next, AuthError<M::Attempt>>
    where M: LoginMethod<U::Credentials>, R: Route<U>;

    /// Commits a brand-new user and starts its login in `Done`. The commit
    /// expects the initial version, so it fails on an id that exists: this
    /// can never log into an account somebody already holds.
    pub async fn sign_up(&self, user: AggregateRoot<U>) -> Result<Login<U, Done>, SignUpError>;

    pub fn pending(&self, store: Arc<dyn LoginStore>) -> PendingLogins<U>;
}

impl<U: UserAggregate> PendingLogins<U> {
    /// Stores the login for `S::TTL` and returns the id the client keeps.
    pub async fn save<S: LoginState>(&self, login: Login<U, S>)
        -> Result<PendingLoginId, PendingLoginError>;
    /// Takes the login back, single use, if it was saved in the state `S`.
    pub async fn resume<S: LoginState>(&self, id: PendingLoginId)
        -> Result<Login<U, S>, ResumeError>;
}
```

`resume` performs these steps:
1. `take` the entry: unknown, expired or already taken is
   `ResumeError::NotInProgress`.
2. Compare its tag with `S::TAG`. A login saved in another state is
   `NotInProgress` too, and is used up: a client cannot replay it at
   another step.
3. Reload the user: missing, deleted or inactive is `Inactive`.
4. Compare the identity's fingerprint with the one pinned at the first
   factor. A password change, a new link or a new TOTP secret since then
   is `CredentialsChanged`, and the login starts over.

The errors: `ResumeError { NotInProgress, Inactive, CredentialsChanged,
Store, Pending }`, `FinishError { Inactive, CredentialsChanged, Store }`,
`SignUpError { NotNew, Store }` (a claim conflict surfaces through
`Error::as_claim_conflict`, as on any commit), and `PendingLoginError
{ Store, Encoding }` as today. `NotNew` means the aggregate holds no pending
event, or its version is not the initial one plus its pending events (it
was loaded), or its id exists. `UserAggregate` requires `'static`, so
`Logins` can keep the store behind a trait object.

**What the library ships.**

| Item | What it is |
|---|---|
| `Done` | The ready-made `Complete` state, tag `"event_sauce.done"`. |
| `SingleFactor` | Marker trait: credentials holding nothing beyond the first factor. `Password<L>`, `Federated` and `PasswordOrFederated<L>` implement it. |
| `AlwaysDone` | `Route<U>` with `Next = Login<U, Done>`, only for `U::Credentials: SingleFactor`. |
| `HasTotp` (feature `totp`) | `totp_secret() -> Option<&TotpSecret>` and `feed_totp_fingerprint`. It no longer extends a `HasSecondFactor`. |
| `Totp` (feature `totp`) | The verifier: `Totp::new(replays: Arc<dyn LoginStore>)`, `with_clock`, and `check(&secret, &code) -> Result<(), Rejected>`, which accepts a time step once through `mark_once`, so replaying a code writes nothing to the user's stream. Apps reuse `check` in their own steps. |
| `NeedTotp` (feature `totp`) | `Step<U>` for `U::Credentials: HasTotp`, tag `"event_sauce.totp"`: `Proof = SecretString`, `Verifier = Totp`, `Next = Login<U, Done>`. |
| `TotpIfEnrolled` (feature `totp`) | `Route<U>` with `Next = TotpOrDone<U>`, an enum `{ Done(Login<U, Done>), Totp(Login<U, NeedTotp>) }`. It fails closed: a user with any TOTP secret goes to `NeedTotp`, and only a user with none at all goes to `Done`. |

So a password step never sends a user with an enrolled second factor
straight to `Done`. `AlwaysDone` does not compile for credentials that
could hold one, unless the app asserts `SingleFactor` for them, and
`TotpIfEnrolled` routes on the secret itself.

**How the guarantee holds.** Each link of the chain is either a type or a
check the library performs:

| Link | Held by |
|---|---|
| A session built from a login needs an `Authenticated<U>` | type: `AuthSession::login` takes only an `Authenticated<U>`, and the `axum-login` backend and its `SessionUser` are crate-private, so no handler reaches `get_user` or the raw `login` (section 4.2) |
| An `Authenticated<U>` needs a finished login in a `Complete` state | type: private fields, `finish` bounded by `S: Complete` |
| A `Login` exists only after a proof | type: built only by `begin` (after `locate` and `verify`), `sign_up` (after committing a new user), `resume` (what `save` stored) and `Advance::to` |
| A login leaves a state only through that state's step | type: `Advance<U, S>` is built only by `prove` for `S: Step<U>`, and for `Entered` by `begin` |
| The states reachable from `S` are the ones `S::Next` names | type: the step returns `S::Next`, and the app reads its declaration |
| A login is used once | type: `prove`, `finish` and `save` consume it, and it is not `Clone`; at runtime, `take` is single use |
| A saved login resumes in the state it was saved in | runtime: tag check in `resume` |
| Nothing changed underneath | runtime: active and fingerprint checks in `resume` and `finish` |

**An app-defined three-factor flow.** A password, then a TOTP code or a
recovery code, then a code sent by email. Users without a second factor
enrolled stop after the password:

```rust
#[derive(Serialize, Deserialize)]
struct NeedSecond;
impl LoginState for NeedSecond { const TAG: &'static str = "app.second"; }

#[derive(Serialize, Deserialize)]
struct NeedEmailCode { code_hash: [u8; 32] }
impl LoginState for NeedEmailCode {
    const TAG: &'static str = "app.email_code";
    const TTL: LoginTtl = LoginTtl::ONE_HOUR;
}

enum AfterPassword {
    Done(Login<Profile, Done>),
    Second(Login<Profile, NeedSecond>),
}

struct ByEnrolment;
impl Route<Profile> for ByEnrolment {
    type Next = AfterPassword;
    fn route(&self, entered: Advance<Profile, Entered>) -> AfterPassword {
        match entered.login().user().identity().credentials() {
            Some(credentials) if !credentials.second_factor_enrolled() => {
                AfterPassword::Done(entered.to(Done))
            }
            _ => AfterPassword::Second(entered.to(NeedSecond)),
        }
    }
}

enum SecondProof { Totp(SecretString), Recovery(RecoveryCode) }

struct SecondVerifier { totp: Totp, recovery: RecoveryCodes, mailer: Mailer }

#[async_trait]
impl Step<Profile> for NeedSecond {
    type Proof = SecondProof;
    type Verifier = SecondVerifier;
    type Next = Login<Profile, NeedEmailCode>;

    async fn verify(advance: Advance<Profile, Self>, v: &SecondVerifier, proof: SecondProof)
        -> Result<Self::Next, Rejected>
    {
        let user = advance.login().user();
        let credentials = user.identity().credentials().ok_or(Rejected::Mismatch)?;
        match proof {
            SecondProof::Totp(code) => {
                let secret = credentials.totp_secret().ok_or(Rejected::Mismatch)?;
                v.totp.check(secret, &code).await?;
            }
            SecondProof::Recovery(code) => v.recovery.check(credentials, &code).await?,
        }
        let code_hash = v.mailer.send_code(user).await.map_err(Rejected::unavailable)?;
        Ok(advance.to(NeedEmailCode { code_hash }))
    }
}

#[async_trait]
impl Step<Profile> for NeedEmailCode {
    type Proof = SecretString;
    type Verifier = ();
    type Next = Login<Profile, Done>;

    async fn verify(advance: Advance<Profile, Self>, _: &(), code: SecretString)
        -> Result<Self::Next, Rejected>
    {
        let expected = &advance.login().state().code_hash;
        if !constant_time_eq(&sha256(code.expose_secret()), expected) {
            return Err(Rejected::Mismatch);
        }
        Ok(advance.to(Done))
    }
}

let password = FirstFactor::new(PasswordLogin::new(Argon2Hasher::default()), ByEnrolment);
```

Each HTTP request resumes the state its endpoint serves, proves, and
either saves the next state or finishes:

```rust
// POST /login
match logins.begin(&password, attempt).await? {
    AfterPassword::Done(login) => auth.finish(login).await,
    AfterPassword::Second(login) => PendingLogin::hold(&pending, login).await,
}
// POST /login/second
let login = pending.resume::<NeedSecond>(id).await?;
PendingLogin::hold(&pending, login.prove(&second, proof).await?).await
// POST /login/email
let login = pending.resume::<NeedEmailCode>(id).await?;
auth.finish(login.prove(&(), code).await?).await
```

`PendingLogin::hold` answers with the pending-login cookie for the login's
`S::TTL` (section 4.2). `AuthSession::finish` takes a `Login<Profile, S>`
whose state is `Complete`, so calling it with a login that waits for a step
does not compile. A branch
the client chooses (TOTP or recovery code) is one state whose `Proof` is
an enum, so each endpoint resumes exactly one state.

A recovery code is single use: `RecoveryCodes::check` records its hash
with `mark_once` to close the race between two logins, and the handler
commits `RecoveryCodeUsed` after `finish`. The app feeds the recovery code
*set*, not the used codes, to the fingerprint, so using a code does not
end the session it opens.

A WebAuthn step is the same shape: its state carries the challenge the
server issued, and its proof is the client's assertion.

**saxy-p's device link.** The new device has no user until the other
device confirms the link, so the link is a first factor, a `LoginMethod`
whose proof the logged-in device supplied:

```rust
struct DeviceLinkLogin { links: Arc<DeviceLinkStore> }

#[async_trait]
impl LoginMethod<PasswordOrFederated<Email>> for DeviceLinkLogin {
    type Attempt = DeviceLinkId;

    /// The confirming device recorded which profile vouched; taking the
    /// link uses it up.
    async fn locate(&self, _: &dyn ClaimLocator, link: &DeviceLinkId)
        -> Result<Option<EntityId>, LocateError>
    {
        self.links.take_confirmed(link).await
    }

    /// Any identity, anonymous included: the other device vouched for it.
    async fn verify(&self, _: &Identity<PasswordOrFederated<Email>>, _: &DeviceLinkId)
        -> Result<(), Rejected>
    {
        Ok(())
    }
}

let device_link = FirstFactor::new(DeviceLinkLogin { links }, AlwaysDone);
let login: Login<Profile, Done> = logins.begin(&device_link, link_id).await?;
```

The QR code, the SSE and the confirmation stay app UX. A link that should
only *add* a proof, for a user who already passed a password, is a later
step instead: a state `AwaitingDevice { link: DeviceLinkId }` whose
verifier checks that the link was confirmed for `advance.login().user()`.

**Limits.** The types guarantee the order of the process, not the quality
of each check:
- The app decides which states are `Complete`. Marking a state complete
  too early is a logic bug no type catches.
- A step's `verify` is code. The types make sure it ran; whether it checks
  the proof well is its own business. The library's steps are tested.
- The session record itself. No library call opens a session without an
  `Authenticated<U>`, but an app that writes `axum-login`'s record by hand
  through `tower_sessions::Session` can (section 4.2, limits).
- `SingleFactor` is an assertion by the credentials type. An app type that
  holds a second factor and implements it anyway gets `AlwaysDone` back.
- Across requests the binding is the server-side store plus a runtime tag
  check, not a type: the client holds only a random `PendingLoginId`. Tags
  must be unique within the app (the library's start with
  `event_sauce.`), and anyone who can write the login store directly can
  forge a pending login.
- `Advance::to` builds any state while the step runs. A step that smuggles
  a login out through a side channel instead of returning it defeats
  `Next`, which only states the intended edges.

**The login store.** In-progress login state goes in a small pluggable
store, separate from the event store:
- saved logins;
- consumed TOTP time steps and recovery codes;
- optionally, the app's device-link challenges.

It cannot live in the session, because device links span two devices and
TOTP replay protection is per user, not per session.

```rust
#[async_trait]
pub trait LoginStore: Send + Sync {
    /// Stores `value` under `key` until `ttl` elapses.
    async fn put(&self, key: &LoginKey, value: &[u8], ttl: LoginTtl) -> Result<(), LoginStoreError>;
    /// Atomically reads and deletes `key`: every entry is single use.
    async fn take(&self, key: &LoginKey) -> Result<Option<Vec<u8>>, LoginStoreError>;
    /// Records `key` if absent; `false` means it was already recorded.
    async fn mark_once(&self, key: &LoginKey, ttl: LoginTtl) -> Result<bool, LoginStoreError>;
}
```

`LoginKey` is a namespaced key such as `pending:{uuid}` or
`totp:{digest}:{step}`. `PendingLogins<U>` serialises
`{ tag, user, fingerprint, started_at, state }` as JSON over the raw
bytes, once for every backend, under `pending:{id}`. `PendingLoginId` is a
random UUID.

`put` and `mark_once` take a `LoginTtl`, a newtype validated at construction:
greater than zero, at most 366 days, rounded up to whole milliseconds
(`LoginTtl::new` fails with `LoginTtlError`). It has infallible constants
(`ONE_MINUTE`, `FIVE_MINUTES`, `ONE_HOUR`, `ONE_DAY`, `MAX`) and
`from_millis(NonZeroU32)`, so no store call fails on its ttl, and a state's
`TTL` is a constant. `take` has no ttl.

| Implementation | Feature | `take` | `mark_once` | Expiry |
|---|---|---|---|---|
| `PostgresLoginStore` (default) | `postgres` | `DELETE … RETURNING` | `INSERT … ON CONFLICT DO NOTHING` | `expires_at` filtered on read, plus `purge_expired()` for a periodic task |
| `RedisLoginStore` | `redis` | `GETDEL` (Redis 6.2 or newer) | `SET NX PX` | native TTL |

`RedisLoginStore` prefixes every key with `event_sauce:login:` unless
`with_prefix` says otherwise.

As decided, there is no in-memory login store. A login in progress must
survive across instances. The crate's unit tests use a private fake. A
login that never waits between requests (`AlwaysDone`) needs no login
store at all.

The Postgres store keeps its table in its own schema, `event_sauce_login`
by default, created by `PostgresLoginStore::migrate()`.

**What this replaces.** The typestate login deletes, in
`event-sauce-auth`: `Authenticator`, `ClaimAuthenticator`, `LoginOutcome`
(with `SecondFactorRequired`), `CompleteError`, `LoginStage`,
`NoSecondFactor`, `TwoFactor`, `SecondFactor`, `HasSecondFactor`, the
non-generic `PendingLogins`, the `PendingLogin<Ch>` record,
`DEFAULT_PENDING_TTL` and `AuthError::Pending`. `Totp` stops implementing
`SecondFactor` and becomes `NeedTotp`'s verifier, holding its replay store
itself. `Clock` stays, a small trait (`now() -> DateTime<Utc>`) with
`SystemClock` and the test double `FixedClock`: `Totp` reads the time step
from it and `Logins` the start of a login.
### 3.7 The user cache

Loading the current user goes through a pluggable cache:

```rust
#[async_trait]
pub trait UserCache<U: UserAggregate>: Debug + Send + Sync {
    /// Returns the cached user only if it is exactly at `version`.
    async fn get(&self, id: EntityId, version: StoredVersion)
        -> Result<Option<Arc<AggregateRoot<U>>>, CacheError>;
    /// Caches `user` at the version it was loaded at. A user with pending
    /// events, or a poisoned one, is skipped: a cache only holds users as stored.
    async fn put(&self, user: Arc<AggregateRoot<U>>) -> Result<(), CacheError>;
}
```

`UserCache` requires `Debug`, so the types holding one stay printable.

`CachedUserLoader<U, S>` is the piece that puts the cache to use, and is
what step 12 builds `get_user` on. `new(store)` loads without a cache,
`with_cache(cache)` or `with_shared_cache(arc)` adds one, and
`load(id) -> Result<Option<Arc<AggregateRoot<U>>>, Error>` does the
following:
1. Read the stored version (one cheap query). A version below
   `StoredVersion::FIRST` means no such user: `None`.
2. Serve a cached user at exactly that version.
3. Otherwise load in full and fill the cache. A missing or deleted user is
   `None`.
4. Return `None` for an inactive user.

A cache is an optimisation, never a source of failure: a cache error is
logged with `tracing` and the user is loaded in full.

Every read is checked against the version, so the caches never need
explicit invalidation. Every instance keeps its own Moka cache, or all of
them share Redis, without coordination. Deactivating or editing a user
bumps the version, and the next read misses.

| Implementation | Feature | Holds | Hit cost |
|---|---|---|---|
| `MokaUserCache` (default) | `moka` | `Arc<AggregateRoot<U>>` in process | one `get_version` query |
| `RedisUserCache` | `redis` | serialised aggregate, encrypted for encrypted users | one `get_version` query + one Redis read; + key lookup and decryption when encrypted |

A miss loads the aggregate: key lookup, latest snapshot, events after the
snapshot, then replay of those events.

**No Postgres user cache.** A Postgres hit would cost the same three
round trips as a snapshot load: version, cache row and key against key,
snapshot and tail. It would only save replaying the tail, and a snapshot
policy removes that tail at no cost. The user aggregate therefore
declares its own strategy with `#[aggregate(..., snapshots(every = 10))]`
(section 3.2). That attribute is a separate core feature, implemented on
its own bookmark. A store-level `per_type_override` still takes
precedence over it.

**`RedisUserCache` never holds plaintext of an encrypted user.** When
`U::is_encrypted()`, it encrypts the payload with the aggregate's own
key, through the store's `CryptoProvider` and `CryptoKeyStore`.
Crypto-shredding a user then also makes its cache entries unreadable, as
it does for events and snapshots. A missing key is a miss, not an error.

The cache finds that key and provider through the event store the users
live in, which the app passes with
`RedisUserCache::new(connection).with_crypto_from(store)`. Entries live for
one day by default, `with_ttl(LoginTtl)` changes it, and keys are prefixed with
`event_sauce:user:` unless `with_prefix` says otherwise.

The trait stays open, so an app can plug in another shared cache.

### 3.8 Dependencies and features

The crate depends on `event-sauce-core`, plus `async-trait`, `chrono`,
`email_address`, `phc`, `secrecy`, `serde`, `serde_json`, `sha2`,
`thiserror`, `tracing` and `uuid`. The optional dependencies come with
their feature.

| Feature | Brings |
|---|---|
| `argon2` (default) | `Argon2Hasher` (`argon2`) |
| `reset` | reset tokens (`base64`, `rand`, `subtle`) |
| `totp` | `Totp`, `HasTotp`, `NeedTotp` and `TotpIfEnrolled` (`totp-rs`, `hex`) |
| `moka` | `MokaUserCache` (`moka`) |
| `postgres` (default) | `PostgresLoginStore` (`sqlx`, workspace version) |
| `redis` | `RedisLoginStore`, `RedisUserCache` (`redis` crate, `zeroize`) |

These stores live here, not in `event-sauce-postgres`. That crate depends
on core only, and the stores depend on auth types.

## 4. `event-sauce-axum`

The crate depends on `axum 0.8`, `tower 0.5` and `event-sauce-core`. The
default `login` feature adds `event-sauce-auth` and `axum-login 0.18` (with
its `tower-sessions 0.14`). The crate asks `event-sauce-auth` for no default
features, so the application picks the hasher itself. The default `app`
feature adds `event-sauce-postgres` and `sqlx`, and turns on
`event-sauce-auth/postgres` and `event-sauce-auth/moka`.

### 4.1 Correlation layer

```rust
Router::new()
    .route(…)
    .layer(CorrelationLayer::new());              // header "x-request-id"
// .layer(CorrelationLayer::header(HeaderName::from_static("x-correlation-id")))
```

This lifts saxy-p's `correlate` into a `tower::Layer`:

- It reuses a well-formed incoming id or mints a UUID v4.
- It wraps the inner future in
  `with_event_context(event_context!(root, correlation_id = id))`.
- It echoes the header on the response.
- It opens a `tracing` span when the `tracing` feature is on. The feature
  is on by default, and `default-features = false` drops the dependency.

Every event committed during the request carries the correlation id,
because `EventMetadata.correlation_id` is filled from the context.

Caveat to document: the event context is a thread-local stack that is
re-entered on every poll (`event_context/stack.rs:21`). A `tokio::spawn`
inside a handler does **not** inherit it. The fix is
`fut.with_event_context(EventContext::current())`.

### 4.2 Login sessions and current user (feature `login`)

```rust
let cache: Arc<dyn UserCache<Profile>> = Arc::new(MokaUserCache::new(10_000));
let auth = AuthLayer::new(CachedUserLoader::new(store.clone()).with_shared_cache(cache.clone()));
let logins = Logins::<Profile>::new(store).with_shared_cache(cache);
let router = auth.layer(Router::new().route(…), session_layer);
```

A session opens only from an `Authenticated<U>`, and that is a type
guarantee, not a convention: the `axum-login` backend is sealed inside the
crate (step 18, section 8). The step 16 backend, `EventSauceBackend<U, S>`,
was named by applications, and its `get_user` and `renew` handed out the
`SessionUser` that `axum_login::AuthSession::login` takes; that was the hole
step 18 closed.

#### The sealed backend

The crate keeps one `axum-login` backend, `Backend<U>`, declared
`pub(crate)` in `login/backend.rs`. It is not re-exported, has no public
alias, and appears in no public signature, so an application can neither
name it nor obtain one.

- `pub(crate)`, not `pub` in a private module. A `pub` type in a private
  module is unnameable but still reachable: a public function may return it
  and only the allow-by-default `unnameable_types` lint notices. A
  `pub(crate)` type in a public signature trips `private_interfaces`
  (denied by `-D warnings`), and as the associated type of a public trait
  impl it is the hard error E0446. The compiler then refuses every leak of
  the backend's type, by signature or by associated type.
- The store type is erased: `Backend<U>` holds an
  `Arc<dyn LoadUser<U>>`, a crate-private object-safe trait implemented for
  every `CachedUserLoader<U, S>` and returning a boxed `Send` future. So the
  session and the extractors are keyed on the user aggregate alone, and
  nothing public needs a backend parameter.
- `User = SessionUser<U>`, now `pub(crate)` too, with the same fields and
  `AuthUser` impl: `id()` is the `EntityId`, `session_auth_hash()` the
  identity's session fingerprint computed once, so a credential change ends
  every session opened before it.
- `Credentials = Infallible`: the backend authenticates nothing, and
  `authenticate` can never be called and serves `axum-login`'s contract
  only. A session opens only through `AuthSession::login` below, which builds
  the `SessionUser` from an `Authenticated<U>` directly.
- `get_user(id)` goes through the `CachedUserLoader` and answers `None` for
  a missing, deleted or inactive user. A hit is version-checked, a miss or a
  mismatch is a full load that fills the cache, and without a cache every
  request is a full load.
- `Error` is `event_sauce_core::Error`: the backend has no failure of its own,
  so it has no error type of its own either.

#### `AuthLayer<U>`: the only way to install it

```rust
pub struct AuthLayer<U: UserAggregate> { /* backend: Backend<U> */ }

impl<U: UserAggregate> AuthLayer<U> {
    pub fn new<S>(loader: CachedUserLoader<U, S>) -> Self
    where S: EventStore + 'static, U::DeletedState: Serialize + DeserializeOwned;

    pub fn layer<St, Sess>(&self, router: Router<St>, sessions: SessionManagerLayer<Sess>) -> Router<St>
    where St: Clone + Send + Sync + 'static, Sess: SessionStore + Clone;
}
// Clone, and a Debug that prints nothing of the backend.
```

`layer` builds the `AuthManagerLayer` on the backend and `sessions` inside
the call and returns the `Router`, whose type erases both. `AuthLayer`
implements no `tower::Layer`: an impl's `Service` associated type would be
`AuthManager<_, Backend<U>>`, a projection a generic function can unify
with `AuthManager<_, B>` to recover `B`. The cache is configured on the
`CachedUserLoader` (`with_cache`, `with_shared_cache`), so `AuthLayer`
repeats none of its builder methods. This is the path of an application
that uses the `login` feature without `app`, and the one `App::layer`
delegates to (section 4.4).

#### `AuthSession<U>`: the session a handler holds

```rust
pub struct AuthSession<U: UserAggregate> { /* inner: axum_login::AuthSession<Backend<U>> */ }

impl<U: UserAggregate> AuthSession<U> {
    pub fn user(&self) -> Option<&Arc<AggregateRoot<U>>>;
    pub async fn login(&mut self, user: Authenticated<U>) -> Result<(), SessionError>;
    pub async fn renew(&mut self) -> Result<Option<Arc<AggregateRoot<U>>>, SessionError>;
    pub async fn logout(&mut self) -> Result<Option<Arc<AggregateRoot<U>>>, SessionError>;
}
impl<St: Send + Sync, U: UserAggregate> FromRequestParts<St> for AuthSession<U> {
    type Rejection = AuthRejection; // MissingLayer, 500, without the layer
}
// Debug prints the session's user id only; no Clone, Deref, AsRef or From
// towards the inner session.

#[derive(Debug, thiserror::Error)]
pub enum SessionError {
    Session(tower_sessions::session::Error),
    Store(event_sauce_core::Error),
}
```

- `login` consumes the `Authenticated<U>`, so one finished login opens one
  session. It builds the session user from it and runs `login` on the inner
  session, which cycles the session id when the session held no user yet.
- `renew` takes no argument: it reloads the user of the request's own
  session, which `axum-login` already checked against its fingerprint when
  the request began, and logs it in again with its current fingerprint.
  The session that changed its password or claimed an anonymous account
  survives the change, every other session of that user ends. It answers
  `None`, and ends the session, when nobody is logged in or the user is now
  gone or inactive. It never changes the user id.
- `logout` ends the session and answers the user it held.
- `SessionError` maps `axum_login::Error<Backend<U>>` through a private
  function, since a public `From` impl would name the backend and let a
  `SessionError: From<axum_login::Error<B>>` bound infer it.

`EventSauceBackend`, `UserBackend` and `SessionUser` leave
the public API, and `BackendError` with them, and so do `EventSauceBackend::new`, `with_cache`,
`with_shared_cache` and `renew` and `App::backend`.

A login handler finishes the login and opens the session at the end of the
process, and holds the login when the process needs another request
(helper 2, section 4.5):

```rust
async fn login(
    mut auth: AuthSession<Profile>,
    State(app): State<AppState>,
    Json(attempt): Json<PasswordAttempt<Email>>,
) -> Result<LoginStep, LoginError> {
    match app.logins.begin(&app.password, attempt).await? {
        TotpOrDone::Done(login) => auth.finish(login).await,
        TotpOrDone::Totp(login) => PendingLogin::hold(&app.pending, login).await,
    }
}

async fn totp(
    mut auth: AuthSession<Profile>,
    State(app): State<AppState>,
    pending: PendingLogin,
    Json(code): Json<TotpCode>,
) -> Result<LoginStep, LoginError> {
    let login = app.pending.resume::<NeedTotp>(pending.id()).await?;
    auth.finish(login.prove(&app.totp, code.0).await?).await
}

async fn change_password(
    mut auth: AuthSession<Profile>,
    user: Registered<Profile>,
    State(app): State<AppState>,
    Json(form): Json<NewPassword>,
) -> Result<StatusCode, ApiError> {
    app.profiles().modify(user.entity_id(), |p| p.change_password(form.hash()?)).await?;
    auth.renew().await?;
    Ok(StatusCode::NO_CONTENT)
}
```

`PendingLogin` carries the `PendingLoginId` between requests, in a
short-lived `HttpOnly`, `Secure`, `SameSite=Lax` cookie:
- `PendingLogin::hold(&pending, login)` saves the login and answers with the
  `Set-Cookie` of its id, which lives `S::TTL`, as long as the saved login.
  `PendingLogin::clear_cookie()` removes it when a login is abandoned, and
  `AuthSession::finish` clears it when the login ends. `PendingLogin::DEFAULT_TTL` is gone with `DEFAULT_PENDING_TTL`.
- As an extractor, `PendingLogin` rejects with `401` when the cookie is
  missing or malformed. `pending.id()` goes to `resume`.
- The cookie holds the id only. Which state the login is in, for which
  user, stays on the server, so a client cannot skip a step by editing it.

Extractors, generic over the user aggregate `U`, which is all they need to
find the `axum_login::AuthSession<Backend<U>>` the layer stored in the
request:

| Extractor | Yields | Rejection |
|---|---|---|
| `CurrentUser<U>` | `Arc<AggregateRoot<U>>`, which derefs to `AggregateRoot<U>` | 401 |
| `MaybeUser<U>` | `Option<Arc<AggregateRoot<U>>>` | never, but 500 when the auth layer is missing |
| `Registered<U>` | user whose `identity()` is `Registered` | 401 or 403 |
| `AuthSession<U>` | the session, to log in, renew or log out | 500 when the auth layer is missing |

All of them read the user axum-login already resolved for the request. So
there is exactly one load per request, or zero with a version-checked cache
hit. Because `CurrentUser` *is* an `AggregateRoot<U>`, it can be passed
straight to `@actor(U)` commands. That removes saxy-p's second replay in
`ActorGuard::resolve`.

Login helper: `auth.login(authenticated)` replaces the nine hand-built
`SessionUser { .. }` literals. Each of them now starts from
`Logins::begin`, `Logins::sign_up` or `AuthSession::renew`.

#### What is sealed, and how

Opening a session for a user takes `axum_login::AuthSession::login` with a
`SessionUser<U>`, and both need the backend's type. Every way to reach
them is closed by the compiler:

| Escape path | Closed by |
|---|---|
| `axum_login::AuthSession<Backend<U>>` as an extractor, `Extension<..>` or `parts.extensions.get::<..>()` | the backend is `pub(crate)`: no path, alias or re-export names it, from this crate or the facade |
| a generic handler `fn h<B: AuthnBackend>(s: axum_login::AuthSession<B>)` | nothing public carries `B`, so routing `h` is E0283, type annotations needed; `http::Extensions` cannot be enumerated |
| inferring `B` from a value: an `AuthManagerLayer<B, _>`, a `Layer::Service`, an `axum_login::Error<B>`, a `SessionError: From<axum_login::Error<B>>` bound | `AuthLayer::layer` and `App::layer` return a `Router`; no `Layer` impl; errors are `SessionError`, which maps the inner error through a private function, with no public impl over the backend |
| a bound `B: AuthnBackend<User = SessionUser<U>>`, or `B: UserBackend` | `SessionUser` is `pub(crate)` and `UserBackend` is deleted |
| a backend instance for `get_user`: `App::backend`, the `backend` field of `axum_login::AuthSession` | `App::backend` is deleted; `AuthSession<U>` keeps the inner session private, with no `Deref`, `AsRef`, `From` or `Clone` |
| a future signature leaking the type | `private_interfaces` (denied in CI) and E0446; `lib.rs` also denies `unnameable_types`, so a later move to a `pub` type in a private module fails too |
| `axum_login::login_required!(Backend)` and its siblings | they take the backend's type |

#### Limits

What the types guarantee: no library call opens a session without
consuming an `Authenticated<U>`, apart from `renew`, which re-keys only the
session the request already holds, for the same user. What they cannot:

- The session record. `tower_sessions::Session` is the application's own
  extractor, `axum-login` keeps its record under the public key
  `axum-login.data` as `{ user_id, auth_hash }`, and the hash is
  `Identity::session_fingerprint()`, which is public. An application that
  writes that record by hand logs anyone in: that is reimplementing
  `axum-login`, not calling this crate. A fingerprint keyed by a server
  secret would close it (section 9).
- `renew` during a concurrent credential change. A handler that calls it
  adopts whatever fingerprint the user has by then, so a session renewed
  right after a change made elsewhere survives that change. Call it only
  after the request's own credential change.
- An application's own `AuthnBackend`, `unsafe` and transmutes are out of
  scope. Its sessions do not feed `CurrentUser`, `MaybeUser`, `Registered`
  or `AuthSession`, which read only the sealed backend's session.

This deletes, in `event-sauce-axum`: the authenticator parameter `A` of
`EventSauceBackend` and `App`, `EventSauceBackend::start` and `complete`,
`BackendError::{SecondFactorRequired, Hash, Pending}`, and the two
`SessionUser` conversions from a raw aggregate (step 16); then the public
`EventSauceBackend`, `SessionUser`, `UserBackend`, `BackendError` and
`App::backend` (step 18).

### 4.3 State: extractors, not a registry

A runtime "store registry" (a `TypeId` map of repositories) was rejected.
Looking up an aggregate type nobody registered would fail at runtime, for
nothing in return: `EventSourcedRepository::new(Arc<S>)` is a cheap `Arc`
clone. Instead:

```rust
#[derive(Clone, FromRef)]
struct AppState {
    store: Store<PostgresEventStore>,   // newtype over Arc<S>
    commands: Arc<CommandRunner<…>>,
    // app services …
}

async fn handler(Repo(profiles): Repo<Profile, PostgresEventStore>, …) { … }
```

- `Store<S>` is a `Clone` newtype over `Arc<S>`.
- `Repo<A, S>` implements `FromRequestParts<St>` for every
  `St: FromRef<…>` that yields `Store<S>`. It builds the repository on the
  fly, so there is nothing to register and no failure mode.
- `UnitOfWork<T>` does the same for `Arc<UnitOfWorkStore<T>>`, whose
  per-aggregate registration is already typed and checked by the builder.

Most of saxy-p's `AppState` boilerplate goes away by itself: derive
`FromRef` (axum's `macros` feature), drop the hand-written `Clone`, and
hold one `Store<S>` instead of passing three repositories around.
saxy-p's handlers mostly go through `CommandRunner` and services, so
`Repo<A, S>` matters more for new apps than for saxy-p.

### 4.4 Simple app builder

`EventSauceApp` wires the common case in a few lines, with Postgres as
the default for everything:

```rust
let app = EventSauceApp::postgres(pool)
    .user::<Profile, _>(PasswordOrFederatedLogin::new(Argon2Hasher::default()))
    .route(TotpIfEnrolled)         // default: AlwaysDone
    .build()
    .await?;                       // runs every migration below

let router = app.layer(Router::new().route(…), session_layer);
// AppState holds `app.store()`, `app.logins()`, `app.pending()`,
// and `app.first_factor()`, all reachable via FromRef; handlers take
// `AuthSession<Profile>` and `CurrentUser<Profile>`
let totp = Totp::new(app.login_store());
```

`user::<U, _>(method)` returns a `UserAppBuilder<U, M, R = AlwaysDone>`,
and `.route(route)` swaps `R`. `build` requires
`M: LoginMethod<U::Credentials>` and `R: Route<U>`, so an app whose
credentials can hold a second factor does not compile until it names a
route that sends users to it: the default fails closed at compile time.

What `build` sets up by default, all on the same pool:

| Piece | Default | Postgres schema | Override |
|---|---|---|---|
| event store | `PostgresEventStore` | its existing schema | `.event_store(store)` |
| login store | `PostgresLoginStore`, behind `app.login_store()` | `event_sauce_login` | `.login_store_schema("…")` |
| user cache | `MokaUserCache`, 10 000 entries, shared by the backend and `Logins` | — | `.user_cache(RedisUserCache::new(conn))`, `.user_cache_capacity(n)`, `.no_user_cache()` |
| first factor | `FirstFactor` over the given login method, routed by `AlwaysDone` | — | `.route(route)` |
| logins | `Logins<U>` on the event store, and `PendingLogins<U>` in the login store | — | — |
| correlation | `CorrelationLayer` on `x-request-id` | — | `.correlation_header(HeaderName)` |

The login store gets its own schema. It can then be dropped, given
permissions, or excluded from backups independently of the event store.
The builder sets no snapshot policy: the user aggregate declares its own
(section 3.7). The `app` feature enables `event-sauce-auth/moka` and
`event-sauce-auth/postgres`.

The builder builds no step verifiers: the states after the first factor
are the app's, so their verifiers are too, built from what `App` exposes
(`Totp::new(app.login_store())` above). The builder does not take a custom
login store either: an app that needs a Redis login store builds
`AuthLayer`, `Logins` and `PendingLogins` by hand and applies the layers
itself, with `AuthLayer::layer` and `CorrelationLayer`.

`App<U, F, S>` replaces the authenticator parameter with the first factor
`F = FirstFactor<M, R>`. `UserAppBuilder::second_factor`, the
`SecondFactorStage` trait and `WithSecondFactor` are deleted: `.route`
covers what they did, and any number of steps after it.

`app.layer(router, session_layer)` applies, outermost first:
1. `CorrelationLayer`;
2. the app's tower-sessions layer;
3. the `axum-login` auth layer on the sealed backend, through the
   `AuthLayer<U>` that `App` holds in place of the backend.

The session layer stays an argument. The builder does not choose a
session store for the app. `App::backend` is deleted (step 18): the
backend is crate-private, so `App` exposes neither it nor its type, and
its `Debug` prints only the correlation layer. `App` is not `Clone` and has
no public field, so nothing else of it reaches the backend.

### 4.5 HTTP helpers (helpers 1 to 4 implemented, step 23 proposed)

Status: helpers 1 to 4 are **implemented** (steps 19 to 22), helper 5 is **proposed**, not built. The two examples and saxy-p repeat the same
HTTP glue around the crate: mapping store and login errors to statuses,
answering a login step, loading the aggregate a path names and checking who
may see it, redirecting a browser to the login page, and building a command
context from the current user. Each of the five helpers below removes one of
these, in build order: helper 2 answers refusals with helper 1's
`LoginError`, and helper 3 reuses helper 1's `DeletedStatus`.

None of them touches the seal of section 4.2. No helper opens a session
except `AuthSession::finish`, which goes through `Login::finish`, the only
source of an `Authenticated<U>`, and then through `AuthSession::login`. No
helper names or returns the backend, `SessionUser` or
`axum_login::AuthSession`. The helpers that read the current user call the
same crate-private `session_user` as `CurrentUser`.

Rejected, not built: a typed command extractor `Cmd<C>`, an event-log query
extractor, OAuth flow state, ETag/If-Match, CSRF, rate limiting,
idempotency keys, SSE and health checks. The examples keep their hand-written
cookie clients (`examples/axum-auth/tests.rs`, `examples/axum-crud/tests/harness.rs`):
`axum-test` would replace their cookie jar with a dev-dependency tree that
has to be audited, and that is not clearly simpler.

| # | Helper | Feature | Replaces |
|---|---|---|---|
| 1 | `StoreError`, `Split`, `SplitDomain`, `DeletedStatus`; `LoginError`, `SessionError: IntoResponse` | none; `login` for the login part | every app's own error-to-status table, and the 401s that hide outages |
| 2 | `AuthSession::finish`, `PendingLogin::hold`, `LoginStep` | `login` | the open/hold/refused glue of every login handler |
| 3 | `Found<A, S>`, `PathAggregate`; `Authorized<A, P, S>`, `Access<A>` (implemented) | none; `login` for `Authorized` | `Path` + `load` + owner check in each handler, saxy-p's load guards |
| 4 | `LoginRedirect`, `AuthRejection::Redirect`, `ReturnTo` (implemented) | `login`; `ReturnTo` needs none | saxy-p's `require_user`, `login_redirect`, `original_uri`, `SafeRedirect` |
| 5 | `Acting<Ctx>`, `FromUser` | `login` | saxy-p's `Acting` and its second actor load |

#### Helper 1: error responses (implemented)

The orphan rule forbids `impl IntoResponse for event_sauce_core::Error`, so
the crate wraps the errors in its own types.

**`StoreError`** (no feature; `src/store_error.rs`):

```rust
/// A store failure, answered with the status of its kind and no detail.
pub struct StoreError { failure: StoreFailure, deleted: DeletedStatus, source: Box<dyn Error + Send + Sync> }

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StoreFailure { NotFound, Deleted, Conflict, Internal }

/// How a deleted aggregate is answered: hidden like a missing one, or gone.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum DeletedStatus { #[default] NotFound, Gone }

impl StoreError {
    pub fn failure(&self) -> StoreFailure;
    pub fn status(&self) -> StatusCode;
    #[must_use] pub fn deleted_as(self, status: DeletedStatus) -> Self;
}
impl From<event_sauce_core::Error> for StoreError;
impl IntoResponse for StoreError;
impl std::error::Error for StoreError; // source() is the wrapped error

/// A failed operation split into the domain error, which goes back to the
/// application, and the store failure, which answers on its own.
pub enum Split<D> { Domain(D), Store(StoreError) }

pub trait SplitDomain {
    type Domain;
    fn split(self) -> Split<Self::Domain>;
}
impl<E> SplitDomain for ModifyError<E> { type Domain = E; }
impl<E> SplitDomain for ExecuteError<E> { type Domain = E; }
impl SplitDomain for UnitOfWorkError { type Domain = Box<dyn Error + Send + Sync>; }
```

| Source | `StoreFailure` | Status |
|---|---|---|
| `Error::NotFound` | `NotFound` | 404 |
| `Error::AggregateDeleted` | `Deleted` | 404, or 410 after `deleted_as(DeletedStatus::Gone)` |
| `Error::ConcurrencyConflict`, `Error::ClaimConflict` | `Conflict` | 409 |
| `ExecuteError::Lifecycle`/`UnitOfWorkError::Lifecycle` with `found: Active` (a create on an existing stream, a restore of an active aggregate) | `Conflict` | 409 |
| the same with `found: Missing` | `NotFound` | 404 |
| the same with `found: Deleted` | `Deleted` | 404 or 410 |
| `ExecuteError::Save` with a conflict left after the `RetryPolicy` gave up | `Conflict` | 409 |
| `ModifyError`/`ExecuteError` `Load` and `Save`, `UnitOfWorkError::Store` | as their inner `Error` | as above |
| any other `Error`, `UnitOfWorkError::{Unregistered, AlreadyCheckedOut, LeakedHandle, DeleteFailed}` | `Internal` | 500 |
| `Domain(e)` of the three | not a `StoreError`: `Split::Domain(e)` | the app's |

The body is a fixed text per status (`not found`, `gone`, `conflict`,
`internal error`) and never the error's `Display`, which names aggregate
types, ids and claim keys. With the `tracing` feature a 500 is logged with
its source at `error` level, a 409 at `debug`.

The deleted status is an option of the response, not of the error, because
the same `Error::AggregateDeleted` can be hidden on one route and gone on
another. `From` picks `NotFound` so that `?` never reveals a deletion
unasked; `deleted_as` opts in. Helper 3 reads the same `DeletedStatus` from
`PathAggregate::DELETED`.

The domain error goes back to the app because only the app knows its status
(a `NoteError::NotOwner` is 404, a `NoteError::EmptyTitle` 422):

```rust
impl<E> From<ModifyError<E>> for ApiError where Self: From<E> {
    fn from(error: ModifyError<E>) -> Self {
        match error.split() {
            Split::Domain(domain) => domain.into(),
            Split::Store(store) => Self::Store(store),
        }
    }
}
```

**`LoginError`** (`login`; `src/login/login_error.rs`):

```rust
/// Why a login did not go on: refused, or unavailable.
pub struct LoginError { failure: LoginFailure } // Refused | Unavailable(Box<dyn Error + Send + Sync>)

impl LoginError {
    /// A refusal the application decides itself, such as a malformed login id.
    pub fn refused() -> Self;
    pub fn is_refused(&self) -> bool;
}
impl<A> From<AuthError<A>> for LoginError;
impl From<FinishError> for LoginError;
impl From<ResumeError> for LoginError;
impl From<Rejected> for LoginError;
impl From<PendingLoginError> for LoginError;
impl From<SessionError> for LoginError;
impl IntoResponse for LoginError;
impl IntoResponse for SessionError; // 500, no detail
```

| Source | Answer |
|---|---|
| `AuthError::{InvalidCredentials, Inactive, Unknown}`, `Rejected::Mismatch`, `FinishError::{Inactive, CredentialsChanged}`, `ResumeError::{NotInProgress, Inactive, CredentialsChanged}`, `LoginError::refused()` | 401, body `login refused`, `Set-Cookie` clearing the pending login |
| `AuthError::{Store, Hash, LoginStore, Unavailable}`, `Rejected::{Hash, LoginStore, Unavailable}`, `FinishError::Store`, `ResumeError::{Store, Pending}`, `PendingLoginError`, `SessionError` | 500, body `internal error`, cookie untouched, logged with `tracing` |

Every refusal answers the same bytes, so a client cannot tell an unknown
email from a wrong password or an inactive account. A 500 leaves the pending
cookie alone: if the store failed before `take`, the login is still there to
retry, and if it failed after, the next attempt is `NotInProgress` and
clears it. An app that handles `AuthError::Unknown { attempt }` itself (the
federated sign-up of section 6, step 4) matches it before `?`.

Code this deletes:
- the 401 answered for outages: `examples/axum-crud/auth_routes.rs:69`
  (`finish().map_err(|_| ApiError::Unauthorized)`), `examples/axum-auth/app.rs:87`
  (`let Ok(authenticated) = login.finish().await else { 401 }`), and the
  `Err(_) => 401` arms of `tests/user_fixture/mod.rs`;
- in `examples/axum-crud/error.rs`, the variants `Unauthorized`, the old `Store`,
  `Login` and `Session`, `impl From<AuthError<A>>`, the store arms
  of `status()` and the body masking of `into_response`; `ApiError` keeps
  the domain variants, `Email`, `Hashing`, `Database` and `Store(StoreError)`;
- in `examples/axum-auth/app.rs`, the status `match`es of `register`,
  `login`, `logout` and `change_password`;
- `tests/extractors.rs:123`, which maps every load failure to 404, now
  returns `StoreError` (404 missing, 500 failing);
- in saxy-p, `ServiceError::from_execute`, `settle_conflict` and
  `unit_of_work_store_failure` (`saxy-p-application/src/command/error.rs`),
  whose lifecycle and conflict arms are the table above.

Tests:
- Unit, `src/store_error/tests.rs`: every row of the first table, with
  `deleted_as` both ways; `split` of each `ModifyError`, `ExecuteError` and
  `UnitOfWorkError` variant, the domain error returned unchanged; the body of
  a 500 does not contain the source's `Display`.
- Unit, `src/login/login_error/tests.rs`: every row of the second table; all
  refusals produce byte-identical responses with the clearing cookie; a 500
  sets no cookie.
- HTTP, `tests/login.rs`: a login against a user store that fails answers
  500 and opens no session, and an unknown email and a wrong password answer
  the same 401; `tests/extractors.rs`: a missing ticket 404, a failing store
  500.

As built, step 19 differs from the text above in four places:
- `LoginError` is a tuple struct over a private enum, so it gets its
  `Display` and `source` from `thiserror` and exposes only `refused` and
  `is_refused`; `thiserror` is therefore a plain dependency of the crate.
- `ApiError` in `examples/axum-crud/error.rs` keeps its `Hashing` variant:
  hashing a new password at registration is not a login step, so it has no
  `LoginError` to go through, and it answers `500` with no detail.
- `Lifecycle` mismatches keep their own source error, a private
  `LifecycleMismatch`, because an `ExecuteError<E>` cannot be boxed without
  bounds on `E`.
- The failing store of the HTTP tests is `tests/broken_store/mod.rs`, an
  `EventStore` whose every operation fails, shared by `tests/login.rs` and
  `tests/extractors.rs`.

#### Helper 2: login-step glue (implemented)

```rust
impl<U: UserAggregate> AuthSession<U> {
    /// Finishes `login` and opens the session for its user.
    pub async fn finish<S: Complete>(&mut self, login: Login<U, S>) -> Result<LoginStep, LoginError>;
}

impl PendingLogin {
    /// Saves `login` and answers with the cookie that carries it, alive for `S::TTL`.
    pub async fn hold<U: UserAggregate, S: LoginState>(
        pending: &PendingLogins<U>,
        login: Login<U, S>,
    ) -> Result<LoginStep, LoginError>;
}

/// Where a login request left the login: opened, or waiting for its next step.
#[must_use]
pub struct LoginStep { /* private: Opened | Pending { id, cookie } */ }

impl LoginStep {
    pub fn is_opened(&self) -> bool;
    pub fn pending(&self) -> Option<PendingLoginId>;
}
impl IntoResponse for LoginStep;      // Opened: 204 + clearing cookie; Pending: 202 + Set-Cookie
impl IntoResponseParts for LoginStep; // the Set-Cookie alone, for (step, Redirect::to(..))
```

| Outcome | Answer |
|---|---|
| `finish` succeeded | 204, pending cookie cleared |
| `hold` saved the login | 202, `Set-Cookie` with `Max-Age = S::TTL` |
| `Err(LoginError)` refused | 401, pending cookie cleared |
| `Err(LoginError)` unavailable | 500 |

Decisions:
- `LoginStep` has no `Refused` variant. A refusal is the `Err` of the
  handler's `Result<LoginStep, LoginError>`, which is already
  `IntoResponse`, so `?` works on every call of the flow.
- An opened step can only come from `AuthSession::finish`: `LoginStep` has
  private fields and no public constructor, and `finish` builds it only after
  `Login::finish` returned the `Authenticated<U>` and `login` consumed it.
  The seal is unchanged.
- `hold` lives on the cookie type, the one that knows the cookie, because a
  crate cannot add a method to `event_sauce_auth::PendingLogins`. `S` is
  inferred from the login, so the cookie's lifetime cannot disagree with the
  stored login's. That removes the turbofish of `set_cookie::<S>`, which the
  `PendingLogin` rustdoc example gets wrong (`::<Done>` for a login waiting
  for another step). `set_cookie` becomes private. `clear_cookie` stays
  public for an "abandon login" route.
- No extractor resumes a pending login. Extractors run before the handler
  and `Json` runs last, so a resume in an extractor would take the
  single-use login before the body is parsed, and a malformed body would
  burn it. `resume` stays in the handler body, after every extractor.
- `IntoResponseParts` lets an HTML login answer
  `(step, return_to.redirect())` (helper 4) and keep the cookie.

The flow of section 4.2 becomes:

```rust
async fn login(
    mut auth: AuthSession<Profile>,
    State(app): State<AppState>,
    Json(attempt): Json<PasswordAttempt<Email>>,
) -> Result<LoginStep, LoginError> {
    match app.logins.begin(&app.first, attempt).await? {
        TotpOrDone::Done(login) => auth.finish(login).await,
        TotpOrDone::Totp(login) => PendingLogin::hold(&app.pending, login).await,
    }
}

async fn totp(
    mut auth: AuthSession<Profile>,
    State(app): State<AppState>,
    pending: PendingLogin,
    Json(code): Json<TotpCode>,
) -> Result<LoginStep, LoginError> {
    let login = app.pending.resume::<NeedTotp>(pending.id()).await?;
    auth.finish(login.prove(&app.totp, code.0).await?).await
}

async fn logout(mut auth: AuthSession<Profile>) -> Result<StatusCode, SessionError> {
    auth.logout().await?;
    Ok(StatusCode::NO_CONTENT)
}
```

Code this deletes: `open_session`, `hold` and `refused` in
`tests/user_fixture/mod.rs`; the `begin`/`finish`/`login` chains of both
examples' `login`; the `Response` building and `set_cookie::<S>` calls in
`docs/axum.md`, section 4.2 above and the `PendingLogin` rustdoc; and
`auth.logout().await.unwrap()` in `docs/axum.md` ("Logging In and Out"),
which panics on a session store failure.

Tests:
- Unit, `src/login/login_step/tests.rs`: the opened response is 204 with
  the clearing cookie; a pending step for a test state with
  `TTL = LoginTtl::ONE_HOUR` sets `Max-Age=3600`, which proves `S` comes
  from the login; `IntoResponseParts` sets only the cookie.
- HTTP, `tests/login.rs`, on the rewritten fixture: the existing three-step
  flow answers 202, 202, 204 and opens one session, unchanged; a step-two
  request with a malformed body answers 422 and the same cookie then still
  finishes the login; a wrong code answers 401 with the clearing cookie and
  a replay of the cookie answers 401.
- trybuild, `tests/ui/`: `finish_an_unfinished_login.rs`,
  `auth.finish(login)` with a `Login<User, NeedCode>`, is E0277 (`NeedCode:
  Complete`); `build_a_login_step.rs`, a `LoginStep { .. }` literal, is E0451;
  `set_the_pending_cookie_by_hand.rs`, `PendingLogin::set_cookie::<Done>`,
  is E0624.

As built, step 20 differs from the text above in three places:
- `LoginStep` is a tuple struct over a private enum, so the `LoginStep { .. }`
  literal of the trybuild case is `LoginStep(..)`, refused with E0423 rather
  than E0451, and a second case, `open_a_login_step_by_hand.rs`, pins that
  its crate-private constructor `opened` is E0624 from outside. The
  constructor of a waiting step is `LoginStep::waiting::<S>(id)`, since
  `pending()` is the accessor.
- `PendingLogin::set_cookie` is `pub(super)`, visible to the `login` module
  and to nobody else, rather than private to `pending.rs`, because
  `LoginStep::waiting` builds the cookie from the state.
- `IntoResponseParts` of an opened step adds the clearing cookie, so a
  `(step, Redirect::to(..))` ends the pending login as well.


#### Helper 3: `Found<A, S>` and `Authorized<A, P, S>` (implemented)

```rust
/// An aggregate that a path parameter names.
pub trait PathAggregate: Aggregate + 'static {
    /// The path parameter that holds its id.
    const PARAM: &'static str = "id";
    /// How a deleted one is answered.
    const DELETED: DeletedStatus = DeletedStatus::NotFound;
}

/// The aggregate the request's path names, loaded once for the request.
pub struct Found<A: Aggregate, S> { /* Arc<AggregateRoot<A>>, PhantomData<fn() -> S> */ }
impl<A, S> Deref for Found<A, S> { type Target = AggregateRoot<A>; }
impl<A, S> Found<A, S> { pub fn arc(&self) -> &Arc<AggregateRoot<A>>; pub fn into_arc(self) -> Arc<AggregateRoot<A>>; }
impl<St, A, S> FromRequestParts<St> for Found<A, S>
where St: Send + Sync, Store<S>: FromRef<St>, S: EventStore + 'static, A: PathAggregate + (the bounds of `Repo`)
{ type Rejection = FoundRejection; }

pub enum FoundRejection {
    MissingParam(&'static str), // 500: the route has no such parameter, a wiring mistake
    BadId,                      // 400
    Store(StoreError),          // 404, 404/410 per A::DELETED, 500
}

/// Who may reach an aggregate of type `A`.
pub trait Access<A: Aggregate> {
    type User: UserAggregate;
    /// Answer for a user who may not: 404 hides that the aggregate exists.
    const DENIED: DeniedStatus = DeniedStatus::NotFound;
    /// Whether an anonymous identity is refused before `allows` runs.
    const REGISTERED: bool = false;
    fn allows(aggregate: &AggregateRoot<A>, user: &AggregateRoot<Self::User>) -> bool;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum DeniedStatus { #[default] NotFound, Forbidden }

/// The aggregate the path names, which the current user may reach under `P`.
pub struct Authorized<A: Aggregate, P: Access<A>, S> { /* Found<A, S>, Arc<AggregateRoot<P::User>> */ }
impl Deref for Authorized<A, P, S> { type Target = AggregateRoot<A>; }
impl Authorized<A, P, S> { pub fn user(&self) -> &Arc<AggregateRoot<P::User>>; pub fn found(&self) -> &Found<A, S>; }
impl FromRequestParts<St> for Authorized<A, P, S> { type Rejection = AccessRejection; }

pub enum AccessRejection { Auth(AuthRejection), Found(FoundRejection), Denied(DeniedStatus) }
```

`Found` and `PathAggregate` need no feature; `Access`, `Authorized` and
`AccessRejection` are behind `login`.

Implemented as written, with two differences: `Access` has no
`Send + Sync + 'static` supertraits, because a policy is a marker that is
never built and `Authorized` holds it as `PhantomData<fn() -> P>`, and
`Authorized` passes the deleted status to the load directly (`404` when
`P::DENIED` is `NotFound`, otherwise `A::DELETED`) instead of rewriting the
rejection afterwards. The trybuild case `write_through_found.rs` is E0596 (a
mutable borrow through a `Deref`), not E0594.

`Authorized` checks, in this order, and answers the first failure:

| Check | Failure | Answer |
|---|---|---|
| 1. a user is logged in | nobody | 401, or the login redirect of helper 4 |
| 2. `P::REGISTERED` and the identity is anonymous | anonymous | 403, or the registration redirect of helper 4 |
| 3. the path parameter `A::PARAM` | missing / not a UUID | 500 / 400 |
| 4. `load_any` through the cache | missing | 404 |
| | deleted | 404 when `P::DENIED` is `NotFound`, otherwise `A::DELETED` |
| | store failure | 500 |
| 5. `P::allows(&aggregate, &user)` | `false` | `P::DENIED`: 404 (default) or 403 |

Decisions on the open points:
- **The path parameter name** is the associated const `PathAggregate::PARAM`,
  default `"id"`, so a flat `/notes/{id}` route needs only
  `impl PathAggregate for Note {}`, and a nested
  `/groups/{group_id}/activities/{activity_id}` sets `PARAM` on each. It
  sits on the aggregate, not on `AggregateId`: an aggregate with raw
  `EntityId`s needs no id type, and `#[derive(AggregateId)]` lives in
  `event-sauce-macros`, which must not know axum. The value is read through
  `RawPathParams` and parsed as a UUID into an `EntityId`.
- **Read-only.** `Found` derefs to `&AggregateRoot<A>` and has no `DerefMut`,
  no public field and no save; writes keep using `Repo::modify` or
  `execute`, which reload and retry on conflicts. A handler that checks
  access with `Authorized` and writes with `modify` loads twice; that is the
  price of not caching a root that a concurrent writer may have moved on.
- **One load per request.** The loaded root is cached in the request
  extensions under a private `Cached<A, S>` key holding the id and the
  `Arc`, so an `Authorized` and a `Found` of the same aggregate in one
  handler, or a `Found` in an app's own extractor, load once. The key
  includes `S`, so two stores of the same aggregate never share an entry.
- **No user.** `Authorized` refuses before it loads anything, so an
  anonymous request cannot probe which ids exist. An anonymous identity
  (`Identity::Anonymous`) reaches `allows` unless `P::REGISTERED` is set.
- **Deleted and denied together.** A deleted aggregate cannot be shown to
  the policy (its `DeletedState` may not carry an owner), so a policy that
  hides existence also hides deletions: 404 whatever `A::DELETED` says.
- **The store type stays a parameter**, as in `Repo<A, S>`: a store found by
  aggregate type alone is the registry section 4.3 rejected. Apps alias it,
  `type Owned<A> = Authorized<A, Owner, PostgresEventStore>;`.
- **Ordering is a type requirement.** A handler that takes
  `Authorized<Note, Owner, S>` cannot run unless the user, the load and the
  check passed, and nothing else has to be layered or ordered. saxy-p's
  `load_group_guard`/`load_activity_guard` (`saxy-p-web/src/web/guards/load.rs`)
  stash a scope that `GetUser`, `GetGroup` and `GetActivity`
  (`extractors.rs:313-410`) read back, and a route that forgets the guard
  fails at runtime with "route not behind a load guard".
- **Limits.** `allows` is synchronous and sees one aggregate and the user.
  A check that needs a projection query (saxy-p's `AccessScope`) or a second
  aggregate (an activity's group) is the app's own extractor, composed from
  `Found` and `CurrentUser`, which share the cache.

```rust
impl PathAggregate for Note {}

struct Owner;
impl Access<Note> for Owner {
    type User = Member;
    fn allows(note: &AggregateRoot<Note>, member: &AggregateRoot<Member>) -> bool {
        note.ensure_owned_by(member).is_ok()
    }
}

type Owned = Authorized<Note, Owner, PostgresEventStore>;

pub async fn read(note: Owned) -> Json<NoteView> {
    Json(NoteView::from(&*note))
}
```

Code this deletes: in `examples/axum-crud/note_routes.rs`, the
`Path(id)`, `load`, `ensure_owned_by` and error of `read`, which becomes
infallible. `remove`, `replace` and `amend` keep `Repo`: they write, and the
events' own validation checks the owner, so `NoteError::NotOwner` keeps its
404 arm in `error.rs`. The
`ticket_title` handler of `tests/extractors.rs` becomes
`Found<Ticket, InMemoryEventStore>`. In saxy-p it replaces the load guards,
`GroupScope`/`ActivityScope` and the `Get*` extractors for every route whose
check is a function of the aggregate and the user.

Tests:
- Unit, `src/found/tests.rs`: parameter lookup (present, missing, not a
  UUID) on a `RawPathParams`-like list; the cache helper with a counting
  loader closure loads once for two extractions of the same id, twice for
  two ids, and keeps two stores apart; the deleted status table.
- Unit, `src/login/authorized/tests.rs`: the order and the answer of each
  row of the table above with a fake user and aggregate, including deleted
  plus `DENIED = NotFound` giving 404 under `DELETED = Gone`.
- HTTP, `tests/found.rs` on the in-memory store: 200, 400, 404, deleted 404
  and 410 (an aggregate with `DELETED = Gone`), a nested route reading the
  right parameter, a route without the parameter 500;
  `tests/login.rs`: owner 200, another member 404, a `Forbidden` policy 403,
  nobody 401, an anonymous identity under `REGISTERED` 403.
- trybuild: `write_through_found.rs`, assigning a field through `Found`, is
  E0594; `authorize_without_access.rs`, `Authorized<Note, Owner, S>` where
  `Owner` implements `Access<Member>` only, is E0277.

#### Helper 4: login redirect for HTML apps (implemented)

```rust
/// Sends a browser that is not logged in to the login page, keeping where it was going.
#[derive(Debug, Clone)]
pub struct LoginRedirect { /* login: ReturnTo, param: &'static str, register: Option<ReturnTo> */ }

impl LoginRedirect {
    pub fn to(login: ReturnTo) -> Self;                 // param "return_to"
    #[must_use] pub fn param(self, name: &'static str) -> Self;
    #[must_use] pub fn register_at(self, page: ReturnTo) -> Self;
}
impl<S> tower::Layer<S> for LoginRedirect { type Service = axum::middleware::AddExtension<S, Self>; }

pub enum AuthRejection {
    Unauthenticated, Forbidden, MissingLayer,
    /// 303 See Other to the login or registration page.
    Redirect { location: HeaderValue },
}

/// A same-origin path, safe to redirect to.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReturnTo(/* String */);
impl ReturnTo {
    pub const fn from_static(path: &'static str) -> Self; // panics on a target that is not local
    pub fn parse(raw: &str) -> Option<Self>;
    pub fn root() -> Self;                               // "/"
    pub fn as_str(&self) -> &str;
    pub fn redirect(&self) -> Redirect;                  // 303
}
impl Default for ReturnTo;          // root
impl fmt::Display for ReturnTo;     // the path
impl<'de> Deserialize<'de> for ReturnTo; // any string; a target that is not local becomes root
impl IntoResponse for ReturnTo;     // 303 to it
```

`LoginRedirect` and the new `AuthRejection` variant are behind `login`;
`ReturnTo` needs no feature.

| Extractor (with `LoginRedirect` installed) | Situation | Answer |
|---|---|---|
| `CurrentUser`, `Registered`, `Authorized`, `Acting` | nobody logged in | 303 to `{login}?{param}={original path and query, percent-encoded}` |
| `Registered`, `Authorized` with `REGISTERED` | anonymous identity | 303 to `register_at` with the same parameter, or 403 when unset |
| any | `MissingLayer` | 500, never a redirect |
| `MaybeUser` | any | never rejects for a missing user |
| without `LoginRedirect` | | 401 / 403, as today |

`ReturnTo::parse` accepts only a path that starts with `/`, not `//`, and
contains no `\` and no control character, the rules of saxy-p's
`safe_local_path` (`saxy-p-web/src/web/redirect.rs`): browsers read `\` as
`/` and drop tabs and newlines, so `/\host` and `/<TAB>/host` leave the
site. A scheme or host cannot start with `/`.

Decisions on the open points:
- **Configuration lives on the router, not on `AuthLayer` or the `App`
  builder.** `LoginRedirect` is a layer that puts itself in the request
  extensions, and the extractors read it there. An app applies it to the
  HTML sub-router and keeps 401 on its JSON API, on one `AuthLayer`. It
  carries no backend and adds nothing to `AuthLayer` or `App`, so the seal
  is untouched. Without it every extractor behaves exactly as today.
- **The original URI** is axum's `OriginalUri`, because `nest()` strips the
  mount prefix from `Parts::uri`; the extractors fall back to `Parts::uri`
  when it is absent (a request built in a test). Only the path and query are
  kept, never a scheme or host. The `login` feature turns on
  `axum/original-uri`, off under the crate's `default-features = false`.
- **Percent-encoding** uses `percent-encoding 2.3`, already in the
  dependency tree through `axum`, so the build adds no crate.
- **303 See Other**, so a POST to a guarded route becomes a GET of the
  login page. An htmx app that needs `HX-Redirect` maps
  `AuthRejection::Redirect { location }` in its own error type.
- **`ReturnTo` deserializes leniently**: an absent or rejected value is
  root, so a forged `return_to` never fails the login form and never leaves
  the site. A strict caller uses `ReturnTo::parse`.

After the login, an HTML handler answers
`Ok((auth.finish(login).await?, form.return_to.redirect()))`, which keeps
the cookie of helper 2.

As built: `ReturnTo` holds a `Cow<'static, str>`, so `from_static` is a `const fn`
that panics (at compile time in a `const`) with "not a local path"; its
validator is one `const fn` over the bytes, which also refuses the C1 controls
(`U+0080` to `U+009F`) that `char::is_control` refuses, a property test checks
it against `parse`. `LoginRedirect::refuse` is crate-private; `CurrentUser`,
`Registered` and `Authorized` call it on the rejection they would have
answered, `Authorized` only after the session check, so `MissingLayer` is
never redirected. The tests use `proptest` and `serde_urlencoded` as
dev-dependencies and axum's `query` feature in the HTTP test.

Code this deletes: nothing in the examples, which are JSON APIs. In saxy-p:
`original_uri`, `login_redirect`, `require_user`, `require_registered_user`
(`extractors.rs:160-205`) and `SafeRedirect`/`safe_local_path`
(`redirect.rs`), which `ReturnTo` replaces with its tests.

Tests:
- Unit, `src/return_to/tests.rs`: saxy-p's tables (local paths accepted;
  `//host`, `http://`, `evil.com`, `\\host` rejected; `/\host`, `/\t/host`,
  `/\n/host` rejected; control characters rejected; `%5C` and `%2F` kept);
  lenient deserialization from a query string; `from_static` panics on
  `//host`.
- Unit, `src/login/redirect/tests.rs`: the location for a path with a query
  and non-ASCII bytes; a custom `param`; registration with and without
  `register_at`.
- HTTP, `tests/login.rs`: a router nested under `/app` with `LoginRedirect`
  sends an anonymous GET of `/app/notes?x=1` to
  `/login?return_to=%2Fapp%2Fnotes%3Fx%3D1`; a sibling API router without it
  answers 401; `Registered` with an anonymous identity redirects to
  `register_at`; `MaybeUser` is not redirected; without `AuthLayer` the
  answer is 500.

#### Helper 5: `Acting<Ctx>`

```rust
/// A command context built from the user who acts.
pub trait FromUser: Sized + Send + 'static {
    type User: UserAggregate;
    fn from_user(user: Arc<AggregateRoot<Self::User>>) -> Self;
}

/// The command context of the current user, for `Repository::execute` and units of work.
pub struct Acting<Ctx>(pub Ctx);
impl<Ctx> Deref for Acting<Ctx> { type Target = Ctx; }
impl<St: Send + Sync, Ctx: FromUser> FromRequestParts<St> for Acting<Ctx> { type Rejection = AuthRejection; }
```

Behind `login`. Rejected with 401, or the login redirect of helper 4, when
nobody is logged in.

```rust
impl FromUser for AppCtx {
    type User = Profile;
    fn from_user(actor: Arc<AggregateRoot<Profile>>) -> Self {
        AppCtx::new(actor, Utc::now())
    }
}

async fn rename(Repo(groups): Repo<Group, Pg>, Acting(ctx): Acting<AppCtx>, ..) -> Result<StatusCode, ApiError> {
    groups.execute(id, &Rename { name }, &ctx, RetryPolicy::default()).await?;
    Ok(StatusCode::NO_CONTENT)
}
```

Decisions:
- **The user is an associated type**, not the trait parameter `FromUser<U>`
  of the first sketch: an extractor `Acting<Ctx>` must find `U` from `Ctx`
  alone, and a parameter would leave it unconstrained (E0207) or force
  `Acting<Ctx, U>`. A context acts for one user aggregate.
- **Synchronous and infallible.** saxy-p's `Acting`
  (`saxy-p-web/src/web/command/mod.rs:241`, 50 uses) is async and fallible
  only because it loads the actor a second time through
  `resolve_actor`, which fails for a missing or deactivated profile. Here
  the user comes from the session the backend already resolved, and
  `get_user` answers `None`, so 401, for a missing or inactive user. What
  remains is building a value.
- **`Arc` by value**, not `&AggregateRoot<U>`: a context that keeps the actor
  (saxy-p's `AppCtx` holds an `AggregateRoot<Profile>`) keeps the `Arc` and
  clones nothing; one that needs the root derefs it.
- A context that needs request data beyond the user (saxy-p's
  `delegating_to(organizers)`, taken from the loaded group) is built in the
  handler from `Acting` and `Authorized`/`Found`.

Code this deletes: nothing in the examples, whose commands are
`@actor(Member)` methods that take `CurrentUser` directly. In saxy-p, the
`Acting` extractor, its `loaded_actor` fallback and its second profile load.

Tests:
- HTTP, `tests/login.rs`: a handler taking `Acting<Ctx>` for a test context
  receives the logged-in user; nobody logged in is 401, and a redirect
  under `LoginRedirect`; a user deactivated since login is 401; a `Command`
  run by `execute` with the context sees that user.

Implemented as written. `FromUser` and `Acting` are in `src/login/acting.rs`,
and `Acting` reads the user through the same `logged_in` function as
`CurrentUser`, so the `303` of `LoginRedirect` applies unchanged. The tests
are in `tests/login.rs`; the example commands are `@actor(Member)` methods,
so neither example uses it. The "Command Contexts" section of `docs/axum.md`
sits after "Guarding Routes", where the other extractors of the user are.

## 5. Changes to existing crates

| Crate | Change |
|---|---|
| `event-sauce-core` | `EventStore::resolve_claim` |
| `event-sauce-memory`, `event-sauce-postgres` | implement `resolve_claim` against the existing claim index |
| `event-sauce-macros` | `user` flag, `#[user(identity)]`/`#[user(active)]` field attributes, `Credentials::claims()` merged into `claims()` |
| `event-sauce` facade | features `auth` (plus `auth-postgres`, `auth-moka`, `auth-redis`, `auth-reset`, `auth-totp`) and `axum` (plus `axum-login`, `axum-app`), all in `full`, re-exported as `event_sauce::auth` and `event_sauce::axum` |
| `.github/workflows/ci.yml` | new `feature-matrix-auth-axum` job that lints each feature of `event-sauce-auth`, `event-sauce-axum` and the facade alone |
| `event-sauce-axum` (steps 19 to 22 implemented, step 23 proposed) | the helpers of section 4.5; the `login` feature also turns on `axum/original-uri` and adds `percent-encoding`, already in the tree through `axum` |

## 6. Migrating saxy-p

1. Replace `AuthState` with `Identity<PasswordOrFederated<Email>>`,
   through an upcaster:
   - `Registered` with a password and a Google subject maps to `Both`;
   - with only one of them, to `Password` or `Federated`;
   - with neither, it has no valid form and is reported by the upcaster
     run before migrating.
   Add `user` and `#[user(identity)]`, and delete the hand-built email
   claim.
2. Run a one-off backfill of the new claim rows. The existing
   `Profile.email` claims become `auth.login`, and Google subjects become
   `auth.federated`. This is done by re-saving claims through a replay.
3. Replace `hash_password`/`verify_password` with `Argon2Hasher`. Existing
   PHC hashes stay valid.
4. Replace `AuthStore` with the `AuthLayer<Profile>` that `App` holds, and
   log in through `Logins::begin` with
   `FirstFactor::new(PasswordOrFederatedLogin::new(..), AlwaysDone)`.
   `PasswordOrFederated<Email>` is `SingleFactor`, so `AlwaysDone` compiles
   as long as saxy-p has no second factor; adding one means an app
   credentials type and a route, which the compiler then demands. Keep the
   Google policy on `AuthError::Unknown` for federated attempts: link the
   subject to the existing profile and call `begin` again, or create the
   profile and call `sign_up`.
5. Replace `CurrentUser`/`RegisteredUser`/`ActorGuard` with the crate
   extractors. Drop the second replay.
6. Turn the device-link login into `DeviceLinkLogin`, a `LoginMethod`
   whose `locate` takes the confirmed link from `DeviceLinkStore`, run as
   `FirstFactor::new(DeviceLinkLogin { .. }, AlwaysDone)` (section 3.6).
   This replaces the hand-rolled `get_profile_aggregate_unchecked` path,
   and the active check then applies to it.
7. Replace the nine `auth_session.login(&SessionUser { .. })` sites with
   `auth.login(authenticated)` on an `AuthSession<Profile>`. Each gets its
   `Authenticated` from a finished login: `begin` for password, Google
   and device link, `sign_up` for `AnonymousCreated` and Google sign-up,
   and `AuthSession::renew` after `AccountClaimed` or a password change
   in the session itself.
8. Wire everything through `EventSauceApp::postgres(pool)`. Its default
   login store creates the `event_sauce_login` schema, and the user cache
   is Moka, in process. This replaces most of `src/main.rs:139-349`.
9. Replace `request_id.rs` with `CorrelationLayer`.
10. Drop the `password_hash` column from `projections.profile_auth`.
    Delete the projection if nothing besides login reads it.
11. Once steps 19 to 23 land: map `ServiceError` through `StoreError` and
    `LoginError`; replace the load guards whose check is a function of the
    aggregate and the user with `Authorized`, `require_user` and
    `SafeRedirect` with `LoginRedirect` and `ReturnTo`, and `Acting` with
    `Acting<AppCtx>` (section 4.5).

## 7. Tests

The lowest level that can prove each behaviour, and only there:
The tests of the HTTP helpers, steps 19 to 23, are listed with each helper
in section 4.5.


- **Unit:**
  - `Identity`, `Password`, `Federated` and `PasswordOrFederated`
    transitions and every error, including `LastLoginMethod`.
  - An app-defined credentials type that implements `HasPassword` and
    logs in through `PasswordLogin`.
  - `LoginId` parsing and normalisation for `Email` and `Username`.
  - `session_fingerprint` changes on each credential change.
  - `ResetTokenHash::verify` with injected times.
  - `PasswordLogin`, `FederatedLogin` and `PasswordOrFederatedLogin` against fixed
    credentials, including the dummy-hash path for unknown logins.
  - `Logins::begin` against fakes:
    - inactive users are rejected for every method;
    - anonymous users are accepted only by methods that allow it;
    - an unknown attempt comes back in `Unknown`;
    - the route receives the located user and its pinned fingerprint.
  - `Logins::sign_up` commits a new user and refuses one that exists
    (`NotNew` for a root past its initial version, a conflict for an id
    taken meanwhile).
  - `Login` and `PendingLogins` against a fake login store:
    - `save` then `resume` in the same state restores the login, once;
    - `resume` in another state is `NotInProgress` and uses the entry up;
    - a credential change between `save` and `resume` is
      `CredentialsChanged`, and so is one between `begin` and `finish`;
    - a user deactivated meanwhile is `Inactive` at `resume` and `finish`;
    - a wrong proof consumes the login;
    - an app-defined three-step flow with a branching proof reaches
      `Done` only through every step.
  - `AlwaysDone` and `TotpIfEnrolled` routing, including a user with an
    unusable TOTP secret going to `NeedTotp`.
  - `Totp::check` with an injected clock, rejecting a reused step.
  - `MokaUserCache`: a version mismatch is a miss.
  - trybuild compile-fail cases: `#[user(identity)]` missing,
    `FederatedLogin` on a password-only user, `remove_password` on
    `Password<L>`, `finish` on a state that is not `Complete`, building
    `Login`, `Advance` or `Authenticated` outside the crate, `AlwaysDone`
    for credentials that are not `SingleFactor`, and
    `SessionUser::from(AggregateRoot<U>)` (replaced in step 18 by the
    sealing cases below).
- **Integration:**
  - `resolve_claim` on memory and postgres (testcontainers).
  - Login claims are enforced on commit.
  - `CorrelationLayer` and the extractors through `tower::ServiceExt::oneshot`.
  - `PostgresLoginStore` and `RedisLoginStore` (testcontainers):
    - `take` is single use under concurrent callers;
    - `mark_once` holds under concurrent callers;
    - expired entries are invisible.
  - `RedisUserCache` (testcontainers):
    - a version mismatch is a miss;
    - an encrypted user is stored only as ciphertext;
    - a shredded key turns a hit into a miss.
  - `EventSauceApp::postgres` creates the login schema and is idempotent
    on a second `build`.
  - `event-sauce-axum`'s `tests/login.rs`: a TOTP login across two
    requests through the `PendingLogin` cookie opens no session before the
    code, one after it, and none on a replayed cookie; `renew` keeps the
    session that changed its own password.
- **System:** the `axum-auth` example
  (`crates/event-sauce/examples/axum-auth/`, tests in `tests.rs`, run by
  `cargo test --examples`) drives register → login → guarded route →
  password change → old session rejected on the in-memory store, with
  `AlwaysDone` and no login store.
- **Step 18, the sealed session** (section 4.2), each at the lowest level
  that proves it:
  - Unit, in `login/auth_session/tests.rs`: `SessionError` from each
    `axum_login::Error` variant; in `login/backend/tests.rs`, against the
    crate-private backend: `get_user` through the erased loader answers
    `None` for a missing user, a stored user and a cache hit. A deleted or
    inactive user is the loader's own case, tested in `event-sauce-auth`, and
    the integration cases below cover the deactivation end to end.
  - Integration, `tests/login.rs` through `oneshot`, every handler on
    `AuthSession<User>` and `CurrentUser<User>`: `login(authenticated)`
    opens a session the next request resolves; `logout` ends it; `renew`
    after the request's own password change keeps that session and ends
    the others; `renew` with nobody logged in answers `None`; `renew` for a
    user deactivated meanwhile answers `None` and ends the session;
    `AuthSession<U>` without `AuthLayer` is rejected with `500`. The
    `src/app/tests.rs` cases that called `app.backend()` move to the
    backend's unit tests or go through `App::layer`.
  - trybuild, `tests/ui/`, one case per escape path, each failing for the
    reason in its `.stderr`:
    - `name_the_backend.rs` and `name_the_backend_by_path.rs`: `use
      event_sauce_axum::EventSauceBackend` is E0432, and
      `event_sauce_axum::login::Backend` is E0603;
    - `generic_backend_handler.rs`: `async fn h<B: AuthnBackend>(s:
      axum_login::AuthSession<B>)`, routed, is E0283;
    - `name_the_session_user.rs`: `event_sauce_axum::SessionUser` is
      E0432; it replaces `session_user_from_aggregate.rs` and
      `session_user_from_shared_aggregate.rs`, which it supersedes;
    - `login_with_an_aggregate.rs`, `login_with_a_shared_aggregate.rs` and
      `login_with_the_current_user.rs`: `AuthSession::login` given an
      `AggregateRoot<U>`, an `Arc<AggregateRoot<U>>` or the `CurrentUser`
      aggregate is E0308;
    - `infer_the_backend_from_the_session_error.rs`: a handler bounded by
      `SessionError: From<axum_login::Error<B>>`, routed, is E0277;
    - `reach_the_inner_session.rs`: `auth.inner` and `auth.backend` on an
      `AuthSession<U>` are E0616 and E0609;
    - `cast_the_session_to_axum_login.rs`: `AuthSession<U>` has no
      `AsRef` and no `Clone`, both E0599;
    - `backend_from_the_app.rs`: `app.backend()` is E0599.
  - The `axum-auth` and `axum-crud` examples run on `AuthLayer`,
    `AuthSession<Member>` and `CurrentUser<Member>`/`Registered<Member>`,
    with their system tests unchanged.

## 8. Build order

Each step is its own commit, test first. Steps 1 to 17 are on
`feat/auth`:

1. `resolve_claim` in core and both backends.
2. `event-sauce-auth`: `Credentials`, `Identity`, `LoginId`/`Email`.
3. `Password`, `Federated`, `PasswordOrFederated` and the capability
   traits.
4. Hasher, `LoginMethod` implementations and `ClaimAuthenticator`.
5. Macro `user` flag with credential claims.
6. `LoginStore` with the Postgres and Redis implementations.
7. `SecondFactor`, `Totp` and `complete`.
8. `UserCache`: `MokaUserCache`, then `RedisUserCache`.
9. `reset` module.
10. `event-sauce-axum`: `CorrelationLayer`.
11. `Store`/`Repo`/`UnitOfWork` extractors.
12. `EventSauceBackend`, `PendingLogin` and the user extractors.
13. `EventSauceApp` builder with the Postgres defaults.
14. Facade features, CI (Redis through testcontainers, so no service),
    docs (`docs/auth.md`, `docs/axum.md`) and the example.

Typestate logins replace what steps 4, 7, 12 and 13 built around the
authenticator. Each step deletes what it supersedes in the same commit:

15. `event-sauce-auth` core and library steps: `Authenticated`, `Login`,
    `LoginState`, `Complete`, `Step`, `Advance`, `Entered`, `Route`,
    `FirstFactor`, `Logins` (`begin`, `sign_up`), the generic
    `PendingLogins<U>` (`save`, `resume`), `ResumeError`, `FinishError`,
    `SignUpError`, `Rejected::Unavailable`; then `Done`, `SingleFactor`,
    `AlwaysDone`, and behind `totp` the reworked `Totp`, `NeedTotp` and
    `TotpIfEnrolled`. Deletes `Authenticator`, `ClaimAuthenticator`,
    `LoginOutcome`, `CompleteError`, `LoginStage`, `NoSecondFactor`,
    `TwoFactor`, `SecondFactor`, `HasSecondFactor`, the old
    `PendingLogins` and `PendingLogin<Ch>`, `DEFAULT_PENDING_TTL` and
    `AuthError::Pending`, with their tests (`tests/claim_authenticator.rs`
    and `tests/second_factor.rs` become `tests/login.rs`) and the compile-fail
    cases of section 7.
16. `event-sauce-axum`: `EventSauceBackend<U, S>` with
    `Credentials = Authenticated<U>` and `renew`, `SessionUser` built only
    from `Authenticated<U>`, `PendingLogin::set_cookie::<S>`, and the
    builder's `.route(..)` with `App` exposing `logins`, `pending`,
    `first_factor` and `login_store`. Deletes `start`, `complete`, the
    `A` parameter, `BackendError::{SecondFactorRequired, Hash, Pending}`,
    `PendingLogin::DEFAULT_TTL`, `UserAppBuilder::second_factor`,
    `SecondFactorStage` and `WithSecondFactor`.
17. The `axum-auth` example on `Logins`, the facade re-exports,
    `docs/auth.md`, `docs/axum.md`, `docs/architecture.md`, the rustdoc,
    and the `CHANGELOG.md` Unreleased entries, rewritten to describe
    typestate logins only.

The review of step 17 found that `get_user` and `renew` hand out the
`SessionUser` that `axum_login::AuthSession::login` takes, so a handler
holding the backend opened a session for any stored user. Step 18 closes
it in the type system (section 4.2) and deletes what it supersedes:

18. `event-sauce-axum`, test first, the trybuild cases of section 7
    before the code: the backend becomes the crate-private `Backend<U>`
    over an erased `dyn LoadUser<U>`; `SessionUser` becomes
    `pub(crate)` and `BackendError` is deleted for `event_sauce_core::Error`; `lib.rs` denies `unnameable_types`; new
    `AuthLayer<U>` (`new(CachedUserLoader<U, S>)`, `layer(router,
    sessions)`), `AuthSession<U>` (`user`, `login(Authenticated<U>)`,
    `renew`, `logout`, extractor) and `SessionError`; `CurrentUser`,
    `MaybeUser` and `Registered` keyed on `U`; `App` holds an
    `AuthLayer<U>` and `App::layer` delegates to it. Deletes the public
    `EventSauceBackend` with `new`, `with_cache`, `with_shared_cache` and
    `renew`, `UserBackend`, `App::backend`, the two
    `session_user_from_*` trybuild cases, and the convention wording of
    commit `f8937646`. Updates the `axum-auth` example (which wires the
    layers itself, the `login`-without-`app` path) and `axum-crud`, the
    rustdoc, `docs/auth.md`, `docs/axum.md`, `docs/architecture.md`,
    `CLAUDE.md` and the `CHANGELOG.md` Unreleased entries, which state the
    guarantee again as a type guarantee, list what is sealed and keep the
    limits of section 4.2: the hand-written
    `axum-login` session record, `renew` during a concurrent credential
    change, and an application's own `AuthnBackend`.

Helpers 1 to 4 of section 4.5 (steps 19 to 22) are built, the last is proposed.
Each step is its
own commit in `event-sauce-axum`, test first (the unit, HTTP and trybuild
cases its helper lists, seen failing for the right reason before the code),
deletes in the same commit the example and test code it supersedes, and
updates the rustdoc of what it touches, `docs/axum.md`, `docs/auth.md` where
logins are shown, the README, the `CHANGELOG.md` Unreleased entries, the
"User Aggregates and HTTP Standards" bullets of `CLAUDE.md` and section 4.5,
whose status line moves to "implemented" with the last one. Each passes the
feature matrix of the crate (none, `login`, `app`, `tracing` alone) and the
clippy of both axum examples with `--no-default-features` and their
`required-features`. The order follows the dependencies: 20 answers
refusals with 19's `LoginError`, 21 and 22 reuse 19's `DeletedStatus` and
`StoreError`, and 22 and 23 build on the rejection 21's `Authorized` shares
with `CurrentUser`.

19. Implemented. Error responses: `StoreError`, `StoreFailure`, `DeletedStatus`,
    `Split`, `SplitDomain` (no feature); `LoginError` and
    `impl IntoResponse for SessionError` (`login`). Rewrites
    `examples/axum-crud/error.rs` around `StoreError` and `LoginError`, the
    status `match`es of `examples/axum-auth/app.rs`, the `Err(_) => 401`
    arms of `tests/user_fixture/mod.rs` and the `map_err(|_| NOT_FOUND)` of
    `tests/extractors.rs`, and adds the failing-store login test.
20. Implemented. Login-step glue: `AuthSession::finish`, `PendingLogin::hold`,
    `LoginStep`; `PendingLogin::set_cookie` becomes private. Rewrites both
    examples' `login` and `logout`, the fixture's `open_session`, `hold`
    and `refused`, the `PendingLogin` rustdoc example (`::<Done>`), the
    multi-step flow and the `logout().await.unwrap()` of `docs/axum.md`,
    and the handlers of section 4.2. Adds the malformed-body test and the
    three trybuild cases.
21. Implemented. `Found<A, S>`, `PathAggregate`, `FoundRejection` (no feature), then
    `Access<A>`, `DeniedStatus`, `Authorized<A, P, S>`, `AccessRejection`
    (`login`). Moves `read` of `axum-crud` onto `Authorized` with an `Owner`
    policy and `tests/extractors.rs`'s `ticket_title` onto `Found`; adds a
    "Loading What the Path Names" section to `docs/axum.md`.
22. Implemented. `ReturnTo` (no feature), then `LoginRedirect` and
    `AuthRejection::Redirect` (`login`, which turns on `axum/original-uri`
    and adds `percent-encoding`). `AuthRejection` loses `Copy` and
    `status` takes `&self`. Adds an "HTML Apps" section to `docs/axum.md`.
23. Implemented. `FromUser` and `Acting<Ctx>` (`login`), with a "Command Contexts"
    section in `docs/axum.md` after "Guarding Routes".

## 9. Open questions

- **Several login ids for one password.** Logging in with either an
  email or a username, against the same password, needs a password type
  holding several identifiers, each contributing its own claim. Since
  `Credentials` and `HasPassword` are open traits, an app can define
  that type itself, so the library does not have to ship it in v1.
- **Claim namespacing.** Unprefixed `"auth.*"` claim types limit a store
  to one `user` aggregate. Prefixing them with the aggregate type would
  lift that, at the cost of `AggregateClaim` taking an owned claim type
  instead of `&'static str`.
- **One-round-trip loads.** The Postgres backend could fetch the key,
  the latest snapshot and the tail in a single query. That would speed
  up every aggregate load, including a user cache miss. It belongs in
  `event-sauce-postgres` and is out of scope for this proposal.
- **WebAuthn.** A step whose state carries the issued challenge makes it
  possible (section 3.6), but the crate does not ship one in v1.
- **A keyed session fingerprint.** `axum-login` stores a session's user id
  and auth hash in a record an application can write through
  `tower_sessions::Session`, and the hash is the public
  `Identity::session_fingerprint()`. Hashing it with a server secret
  before it becomes the session auth hash would make a hand-written record
  useless, at the cost of a secret every instance shares and a rotation
  that ends every session (section 4.2, limits).
