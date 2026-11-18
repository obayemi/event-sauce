# Proposal: pluggable auth and axum integration

Status: **implemented**. Every step of the build order (section 8) is on
`feat/auth`. Where the code deliberately differs from the first draft of
this proposal, the sections below describe the code. Supersedes the "extract `event-sauce-auth` from
saxy-p" plan in saxy-p's `PLAN.md`.

Two new optional crates:

- `event-sauce-auth`: a `user` marker on one aggregate, a typed login
  model, and a pluggable `Authenticator` trait for login.
- `event-sauce-axum`: correlation-id middleware, a cached current-user
  extractor, an `axum-login` backend, and state helpers.

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

Non-goals:

- **Authorization roles.** `is_admin`, group membership and scopes stay in
  the app. The superseded plan put `is_admin`/`is_anonymous` into the
  library trait. That mixes authorization into authentication, and saxy-p
  already models authorization separately (`AccessScope`, 170 `@actor`
  validators).
- **Session storage.** That is tower-sessions' job.
- **Rate limiting and lockout.** Both can be added by wrapping
  `Authenticator` in a decorator (see section 3.5). They are not part of
  v1.
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
| `Password<L: LoginId>` | `{ login: L, hash: PasswordHash }` | `"auth.login"` keyed by `L` | `HasPassword` (always `Some`) |
| `Federated` | `NonEmpty<FederatedId>` | one `"auth.federated"` per link, keyed by `[provider, subject]` | `HasFederated` (always `Some`) |
| `PasswordOrFederated<L>` | `enum { Password(Password<L>), Federated(Federated), Both(Password<L>, Federated) }` | those of what it holds | both |

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
    Clone + Eq + Debug + Serialize + DeserializeOwned + Send + Sync + 'static
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

`LocateError` is an alias of `event_sauce_core::Error`. `Rejected` is an
enum with three variants: `Mismatch` (the attempt does not match what is
stored), `Hash(HashError)` and `LoginStore(LoginStoreError)`.

The library ships three:

- **`PasswordLogin<H: PasswordHasher>`** implements `LoginMethod<C>`
  for every `C: HasPassword`:
  - Its attempt is `PasswordAttempt<C::Login> { login, password: SecretString }`.
  - `verify` runs `H::verify` against the stored hash.
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

saxy-p's QR device link is such an app method, `DeviceLinkLogin`:
- `locate` reads the confirmed link from saxy-p's `DeviceLinkStore`.
- `verify` accepts any identity, anonymous included, because the
  already-logged-in device vouched for it.

The QR code or number, the SSE and the confirmation between the two
devices stay app UX. The active check now applies to device links too,
which `get_profile_aggregate_unchecked` (`profile.rs:779`) skips today.

The hashing trait:

```rust
pub trait PasswordHasher: Send + Sync {
    fn hash(&self, password: &SecretString) -> Result<PasswordHash, HashError>;
    fn verify(&self, password: &SecretString, hash: &PasswordHash) -> Result<bool, HashError>;
}
// feature "argon2" (default): Argon2Hasher, with configurable params.
```

On top sits the `Authenticator` trait:

```rust
#[async_trait]
pub trait Authenticator<U: UserAggregate>: Send + Sync {
    type Attempt: Send + Sync;
    async fn authenticate(&self, attempt: Self::Attempt)
        -> Result<LoginOutcome<U>, AuthError<Self::Attempt>>;
}

pub enum LoginOutcome<U: Aggregate> {
    /// Fully authenticated: the caller may open a session.
    Authenticated(AggregateRoot<U>),
    /// First factor accepted; a second factor is required (section 3.6).
    SecondFactorRequired(PendingLoginId),
}
// `LoginOutcome` is `#[non_exhaustive]`.

#[derive(Debug, thiserror::Error)]
pub enum AuthError<A> {
    #[error("invalid credentials")] InvalidCredentials,
    #[error("account inactive")]    Inactive,
    #[error("no user for this login")] Unknown { attempt: A },
    #[error(transparent)]           Store(#[from] event_sauce_core::Error),
    #[error(transparent)]           Hash(#[from] HashError),
    #[error(transparent)]           Pending(#[from] PendingLoginError),
}
```

`Rejected` converts into `AuthError`: `Mismatch` becomes
`InvalidCredentials`, `Hash` stays `Hash`, and a login store failure
becomes `Pending`.

The default implementation is `ClaimAuthenticator<U, M, S, T = NoSecondFactor>`,
for any `M: LoginMethod<U::Credentials>` and any event store `S`. It is the
generic code every backend gets, and `ClaimAuthenticator::new(store: Arc<S>,
method)` builds it. `S` is inferred, so callers name only the user type. The
fourth parameter is the login stage of section 3.6.

1. `method.locate(&*store, &attempt)`: the event store is the locator.
2. If no user matches, run `on_unknown` and return `Unknown { attempt }`.
   `Unknown` hands the attempt back so the app can apply its own policy:
   - for a federated attempt, link the subject to an existing user by
     email, or create a profile (what saxy-p does in
     `auth.rs:161-202`);
   - for a password attempt, map it to `InvalidCredentials` before it
     reaches the client.
3. `load_by_id`, which decrypts the aggregate. Snapshots bound the replay
   cost.
4. `is_active()`, otherwise return `Inactive`. This check runs for every
   method.
5. `method.verify(identity, &attempt)`.
6. Ask the login stage `T` (section 3.6). If the user's credentials require
   a second factor, it stores a pending login and the outcome is
   `SecondFactorRequired`. Otherwise the outcome is `Authenticated`.

saxy-p's login becomes:

```rust
ClaimAuthenticator::<Profile, _, _>::new(
    store.clone(),
    PasswordOrFederatedLogin::new(Argon2Hasher::default()),
)
```

A password-only app passes `PasswordLogin` alone. Passing
`FederatedLogin` to a password-only `Profile` fails to compile, because
`Password<Email>` does not implement `HasFederated`.

### 3.5 Password reset tokens (optional `reset` module)

saxy-p's `pending_password_reset` generalises into
`ResetToken::issue() -> (ResetToken, ResetTokenHash)`:

- The token is 32 random bytes, encoded base64url, and shown to the user
  once.
- The hash is SHA-256 and is stored in an event, never the token itself.
- `ResetTokenHash::verify(&token, issued_at, now, ttl)`. The clock is a
  parameter, so the check is pure.

Lockout and rate limiting would be `Authenticator` decorators, for
example `Throttled<A, T: AttemptStore>`, outside v1.

### 3.6 Multi-step logins and the login store

**Second factors.** A second factor (TOTP, WebAuthn) is stored in the
app's credentials type and verified by a `SecondFactor` implementation:

```rust
/// Credentials that may demand a second factor after the first succeeds.
pub trait HasSecondFactor: Credentials {
    fn requires_second_factor(&self) -> bool;
}

#[async_trait]
pub trait SecondFactor<C: HasSecondFactor>: Send + Sync {
    /// Sent to the client: nothing for TOTP, a signed nonce for WebAuthn.
    type Challenge: Serialize + DeserializeOwned + Send + Sync;
    type Response: Send + Sync;

    fn challenge(&self, stored: &C) -> Self::Challenge;
    async fn verify(&self, stored: &C, challenge: &Self::Challenge,
                    response: &Self::Response, store: &dyn LoginStore)
        -> Result<(), Rejected>;
}
```

The library ships `Totp` behind the `totp` feature:
- The secret lives in the app's credentials type, through a `HasTotp`
  accessor, and is part of `fingerprint()`. Enrolling a second factor
  therefore logs out other sessions.
- `verify` rejects a time step that was already used, through
  `LoginStore::mark_once`. This replay protection writes nothing to the
  user's event stream.
- `Totp::with_clock` injects the `Clock` that picks the time step.
  `Clock` is a small trait (`now() -> DateTime<Utc>`) with `SystemClock`
  and the test double `FixedClock`. Everything that reads the time takes
  one: `Totp`, and `TwoFactor` for `PendingLogin::created_at`.

WebAuthn is left to the app or a later release. The `Challenge` type is
what makes it possible.

**The login stage.** The fourth parameter of `ClaimAuthenticator` is a
`LoginStage<C>`, which decides after the first factor whether the login is
complete:

```rust
#[async_trait]
pub trait LoginStage<C: Credentials>: Send + Sync {
    /// `Some(id)` when a pending login was stored, `None` when the login is complete.
    async fn begin(&self, user: EntityId, identity: &Identity<C>)
        -> Result<Option<PendingLoginId>, PendingLoginError>;
}
```

- `NoSecondFactor` is the default stage. It never holds a login back, so an
  app whose credentials implement `HasSecondFactor` must not keep it.
- `TwoFactor<F>` holds back the logins whose credentials report
  `requires_second_factor()`. It is opt-in:
  `.with_second_factor(TwoFactor::new(factor, login_store))` turns a
  `ClaimAuthenticator<U, M, S>` into a
  `ClaimAuthenticator<U, M, S, TwoFactor<F>>`, which compiles only for
  `U::Credentials: HasSecondFactor` and `F: SecondFactor<U::Credentials>`.
  A pending login lives five minutes by default, `with_pending_ttl(ttl)`
  changes it, and `with_clock(clock)` injects the clock.

**Completing the login.** `complete(pending_id, response)` exists on the
`TwoFactor` variant only, so an authenticator with no second factor cannot
call it. It returns `Result<AggregateRoot<U>, CompleteError>`, and
performs these steps:
1. `take` the pending login from the store. A pending login is single
   use and expires; an unknown, expired or already taken one is
   `CompleteError::NotInProgress`.
2. Reload the user and re-check that it is active (`Inactive` otherwise).
3. Run `SecondFactor::verify`; a wrong response is `InvalidResponse`. The
   pending login was already taken, so a wrong response ends the login and
   the user starts again from the first factor.
4. Return the user.

`CompleteError` also carries `Store`, `Hash` and `Pending`. No session
exists before step 4. The pending login is not a partial session.

**The login store.** In-progress login state goes in a small pluggable
store, separate from the event store:
- pending second factors;
- consumed TOTP time steps;
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
`totp:{user}:{step}`. `PendingLogins`, which is not generic, wraps an
`Arc<dyn LoginStore>` and a ttl, and serialises
`PendingLogin<Ch> { user, challenge, created_at }` as JSON over the raw
bytes, once for every backend. `PendingLoginId` is a random UUID.

`put` and `mark_once` take a `LoginTtl`, a newtype validated at construction:
greater than zero, at most 366 days, rounded up to whole milliseconds
(`LoginTtl::new` fails with `LoginTtlError`). It has infallible constants
(`ONE_MINUTE`, `FIVE_MINUTES`, `ONE_HOUR`, `ONE_DAY`, `MAX`) and
`from_millis(NonZeroU32)`, so no store call fails on its ttl. `take` has no
ttl.

| Implementation | Feature | `take` | `mark_once` | Expiry |
|---|---|---|---|---|
| `PostgresLoginStore` (default) | `postgres` | `DELETE … RETURNING` | `INSERT … ON CONFLICT DO NOTHING` | `expires_at` filtered on read, plus `purge_expired()` for a periodic task |
| `RedisLoginStore` | `redis` | `GETDEL` (Redis 6.2 or newer) | `SET NX PX` | native TTL |

`RedisLoginStore` prefixes every key with `event_sauce:login:` unless
`with_prefix` says otherwise.

As decided, there is no in-memory login store. A login in progress must
survive across instances. The crate's unit tests use a private fake.

The Postgres store keeps its table in its own schema, `event_sauce_login`
by default, created by `PostgresLoginStore::migrate()`.

### 3.7 The user cache

Loading the current user goes through a pluggable cache:

```rust
#[async_trait]
pub trait UserCache<U: UserAggregate>: Debug + Send + Sync {
    /// Returns the cached user only if it is exactly at `version`.
    async fn get(&self, id: EntityId, version: AggregateVersion)
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
1. Read the stored version (one cheap query). The initial version means no such
   user: `None`.
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
| `totp` | `Totp` second factor (`totp-rs`, `hex`) |
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
// .layer(CorrelationLayer::header("x-correlation-id"))
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

### 4.2 Login backend and current user (feature `login`)

```rust
let authenticator = ClaimAuthenticator::<Profile, _, _>::new(
    store.clone(),
    PasswordLogin::new(Argon2Hasher::default()),
);
let backend = EventSauceBackend::new(authenticator, store)
    .with_cache(MokaUserCache::new(10_000));    // any UserCache<U>
let auth_layer = AuthManagerLayerBuilder::new(backend, session_layer).build();
```

`EventSauceBackend<U, A, S>` implements `axum_login::AuthnBackend`. `A` is
the authenticator and `S` the event store the users live in:

- `Credentials = A::Attempt`: the submitted form is whatever the chosen
  login methods accept (`PasswordOrFederatedAttempt` for
  saxy-p).
- `User = SessionUser<U>`, which shares an `Arc<AggregateRoot<U>>` and
  implements `AuthUser`:
  - `id()` is the `EntityId`.
  - `session_auth_hash()` is `identity().session_fingerprint()`, computed
    once when the `SessionUser` is built. This fixes the bug where changing
    a password left other sessions logged in.
- `authenticate` delegates to the `Authenticator`. A wrong password, an
  unknown login and an inactive user are `Ok(None)`, as axum-login expects.
- `get_user(id)` goes through a `CachedUserLoader` (section 3.7) and
  returns `None` for a missing, deleted or inactive user.

`get_user` goes through the configured `UserCache`:
- A hit is checked against the version, so it never serves a stale or
  deactivated user.
- A miss, or a version mismatch, means a full load, which then fills the
  cache.
- Without `with_cache`, every request does a full load, as today.

Second factor: `authenticate` fails with
`BackendError::SecondFactorRequired(id)` when the outcome is
`SecondFactorRequired`, so no session is opened. A handler that serves such
logins calls `backend.start(attempt) -> LoginOutcome` instead, and, once the
client answers, `backend.complete(pending, response) -> SessionUser<U>`.
`complete` exists only when the authenticator is a
`ClaimAuthenticator` with a `TwoFactor` stage.

`PendingLogin` carries the pending id between the two requests, in a
short-lived `HttpOnly`, `Secure`, `SameSite=Lax` cookie:
- `PendingLogin::set_cookie(id, ttl: LoginTtl)` is the `Set-Cookie` value the handler
  answers with after `start`, and `PendingLogin::clear_cookie()` removes it
  once the login is done.
- As an extractor, `PendingLogin` rejects with `401` when the cookie is
  missing or malformed. `pending.id()` goes to `complete`.
- The handler calls `auth_session.login` only after `complete` succeeds.

Extractors, generic over the backend type `B` (a `UserBackend`, which every
`EventSauceBackend` is) so that they find the `AuthSession<B>` axum-login
stored in the request, and know the aggregate it yields:

| Extractor | Yields | Rejection |
|---|---|---|
| `CurrentUser<B>` | `Arc<AggregateRoot<U>>`, which derefs to `AggregateRoot<U>` | 401 |
| `MaybeUser<B>` | `Option<…>` | never, but 500 when the auth layer is missing |
| `Registered<B>` | user whose `identity()` is `Registered` | 401 or 403 |

All three read the user axum-login already resolved for the request
(`AuthSession::user`). So there is exactly one load per request, or zero
with a version-checked cache hit. Because `CurrentUser` *is* an
`AggregateRoot<U>`, it can be passed straight to `@actor(U)` commands.
That removes saxy-p's second replay in `ActorGuard::resolve`.

Login helper: `auth_session.login(&SessionUser::from(root))` replaces the
nine hand-built `SessionUser { .. }` literals.

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
    .build()
    .await?;                       // runs every migration below

let router = app.layer(Router::new().route(…), session_layer);
// AppState holds `app.store()` and `app.backend()`, both reachable via FromRef
```

What `build` sets up by default, all on the same pool:

| Piece | Default | Postgres schema | Override |
|---|---|---|---|
| event store | `PostgresEventStore` | its existing schema | `.event_store(store)` |
| login store | `PostgresLoginStore` | `event_sauce_login` | `.login_store_schema("…")` |
| user cache | `MokaUserCache`, 10 000 entries | — | `.user_cache(RedisUserCache::new(conn))`, `.user_cache_capacity(n)`, `.no_user_cache()` |
| authenticator | `ClaimAuthenticator` over the given login method | — | `.second_factor(factor)` keeps pending logins in the login store |
| correlation | `CorrelationLayer` on `x-request-id` | — | `.correlation_header("…")` |

The login store gets its own schema. It can then be dropped, given
permissions, or excluded from backups independently of the event store.
The builder sets no snapshot policy: the user aggregate declares its own
(section 3.7). The `app` feature enables `event-sauce-auth/moka` and
`event-sauce-auth/postgres`.

The builder does not take a custom login store or a custom authenticator:
an app that needs a Redis login store or its own `Authenticator` builds an
`EventSauceBackend` by hand and applies the layers itself.

`app.layer(router, session_layer)` applies, outermost first:
1. `CorrelationLayer`;
2. the app's tower-sessions layer;
3. the `axum-login` auth layer built on `EventSauceBackend`.

The session layer stays an argument. The builder does not choose a
session store for the app.

## 5. Changes to existing crates

| Crate | Change |
|---|---|
| `event-sauce-core` | `EventStore::resolve_claim` |
| `event-sauce-memory`, `event-sauce-postgres` | implement `resolve_claim` against the existing claim index |
| `event-sauce-macros` | `user` flag, `#[user(identity)]`/`#[user(active)]` field attributes, `Credentials::claims()` merged into `claims()` |
| `event-sauce` facade | features `auth` (plus `auth-postgres`, `auth-moka`, `auth-redis`, `auth-reset`, `auth-totp`) and `axum` (plus `axum-login`, `axum-app`), all in `full`, re-exported as `event_sauce::auth` and `event_sauce::axum` |
| `.github/workflows/ci.yml` | new `feature-matrix-auth-axum` job that lints each feature of `event-sauce-auth`, `event-sauce-axum` and the facade alone |

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
4. Replace `AuthStore` with `EventSauceBackend<Profile, _, _>` over
   `PasswordOrFederatedLogin`. Keep the Google
   link/create policy on `AuthError::Unknown` for federated attempts.
5. Replace `CurrentUser`/`RegisteredUser`/`ActorGuard` with the crate
   extractors. Drop the second replay.
6. Turn the device-link login into `DeviceLinkLogin`, a `LoginMethod`
   whose `locate` reads `DeviceLinkStore`. This replaces the hand-rolled
   `get_profile_aggregate_unchecked` path, and the active check then
   applies to it.
7. Wire everything through `EventSauceApp::postgres(pool)`. Its default
   login store creates the `event_sauce_login` schema, and the user cache
   is Moka, in process. This replaces most of `src/main.rs:139-349`.
8. Replace `request_id.rs` with `CorrelationLayer`.
9. Drop the `password_hash` column from `projections.profile_auth`.
   Delete the projection if nothing besides login reads it.

## 7. Tests

The lowest level that can prove each behaviour, and only there:

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
  - `ClaimAuthenticator` against fakes:
    - inactive users are rejected for every method;
    - anonymous users are accepted only by methods that allow it;
    - `SecondFactorRequired` is returned, then `complete` succeeds only
      once.
  - `Totp` verification with an injected clock, rejecting a reused step.
  - `MokaUserCache`: a version mismatch is a miss.
  - trybuild compile-fail cases: `#[user(identity)]` missing,
    `FederatedLogin` on a password-only user, `remove_password` on
    `Password<L>`.
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
- **System:** the `axum-auth` example
  (`crates/event-sauce/examples/axum-auth/`, tests in `tests.rs`, run by
  `cargo test --examples`) drives register → login → guarded route →
  password change → old session rejected on the in-memory store. The TOTP
  or second-factor login is covered by `event-sauce-axum`'s integration
  tests `tests/login.rs` and `tests/app.rs`.

## 8. Build order

Each step is its own commit, test first:

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
- **WebAuthn.** `SecondFactor::Challenge` makes it possible, but the
  crate does not ship it in v1.
