# Proposal: pluggable auth and axum integration

Status: **proposed**, not implemented. Supersedes the
"extract `event-sauce-auth` from saxy-p" plan in saxy-p's `PLAN.md`.

Two new optional crates:

- `event-sauce-auth`: a `user` marker on one aggregate, a typed login
  model, and a pluggable `Authenticator` trait for login.
- `event-sauce-axum`: correlation-id middleware, a cached current-user
  extractor, an `axum-login` backend, and state helpers.

Both are reached from the facade through the `auth` and `axum` features.

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
    fn fingerprint(&self, digest: &mut Sha256);
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
| `Federated::unlink(&provider)` | Errors with `LastLoginMethod` when it is the only link. |
| `PasswordOrFederated::set_password(p)` | Adds or replaces the password; `Federated` becomes `Both`. |
| `PasswordOrFederated::remove_password()` | Errors with `LastLoginMethod` unless federated links remain. |
| `PasswordOrFederated::link(id)` | Adds a link; `Password` becomes `Both`. |
| `PasswordOrFederated::unlink(&provider)` | Errors with `LastLoginMethod` if it removes the last way to log in. |

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
    /// claim through the `ClaimLocator` helper; delegated methods (device
    /// links) look the user up in their own store.
    async fn locate(&self, attempt: &Self::Attempt) -> Result<Option<EntityId>, LocateError>;

    /// Checks the attempt against the located user's identity. The method
    /// decides whether `Identity::Anonymous` may log in this way: credential
    /// methods reject it, device links accept it.
    async fn verify(&self, identity: &Identity<C>, attempt: &Self::Attempt) -> Result<(), Rejected>;

    /// Runs when no user holds the claim. Defaults to doing nothing;
    /// password methods override it to equalise timing.
    async fn on_unknown(&self, _attempt: &Self::Attempt) {}
}
```

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

#[derive(Debug, thiserror::Error)]
pub enum AuthError<A> {
    #[error("invalid credentials")] InvalidCredentials,
    #[error("account inactive")]    Inactive,
    #[error("no user for this login")] Unknown { attempt: A },
    #[error(transparent)]           Store(#[from] event_sauce_core::Error),
    #[error(transparent)]           Hash(#[from] HashError),
}
```

The default implementation is `ClaimAuthenticator<U, M>`, for any
`M: LoginMethod<U::Credentials>`. It is the generic code every backend
gets:

1. `method.locate(&attempt)`.
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
6. If the user's credentials require a second factor (section 3.6), store
   a pending login and return `SecondFactorRequired`. Otherwise return
   `Authenticated`.

saxy-p's login becomes:

```rust
ClaimAuthenticator::<Profile, _>::new(
    repo,
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

WebAuthn is left to the app or a later release. The `Challenge` type is
what makes it possible.

**Completing the login.** `ClaimAuthenticator::complete(pending_id,
response)` performs these steps:
1. `take` the pending login from the store. A pending login is single
   use and expires.
2. Reload the user and re-check that it is active.
3. Run `SecondFactor::verify`.
4. Return `Authenticated`.

No session exists before step 4. The pending login is not a partial
session.

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
    async fn put(&self, key: &LoginKey, value: &[u8], ttl: Duration) -> Result<(), LoginStoreError>;
    /// Atomically reads and deletes `key`: every entry is single use.
    async fn take(&self, key: &LoginKey) -> Result<Option<Vec<u8>>, LoginStoreError>;
    /// Records `key` if absent; `false` means it was already recorded.
    async fn mark_once(&self, key: &LoginKey, ttl: Duration) -> Result<bool, LoginStoreError>;
}
```

`LoginKey` is a namespaced key such as `pending:{uuid}` or
`totp:{user}:{step}`. A typed `PendingLogins<S>` wrapper serialises
`PendingLogin { user, method, challenge, created_at }` over the raw
bytes, once for every backend.

| Implementation | Feature | `take` | `mark_once` | Expiry |
|---|---|---|---|---|
| `PostgresLoginStore` (default) | `postgres` | `DELETE … RETURNING` | `INSERT … ON CONFLICT DO NOTHING` | `expires_at` filtered on read, plus `purge_expired()` for a periodic task |
| `RedisLoginStore` | `redis` | `GETDEL` | `SET NX EX` | native TTL |

As decided, there is no in-memory login store. A login in progress must
survive across instances. The crate's unit tests use a private fake.

The Postgres store keeps its table in its own schema, `event_sauce_login`
by default, created by `PostgresLoginStore::migrate()`.

### 3.7 The user cache

Loading the current user goes through a pluggable cache:

```rust
#[async_trait]
pub trait UserCache<U: UserAggregate>: Send + Sync {
    /// Returns the cached user only if it is exactly at `version`.
    async fn get(&self, id: EntityId, version: AggregateVersion)
        -> Result<Option<Arc<AggregateRoot<U>>>, CacheError>;
    async fn put(&self, user: Arc<AggregateRoot<U>>) -> Result<(), CacheError>;
}
```

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

The trait stays open, so an app can plug in another shared cache.

### 3.8 Dependencies and features

The crate depends on `event-sauce-core`, plus `argon2`, `sha2`, `rand`,
`secrecy`, `email_address` and `thiserror`.

| Feature | Brings |
|---|---|
| `argon2` (default) | `Argon2Hasher` |
| `reset` | reset tokens |
| `totp` | `Totp` second factor |
| `moka` | `MokaUserCache` |
| `postgres` (default) | `PostgresLoginStore` (sqlx, workspace version) |
| `redis` | `RedisLoginStore`, `RedisUserCache` (`redis` crate, tokio) |

These stores live here, not in `event-sauce-postgres`. That crate depends
on core only, and the stores depend on auth types.

## 4. `event-sauce-axum`

The crate depends on `axum 0.8`, `tower 0.5`, `event-sauce-core` and
`event-sauce-auth`. `axum-login 0.18` comes in behind the `login`
feature, which is on by default.

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
- It opens a `tracing` span when the `tracing` feature is on.

Every event committed during the request carries the correlation id,
because `EventMetadata.correlation_id` is filled from the context.

Caveat to document: the event context is a thread-local stack that is
re-entered on every poll (`event_context/stack.rs:21`). A `tokio::spawn`
inside a handler does **not** inherit it. The fix is
`fut.with_event_context(EventContext::current())`.

### 4.2 Login backend and current user (feature `login`)

```rust
let backend = EventSauceBackend::<Profile, _>::new(
    ClaimAuthenticator::new(repo.clone(), PasswordLogin::new(Argon2Hasher::default())),
    repo,
).with_cache(MokaUserCache::new(10_000));    // any UserCache<U>
let auth_layer = AuthManagerLayerBuilder::new(backend, session_layer).build();
```

`EventSauceBackend<U, A>` implements `axum_login::AuthnBackend`:

- `Credentials = A::Attempt`: the submitted form is whatever the chosen
  login methods accept (`PasswordOrFederatedAttempt` for
  saxy-p).
- `User = SessionUser<U>`, a newtype over `Arc<AggregateRoot<U>>` that
  implements `AuthUser`:
  - `id()` is the `EntityId`.
  - `session_auth_hash()` is `identity().session_fingerprint()`. This fixes
    the bug where changing a password left other sessions logged in.
- `authenticate` delegates to the `Authenticator`.
- `get_user(id)` loads the user through the cache and returns `None` for
  inactive users.

`get_user` goes through the configured `UserCache` (section 3.7):
- A hit is checked against the version, so it never serves a stale or
  deactivated user.
- A miss, or a version mismatch, means a full load, which then fills the
  cache.
- Without `with_cache`, every request does a full load, as today.

Second factor: `authenticate` returns an error to axum-login when the
outcome is `SecondFactorRequired`, so no session is opened. The backend
exposes `start(attempt) -> LoginOutcome` and `complete(pending, response)`
for that case. The `PendingLogin` extractor reads the pending id from a
short-lived cookie, and the handler calls `auth_session.login` only after
`complete` succeeds.

Extractors:

| Extractor | Yields | Rejection |
|---|---|---|
| `CurrentUser<U>` | `Arc<AggregateRoot<U>>`, which derefs to `AggregateRoot<U>` | 401 |
| `MaybeUser<U>` | `Option<…>` | never |
| `Registered<U>` | user whose `identity()` is `Registered` | 401 or 403 |

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
    .user::<Profile>(PasswordOrFederatedLogin::new(Argon2Hasher::default()))
    .build()
    .await?;                       // runs every migration below

let router = app.layer(Router::new().route(…), session_layer);
// AppState holds `app.state()`: Store<S> + backend, both reachable via FromRef
```

What `build` sets up by default, all on the same pool:

| Piece | Default | Postgres schema | Override |
|---|---|---|---|
| event store | `PostgresEventStore` | its existing schema | `.event_store(store)` |
| login store | `PostgresLoginStore` | `event_sauce_login` | `.login_store(RedisLoginStore::new(conn))`, `.login_store_schema("…")` |
| user cache | `MokaUserCache`, 10 000 entries | — | `.user_cache(RedisUserCache::new(conn))`, `.user_cache_capacity(n)`, `.no_user_cache()` |
| authenticator | `ClaimAuthenticator` over the given login method | — | `.authenticator(custom)` |
| correlation | `CorrelationLayer` on `x-request-id` | — | `.correlation_header("…")` |

The login store gets its own schema. It can then be dropped, given
permissions, or excluded from backups independently of the event store.
The builder sets no snapshot policy: the user aggregate declares its own
(section 3.7). `event-sauce-axum` enables `event-sauce-auth/moka`.

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
| `event-sauce` facade | features `auth` and `axum` (both in `full`), re-exported as `event_sauce::auth` and `event_sauce::axum` |
| `.github/workflows/ci.yml` | add both crates to the feature-combination job |

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
4. Replace `AuthStore` with `EventSauceBackend<Profile, _>` over
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
- **System:** one example app (`examples/axum-login.rs`) running
  register → login → guarded command → password change → old session
  rejected, plus login with TOTP enrolled.

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
14. Facade features, CI (adding a Redis service, or testcontainers),
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
