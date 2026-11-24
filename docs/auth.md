# Authentication Guide

This guide explains how to log users in with event-sauce: how credentials live
inside an aggregate, how a login attempt becomes an authenticated user, and
how multi-step logins, caches and password reset tokens fit around it. The
HTTP side (sessions, extractors, the app builder) is in the
[axum guide](axum.md).

## Table of Contents

- [Overview](#overview)
- [Installation](#installation)
- [Credentials Inside the Aggregate](#credentials-inside-the-aggregate)
  - [Choosing a Credentials Type](#choosing-a-credentials-type)
  - [Login Identifiers](#login-identifiers)
  - [Rules Are Plain Methods](#rules-are-plain-methods)
  - [Writing Your Own Credentials](#writing-your-own-credentials)
- [The User Aggregate](#the-user-aggregate)
- [Logging In](#logging-in)
  - [Login Methods](#login-methods)
  - [Starting a Login](#starting-a-login)
  - [Password Hashing](#password-hashing)
- [Multi-step Logins](#multi-step-logins)
  - [The Login Store](#the-login-store)
  - [TOTP](#totp)
- [The User Cache](#the-user-cache)
- [Password Reset Tokens](#password-reset-tokens)
- [Sessions and the Fingerprint](#sessions-and-the-fingerprint)
- [Feature Flags](#feature-flags)
- [Testing](#testing)
- [Best Practices](#best-practices)

## Overview

The library does not pick how your users log in. It gives you:

- **`Identity<C>`**, a field type for the user aggregate, generic over one
  credentials type `C` that you name (`Password<Email>`,
  `PasswordOrFederated<Email>`, or your own).
- **`#[aggregate(user)]`**, which makes the aggregate a `UserAggregate` and
  adds the credentials' uniqueness claims to its own.
- **`LoginMethod`** and **`Logins`**, which turn a submitted form into a
  login without any plaintext login projection.
- **Typestate logins** (`Login`, `Step`, `Complete`, `Authenticated`) and a
  **`LoginStore`** for logins that take more than one step: no session can open
  before the whole process ended.
- **`UserCache`**, which keeps the per-request user load cheap.

Credentials live **inside** the aggregate. When the aggregate is encrypted,
so are its password hash and its provider links, and crypto-shredding the
user leaves nothing readable behind. Logins resolve through
[claims](claims.md): the store keeps only a SHA-256 of each login id.

## Installation

```toml
[dependencies]
event-sauce = { version = "0.1", features = ["auth"] }
```

`auth` brings the Argon2 hasher. Everything is reachable as
`event_sauce::auth`. The optional parts have their own features, listed in
[Feature Flags](#feature-flags). If you use the crates directly, depend on
`event-sauce-auth`; the macro finds it either way.

## Credentials Inside the Aggregate

### Choosing a Credentials Type

The identity field of a user aggregate is an `Identity<C>`:

```rust
pub enum Identity<C: Credentials> {
    /// Holds a session, owns data, cannot log in with anything.
    Anonymous,
    /// Can log in with what `C` holds.
    Registered(C),
}
```

| The app wants | Field type |
|---|---|
| password only | `Identity<Password<Email>>` |
| federated only (an external identity provider) | `Identity<Federated>` |
| password, federated, or both | `Identity<PasswordOrFederated<Email>>` |
| anything else | `Identity<MyCredentials>` |

Each credentials type carries its own identifier: a password login names a
`LoginId`, a federated login names a provider subject. There is no separate
login field a federated-only app would have to fill with a dummy.

`PasswordOrFederated` is a concrete enum with three variants (`Password`,
`Federated`, `Both`), so "registered with no way to log in" cannot be
represented.

### Login Identifiers

A `LoginId` is the identifier inside a `Password`. The library ships:

- `Email`, trimmed, lowercased and syntax-checked;
- `Username`, trimmed, lowercased and limited to `a-z`, `0-9`, `_`, `-` and
  `.`, between `Username::MIN_LEN` and `Username::MAX_LEN` characters.

`LoginId::parse(raw)` normalises raw user input. `AsRef<str>` is its canonical string: it keys the `auth.login` claim and feeds the session fingerprint, so an implementation only has to expose the normalised form. A different alphabet or
normalisation, such as an allowed set of Unicode letters with confusable
rejection, is a `LoginId` of your own: without it, two look-alike names
both pass the uniqueness claim.

### Rules Are Plain Methods

The rules that keep an identity valid are pure methods on the concrete
types. Call them from your `define_events!` apply closures and `@validate`
blocks:

| Method | Rule |
|---|---|
| `Identity::register(c)` | `Anonymous` becomes `Registered`; `AlreadyRegistered` otherwise |
| `Identity::credentials()` | `None` when anonymous |
| `Identity::session_fingerprint()` | SHA-256 over the credentials; any change invalidates other sessions |
| `Password::set_hash(hash)`, `Password::change_login(id)` | always allowed; the claim follows on commit |
| `Federated::link(id)` / `unlink(&id)` / `unlink_provider(&provider)` | `AlreadyLinked`, `NotLinked`, `LastLoginMethod` |
| `PasswordOrFederated::set_password` / `remove_password` | `remove_password` fails with `LastLoginMethod` unless links remain |
| `PasswordOrFederated::link` / `unlink` / `unlink_provider` | `unlink` and `unlink_provider` cannot remove the last way to log in |

The rules fail with `LoginMethodError` and `IdentityError`. Each type
exposes only what makes sense for it: `Password<L>` has no
`remove_password`, so a password-only app cannot reach the "no way to log
in" state.

### Writing Your Own Credentials

A credentials type implements `Credentials`:

```rust
pub trait Credentials:
    Clone + Eq + Debug + Serialize + DeserializeOwned + Send + Sync + 'static
{
    /// Uniqueness claims for the identifiers these credentials answer to.
    fn claims(&self) -> Vec<AggregateClaim>;

    /// Feeds every secret and identifier that grants access, so any change
    /// to them changes the session fingerprint.
    fn fingerprint(&self, fingerprint: &mut Fingerprint);
}
```

and, for each login method it supports, a small capability trait:
`HasPassword` (an accessor to the `Password`), `HasFederated`,
`HasTotp`. `PasswordLogin` needs `HasPassword`, so
configuring a login method the type does not support fails to compile.

## The User Aggregate

Mark one aggregate as the user with the `user` flag and the field markers:

```rust
#[aggregate(event = "MemberEvent", error = "MemberError", init, user)]
#[derive(Debug, Serialize, Deserialize)]
pub struct Member {
    #[id]
    id: EntityId,
    #[user(identity)]
    identity: Identity<Password<Email>>,
    #[user(active)]
    is_active: bool,
    nickname: String,
}
```

The flag generates `UserAggregate` for the type, and merges the credentials'
`auth.login` and `auth.federated` claims into `claims()` while the user is
registered, next to the claims of your own `aggregate_claims()`. Two
registrations with the same email therefore conflict, and the conflict
surfaces like any other claim conflict (`Error::as_claim_conflict`).

- `#[user(identity)]` is required, once, on a field of type `Identity<C>`.
  `Identity<C>` implements `Default` as `Anonymous`, so a user aggregate
  does not need an `init` event: without one, every other field must be
  `Default` as for any aggregate, and a registration event calls
  `Identity::register`.
- `#[user(active)]` is optional, on a `bool`. Without it the user is always
  active. An inactive user cannot log in with any method, and an inactive
  user already logged in is no longer served by the cache loader.
- The credential claim types are not prefixed with the aggregate type, so a
  store holds a single `user` aggregate.

You write the events: a registration event carrying the credentials (an
`@init` event, or a regular one calling `Identity::register` on an
aggregate without `init`), plus whatever changes them (`PasswordChanged`, a
second factor enrolment, a deactivation). See
`crates/event-sauce/examples/axum-auth` for a complete aggregate, and `crates/event-sauce/examples/axum-crud` for one that
declares snapshots and acts as the actor of another aggregate.

## Logging In

### Login Methods

A `LoginMethod<C>` checks an attempt against the stored identity. It is
separate from what is stored, so you can swap how a method verifies (another
hasher, an LDAP bind) without touching the credentials:

| Method | Needs | Attempt |
|---|---|---|
| `PasswordLogin<H>` | `C: HasPassword` | `PasswordAttempt { login, password }` |
| `FederatedLogin` | `C: HasFederated` | `VerifiedFederated { id, email }` |
| `PasswordOrFederatedLogin<H>` | both | `PasswordOrFederatedAttempt` |

`FederatedLogin` runs no OAuth flow. Your callback checks the provider's
token, then builds the `VerifiedFederated` attempt: the name says the proof
is already done. Linking a new provider subject to an existing user by the
provider's email is your policy, applied when `Logins::begin` hands an
unknown attempt back.

`PasswordLogin` verifies against a fixed dummy hash when the login is
unknown, so the response time does not reveal which logins exist.

Your own `LoginMethod` implements three functions: `locate` (find the user
id, usually by claim), `verify`, and an optional `on_unknown`.

### Starting a Login

`Logins<U>` starts logins for the users of one event store. A `FirstFactor`
pairs a login method with a `Route`, which picks, per user, where the login
goes once the first factor passed:

```rust
let logins = Logins::<Member>::new(store.clone());
let password = FirstFactor::new(PasswordLogin::new(Argon2Hasher::default()), AlwaysDone);
let login: Login<Member, Done> = logins.begin(&password, attempt).await?;
```

`begin` goes through the same steps for every attempt:

1. the method locates the user, resolving a claim through
   `EventStore::resolve_claim` (one indexed lookup);
2. when nobody matches, `on_unknown` runs and the attempt comes back in
   `AuthError::Unknown`, so the app can link or create a user;
3. the user loads (decrypted, from its latest snapshot, through the shared
   cache when `with_shared_cache` set one);
4. an inactive user is refused with `AuthError::Inactive`, whatever the
   method;
5. the method verifies the attempt;
6. the session fingerprint and the start time are pinned, and the route picks
   the state the login moves to.

`AuthError` separates `InvalidCredentials`, `Inactive`, `Unknown`, `Store`,
`Hash`, `LoginStore` and `Unavailable`, the last one for a verifier that could
not check the attempt at all (`Rejected::unavailable(error)`).

The first three are refusals and the others are outages. In an axum handler
`event-sauce-axum`'s `LoginError` converts both with `?`: every refusal
answers the same `401`, so a client cannot tell an unknown email from a wrong
password, and an outage answers `500` with no detail (see the [axum
guide](axum.md#refusing-a-login)).

What `begin` returns is a `Login`, not a session. Only `Login::finish` yields
the `Authenticated<U>` a session is opened from, and `finish` exists only for
a `Complete` state such as `Done`. `Logins::sign_up(user)` commits a brand-new
user, at its initial version so it can never log into an account that exists,
and starts its login in `Done`.

### Password Hashing

`PasswordHasher` has two async methods, `hash` and `verify`. `Argon2Hasher`
ships behind the `argon2` feature, with configurable parameters, and runs the
Argon2 work on tokio's blocking pool so it never stalls the executor. A
task that fails there surfaces as a `HashError`:

```rust
let hasher = Argon2Hasher::default();
let cheap = Argon2Hasher::new(argon2::Params::new(8, 1, 1, None)?);
let hash = hasher.hash(&SecretString::from("correct horse")).await?;
```

Use cheap parameters in tests, never in production. `PasswordHash` is a PHC
string newtype with a redacted `Debug`.

## Multi-step Logins

A login can take more than one proof: a password then a TOTP code, a TOTP
code or a recovery code, a code sent by email, a step another device
performs. Whatever the process, **no session can open before it ends**,
because a session needs an `Authenticated<U>` and the only way to get one is
to finish a login that reached a `Complete` state:

| Link of the chain | Held by |
|---|---|
| A session opens only from an `Authenticated<U>` | type: private fields, no `Default`, no `Deserialize`; `event-sauce-axum`'s `AuthSession::login` takes only an `Authenticated<U>`, and the `axum-login` backend and its user type are crate-private, so `axum_login::AuthSession::login` cannot be reached (see the limits below) |
| An `Authenticated<U>` needs a finished login in a `Complete` state | type: `finish` is bounded by `S: Complete` |
| A `Login` exists only after a proof | type: built only by `begin`, `sign_up`, `PendingLogins::resume` and `Advance::to` |
| A login leaves a state only through that state's step | type: `Advance<U, S>` is built only by `prove` for `S: Step<U>`, and for `Entered` by `begin` |
| A login is used once | type: `prove`, `finish` and `save` consume it, it is not `Clone` |
| A saved login resumes in the state it was saved in | runtime: tag check in `resume` |
| Nothing changed underneath | runtime: active and fingerprint checks in `resume` and `finish` |

You name the states. A state is plain data, saved with the login while the
client is away, and its `Step` is the only way out of it:

```rust
#[derive(Serialize, Deserialize)]
struct NeedEmailCode { code_hash: [u8; 32] }

impl LoginState for NeedEmailCode {
    const TAG: &'static str = "app.email_code";
    const TTL: LoginTtl = LoginTtl::ONE_HOUR;
}

#[async_trait]
impl Step<Member> for NeedEmailCode {
    type Proof = SecretString;
    type Verifier = ();
    type Next = Login<Member, Done>;

    async fn verify(advance: Advance<Member, Self>, (): &(), code: SecretString)
        -> Result<Login<Member, Done>, Rejected>
    {
        if !matches_hash(&advance.login().state().code_hash, &code) {
            return Err(Rejected::Mismatch);
        }
        Ok(advance.to(Done))
    }
}
```

`Step::Next` lists the states a step can lead to: one `Login<U, T>`, or an
application enum of them, picked per user. A `Route` does the same after the
first factor, which is how a flow branches. Each request of a flow resumes the
state its endpoint serves, proves, and either saves the next state or
finishes:

```rust
let pending = logins.pending(login_store);
let id = pending.save(login).await?;                       // POST /login
let login = pending.resume::<NeedTotp>(id).await?;         // POST /login/totp
let authenticated = login.prove(&totp, code).await?.finish().await?;
```

A saved login is single use and expires after the `TTL` of its state (five
minutes by default). `resume` checks the tag, so a login saved in another
state is `ResumeError::NotInProgress` and is used up. It also reloads the
user: gone or inactive is `Inactive`, a changed session fingerprint (a new
password, a new TOTP secret) is `CredentialsChanged`, and the login starts
over. `finish` makes the same checks once more.

The library ships `Done`, `AlwaysDone` (the route of single-factor
credentials: `Password`, `Federated` and `PasswordOrFederated` implement the
`SingleFactor` marker), and, behind `totp`, `NeedTotp`, `TotpIfEnrolled` and
`Totp`. `TotpIfEnrolled` fails closed: a user with any TOTP secret goes to
`NeedTotp`, and only a user with none goes to `Done`. `AlwaysDone` does not
compile for credentials that are not `SingleFactor`.

The types guarantee the order of the process, not the quality of each check:
the app decides which states are `Complete`, a step's `verify` is ordinary
code, `SingleFactor` is an assertion by the credentials type, and tags must
be unique. Anyone who can write the login store directly can forge a pending
login. The chain holds up to the session: `event-sauce-axum` seals the `axum-login`
backend, which answers any stored user, behind `AuthSession<U>`, whose `login`
takes only an `Authenticated<U>`, so no library call opens a session without a
finished login (the [axum guide](axum.md#what-is-sealed) lists what is sealed
and what stays outside the types).

### The Login Store

In-progress login state lives in a small pluggable store, separate from the
event store, because device links span two devices and TOTP replay protection
is per user, not per session:

```rust
#[async_trait]
pub trait LoginStore: Send + Sync {
    async fn put(&self, key: &LoginKey, value: &[u8], ttl: LoginTtl) -> Result<(), LoginStoreError>;
    async fn take(&self, key: &LoginKey) -> Result<Option<Vec<u8>>, LoginStoreError>;
    async fn mark_once(&self, key: &LoginKey, ttl: LoginTtl) -> Result<bool, LoginStoreError>;
}
```

Every entry is single use (`take` reads and deletes atomically). A `LoginTtl`
is never zero and never longer than 366 days, so every store accepts it:
`LoginTtl::new(duration)` checks and rounds up to whole milliseconds, and
`LoginTtl::ONE_MINUTE`, `FIVE_MINUTES`, `ONE_HOUR`, `ONE_DAY`, `MAX` and
`LoginTtl::from_millis(NonZeroU32)` cannot fail.

| Implementation | Feature | Expiry |
|---|---|---|
| `PostgresLoginStore` | `postgres` | `expires_at` filtered on read, plus `purge_expired()` for a periodic task |
| `RedisLoginStore` | `redis` (Redis 6.2 or newer) | native TTL |

`PostgresLoginStore::migrate()` creates its table in its own schema,
`event_sauce_login` by default. There is no in-memory login store on
purpose: a login in progress must survive across instances.

### TOTP

`Totp` (feature `totp`) verifies time-based one-time codes. It is the verifier
of the `NeedTotp` step, `Totp::new(login_store)`, and `Totp::check(secret, code)`
is there for the steps of your own. The secret lives in your credentials type
behind `HasTotp`, and is part of the fingerprint, so enrolling a second
factor logs out the user's other sessions. A time step that was already used
is rejected through `LoginStore::mark_once`; that replay protection writes
nothing to the user's event stream. `Totp::with_clock` injects the `Clock`
that picks the time step.

`otpauth_uri(issuer, account, &secret)` builds the `otpauth://totp/` URI an
authenticator app scans or opens, with the parameters `Totp` checks codes
with (SHA-1, six digits, thirty seconds):

```rust
use event_sauce::auth::{otpauth_uri, TotpSecret};

let secret = TotpSecret::generate();
let uri = otpauth_uri("Example", "ada@example.com", &secret);
assert!(uri.starts_with("otpauth://totp/Example:ada%40example.com?secret="));
```

## The User Cache

Loading the current user goes through `CachedUserLoader`:

1. read the stored version (one cheap query); a version below the first
   stored one means there is no such user;
2. serve a cached user that is at exactly that `StoredVersion`;
3. otherwise load in full and fill the cache;
4. a missing, deleted or inactive user is `None`.

Every read is checked against the version, so a cache never needs explicit
invalidation: a password change or a deactivation bumps the version and the
next read misses. A cache failure is logged and the user is loaded in full:
a cache is never a source of failure.

| Implementation | Feature | Holds |
|---|---|---|
| `MokaUserCache` | `moka` | `Arc<AggregateRoot<U>>` in process |
| `RedisUserCache` | `redis` | the serialised aggregate, shared by every instance |

`RedisUserCache` never holds the plaintext of an encrypted user: when
`U::is_encrypted()` it encrypts the payload with the aggregate's own key, so
crypto-shredding a user makes its cache entries unreadable too. Give it the
event store the users live in with `with_crypto_from(store)`.

There is no Postgres user cache: a hit would cost the same round trips as a
snapshot load. Declare a snapshot policy on the user aggregate instead, for
example `#[aggregate(..., snapshots(every = 10))]`.

## Password Reset Tokens

The `reset` feature adds the token primitives, not a delivery channel:

```rust
let (token, hash) = ResetToken::issue();
// send `token.as_str()` to the user once; store `hash` in an event
hash.verify(&token, issued_at, now, ttl)?;
```

The token is 32 random bytes, base64url encoded, shown once. Only its
SHA-256 goes into an event. The clock is a parameter, so verification is
pure. Which address the token goes to, and how, is up to the app.

## Sessions and the Fingerprint

`Identity::session_fingerprint()` hashes every secret and identifier that
grants access. The HTTP layer ties each session to that value: when a
password changes, a second factor is enrolled or the credentials change in
any other way, the fingerprint changes and every other session of the user
ends on its next request. Credentials types feed the fingerprint from
`Credentials::fingerprint`; forget a field there and changing it will not
log anyone out.

`Fingerprint` feeds only length-prefixed fields: `field(&[u8])` and
`str_field(&str)`. Two different credential states therefore never produce
the same byte stream, however their fields are split.

## Feature Flags

| `event-sauce-auth` feature | Facade feature | Brings |
|---|---|---|
| `argon2` (default) | `auth` | `Argon2Hasher` |
| `postgres` (default) | `auth-postgres` | `PostgresLoginStore` |
| `moka` | `auth-moka` | `MokaUserCache` |
| `redis` | `auth-redis` | `RedisLoginStore`, `RedisUserCache` |
| `reset` | `auth-reset` | reset tokens |
| `totp` | `auth-totp` | the `Totp` verifier, `NeedTotp`, `TotpIfEnrolled` and `otpauth_uri` |

Through the facade, `auth` is the base, and every other feature implies it.

## Testing

- The unit tests of your user aggregate need no store: apply events and call
  the rules.
- `Logins` works over `InMemoryEventStore`, so login tests run without a
  database, whole multi-step flows included. Use cheap Argon2 parameters.
- A `LoginStore` fake is a few lines over a `HashMap`; the crate's own tests
  do the same. Real Postgres and Redis stores are tested against
  testcontainers, so they need Docker.
- The [axum example](../crates/event-sauce/examples/axum-auth/tests.rs)
  drives the whole register, login and guarded route flow with `oneshot`.
- The [axum CRUD example](../crates/event-sauce/examples/axum-crud/tests/http.rs)
  does the same on a Postgres container, with notes that only their owner
  reaches, and checks that a password change ends every session, the
  caller's included.

## Best Practices

1. **Keep credentials in the user aggregate**, not in a projection. A
   plaintext login table survives crypto-shredding.
2. **Feed every secret to `fingerprint`.** It is what ends stale sessions.
3. **Encrypt the user aggregate** (`#[aggregate(encrypted)]`) when it holds
   credentials.
4. **Snapshot the user aggregate.** Every cache miss replays it.
5. **Never answer "unknown login" differently from "wrong password"** to the
   client. The library already equalises timing.
6. **Use real Argon2 parameters in production**, cheap ones in tests only.

## Related Documentation

- [Axum Guide](axum.md) - sessions, extractors and the app builder
- [Claims Guide](claims.md) - the uniqueness mechanism behind logins
- [Privacy & Encryption](privacy.md) - encrypting and shredding user data
- [Design proposal](proposals/auth-and-axum.md) - the reasoning behind the design
