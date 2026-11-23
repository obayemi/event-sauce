# axum Guide

This guide explains how to serve an event-sauce application with axum:
tagging every event with the request id, reaching stores and repositories
from handlers, logging users in with `axum-login`, and wiring the whole stack
on Postgres in one builder. The login model itself (credentials, login
methods, typestate logins) is in the [auth guide](auth.md).

## Table of Contents

- [Overview](#overview)
- [Installation](#installation)
- [The Correlation Layer](#the-correlation-layer)
- [State: Store, Repo and UnitOfWork](#state-store-repo-and-unitofwork)
- [Answering Errors](#answering-errors)
- [Loading What the Path Names](#loading-what-the-path-names)
- [Login with axum-login](#login-with-axum-login)
  - [The Session](#the-session)
  - [Logging In and Out](#logging-in-and-out)
  - [Refusing a Login](#refusing-a-login)
  - [Guarding Routes](#guarding-routes)
  - [Logins With Several Steps](#logins-with-several-steps)
  - [Sessions End When Credentials Change](#sessions-end-when-credentials-change)
- [The App Builder](#the-app-builder)
- [A Runnable Example](#a-runnable-example)
- [Feature Flags](#feature-flags)
- [Testing](#testing)
- [Best Practices](#best-practices)

## Overview

`event-sauce-axum` is a thin layer over `axum 0.8` and `tower 0.5`:

- **`CorrelationLayer`** runs each request in an event context whose
  correlation id is the request id.
- **`Store`, `Repo` and `UnitOfWork`** are extractors over your application
  state. There is no registry: a repository is built per request from the
  shared store.
- **`AuthSession`** opens an `axum-login` session from a finished login and
  from nothing else, **`AuthLayer`** installs it, and **`CurrentUser`**,
  **`MaybeUser`** and **`Registered`** hand the logged-in user to handlers.
- **`Found`** loads the aggregate a path names, once per request and read
  only, and **`Authorized`** also checks that the current user may reach it.
- **`StoreError`** and **`LoginError`** answer the failures of a store and of a
  login with the right status and no detail.
- **`EventSauceApp::postgres`** builds all of the above on one `PgPool`.

## Installation

```toml
[dependencies]
event-sauce = { version = "0.1", features = ["axum-login"] }
axum = "0.8"
axum-login = "0.18"
tower-sessions = "0.14"
```

Through the facade, everything is reachable as `event_sauce::axum`. The
facade features nest:

| Feature | Adds |
|---|---|
| `axum` | the correlation layer, the `Store`, `Repo`, `UnitOfWork` and `Found` extractors and `StoreError` |
| `axum-login` | `AuthLayer`, `AuthSession`, `PendingLogin`, `LoginStep`, `LoginError`, `Authorized` and the user extractors; implies `auth` |
| `axum-app` | `EventSauceApp::postgres`; implies `axum-login` and `postgres` |

Your application depends on `axum`, `axum-login` and `tower-sessions`
itself, with the versions above: `axum-login` 0.18 needs `tower-sessions`
0.14, and a session store crate of your choice.

## The Correlation Layer

```rust
let router = Router::new()
    .route("/orders", post(create_order))
    .layer(CorrelationLayer::new());              // header "x-request-id"
// CorrelationLayer::header(HeaderName::from_static("x-correlation-id")) for another header
```

For each request the layer:

- reuses the incoming id when it is a valid `RequestId` (1 to 128
  characters of `A-Za-z0-9._-`), and mints a UUID v4 otherwise;
- runs the handler in a root `EventContext` carrying that correlation id, so
  every event committed while serving the request is stored with it (see the
  [event context guide](event-context.md));
- echoes the id in the response header;
- opens a `request` tracing span when the `tracing` feature is on (the
  default).

**Spawned tasks do not inherit the context.** The event context is a stack
re-entered on every poll of the request future, so a `tokio::spawn` in a
handler starts with none. Pass it along explicitly:

```rust
tokio::spawn(work.with_event_context(EventContext::current()));
```

## State: Store, Repo and UnitOfWork

A runtime registry of repositories was rejected: asking for an aggregate
type nobody registered would fail at run time, for no gain, since
`EventSourcedRepository::new(Arc<S>)` is a cheap `Arc` clone. Instead the
extractors read your state through axum's `FromRef`:

```rust
#[derive(Clone, FromRef)]
struct AppState {
    store: Store<PostgresEventStore>,
}

async fn rename(Repo(orders): Repo<Order, PostgresEventStore>, /* ... */) { /* ... */ }
```

- `Store<S>` is a `Clone` newtype over `Arc<S>`; `.arc()` borrows the `Arc`.
- `Repo<A, S>` builds the `EventSourcedRepository` of aggregate `A` over the
  state's `Store<S>`. It never rejects.
- `UnitOfWork<T>` does the same for `Arc<UnitOfWorkStore<T>>`, see the
  [unit of work guide](unit-of-work.md).

## Answering Errors

The orphan rule forbids answering `event_sauce_core::Error` directly, so the
crate wraps it. **`StoreError`** is the error a handler returns when a store
operation fails, usually through `?`:

| Failure | Status | Body |
|---|---|---|
| not found | `404` | `not found` |
| deleted | `404`, or `410` after `deleted_as(DeletedStatus::Gone)` | `not found`, or `gone` |
| concurrency or claim conflict, an aggregate that already exists | `409` | `conflict` |
| anything else | `500` | `internal error` |

The body is a fixed text per status, never the error's own message, which
names aggregate types, ids and claim keys. With the `tracing` feature a `500`
is logged with its cause at `error` level and a `409` at `debug`. A deleted
aggregate is hidden as a missing one unless the response opts in, because
the same deletion can be hidden on one route and reported on another:

```rust
type Notes = Repo<Note, PostgresEventStore>;

async fn read(Repo(notes): Notes, Path(id): Path<NoteId>) -> Result<String, StoreError> {
    Ok(notes.load(id).await?.title.clone())
}

async fn read_or_gone(Repo(notes): Notes, Path(id): Path<NoteId>) -> Result<String, StoreError> {
    let note = notes
        .load(id)
        .await
        .map_err(|error| StoreError::from(error).deleted_as(DeletedStatus::Gone))?;
    Ok(note.title.clone())
}
```

`ModifyError<E>`, `ExecuteError<E>` and `UnitOfWorkError` hold a domain error
next to the store failures. Only the application knows the status of its own
error (a `NotOwner` is `404`, an `EmptyTitle` is `422`), so `split()` hands
the domain error back and turns the rest into a `StoreError`:

```rust
impl<E> From<ModifyError<E>> for ApiError
where
    Self: From<E>,
{
    fn from(error: ModifyError<E>) -> Self {
        match error.split() {
            Split::Domain(domain) => domain.into(),
            Split::Store(store) => Self::Store(store),
        }
    }
}
```

`ExecuteError::Lifecycle` and `UnitOfWorkError::Lifecycle` answer by what
they found: an aggregate that already exists is a `409`, a missing one a
`404` and a deleted one a deletion. `UnitOfWorkError::Unregistered`,
`AlreadyCheckedOut`, `LeakedHandle` and `DeleteFailed` are `500`.

`LoginError`, for the login handlers, is in [Refusing a Login](#refusing-a-login).

## Loading What the Path Names

A handler that reads one aggregate does not need `Path`, `load` and an error
table. **`Found<A, S>`** extracts the id from the path parameter that
`PathAggregate::PARAM` names (`"id"` by default), loads the aggregate from the
state's `Store<S>` and derefs to its `AggregateRoot`:

```rust
impl PathAggregate for Note {}

async fn read(note: Found<Note, PostgresEventStore>) -> String {
    note.title.clone()
}
```

| Failure | Status |
|---|---|
| the parameter is not an id | `400` |
| the route has no such parameter | `500` |
| missing | `404` |
| deleted | `404`, or `410` when `PathAggregate::DELETED` is `DeletedStatus::Gone` |
| the store failed | `500` |

A nested route names its parameters on each aggregate: set `PARAM` to
`"group_id"` on `Group` and to `"activity_id"` on `Activity`, and
`/groups/{group_id}/activities/{activity_id}` needs no `Path` at all.

`Found` is read only: it has no `DerefMut` and no way to save. Writes keep
using `Repo::modify` or `execute`, which reload the aggregate and retry on
conflicts, so a handler that checks access with `Found` and then writes loads
twice. The loaded aggregate is kept in the request, so two extractors that
ask for the same aggregate in the same store load it once.

With the `login` feature, **`Authorized<A, P, S>`** adds the question of who
may reach it. `P` is a marker type that implements `Access<A>`, a function of
the aggregate and the current user:

```rust
struct Owner;

impl Access<Note> for Owner {
    type User = Member;
    const REGISTERED: bool = true;

    fn allows(note: &AggregateRoot<Note>, member: &AggregateRoot<Member>) -> bool {
        note.ensure_owned_by(member).is_ok()
    }
}

async fn read(note: Authorized<Note, Owner, PostgresEventStore>) -> String {
    note.title.clone()
}
```

`Authorized` checks, in this order, and answers the first failure: somebody is
logged in (`401`), the identity is registered when `REGISTERED` is set (`403`),
the path parameter and the load (as `Found`), and `Access::allows`. A user who
does not pass is answered `404` by default, so a caller cannot tell a note it
may not read from one that does not exist; set `const DENIED: DeniedStatus =
DeniedStatus::Forbidden` for `403`. Nothing is loaded for a request without a
user, so ids cannot be probed anonymously, and a policy that hides existence
also hides deletions: a deleted aggregate is `404` whatever
`PathAggregate::DELETED` says. The handler cannot run unless the user, the
load and the check passed, so there is no guard layer whose order matters.

`allows` is synchronous and sees one aggregate and the user. A check that needs
a projection query or a second aggregate is the application's own extractor,
built from `Found` and `CurrentUser`, which share the loaded aggregate.

## Login with axum-login

A login is a process the [auth guide](auth.md#multi-step-logins) types: it
runs through `Logins`, and only its end yields an `Authenticated<U>`. The
axum integration turns that end into a session, and carries the process
across requests when it needs more than one.

### The Session

```rust
let cache: Arc<dyn UserCache<Member>> = Arc::new(MokaUserCache::new(10_000));
let loader = CachedUserLoader::<Member, _>::new(store.clone()).with_shared_cache(cache.clone());
let logins = Logins::<Member>::new(store).with_shared_cache(cache);
let router = AuthLayer::new(loader).layer(routes, session_layer);
```

`AuthLayer<U>` installs the `axum-login` machinery for the users of `U`.
`layer` takes your router and your session layer, and returns a `Router`:
nothing in its signature names the backend underneath. A handler holds the
session as `AuthSession<U>`:

| Method | Does |
|---|---|
| `user()` | the logged-in `Arc<AggregateRoot<U>>`, if any |
| `finish(login)` | finishes a `Login<U, S: Complete>` and opens its session, answering a `LoginStep` |
| `login(authenticated)` | opens a session from an `Authenticated<U>`, which it consumes |
| `renew()` | reloads the user of this session after the request changed its own credentials |
| `logout()` | ends the session and answers the user it held |

They fail with `SessionError`: `Session` for the session store, `Store` for
the user store. `AuthSession<U>` rejects with `500` when the layer is missing.

- Users resolve through the `CachedUserLoader` of the [auth
  guide](auth.md#the-user-cache): a hit is version-checked, so a stale or
  deactivated user is never served. The cache is set on the loader, and
  `Logins` can share it. Without a cache, every request loads the user in
  full.
- The session auth hash is the identity's session fingerprint, so a
  credential change ends every session opened before it.
- `renew()` logs the session in again with the new fingerprint, so the
  session that changed a password does not end with the others.

#### What is sealed

The backend underneath, `Backend<U>`, answers any stored user for
`get_user`, and `axum_login::AuthSession::login` takes that same user type.
Whoever could name the backend, or get a backend value, could open a session
for a user who never finished a login. So the crate seals it:

- the backend and its user type are `pub(crate)`: no path, alias or
  re-export names them, so `axum_login::AuthSession<...>` cannot be written
  as an extractor, and a generic `fn h<B: AuthnBackend>(s:
  axum_login::AuthSession<B>)` cannot be routed (E0283, type annotations
  needed);
- `AuthLayer::layer` and `App::layer` return a `Router`, there is no
  `tower::Layer` impl, and errors are `SessionError`, which maps the inner
  error through a private function: no public impl, `From` included, mentions
  the backend, so no value or bound of an application can infer it;
- `AuthSession<U>` keeps the inner session private, with no `Deref`,
  `AsRef`, `From` or `Clone`, and the `App` has no `backend()`;
- `AuthSession::login` and `AuthSession::finish` open a session from an
  `Authenticated<U>` and from a `Login` in a `Complete` state, which only the
  end of a typestate login yields. `LoginStep` has no public constructor, so
  an opened step exists only once a session opened. `renew` re-keys only the session the request
  already holds, for the same user, and never changes the user.

The compiler checks each of those: the `tests/ui` cases of the crate fail to
compile an import of the backend, a generic handler, a `login` with an
aggregate, and a peek into the session or the app.

What stays outside the types:

- the session record itself. `tower_sessions::Session` is your own
  extractor, `axum-login` keeps its record under the public key
  `axum-login.data` as `{ user_id, auth_hash }`, and the hash is the public
  `Identity::session_fingerprint()`. An application that writes that record
  by hand reimplements `axum-login` and logs anyone in; a fingerprint keyed
  by a server secret would close it;
- `renew()` during a concurrent credential change. It adopts whatever
  fingerprint the user has by then, so a session renewed right after a
  change made elsewhere survives that change. Call it only after the
  request's own credential change;
- your own `AuthnBackend` implementation, `unsafe` and transmutes. Their
  sessions feed none of `CurrentUser`, `MaybeUser`, `Registered` and
  `AuthSession`, which read only the sealed backend's session.

You choose the session store and the session layer. The crate does not.

### Logging In and Out

A login handler begins the login, and `AuthSession::finish` ends it and opens
the session. A flow with no step after the password finishes in the same
request:

```rust
async fn login(
    mut auth: AuthSession<Member>,
    State(app): State<AppState>,
    Json(form): Json<LoginForm>,
) -> Result<LoginStep, LoginError> {
    let email = Email::parse(&form.email).map_err(|_| LoginError::refused())?;
    let attempt = PasswordAttempt::new(email, form.password.into());
    let login = app.logins.begin(&app.first, attempt).await?;
    auth.finish(login).await
}

async fn logout(mut auth: AuthSession<Member>) -> Result<StatusCode, SessionError> {
    auth.logout().await?;
    Ok(StatusCode::NO_CONTENT)
}
```

`finish` runs `Login::finish`, which rereads the user and checks it is still
active with the same credentials, then opens the session. It takes only a
`Login<U, S>` whose state is `Complete`, so a login that waits for a step
cannot open one. Its answer, a `LoginStep`, is `204` with the pending login
cookie cleared. A refusal is the `Err` of the handler, a `LoginError`, so `?`
works on every call of the flow. `logout` returns the `SessionError` instead
of unwrapping it: a failing session store answers `500` and never panics the
handler.

A flow that signs a user up and logs them in at once uses
`Logins::sign_up(root)`, whose `Login<U, Done>` finishes the same way.

### Refusing a Login

**`LoginError`** is the error of every login step. It converts with `?` from
`AuthError`, `Rejected`, `FinishError`, `ResumeError`, `PendingLoginError` and
`SessionError`, and answers one of two things:

| Failure | Status | Body | Pending cookie |
|---|---|---|---|
| a refusal: invalid credentials, an inactive or unknown user, a mismatch, a login not in progress, credentials changed during the login, or `LoginError::refused()` | `401` | `login refused` | cleared |
| an outage: a user store, login store, hasher, verifier or session failure | `500` | `internal error` | untouched |

Every refusal answers the same bytes, so a client cannot tell an unknown
email from a wrong password or an inactive account. A `500` leaves the
pending cookie alone: when the store failed before the pending login was
taken, it is still there to retry, and when it failed after, the next
attempt is refused and clears it. `LoginError::refused()` is for a refusal
the application decides itself, such as a malformed email. `SessionError`
answers a `500` on its own, for `logout` and `renew`.

An application that signs unknown users up, as in the federated flow of the
[auth guide](auth.md), matches `AuthError::Unknown { attempt }` before the
`?`.

### Guarding Routes

The extractors are generic over the user aggregate `U`, which is all they need
to find the session `AuthLayer` installed:

| Extractor | Yields | Rejection |
|---|---|---|
| `CurrentUser<U>` | the logged-in `AggregateRoot<U>` (via `Deref`) | `401` |
| `MaybeUser<U>` | `Option` of the same | none, but `500` when the auth layer is missing |
| `Registered<U>` | like `CurrentUser`, only when the identity is `Registered` | `401`, or `403` for an anonymous identity |

```rust
async fn me(member: CurrentUser<Member>) -> String {
    member.nickname.clone()
}
```

All three read the user axum-login already resolved for the request, so a
request does exactly one user load, or none on a cache hit. The yielded user
is an `AggregateRoot<U>`, so it passes straight to `@actor(U)` commands.

A missing `AuthLayer` is a wiring error, answered with `500`.

### Logins With Several Steps

When the process needs another request, a handler returns
`PendingLogin::hold(&app.pending, login)`, which saves the login in the
`PendingLogins` and answers with a cookie holding the id. The next request
resumes it, proves the next step and either holds the next login or finishes.
Here is the password-then-TOTP flow, with `TotpIfEnrolled` as the route:

```rust
async fn login(
    mut auth: AuthSession<Member>,
    State(app): State<AppState>,
    Json(attempt): Json<PasswordAttempt<Email>>,
) -> Result<LoginStep, LoginError> {
    match app.logins.begin(&app.first, attempt).await? {
        TotpOrDone::Done(login) => auth.finish(login).await,
        TotpOrDone::Totp(login) => PendingLogin::hold(&app.pending, login).await,
    }
}

async fn totp(
    mut auth: AuthSession<Member>,
    State(app): State<AppState>,
    pending: PendingLogin,
    Json(code): Json<TotpCode>,
) -> Result<LoginStep, LoginError> {
    let login = app.pending.resume::<NeedTotp>(pending.id()).await?;
    auth.finish(login.prove(&app.totp, code.0).await?).await
}
```

A flow with more steps repeats the pattern: `resume::<Step>`, `prove`, then
`PendingLogin::hold` the next login, whose cookie replaces the previous one,
until `AuthSession::finish`.

- `LoginStep` is what both return: `204` with the pending cookie cleared when
  the session opened, `202` with the cookie when the login waits. Put it in a
  tuple with another response, `(step, Redirect::to("/"))`, to add its
  `Set-Cookie` to it.
- `hold` infers the state from the login, so the cookie lives as long as the
  saved login, which is `S::TTL`, with no type argument to get wrong. The
  cookie is `HttpOnly`, `Secure`, `SameSite=Lax` and `Path=/`.
  `PendingLogin::clear_cookie()` removes it, for a route that abandons the
  login.
- Resume in the handler body, never in an extractor: extractors run before
  `Json` parses the body, so a resume there would take the single-use login
  and a malformed body would burn it. A request whose body does not parse
  answers `422` and the same cookie still finishes the login.
- As an extractor, `PendingLogin` rejects with `401` when the cookie is
  missing or malformed.
- The cookie holds the id only. The state the login is in and the user it
  belongs to stay in the login store, so a client cannot skip a step by
  editing it.
- `resume` takes the login out of the store, so a pending login is single
  use. A replayed or forged cookie, a cookie of another state than the one
  the handler resumes, and a second attempt all end in `401`, and a login
  that was resumed and failed is over. It also refuses a user that was
  deactivated, or whose credentials changed, since the login was saved.

### Sessions End When Credentials Change

The session auth hash is the identity's session fingerprint. Changing the
password, enrolling a second factor, or changing any credential bumps it, and
every session of that user ends on its next request: axum-login compares the
hash stored in the session with the one of the freshly loaded user.
Deactivating a user does the same, because `get_user` answers `None` for an
inactive one. Both hold with and without a cache.

The session that changes its own credentials ends with the others, unless the
handler calls `auth.renew()` after the change, which logs the session in
again with the new fingerprint.

## The App Builder

With the `app` feature, `EventSauceApp` wires the common case on Postgres:

```rust
let app = EventSauceApp::postgres(pool)
    .user::<Member, _>(PasswordOrFederatedLogin::new(Argon2Hasher::default()))
    .route(TotpIfEnrolled)         // default: AlwaysDone
    .build()
    .await?;                       // runs every migration below

let router = app.layer(Router::new().route("/me", get(me)), session_layer);
```

Call it as `user::<U, _>(method)`: Rust cannot infer only the method type.
`build` validates the login schema first, then runs the event store and the
login store migrations. It is idempotent.

| Piece | Default | Override |
|---|---|---|
| event store | `PostgresEventStore` on the pool | `.event_store(store)` |
| login store | `PostgresLoginStore` in the `event_sauce_login` schema | `.login_store_schema("...")` |
| user cache | `MokaUserCache`, 10 000 entries | `.user_cache(cache)`, `.user_cache_capacity(n)`, `.no_user_cache()` |
| first factor | `FirstFactor` over the given login method, routed by `AlwaysDone` | `.route(route)` |
| logins | `Logins<U>` on the event store and `PendingLogins<U>` in the login store, sharing the user cache with the auth layer | none |
| correlation | `CorrelationLayer` on `x-request-id` | `.correlation_header(HeaderName)` |

`.route(route)` swaps the default `AlwaysDone`, which ends every login right
after the first factor. `build` requires `R: Route<U>`, and `AlwaysDone` is a
route only for credentials marked `SingleFactor`, so an app whose credentials
can hold a second factor does not compile until it names a route that sends
users to it. The builder builds no verifier for the later steps, because those
states are the app's: build them from what the app exposes, as in
`Totp::new(app.login_store())`.

The login store gets its own schema, so it can be dropped, granted or
excluded from backups independently of the event store. The builder sets no
snapshot policy: the user aggregate declares its own.

`app.layer(router, session_layer)` applies, outermost first, the correlation
layer, your session layer, then the `axum-login` layer, through the `AuthLayer`
the app holds.
The session layer stays an argument: the builder does not choose a session
store for you.

The app gives `app.store()` (a `Store<PostgresEventStore>`),
`app.logins()`, `app.pending()`, `app.first_factor()` and `app.login_store()`
instead of an opaque state. Keep your own `AppState` with `FromRef`, as in [State](#state-store-repo-and-unitofwork).

The builder is Postgres-only for now. It takes no custom login store (a Redis
one, say): an app that needs one builds `AuthLayer`, `Logins` and
`PendingLogins` by hand and applies the layers itself, as in
[The Session](#the-session). `.user_cache(...)` accepts any `UserCache`, so
`RedisUserCache` works with the builder.

## A Runnable Example

`crates/event-sauce/examples/axum-auth` is a complete app on the in-memory
store, so it needs no Docker:

```bash
cargo run --example axum-auth --features axum-login
```

It has a `Member` user aggregate (`#[aggregate(user)]`), registration, a
password login (a `FirstFactor` routed by `AlwaysDone`), logout, a guarded
`/me` and a guarded password change that ends every session. Its tests drive the same flow through the router with
`tower::ServiceExt::oneshot`, so `cargo test --examples` keeps it honest.

`crates/event-sauce/examples/axum-crud` is the same login on Postgres with
a resource behind it, through `EventSauceApp::postgres`:

```bash
DATABASE_URL=postgres://postgres:postgres@localhost/postgres \
  cargo run --example axum-crud --features axum-app
```

Members register, log in and manage their own `Note`s with `POST` and
`GET /notes`, and `GET`, `PUT`, `PATCH` and `DELETE /notes/{id}`. The
handlers stay thin: `Authorized<Note, Owner, _>` reads a note for its owner
only, `Repo<Note, _>` and `Registered<Member>` extract the repository and the
member for the writes, and the member passes straight to the `@actor(Member)`
commands, so ownership is the validation of the events and a foreign note
answers `404`. `GET /notes` reads a Postgres projection (see
the [projections guide](projections.md)) that a background task keeps up to
date, and every event is stored with the request id. Its tests start a
Postgres container, so they need Docker.

## Feature Flags

| `event-sauce-axum` feature | Facade feature | Adds |
|---|---|---|
| `tracing` (default) | `axum` | the `request` tracing span |
| `login` (default) | `axum-login` | `AuthLayer`, `AuthSession`, `PendingLogin`, `LoginStep`, `LoginError`, `Authorized`, user extractors |
| `app` (default) | `axum-app` | `EventSauceApp` and the Postgres defaults |

Through the facade, `tracing` comes with `axum`. `app` implies `login`, the
`postgres` login store and the Moka cache.

## Testing

Drive a real router without a socket:

```rust
let response = router.clone().oneshot(request).await?;
```

- Use `tower-sessions`' `MemoryStore` and the in-memory event store, and cheap
  Argon2 parameters.
- Keep a small client that stores the cookies from `Set-Cookie` and sends them
  back: it is what lets a test log in, then call a guarded route, then assert
  that a password change ended the session.
- Test the app builder against a real Postgres through testcontainers; it
  needs Docker, like the rest of the Postgres tests.

## Best Practices

1. **Put the correlation layer outermost**, so everything downstream, the
   auth layer included, runs inside the request's event context.
2. **Use `Registered` rather than `CurrentUser`** on routes that need a user
   who can log in, if your app allows anonymous sessions.
3. **Keep the session layer your choice**, with a store that suits your
   deployment: a memory store does not survive restarts or share across
   instances.
4. **Cache users**, and snapshot the user aggregate; both cost nothing in
   correctness, because every read is version-checked.
5. **Never `tokio::spawn` event work without carrying the context.**
6. **Extract the aggregate a route names with `Found` or `Authorized`**
   rather than `Path` and `load`, and keep `Repo` for the writes.
   `Authorized` hides a foreign aggregate as `404` unless its policy says
   otherwise.

## Related Documentation

- [Auth Guide](auth.md) - credentials, login methods, typestate logins
- [Event Context](event-context.md) - what the correlation layer sets
- [Unit of Work](unit-of-work.md) - the `UnitOfWork` extractor's store
- [Design proposal](proposals/auth-and-axum.md) - the reasoning behind the design
