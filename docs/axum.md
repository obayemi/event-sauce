# axum Guide

This guide explains how to serve an event-sauce application with axum:
tagging every event with the request id, reaching stores and repositories
from handlers, logging users in with `axum-login`, and wiring the whole stack
on Postgres in one builder. The login model itself (credentials, login
methods, second factors) is in the [auth guide](auth.md).

## Table of Contents

- [Overview](#overview)
- [Installation](#installation)
- [The Correlation Layer](#the-correlation-layer)
- [State: Store, Repo and UnitOfWork](#state-store-repo-and-unitofwork)
- [Login with axum-login](#login-with-axum-login)
  - [The Backend](#the-backend)
  - [Logging In and Out](#logging-in-and-out)
  - [Guarding Routes](#guarding-routes)
  - [Logins With a Second Factor](#logins-with-a-second-factor)
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
- **`EventSauceBackend`** plugs an `Authenticator` into `axum-login`, and
  **`CurrentUser`**, **`MaybeUser`** and **`Registered`** hand the logged-in
  user to handlers.
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
| `axum` | the correlation layer and the `Store`, `Repo` and `UnitOfWork` extractors |
| `axum-login` | the `axum-login` backend, `SessionUser`, `PendingLogin` and the user extractors; implies `auth` |
| `axum-app` | `EventSauceApp::postgres`; implies `axum-login` and `postgres` |

Your application depends on `axum`, `axum-login` and `tower-sessions`
itself, with the versions above: `axum-login` 0.18 needs `tower-sessions`
0.14, and a session store crate of your choice.

## The Correlation Layer

```rust
let router = Router::new()
    .route("/orders", post(create_order))
    .layer(CorrelationLayer::new());              // header "x-request-id"
// CorrelationLayer::header("x-correlation-id") for another header
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

## Login with axum-login

### The Backend

```rust
let authenticator = ClaimAuthenticator::<Member, _, _>::new(
    store.clone(),
    PasswordLogin::new(Argon2Hasher::default()),
);
let backend = EventSauceBackend::new(authenticator, store.clone())
    .with_cache(MokaUserCache::new(10_000));        // any UserCache<U>
let auth_layer = AuthManagerLayerBuilder::new(backend, session_layer).build();
```

`EventSauceBackend<U, A, S>` implements `axum_login::AuthnBackend`:

- `Credentials` is whatever the authenticator's attempt is
  (`PasswordAttempt<Email>` for a password login);
- `User` is `SessionUser<U>`, a shared `Arc<AggregateRoot<U>>`, whose
  `id()` is the `EntityId` and whose `session_auth_hash()` is the identity's
  session fingerprint;
- `authenticate` delegates to the authenticator. A wrong password, an unknown
  login and an inactive user all answer `Ok(None)`;
- `get_user` goes through the `CachedUserLoader` of the [auth
  guide](auth.md#the-user-cache): a hit is version-checked, so a stale or
  deactivated user is never served. Without `with_cache`, every request loads
  the user in full.

You choose the session store and the session layer. The backend does not.

### Logging In and Out

```rust
async fn login(mut auth: AuthSession<Backend>, Json(form): Json<LoginForm>) -> StatusCode {
    let attempt = PasswordAttempt::new(Email::parse(&form.email).unwrap(), form.password.into());
    match auth.authenticate(attempt).await {
        Ok(Some(member)) => { auth.login(&member).await.unwrap(); StatusCode::NO_CONTENT }
        Ok(None) => StatusCode::UNAUTHORIZED,
        Err(_) => StatusCode::INTERNAL_SERVER_ERROR,
    }
}

async fn logout(mut auth: AuthSession<Backend>) -> StatusCode {
    auth.logout().await.unwrap();
    StatusCode::NO_CONTENT
}
```

`SessionUser::from(root)` builds the session user of an aggregate you
already hold, for flows that log a user in without a form, such as
registration that signs in directly.

### Guarding Routes

The extractors are generic over the backend type `B`, because axum-login
stores its `AuthSession<B>` in the request by backend type. `B` is a
`UserBackend`, which every `EventSauceBackend` is, and tells the extractor
which aggregate it yields:

| Extractor | Yields | Rejection |
|---|---|---|
| `CurrentUser<B>` | the logged-in `AggregateRoot<U>` (via `Deref`) | `401` |
| `MaybeUser<B>` | `Option` of the same | none, but `500` when the auth layer is missing |
| `Registered<B>` | like `CurrentUser`, only when the identity is `Registered` | `401`, or `403` for an anonymous identity |

```rust
async fn me(member: CurrentUser<Backend>) -> String {
    member.nickname.clone()
}
```

All three read the user axum-login already resolved for the request, so a
request does exactly one user load, or none on a cache hit. The yielded user
is an `AggregateRoot<U>`, so it passes straight to `@actor(U)` commands.

A missing `axum-login` layer is a wiring error, answered with `500`.

### Logins With a Second Factor

`authenticate` fails with `BackendError::SecondFactorRequired(id)` when the
authenticator holds the login back, so no session opens by accident. A
handler serving such logins calls `backend.start(attempt)` instead, and
carries the pending id to the next request in a short-lived cookie:

```rust
async fn login(mut auth: AuthSession<Backend>, Json(form): Json<LoginForm>) -> Response {
    match auth.backend.start(attempt(form)).await {
        Ok(LoginOutcome::Authenticated(root)) => {
            auth.login(&SessionUser::from(root)).await.unwrap();
            StatusCode::NO_CONTENT.into_response()
        }
        Ok(LoginOutcome::SecondFactorRequired(id)) => (
            StatusCode::ACCEPTED,
            [(SET_COOKIE, PendingLogin::set_cookie(id, PendingLogin::DEFAULT_TTL))],
        ).into_response(),
        _ => StatusCode::UNAUTHORIZED.into_response(),
    }
}

async fn second_factor(
    mut auth: AuthSession<Backend>,
    pending: PendingLogin,
    Json(form): Json<CodeForm>,
) -> Response {
    let cleared = [(SET_COOKIE, PendingLogin::clear_cookie())];
    match auth.backend.complete(pending.id(), &form.code).await {
        Ok(user) => { auth.login(&user).await.unwrap(); (StatusCode::NO_CONTENT, cleared).into_response() }
        Err(_) => (StatusCode::UNAUTHORIZED, cleared).into_response(),
    }
}
```

- The cookie is `HttpOnly`, `Secure`, `SameSite=Lax` and `Path=/`.
- As an extractor, `PendingLogin` rejects with `401` when the cookie is
  missing or malformed.
- `complete` exists only when the authenticator has a `TwoFactor` stage.
  Call `auth.login` only after it succeeds.
- A pending login is single use: a replayed or forged cookie, a wrong code
  and a second attempt all end in `401`.

### Sessions End When Credentials Change

The session auth hash is the identity's session fingerprint. Changing the
password, enrolling a second factor, or changing any credential bumps it, and
every session of that user ends on its next request: axum-login compares the
hash stored in the session with the one of the freshly loaded user.
Deactivating a user does the same, because `get_user` answers `None` for an
inactive one. Both hold with and without a cache.

## The App Builder

With the `app` feature, `EventSauceApp` wires the common case on Postgres:

```rust
let app = EventSauceApp::postgres(pool)
    .user::<Member, _>(PasswordLogin::new(Argon2Hasher::default()))
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
| authenticator | `ClaimAuthenticator` over the given login method | `.second_factor(factor)` keeps pending logins in the login store |
| correlation | `CorrelationLayer` on `x-request-id` | `.correlation_header("...")` |

The login store gets its own schema, so it can be dropped, granted or
excluded from backups independently of the event store. The builder sets no
snapshot policy: the user aggregate declares its own.

`app.layer(router, session_layer)` applies, outermost first, the correlation
layer, your session layer, then the `axum-login` layer built on the backend.
The session layer stays an argument: the builder does not choose a session
store for you.

The app gives `app.store()` (a `Store<PostgresEventStore>`) and
`app.backend()` instead of an opaque state. Keep your own `AppState` with
`FromRef`, as in [State](#state-store-repo-and-unitofwork).

The builder is Postgres-only for now. It takes neither a custom login store
(a Redis one, say) nor a custom authenticator: an app that needs them builds
`EventSauceBackend` by hand and applies the layers itself, as in [The
Backend](#the-backend). `.user_cache(...)` accepts any `UserCache`, so
`RedisUserCache` works with the builder.

## A Runnable Example

`crates/event-sauce/examples/axum-auth` is a complete app on the in-memory
store, so it needs no Docker:

```bash
cargo run --example axum-auth --features axum-login
```

It has a `Member` user aggregate (`#[aggregate(user)]`), registration, a
password login, logout, a guarded `/me` and a guarded password change that
ends every session. Its tests drive the same flow through the router with
`tower::ServiceExt::oneshot`, so `cargo test --examples` keeps it honest.

## Feature Flags

| `event-sauce-axum` feature | Facade feature | Adds |
|---|---|---|
| `tracing` (default) | `axum` | the `request` tracing span |
| `login` (default) | `axum-login` | backend, `SessionUser`, `PendingLogin`, user extractors |
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

## Related Documentation

- [Auth Guide](auth.md) - credentials, login methods, second factors
- [Event Context](event-context.md) - what the correlation layer sets
- [Unit of Work](unit-of-work.md) - the `UnitOfWork` extractor's store
- [Design proposal](proposals/auth-and-axum.md) - the reasoning behind the design
