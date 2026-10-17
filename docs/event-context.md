# Event Context

An event context is ambient metadata that tags every event applied while it is
active. It works like a `tracing` span: you enter it once, near the edge of the
system (an HTTP handler, a message consumer, a job), and every event applied
below that point carries its tenant, correlation id and extra keys in
`EventMetadata`, without threading them through every function signature.

```rust
use event_sauce::event_context;

event_context!(root, tenant = "acme", correlation_id = "req-42", region = "eu").in_scope(|| {
    // every event applied here is stored with
    //   tenant: "acme", correlation_id: "req-42", additional: {"region": "eu"}
});
```

## What an event gets

When an event is applied inside a scope, the aggregate captures the effective
context into the event's `EventMetadata`:

| Field              | Type             | Source                                                  |
|--------------------|------------------|---------------------------------------------------------|
| `tenant`           | `Option<String>` | the innermost tenant, set on a root context             |
| `correlation_id`   | `Option<String>` | the first correlation id of the flow (outermost)        |
| `correlation_path` | `Vec<String>`    | every correlation id of the flow, outermost first       |
| `command`          | `Option<String>` | the innermost command name (see [Integration](#integration)) |
| `additional`       | `Option<Map<String, Value>>` | the key/value pairs, merged, innermost winning          |

Outside any scope an applied event carries no context metadata at all.

## API

| API                                       | Use it to                                                              |
|-------------------------------------------|------------------------------------------------------------------------|
| `event_context!(..)`                      | build a context with the `tracing`-like field grammar                  |
| `EventContext::new()`                     | build an empty context layered on the active one                       |
| `EventContext::root()`                    | build an empty context that starts a new flow and may carry a tenant   |
| `.correlation_id(id)`, `.with(key, v)`    | set the correlation id or an extra key on a context                    |
| `.tenant(t)`                              | set the tenant, on a root context only                                 |
| `ctx.in_scope(\|\| ..)`                    | run a synchronous closure inside the context                           |
| `ctx.entered()`                           | activate the context until the returned guard is dropped               |
| `future.with_event_context(ctx)`          | run a future inside the context, across `.await` points and threads    |
| `#[with_event_context(..)]`               | run a whole function inside the context                                |
| `EventContext::current()`                 | snapshot the effective context, to hand to a spawned task              |

### Scopes

```rust
use event_sauce::EventContext;

let answer = EventContext::root()
    .tenant("acme")
    .correlation_id("req-42")
    .in_scope(|| {
        // apply events here
        42
    });
assert_eq!(answer, 42);
```

`in_scope` restores the previous context when the closure returns, also when
it panics.

### Async code

A scope is bound to the current thread, so it cannot follow an `async` block
across `.await` points. Wrap the future instead:

```rust
use event_sauce::{EventContext, WithEventContextExt};

async fn run() {
    let work = async {
        // apply events, `.await` freely
    };
    work.with_event_context(EventContext::root().tenant("acme").correlation_id("req-42"))
        .await;
}
```

The wrapper activates the context on every poll, so it holds wherever the task
is resumed.

### Spawned tasks

Spawning does **not** carry the context: a task started with `tokio::spawn`
begins with no active context, whatever the spawning task had. Snapshot it with
`EventContext::current()` and wrap the spawned future:

```rust
use event_sauce::{EventContext, WithEventContextExt};

async fn run() {
    let work = async {
        // events applied here keep the tenant, correlation path and extra keys
    };
    tokio::spawn(work.with_event_context(EventContext::current()))
        .await
        .unwrap();
}
```

`current()` returns a root context holding the tenant, command, extra keys and
the whole correlation path of the caller. Contexts nested in it extend that
path.

### The attribute

`#[with_event_context(..)]` takes the same arguments as `event_context!` and
wraps the function body:

```rust
use event_sauce::with_event_context;

#[with_event_context(root, tenant = "acme", correlation_id = "req-42", %attempt)]
fn handle(attempt: u32) -> u32 {
    if attempt > 3 {
        return 0;
    }
    attempt
}
```

- a sync function runs its body in `in_scope`; `return` and `?` keep returning
  from the function
- an `async fn` awaits its body wrapped in `with_event_context`, so the
  context follows the future across awaits while the body keeps borrowing the
  parameters
- on a method of an `#[async_trait]` impl the attribute goes on the method
  itself
- the arguments are evaluated when the function is called, before the body
  runs, so they may use the parameters

## Grammar

`event_context!` and `#[with_event_context]` share one grammar. An optional
leading `root` selects `EventContext::root()`, otherwise the context is
`EventContext::new()`. Fields follow, in any order, with an optional trailing
comma:

| Field                    | Expands to                                  |
|--------------------------|---------------------------------------------|
| `tenant = expr`          | `.tenant(expr)`, only valid after `root`    |
| `correlation_id = expr`  | `.correlation_id(expr)`                     |
| `key = expr`             | `.with("key", expr)`                        |
| `key = %expr`            | `.with("key", display(expr))`               |
| `"some.key" = expr`      | `.with("some.key", expr)`                   |
| `"some.key" = %expr`     | `.with("some.key", display(expr))`          |
| `ident`                  | `.with("ident", ident)`                     |
| `%ident`                 | `.with("ident", display(ident))`            |

```rust
use event_sauce::event_context;

let attempt = 2_u32;
let ip = std::net::Ipv4Addr::LOCALHOST;
let ctx = event_context!(
    root,
    tenant = "acme",
    correlation_id = "req-42",
    region = "eu",
    "http.method" = "GET",
    client = %ip,
    attempt,
);
ctx.in_scope(|| ());
```

A `tenant` field without `root` does not compile.

## Context values

Extra keys accept anything that implements `Into<serde_json::Value>`. The
conversion is **infallible**: a context is ambient metadata, and attaching a
value must never be able to fail a command. That is why there is no blanket
implementation over `serde::Serialize`, whose serialization can fail.

serde_json provides it for `bool`, every integer up to 64 bits, `isize`,
`usize`, `f32`, `f64` (non-finite floats become `null`), `String`, `&str`,
`Cow<str>`, `Map`, `Vec<T>`, arrays, slices and `Option<T>` of values, and
`serde_json::Value` itself. It does not provide it for references such as
`&String` (use `.as_str()` or a clone), nor for `i128`, `u128`, `char`, `Uuid`
or `DateTime`.

For anything else that implements `Display`, use the `%` sigil, which stores
the value as a string through `display()`:

```rust
use event_sauce::display;
use serde_json::{json, Value};

let value: Value = display(std::net::Ipv4Addr::LOCALHOST).into();
assert_eq!(value, json!("127.0.0.1"));
```

Implement `From<YourType> for serde_json::Value` for a domain type when a
structured value reads better than its string form:

```rust
use serde_json::{json, Value};

struct Region(&'static str);

impl From<Region> for Value {
    fn from(region: Region) -> Self {
        json!({ "name": region.0 })
    }
}
```

## Nesting rules

Contexts nest. The effective context is resolved from the active contexts,
outermost first, starting at the innermost root:

- **Correlation id: the outermost wins.** The id a request arrived with is
  never overwritten by a deeper layer.
- **Correlation path: every id, outermost first.** Each layer that sets a
  correlation id appends it, so `correlation_path` records the flow from the
  entry point down to the event. A repeated consecutive id is kept once.
- **Root starts a new flow.** A root context inherits nothing from the contexts
  around it: tenant, correlation id, command and extra keys of the outer
  contexts are ignored while it is active.
- **Tenant: root only.** `tenant` exists on `EventContext<Root>` only, so
  setting it on a nested context is a compile error. The innermost tenant of
  the active contexts applies.
- **Additional keys: merged, innermost wins.** Keys from every layer are
  merged, and the innermost value replaces an outer one under the same key.

```rust
use event_sauce::EventContext;

EventContext::root().tenant("acme").correlation_id("req-42").in_scope(|| {
    EventContext::new().correlation_id("step-1").with("region", "eu").in_scope(|| {
        // tenant: "acme"
        // correlation_id: "req-42"
        // correlation_path: ["req-42", "step-1"]
        // additional: {"region": "eu"}
    });
});
```

## Captured at apply

The context is read when the event is **applied**, not when the aggregate is
saved. An event applied inside a scope keeps that scope's metadata even if the
aggregate is saved after the scope has ended, and an event applied outside a
scope stays untagged even if it is saved inside one:

```rust
EventContext::root().tenant("acme").in_scope(|| account.deposit(5))?; // tagged
account.deposit(7)?; // not tagged, whatever scope the save runs in
```

An explicit `apply_with_metadata` keeps every field it set; the context only
fills the ones it left empty.

## Integration

The execution entry points open a context themselves, so a flow always has a
correlation id.

- **`Repository::execute` and `UnitOfWorkStore::execute`** run the command in a
  nested context that records `Command::name()` (or `UnitOfWorkCommand::name()`)
  as the events' `command`, and adds a fresh correlation id (a UUID string)
  when no scope already provides one. An outer scope's correlation id and
  tenant therefore apply to the command's events.
- **`Repository::modify` and `Repository::modify_deleted`** add the correlation
  id the same way; a closure has no name, so `command` stays empty.
- **Policies.** A policy reaction runs in a root context seeded from its source
  event: the tenant, the correlation id and the correlation path of the event
  that triggered it, with `command` set to the policy's `name()`. A cascade
  therefore stays in one tenant and one flow with no code in the handler. The
  source event's `command` and extra keys are not carried over, and any ambient
  context around the `PolicyRunner` is ignored: the reaction always starts from
  a fresh root. A source event with no metadata correlates its reactions to its
  own id, as a string. See [Policies](policies.md).

```rust
use event_sauce::{event_context, RetryPolicy, WithEventContextExt};

let executed = repo
    .execute(id, &Deposit(40), &(), RetryPolicy::default())
    .with_event_context(event_context!(root, tenant = "acme", correlation_id = "req-42"))
    .await?;
```

## Guard warnings

`EventContext::entered()` returns a guard that deactivates the context when
dropped. It follows the rules of `tracing::Span::enter`:

- **Never hold the guard across an `.await`.** The task may resume on another
  thread, leaving this one with a stale context and the next one without it.
  The guard is not `Send`, so `tokio::spawn` rejects a future that holds it,
  but `block_on` and `spawn_local` accept it: use `with_event_context` in async
  code.
- **Never `std::mem::forget` the guard.** The context stack is thread-local and
  truncated on drop; a leaked guard leaves its context, and the contexts
  entered after it, active on that thread for good.

## Storage and privacy

The context fields are stored in plain text in the event envelope's metadata,
next to the causation fields: they are **not** crypto-shredded. Encrypting an
aggregate or its fields protects event data, never metadata, so keep personal
data out of tenant, correlation ids and extra keys. See
[Privacy](privacy.md).

On PostgreSQL the fields live in the `tenant`, `command` and `correlation_path`
columns, alongside `correlation_id` and `causation_chain`; the extra keys live
in the `metadata` JSONB column; see
[PostgreSQL Production Setup](postgres-production.md).

## See also

- [Audit Log](audit-log.md): causation chains and correlation ids
- [Commands](commands.md) and [Unit of Work](unit-of-work.md): command names
- [Policies](policies.md): reactions inherit the source event's context
