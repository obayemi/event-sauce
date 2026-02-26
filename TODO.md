- [ ] remove unsafe or code that panics everywhere

- [ ] add the snapshot store as optional attribute to the event store to allow not requiring it when building subscription

- [ ] make EventStore object safe by using pin<box<dyn Stream>> instead of "impl stream" to allow Arc<dyn EventStore>

- [ ] is there a best way to implement the "aggregate_type" method that is consistent across rust versions ?

- [x] update the postgresbackend to store arcs instead of actual values to allow easy sharing without requireing creating new arcs

- [x] Aggregate / uninitialized aggregate system to have one or many "initialization" events, and allow stricter Entity design without needing to accomodate uninitialized states at aggregate creation. add "init" flag to those events in the macro to allow them to take an emptyaggregate and  return a full aggregate
  - [x] base implementation
  - [x] fix projections

- [x] design an api to add @init commands that will be defined on the repository
  - [x] init creation functions: `Aggregate::cmd()` and `Aggregate::cmd_with_id()`
  - [x] remove @with_init — unified entry points for command_handler! and define_events!
  - [x] update postgres-quickstart to use commands everywhere

- [ ] add a mechanism to easily send event author user_id to every event

  `EventEnvelope` already has `created_by: Option<Uuid>` and `with_created_by()`, but it's never
  populated automatically. The question is how to thread actor identity from command invocation
  down to envelope creation at `commit()` time.

  ### Approach 1: Explicit field on AggregateRoot

  Store `created_by: Option<Uuid>` on `AggregateRoot`. Set it once; `commit()` attaches it to all
  pending envelopes automatically.

  ```rust
  let mut order = repo.load(id).await?;
  order.set_actor(user_id);
  order.add_item(product, qty)?;
  repo.save(&mut order).await?;
  // or: repo.save_as(&mut order, user_id).await?;
  ```

  **Pros:** fully explicit, no hidden state, easy to test, works in any runtime.
  **Cons:** must thread `user_id` from handler to domain; easy to forget (unless enforced).

  **Variant 1a — mandatory at runtime:** `commit()` returns error if actor not set.
  **Variant 1b — type-state:** `AggregateRoot<A>.with_actor(id) → ActorAggregateRoot<A>` — compile-time enforcement, heavier API.

  ### Approach 2: Task-local / thread-local context

  Use `tokio::task_local!` to store actor identity; `commit()` reads from context.

  ```rust
  // Middleware
  event_sauce::with_actor(user_id, async { next.run(req).await }).await;

  // Domain — zero plumbing
  order.add_item(product, qty)?;
  repo.save(&mut order).await?;  // actor auto-attached from context
  ```

  **Pros:** zero boilerplate in domain code, impossible to forget (if middleware set up), familiar
  pattern (tracing spans, OpenTelemetry).
  **Cons:** hidden state, runtime-dependent (Tokio), testing needs explicit `with_actor()` setup,
  silently `None` if middleware misconfigured, doesn't compose across `tokio::spawn` boundaries.

  ### Approach 3: CommandContext object

  Introduce `CommandContext { actor_id, correlation_id, causation_id, ... }` passed alongside commands.

  ```rust
  // command_handler! generates ctx parameter:
  order.add_item(&ctx, product, qty)?;
  // or via repository:
  repo.execute(&mut order, &ctx, |o| o.add_item(product, qty)).await?;
  ```

  **Pros:** explicit, composable, groups all cross-cutting concerns, fills both `created_by` and
  `metadata` in one shot, extensible.
  **Cons:** every command signature changes (API-breaking), must thread through all layers.

  ### Approach 4: Repository-level context (recommended)

  Set context on `Repository` (or a scoped handle); auto-attaches to all operations through it.

  ```rust
  let repo = repo.with_context(CommandContext { actor_id: user_id, .. });
  let mut order = repo.load(id).await?;
  order.add_item(product, qty)?;
  repo.save(&mut order).await?;  // context auto-attached at commit
  ```

  **Pros:** explicit (in the type), set once per request scope, no hidden global state, works with
  DI, extensible.
  **Cons:** requires new repo handle per request (lightweight), still must thread scoped repo from
  handler.

  ### Recommendation

  **Approach 4 (repository-level context)** with **Approach 2 (task-local) as optional fallback**.
  Repository is the natural mediation point for persistence. A `CommandContext` on the repo is
  explicit and testable. For web frameworks, an optional `task_local!`-based `EventSauceContext`
  that `commit()` falls back to gives zero-boilerplate convenience. Users choose: explicit for
  libraries/tests, implicit for web handlers.

- [ ] make sure that postgres event store's table definitions create the tables as append only (no update, no delete)
