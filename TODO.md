- [ ] add "from_actor" property to event to allow filling them with data from the actor
- [ ] allow validate_spec to use "on event" syntax
- [ ] find  ways  to integrate specifications more tightly with error typings to avoid needing to validate speficications manually for error types, and also to avoid requireing to use validate_or to handle errors
- [ ] update postgres example to use function based specifications instead of macro one.
        also update all validation to be based on specificationns instead of being written in the validation functions, and all specifications to be defined as functios instead of using the macro syntax
- [.] allow "deletion" of aggregates "deleted", and provide with "deletion" events (like init ones, but return a DeletedAggregat variant)
- [ ] add a way to create non event-stored entities / aggregates. this should be a flag in the #aggregate macro, and require an other store type that does store data dyrectly instead of events (also, should still 

- [x] add encrypted property to events

- [x] rename private to "encrypt" for crypto stuff
- [x] add a way to define events that must have an "actor", being an other entity, that can be used for permssions validation and from wich the id will be stored in the event's creatd_by 
- [x] add "privacy Aggregate" system, allowing to make the events for a type of aggregate encrypted, and have a cryptography store to store encryption keys by aggregateId so that events can be seamlessly decoded by the store, and return EncriptedAggregate for aggregates where key has been lost

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



         @validate_spec(|order, actor, _evt| IsOwner { actor_id: actor.entity_id() } & ValidPrice & OrderIsPending )

