# Unit of Work

A [command](commands.md) changes one aggregate. When an operation must change
several, such as placing an order that also takes stock from a product, you
want all of it to land or none of it, whatever way each aggregate is
persisted. A unit of work loads and changes any mix of event-sourced and
state-stored aggregates and commits them in one atomic write.

The command never names a persistence style. Each aggregate type is
registered once, at the composition root, as event-sourced or state-stored.

```toml
event-sauce = { version = "0.1", features = ["unit-of-work", "memory"] }
```

`unit-of-work` implies `commands`, `event-sourcing` and `state-store`.

## Registration

```rust
let events = Arc::new(InMemoryEventStore::new());
let states = Arc::new(InMemoryStateStore::new());

let store = UnitOfWorkStore::new(InMemoryTransactionalStore::new(events, states))
    .event_sourced::<Order>()
    .state_stored::<Product>();
```

`UnitOfWorkStore` wraps a `TransactionalStore`: an event store and a state
store plus an atomic `commit_atomic`. Loading a type that was not registered
fails with `UnitOfWorkError::Unregistered`.

## Writing a command

```rust
struct PlaceOrder { order_id: EntityId, product_id: EntityId, quantity: u32 }

#[async_trait]
impl<T: TransactionalStore + 'static> UnitOfWorkCommand<T> for PlaceOrder {
    async fn handle(&self, uow: &UnitOfWork<'_, T>, _ctx: &()) -> Result<(), UnitOfWorkError> {
        let mut product = uow.load::<Product>(self.product_id).await?;
        product.remove_stock(self.quantity)?;
        let unit_price = product.price;
        drop(product);

        let mut order = uow.create::<Order>(self.order_id, |root| {
            root.place(self.product_id, self.quantity, unit_price)
        })?;
        order.confirm()?;
        Ok(())
    }
}

let executed = store.execute(&place, &(), RetryPolicy::default()).await?;
```

`UnitOfWorkCommand::name()` returns `None` by default; override it to record the
command's name as the `command` of every event the unit of work produces.
`execute` also runs the handler in an [event context](event-context.md) and
adds a correlation id (a fresh UUID string) unless an enclosing scope already
provides one, so every event of one command shares it.

The handler never saves. Once it returns `Ok`, `execute` commits every change;
returning `Err` discards all of them. Domain errors and `CommandError`s
convert into `UnitOfWorkError` with `?`.

| Call                 | What it does                                                                  |
|----------------------|-------------------------------------------------------------------------------|
| `uow.load::<A>(id)`  | checks out an active aggregate as a `Tracked<A>`; fails if missing or deleted |
| `uow.peek::<A>(id)`  | reads committed state, active or deleted, without tracking it                 |
| `uow.create::<A>(id, f)` | initializes a new aggregate with `f`; the commit fails if it already exists |
| `tracked.delete_with(f)` | turns the aggregate into its deleted state with `f`                      |

`Tracked` dereferences to the `AggregateRoot`, so events are applied to it as
usual, and dropping it checks the root back into the unit of work.

## Auto-commit and the read set

Every aggregate the command loaded is part of the commit:

- changed aggregates are written under optimistic concurrency, against the
  version they were loaded at
- aggregates loaded but left unchanged are only asserted to still be at that
  version

The second rule is the read set. Take a command that quotes an order from a
product's price without changing the product. A concurrent writer reprices the
product between the command's load and its commit:

1. attempt 1 loads the product at price 250 and creates an order priced 250
2. another writer sets the price to 300
3. the commit asserts the product is still at the version attempt 1 read; it
   is not, so nothing is written and the commit is a concurrency conflict
4. `execute` retries the whole command: attempt 2 loads price 300 and the
   order is created at 300

The stale quote never lands. A `peek` opts out of this protection on purpose:
the aggregate is read, not tracked, and its later change does not conflict.
Use it when a stale read is acceptable.

Only a concurrency conflict is retried, up to the `RetryPolicy` budget;
`UnitOfWorkExecuted::attempts` says how many attempts it took. The handler may
therefore run more than once and must have no side effect outside the unit of
work.

## AlreadyCheckedOut

While a `Tracked` handle is alive, loading the same aggregate again fails with
`UnitOfWorkError::AlreadyCheckedOut`. A transfer between two products whose
ids turn out equal hits it:

```rust
let mut from = uow.load::<Product>(self.from).await?;
let mut to = uow.load::<Product>(self.to).await?; // same id: AlreadyCheckedOut
```

Nothing is committed. Once the first handle is dropped, loading again hands
out the same, already modified root.

## PostgreSQL

```rust
let events = Arc::new(PostgresEventStore::new(pool.clone()));
let states = Arc::new(PostgresStateStore::new(pool));
let stores = PostgresTransactionalStore::new(events, states)?;
```

Both stores must point at the same database: `commit_atomic` runs every write
on one connection inside one transaction, and a second database could not
share it. `PostgresTransactionalStore::new` refuses a pair whose pools differ
in host, port or database. Share one schema (the default `event_sauce`) so
uniqueness claims are visible to both stores. Deadlocks and serialization
failures are reported as concurrency conflicts, so they are retried like any
other.

## Choosing an approach

- **One aggregate changes**: a [command](commands.md) and `repo.execute`
- **Several aggregates must change together, or a decision rests on another
  aggregate's current state**: a unit of work
- **The follow-up change may happen after the first commits**: a process
  aggregate driven by a [policy](policies.md). It is eventually consistent but
  needs no shared transaction, and each step is its own small commit
- **Independent changes where partial success is acceptable**: two
  `execute` calls. Nothing makes them atomic, so a failure of the second
  leaves the first in place

A unit of work buys atomicity across aggregates at the price of one wider
commit that conflicts more often under contention; keep each one to the
aggregates the invariant really spans.

## See also

- [Commands](commands.md)
- [State Storage](state-storage.md)
- `examples/unit-of-work.rs` (`cargo run --example unit-of-work --features unit-of-work`)
