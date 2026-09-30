# event-sauce Documentation

Welcome to the event-sauce documentation! This directory contains
comprehensive guides to help you build event-driven applications in Rust —
aggregates whose every state change is an explicit, validated event. The
library is CQRS-oriented: aggregates are the write model (DDD aggregates
make poor read models), and queries go through projections built from your
events.

## Getting Started

- **[Getting Started Guide](getting-started.md)** - Start here if you're new to event-sauce or event-driven modeling
- **[State Storage](state-storage.md)** - Choosing a persistence style: what each mode guarantees, in-transaction projections, and the transactional outbox

## Core Concepts

- **[Aggregates](aggregates.md)** - Business entities and consistency boundaries
- **[Events](events.md)** - Domain events and event modeling
- **[Validation](validation.md)** - Event validation and business rules
- **[Claims](claims.md)** - Cross-aggregate uniqueness constraints (e.g., unique emails)

## Event Sourcing

- **[Event Store](event-store.md)** - Persisting and loading event streams
- **[Projections](projections.md)** - Building read models with transactional, durable delivery
- **[Policies](policies.md)** - Cross-aggregate event orchestration with causation tracking
- **[Audit Log](audit-log.md)** - Event log queries, actor tracking, and causation tracing

## Production Deployment

- **[PostgreSQL Production Setup](postgres-production.md)** - Complete guide to deploying with PostgreSQL (connection pooling, snapshots, HA, monitoring)
- **[Privacy & Crypto-Shredding](privacy.md)** - GDPR-style encryption for encrypted aggregates with right-to-be-forgotten support

## Guides

- **[Architecture Overview](architecture.md)** - Understand the system design, crate structure, and design decisions
- **[TDD Workflow](tdd-workflow.md)** - Learn how to use Test-Driven Development with event sourcing

## Examples

The [`../crates/event-sauce/examples/`](../crates/event-sauce/examples/) directory contains complete, runnable examples:

1. **[apply-event.rs](../crates/event-sauce/examples/apply-event.rs)** - Manual event implementation with the `ApplyEvent` trait (bank account)
2. **[actor-events.rs](../crates/event-sauce/examples/actor-events.rs)** - Permission-validated events with type-safe actors
3. **[delete-events.rs](../crates/event-sauce/examples/delete-events.rs)** - Type-state delete transitions (`AggregateRoot<A>` → `DeletedAggregateRoot<A>`)
4. **[upcasting.rs](../crates/event-sauce/examples/upcasting.rs)** - Event upcasting: on-load schema migration for historical payloads
5. **[policy.rs](../crates/event-sauce/examples/policy.rs)** - Cross-aggregate event orchestration with cascading reactions and causation tracking
6. **[save-all.rs](../crates/event-sauce/examples/save-all.rs)** - Multi-aggregate atomic writes with `Repository::save_all`
7. **[crypto-shredding.rs](../crates/event-sauce/examples/crypto-shredding.rs)** - Full-aggregate encryption and crypto-shredding for GDPR compliance (requires `memory` + `crypto` features)
8. **[field-encryption.rs](../crates/event-sauce/examples/field-encryption.rs)** - Selective field-level encryption (requires `memory` + `crypto` features)
9. **[state-stored-order.rs](../crates/event-sauce/examples/state-stored-order.rs)** - One codebase, two persistence styles: the same aggregate against both `EventStore` and `StateStore` (requires `memory` + `state-store` + `event-sourcing` features)
10. **[postgres-quickstart.rs](../crates/event-sauce/examples/postgres-quickstart.rs)** - Full-featured example with typed aggregate IDs, projections, and the PostgreSQL backend (requires `postgres` + `crypto` features; needs Docker for testcontainers)
11. **[projection-worker.rs](../crates/event-sauce/examples/projection-worker.rs)** - Running a postgres-backed projection in its own process, alongside other workers (requires `postgres` feature; needs Docker)

## Quick Links

- [Main README](../README.md) - Project overview
- [API Documentation](https://docs.rs/event-sauce) - Detailed API docs
- [CLAUDE.md](../CLAUDE.md) - Development guidelines

## What is Event Sourcing?

Event sourcing is a pattern where you store all changes to application state as a sequence of events. Instead of storing just the current state, you store the history of all state changes.

### Benefits

- **Complete Audit Trail** - Every change is recorded
- **Time Travel** - Reconstruct state at any point in time
- **Event Replay** - Rebuild state from events
- **Event-Driven Architecture** - React to domain events in real-time
- **Natural Fit for TDD** - Events make testing straightforward

### Core Concepts

1. **Aggregates** - Business entities that enforce consistency rules
2. **Events** - Facts about what happened (past tense)
3. **Commands** - Requests to do something (can fail)
4. **Event Store** - Append-only log of events
5. **Projections** - Read models built from events

## Learning Path

We recommend following this path:

1. **Start** → [Getting Started Guide](getting-started.md)
2. **Build** → Run the `actor-events` (in-memory) or `postgres-quickstart` (needs Docker) example
3. **Learn** → Read [Architecture Overview](architecture.md)
4. **Practice** → Follow [TDD Workflow](tdd-workflow.md)
5. **Explore** → Read [Projections & Event Bus](projections.md) and try the `projection-worker` example
6. **Production** → Study [PostgreSQL Production Setup](postgres-production.md)

## Common Patterns

### The Command Pattern

```rust
// Command: Request to do something
pub async fn handle_add_item_command<R: Repository<ShoppingCart>>(
    repo: &R,
    cart_id: EntityId,
    product: Product,
) -> Result<(), CartError> {
    // 1. Load aggregate
    let mut cart: AggregateRoot<ShoppingCart> = repo.load(cart_id).await?;

    // 2. Execute command
    cart.add_item(product)?;

    // 3. Save events
    repo.save(&mut cart).await?;

    Ok(())
}
```

### The Repository Pattern

Don't hand-roll a repository — the `Repository` trait ships with the
library:

```rust
async fn checkout<R: Repository<ShoppingCart>>(
    repo: &R,
    cart_id: EntityId,
) -> Result<()> {
    repo.modify(cart_id, |cart| cart.checkout()).await?;
    Ok(())
}

// At the composition root:
let store = Arc::new(PostgresEventStore::new(pool));
let repo = store.repository::<ShoppingCart>(); // EventSourcedRepository
checkout(&repo, cart_id).await?;
```

### The Projection Pattern

Read models are postgres-backed and transactional. Implement
`event_sauce::postgres::PostgresProjection` and run it through the backend —
the runner applies each event and advances the checkpoint inside the same
transaction.

```rust
struct CartSummaryProjection;

#[async_trait::async_trait]
impl event_sauce::postgres::PostgresProjection for CartSummaryProjection {
    const NAME: &'static str = "CartSummaryProjection";

    fn handled_event_types() -> Option<Vec<&'static str>> {
        Some(vec!["Cart.ItemAdded", "Cart.CheckedOut"])
    }

    async fn handle(
        &mut self,
        envelope: &event_sauce::EventEnvelope,
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    ) -> event_sauce::Result<()> {
        // UPDATE/INSERT against your read-model table through `tx`.
        let _ = (envelope, tx);
        Ok(())
    }
}

backend.run_postgres_projection(&mut CartSummaryProjection).await?;
```

See [projections.md](projections.md) for the full guide.

## Testing Strategies

Event sourcing makes testing natural:

```rust
#[test]
fn test_business_rule() {
    // GIVEN: Initial state (events)
    let mut cart = AggregateRoot::<ShoppingCart>::new(EntityId::new());
    cart.add_item(product_a, 1).unwrap();

    // WHEN: Execute command
    let result = cart.add_item(product_b, -1);

    // THEN: Verify result and events
    assert!(result.is_err());
    assert_eq!(cart.pending_events().len(), 1); // Only first add
}
```

## Best Practices

1. **Keep aggregates small** - Single consistency boundary
2. **Events are immutable** - Never change historical events
3. **Commands can fail** - Validate before creating events
4. **Use projections for queries** - Never query aggregates
5. **Test with events** - Given events, when command, then events
6. **Version your events** - Plan for schema evolution

## Need Help?

- Check the [examples](../crates/event-sauce/examples/)
- Read the [Getting Started Guide](getting-started.md)
- Review the [API documentation](https://docs.rs/event-sauce)
- Open an issue on [GitHub](https://github.com/obayemi/event-sauce)

## Contributing

See [CLAUDE.md](../CLAUDE.md) for development guidelines.

All contributions must:
- Follow TDD (write tests first)
- Keep line coverage above the 90% CI floor (100% is the target)
- Pass clippy with zero warnings
- Include documentation
- Use Jujutsu for version control

---

**Happy event sourcing!** 🚀
