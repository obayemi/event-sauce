# event-sauce Documentation

Welcome to the event-sauce documentation! This directory contains comprehensive guides to help you build event-sourced applications in Rust.

## Getting Started

- **[Getting Started Guide](getting-started.md)** - Start here if you're new to event-sauce or event sourcing

## Core Concepts

- **[Aggregates](aggregates.md)** - Business entities and consistency boundaries
- **[Events](events.md)** - Domain events and event modeling
- **[Event Store](event-store.md)** - Persisting and loading event streams
- **[Projections & Subscriptions](projections.md)** - Building read models with durable, guaranteed delivery
- **[Validation](validation.md)** - Event validation and business rules

## Production Deployment

- **[PostgreSQL Production Setup](postgres-production.md)** - Complete guide to deploying with PostgreSQL (connection pooling, snapshots, HA, monitoring)
- **[Privacy & Crypto-Shredding](privacy.md)** - GDPR-style encryption for encrypted aggregates with right-to-be-forgotten support

## Guides

- **[Architecture Overview](architecture.md)** - Understand the system design, crate structure, and design decisions
- **[TDD Workflow](tdd-workflow.md)** - Learn how to use Test-Driven Development with event sourcing

## Examples

The [`../crates/event-sauce/examples/`](../crates/event-sauce/examples/) directory contains complete, runnable examples:

1. **counter.rs** - Basic event sourcing concepts
2. **shopping-cart.rs** - Complex aggregate with business rules
3. **bank-account.rs** - Full workflow with derive macros
4. **task-projections.rs** - Building read models from events
5. **crypto-shredding.rs** - Privacy & crypto-shredding for GDPR compliance (requires `crypto` feature)

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
2. **Build** → Run the `postgres-quickstart` or `bank-account` example
3. **Learn** → Read [Architecture Overview](architecture.md)
4. **Practice** → Follow [TDD Workflow](tdd-workflow.md)
5. **Explore** → Read [Projections & Event Bus](projections.md) and try the `task-projections` example
6. **Production** → Study [PostgreSQL Production Setup](postgres-production.md)

## Common Patterns

### The Command Pattern

```rust
// Command: Request to do something
pub async fn handle_add_item_command(
    store: &impl EventStore,
    cart_id: EntityId,
    product: Product,
) -> Result<(), CartError> {
    // 1. Load aggregate
    let mut cart: AggregateRoot<ShoppingCart> = load(store, cart_id).await?;

    // 2. Execute command
    cart.add_item(product)?;

    // 3. Save events
    store.commit(&mut cart).await?;

    Ok(())
}
```

### The Repository Pattern

```rust
pub struct CartRepository {
    store: Arc<dyn EventStore>,
}

impl CartRepository {
    pub async fn save(&self, cart: &mut AggregateRoot<ShoppingCart>) -> Result<()> {
        self.store.commit(cart).await
    }

    pub async fn load(&self, id: EntityId) -> Result<AggregateRoot<ShoppingCart>> {
        load(&*self.store, id).await
    }
}
```

### The Projection Pattern

```rust
// Using the projection! macro (recommended):
projection! {
    struct CartSummaryProjection {
        state: CartSummaryState,

        on CartEvent::ItemAdded |proj, event| {
            // update read model
        },

        on CartEvent::CheckedOut |proj, event| {
            // update read model
        },
    }
}

// Auto-configured subscription:
let mut sub = store.projection_subscription::<CartSummaryProjection>().build()?;
sub.run_projection(&mut projection).await?;
```

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
- Open an issue on [GitHub](https://github.com/yourusername/event-sauce)

## Contributing

See [CLAUDE.md](../CLAUDE.md) for development guidelines.

All contributions must:
- Follow TDD (write tests first)
- Maintain 100% code coverage (enforced by CI)
- Pass clippy with zero warnings
- Include documentation
- Use Jujutsu for version control

---

**Happy event sourcing!** 🚀
