# event-sauce Examples

This directory contains examples demonstrating how to use event-sauce for event sourcing in Rust.

## Running Examples

All examples can be run using:

```bash
cargo run -p event-sauce --example <example-name>
```

## Available Examples

### Bank Account (`bank-account.rs`)

A simple but complete bank account implementation demonstrating:

- **Derive Macros**: Using `#[derive(Aggregate)]` and `#[derive(Event)]` to eliminate boilerplate
- **Event Sourcing Basics**: Creating aggregates, applying events, maintaining consistency
- **Business Logic**: Enforcing domain rules (overdraft protection, validation)
- **Event Inspection**: Examining event metadata and history

```bash
cargo run -p event-sauce --example bank-account
```

**Key Features Demonstrated:**
- Custom aggregate IDs with Display trait
- Event enums with timestamps
- Command methods that generate events
- Event application to update state
- Pending events collection
- Business rule validation

**Code Highlights:**

```rust
// Define events with the derive macro
#[derive(Event, Debug, Clone)]
#[event(version = 1, type_prefix = "Account")]
enum AccountEvent {
    Opened {
        account_id: String,
        owner: String,
        initial_balance: i64,
        timestamp: DateTime<Utc>,
    },
    Deposited {
        amount: i64,
        timestamp: DateTime<Utc>,
    },
    // ... more variants
}

// Define aggregate with the derive macro
#[derive(Aggregate, Debug, Clone)]
#[aggregate(id = "AccountId", event = "AccountEvent")]
struct BankAccount {
    #[aggregate_id]
    id: AccountId,
    balance: i64,
    #[aggregate_version]
    version: Version,
    #[aggregate_events]
    pending_events: Vec<AccountEvent>,
}
```

### Task Management with Projections (`task-projections.rs`)

A comprehensive example demonstrating how to build read models (projections) from event streams:

- **Multiple Projections**: Building different views from the same events
- **Event Filtering**: Subscribing to specific event types
- **Checkpointing**: Tracking projection progress for resumability
- **Async Streaming**: Processing events with backpressure
- **Real-world Domain**: Task management with assignments and status changes

```bash
cargo run -p event-sauce --example task-projections --features "memory,projections"
```

**Key Features Demonstrated:**
- Creating custom projection implementations
- Using `ProjectionRunner` to execute projections
- Checkpoint management with `CheckpointStore`
- Event bus subscriptions with filters
- Building multiple read models from same event stream
- Idempotent event processing

**Projections Implemented:**
1. **TaskCountByStatusProjection** - Maintains counts by task status
2. **TasksByAssigneeProjection** - Tracks which tasks are assigned to whom
3. **CompletedTasksProjection** - Counts completed tasks

**Example Output:**
```
📊 Task Count by Status:
  - Todo: 1
  - In Progress: 1
  - Completed: 1

👥 Tasks by Assignee:
  - Alice: 2 task(s)
  - Charlie: 1 task(s)

✅ Completed Tasks: 1
```

## Learning Path

1. **Start here**: `bank-account.rs` - Learn the basics of aggregates and events
2. **Next**: `task-projections.rs` - Learn to build read models with projections
3. **Coming soon**: More examples with sagas and persistence

## Tips

- All examples use in-memory storage for simplicity
- Check the source code comments for detailed explanations
- Examples follow the same structure:
  1. Domain model definition (IDs, events, errors)
  2. Aggregate implementation (commands, business logic)
  3. Example scenario demonstrating features

## Next Steps

After running these examples:

1. Read the [Architecture Guide](../../../docs/architecture.md)
2. Explore the [API Documentation](https://docs.rs/event-sauce)
3. Try modifying the examples to add your own features
4. Build your own event-sourced application!

## Need Help?

- Check the main [README](../../../README.md)
- Read the [CLAUDE.md](../../../CLAUDE.md) development guide
- Open an issue on GitHub
