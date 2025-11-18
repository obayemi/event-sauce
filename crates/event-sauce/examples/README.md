# event-sauce Examples

This directory contains examples demonstrating how to use event-sauce for event sourcing in Rust.

## Running Examples

All examples can be run using:

```bash
cargo run -p event-sauce --example <example-name>
```

## Available Examples

### PostgreSQL Quick Start (`postgres-quickstart.rs`) 🆕

**The simplest way to get started with PostgreSQL-backed event sourcing.**

A beginner-friendly example demonstrating:

- **PostgreSQL Setup**: Connect to PostgreSQL and run migrations
- **Event Store Creation**: Initialize a production-ready event store
- **Basic CRUD**: Create, update, and load aggregates from the database
- **Optimistic Concurrency**: Automatic version control and conflict detection
- **Error Handling**: Graceful error handling with domain-specific errors

```bash
# Prerequisites: PostgreSQL running on localhost
docker run -d -p 5432:5432 \
  -e POSTGRES_PASSWORD=postgres \
  -e POSTGRES_DB=eventsauce \
  postgres:16-alpine

# Run the example
cargo run -p event-sauce --example postgres-quickstart --features "postgres,macros"
```

**Key Features Demonstrated:**
- Connecting to PostgreSQL with `sqlx`
- Running database migrations automatically
- Creating a `PostgresEventStore`
- Full aggregate lifecycle (create → modify → save → load)
- Domain validation and business rules
- Version tracking across database operations

**Perfect for:**
- First-time users wanting to see PostgreSQL in action
- Setting up a new production project
- Understanding the complete flow from connection to persistence

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
- **Modern Event Pattern**: Using `#[derive(Event)]` with separate event structs

```bash
cargo run -p event-sauce --example task-projections --features "memory,projections,macros"
```

**Key Features Demonstrated:**
- Creating custom projection implementations
- Using `ProjectionRunner` to execute projections
- Checkpoint management with `CheckpointStore`
- Event bus subscriptions with filters
- Building multiple read models from same event stream
- Idempotent event processing
- Modern event pattern with `#[derive(Event)]` macro

**Projections Implemented:**
1. **TaskCountByStatusProjection** - Maintains counts by task status
2. **TasksByAssigneeProjection** - Tracks which tasks are assigned to whom
3. **CompletedTasksProjection** - Counts completed tasks

**Code Highlights:**

```rust
// Modern event pattern with separate structs
#[derive(Debug, Clone, Serialize, Deserialize)]
struct Created {
    task_id: Uuid,
    title: String,
    assignee: String,
    timestamp: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct StatusChanged {
    task_id: Uuid,
    old_status: TaskStatus,
    new_status: TaskStatus,
    timestamp: DateTime<Utc>,
}

// Event enum wrapping individual events
#[derive(Event, Debug, Clone, Serialize, Deserialize)]
#[event(version = 1, type_prefix = "Task")]
enum TaskEvent {
    Created(Created),
    StatusChanged(StatusChanged),
    Assigned(Assigned),
    Completed(Completed),
}

// Projections handle events using pattern matching
match event_data {
    TaskEvent::Created(Created { task_id, assignee, .. }) => {
        // Update projection state
    }
    TaskEvent::StatusChanged(StatusChanged { old_status, new_status, .. }) => {
        // Update projection state
    }
    _ => {}
}
```

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

### For Beginners:
1. **PostgreSQL Users**: Start with `postgres-quickstart.rs` - Get up and running with PostgreSQL
2. **In-Memory/Learning**: Start with `bank-account.rs` - Learn the basics without database setup

### Next Steps:
3. `task-projections.rs` - Learn to build read models with projections
4. `snapshotting-postgres.rs` - Optimize performance with snapshots
5. Coming soon: More examples with sagas and complex workflows

## Tips

- Most examples use in-memory storage for simplicity (except `postgres-quickstart.rs` and `snapshotting-postgres.rs`)
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
