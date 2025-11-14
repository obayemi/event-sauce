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

## Learning Path

1. **Start here**: `bank-account.rs` - Learn the basics of aggregates and events
2. **Coming soon**: More examples with projections, sagas, and persistence

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
