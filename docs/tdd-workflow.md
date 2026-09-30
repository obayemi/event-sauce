# Test-Driven Development Workflow

This guide shows how to use TDD (Test-Driven Development) when building event-sourced applications with event-sauce.

## Table of Contents

1. [Why TDD for Event Sourcing?](#why-tdd-for-event-sourcing)
2. [The RED-GREEN-REFACTOR Cycle](#the-red-green-refactor-cycle)
3. [Testing Patterns](#testing-patterns)
4. [Example: Building a Shopping Cart](#example-building-a-shopping-cart)
5. [Coverage and Quality](#coverage-and-quality)

## Why TDD for Event Sourcing?

Event sourcing and TDD are a perfect match:

1. **Events are facts** - Easy to test: given events, when command, then events
2. **No mocking needed** - Use in-memory store for fast tests
3. **Behavior-focused** - Tests describe what the system does
4. **Regression prevention** - Events provide complete test data
5. **Living documentation** - Tests show how to use the API

## The RED-GREEN-REFACTOR Cycle

```
RED ─────→ GREEN ─────→ REFACTOR ─────┐
 ↑                                     │
 └─────────────────────────────────────┘
```

### RED: Write a Failing Test

Write the smallest test that fails for the right reason.

```rust
#[test]
fn test_add_item_to_cart() {
    let mut cart = AggregateRoot::<ShoppingCart>::new(EntityId::new());

    // This will fail because ShoppingCart doesn't exist yet
    cart.add_item(EntityId::new(), "Coffee", 1299, 2).unwrap();

    assert_eq!(cart.item_count(), 1);
}
```

### GREEN: Make It Pass

Write the **minimum** code to pass the test.

```rust
#[aggregate(event = "CartEvent", error = "CartError")]
#[derive(Default)]
pub struct ShoppingCart {
    #[id]
    id: EntityId,
    items: Vec<CartItem>,
}

impl ShoppingCart {
    pub fn item_count(&self) -> usize {
        self.items.len()
    }
}

impl AggregateRoot<ShoppingCart> {
    pub fn add_item(&mut self, product_id: EntityId, name: &str, price: i64, qty: u32) -> Result<(), CartError> {
        let event = ItemAddedEvent {
            product_id,
            name: name.to_string(),
            price,
            quantity: qty,
            timestamp: Utc::now(),
        };
        self.apply(event)
    }
}
```

### REFACTOR: Improve Design

Now improve the code while keeping tests green:

```rust
// Use define_events! for less boilerplate
define_events! {
    pub enum CartEvent for ShoppingCart {
        ItemAdded {
            product_id: EntityId,
            name: String,
            price: i64,
            quantity: u32,
        } => |cart, event| {
            cart.items.push(CartItem {
                product_id: event.product_id,
                name: event.name.clone(),
                price: event.price,
                quantity: event.quantity,
            });
        },
    }
}

// Use command_handler! for command methods
command_handler! {
    impl ShoppingCart {
        @clock fn add_item(product_id: EntityId, name: String, price: i64, quantity: u32)
            -> ItemAddedEvent { product_id, name, price, quantity };
    }
}
```

## Testing Patterns

### Pattern 1: Given-When-Then

Test business logic with clear structure:

```rust
#[test]
fn test_removing_item_decreases_quantity() {
    // GIVEN: A cart with 2 coffees
    let mut cart = AggregateRoot::<ShoppingCart>::new(EntityId::new());
    let product_id = EntityId::new();
    cart.add_item(product_id, "Coffee", 1299, 2).unwrap();

    // WHEN: Remove 1 coffee
    cart.remove_item(product_id, 1).unwrap();

    // THEN: Quantity is 1
    assert_eq!(cart.item_quantity(product_id), Some(1));
}
```

### Pattern 2: Event-Based Testing

Test by verifying events:

```rust
#[test]
fn test_adding_item_produces_event() {
    let mut cart = AggregateRoot::<ShoppingCart>::new(EntityId::new());

    cart.add_item(EntityId::new(), "Coffee", 1299, 2).unwrap();

    // Verify the event
    let events = cart.pending_events();
    assert_eq!(events.len(), 1);
}
```

### Pattern 3: State Reconstruction

Test that events rebuild state correctly:

```rust
#[test]
fn test_events_reconstruct_state() {
    let product_id = EntityId::new();

    // Build up events
    let mut cart = AggregateRoot::<ShoppingCart>::new(EntityId::new());
    cart.add_item(product_id, "Coffee", 1299, 2).unwrap();
    cart.add_item(product_id, "Coffee", 1299, 1).unwrap();
    cart.remove_item(product_id, 1).unwrap();

    // Replay events
    let events = cart.pending_events().to_vec();
    let mut replayed = AggregateRoot::<ShoppingCart>::new(EntityId::new());

    for event in &events {
        replayed.apply_unchecked(event);
    }

    // State should match
    assert_eq!(replayed.item_quantity(product_id), cart.item_quantity(product_id));
    assert_eq!(replayed.total(), cart.total());
}
```

### Pattern 4: Invariant Testing

Test business rules are enforced:

```rust
#[test]
fn test_cannot_add_zero_quantity() {
    let mut cart = AggregateRoot::<ShoppingCart>::new(EntityId::new());

    let result = cart.add_item(EntityId::new(), "Coffee", 1299, 0);

    assert!(result.is_err());
    assert_eq!(cart.item_count(), 0); // State unchanged
}

#[test]
fn test_cannot_remove_more_than_available() {
    let mut cart = AggregateRoot::<ShoppingCart>::new(EntityId::new());
    let product_id = EntityId::new();

    cart.add_item(product_id, "Coffee", 1299, 2).unwrap();

    let result = cart.remove_item(product_id, 3);

    assert!(result.is_err());
    assert_eq!(cart.item_quantity(product_id), Some(2)); // Unchanged
}
```

### Pattern 5: Property-Based Testing

Use proptest for exhaustive testing:

```rust
use proptest::prelude::*;

#[proptest]
fn test_total_never_negative(operations: Vec<CartOperation>) {
    let mut cart = AggregateRoot::<ShoppingCart>::new(EntityId::new());

    for op in operations {
        let _ = cart.apply_operation(op);
    }

    assert!(cart.total() >= 0);
}

#[proptest]
fn test_version_always_increases(operations: Vec<CartOperation>) {
    let mut cart = AggregateRoot::<ShoppingCart>::new(EntityId::new());
    let mut prev_version = cart.version();

    for op in operations {
        let _ = cart.apply_operation(op);
        assert!(cart.version() >= prev_version);
        prev_version = cart.version();
    }
}
```

## Example: Building a Shopping Cart

Let's build a shopping cart feature using TDD.

### Iteration 1: Add Item

**RED - Write the test:**

```rust
#[test]
fn test_add_item_to_empty_cart() {
    let mut cart = AggregateRoot::<ShoppingCart>::new(EntityId::new());
    let product = EntityId::new();

    cart.add_item(product, "Coffee", 1299, 2).unwrap();

    assert_eq!(cart.item_count(), 1);
    assert_eq!(cart.item_quantity(product), Some(2));
}
```

**GREEN - Minimal implementation:**

```rust
use event_sauce::prelude::*;
use thiserror::Error;

#[derive(AggregateError, Debug, Error)]
enum CartError {
    #[error("Invalid quantity: {0}")]
    InvalidQuantity(u32),
    #[error("Item not found")]
    ItemNotFound(EntityId),
    #[error("Insufficient quantity: available={available}, requested={requested}")]
    InsufficientQuantity { available: u32, requested: u32 },
}

#[aggregate(event = "CartEvent", error = "CartError")]
#[derive(Default)]
pub struct ShoppingCart {
    #[id]
    id: EntityId,
    items: HashMap<EntityId, CartItem>,
}

define_events! {
    pub enum CartEvent for ShoppingCart {
        ItemAdded {
            product_id: EntityId,
            name: String,
            price: i64,
            quantity: u32,
        } => |cart, event| {
            cart.items.entry(event.product_id)
                .and_modify(|item| item.quantity += event.quantity)
                .or_insert(CartItem {
                    name: event.name.clone(),
                    price: event.price,
                    quantity: event.quantity,
                });
        },
    }
}

command_handler! {
    impl ShoppingCart {
        @clock fn add_item(product_id: EntityId, name: String, price: i64, quantity: u32)
            -> ItemAddedEvent { product_id, name, price, quantity };
    }
}

impl ShoppingCart {
    pub fn item_count(&self) -> usize {
        self.items.len()
    }

    pub fn item_quantity(&self, id: EntityId) -> Option<u32> {
        self.items.get(&id).map(|item| item.quantity)
    }
}
```

### Iteration 2: Add Same Item Twice

**RED:**

```rust
#[test]
fn test_add_same_item_increases_quantity() {
    let mut cart = AggregateRoot::<ShoppingCart>::new(EntityId::new());
    let product = EntityId::new();

    cart.add_item(product, "Coffee", 1299, 2).unwrap();
    cart.add_item(product, "Coffee", 1299, 1).unwrap();

    assert_eq!(cart.item_count(), 1);
    assert_eq!(cart.item_quantity(product), Some(3));
}
```

**GREEN:** Already handled by the `and_modify` in the apply logic above.

### Iteration 3: Validate Quantity

**RED:**

```rust
#[test]
fn test_cannot_add_zero_quantity() {
    let mut cart = AggregateRoot::<ShoppingCart>::new(EntityId::new());

    let result = cart.add_item(EntityId::new(), "Coffee", 1299, 0);

    assert!(matches!(result, Err(CartError::InvalidQuantity(0))));
}
```

**GREEN:** Add validation to the event:

```rust
define_events! {
    pub enum CartEvent for ShoppingCart {
        ItemAdded {
            product_id: EntityId,
            name: String,
            price: i64,
            quantity: u32,
        }
        @validate |_cart, event| {
            if event.quantity == 0 {
                return Err(CartError::InvalidQuantity(event.quantity));
            }
            Ok(())
        }
        => |cart, event| {
            cart.items.entry(event.product_id)
                .and_modify(|item| item.quantity += event.quantity)
                .or_insert(CartItem {
                    name: event.name.clone(),
                    price: event.price,
                    quantity: event.quantity,
                });
        },
    }
}
```

### Iteration 4: Remove Item

**RED:**

```rust
#[test]
fn test_remove_item_decreases_quantity() {
    let mut cart = AggregateRoot::<ShoppingCart>::new(EntityId::new());
    let product = EntityId::new();

    cart.add_item(product, "Coffee", 1299, 3).unwrap();
    cart.remove_item(product, 1).unwrap();

    assert_eq!(cart.item_quantity(product), Some(2));
}
```

**GREEN:** Add ItemRemoved event and remove_item command:

```rust
define_events! {
    pub enum CartEvent for ShoppingCart {
        ItemAdded { ... } => ...,

        ItemRemoved { product_id: EntityId, quantity: u32 }
        @validate |aggregate, event| {
            let current = aggregate.items.get(&event.product_id)
                .ok_or(CartError::ItemNotFound(event.product_id))?;
            if current.quantity < event.quantity {
                return Err(CartError::InsufficientQuantity {
                    available: current.quantity,
                    requested: event.quantity,
                });
            }
            Ok(())
        }
        => |cart, event| {
            if let Some(item) = cart.items.get_mut(&event.product_id) {
                item.quantity -= event.quantity;
                if item.quantity == 0 {
                    cart.items.remove(&event.product_id);
                }
            }
        },
    }
}

command_handler! {
    impl ShoppingCart {
        @clock fn add_item(product_id: EntityId, name: String, price: i64, quantity: u32)
            -> ItemAddedEvent { product_id, name, price, quantity };
        @clock fn remove_item(product_id: EntityId, quantity: u32)
            -> ItemRemovedEvent { product_id, quantity };
    }
}
```

## Coverage and Quality

### Generate Coverage Reports

```bash
# Install cargo-llvm-cov
cargo install cargo-llvm-cov

# Generate HTML report
cargo llvm-cov --workspace --all-features --all-targets --html
open target/llvm-cov/html/index.html

# Generate LCOV for CI
cargo llvm-cov --workspace --all-features --all-targets --lcov --output-path coverage.lcov

# Check coverage percentage
cargo llvm-cov --workspace --all-features --all-targets --summary-only
```

### Aim for 100% Coverage (90% is the CI floor)

```bash
# CI fails below 90% line coverage; 100% is the target
cargo llvm-cov --workspace --all-features --all-targets --fail-under-lines 90
```

### Run Clippy

```bash
# Zero warnings policy
cargo clippy --workspace --all-features --all-targets -- -D warnings
```

### Run Tests

```bash
# All tests
cargo test --workspace --all-features --all-targets

# With output
cargo test --workspace -- --nocapture

# Specific test
cargo test test_add_item_to_cart -- --nocapture
```

## Testing Checklist

For each aggregate, test:

- [ ] Commands produce correct events
- [ ] Events update state correctly
- [ ] Business rules are enforced
- [ ] Invalid inputs are rejected
- [ ] State is unchanged on errors
- [ ] Events can reconstruct state
- [ ] Concurrent modifications handled
- [ ] Edge cases covered
- [ ] Error messages are clear

## Best Practices

1. **Test behavior, not implementation** - Don't test private methods
2. **One assertion per concept** - Multiple assertions for related checks are OK
3. **Use descriptive names** - `test_cannot_remove_more_items_than_in_cart`
4. **Arrange-Act-Assert** - Clear test structure
5. **Fast tests** - Use in-memory store, no I/O
6. **Independent tests** - No shared state
7. **Test errors** - Verify error handling
8. **Property tests** - Catch edge cases

## Common Pitfalls

### Testing Implementation Details (Bad)

```rust
#[test]
fn test_internal_hashmap_size() {
    let cart = AggregateRoot::<ShoppingCart>::new(EntityId::new());
    // BAD: Testing internal structure directly
}
```

### Testing Behavior (Good)

```rust
#[test]
fn test_new_cart_is_empty() {
    let cart = AggregateRoot::<ShoppingCart>::new(EntityId::new());
    assert_eq!(cart.item_count(), 0); // GOOD: Testing public API
}
```

### Not Testing Event Replay (Bad)

```rust
#[test]
fn test_add_item() {
    let mut cart = AggregateRoot::<ShoppingCart>::new(EntityId::new());
    cart.add_item(EntityId::new(), "Coffee", 1299, 2).unwrap();
    assert_eq!(cart.item_count(), 1); // Incomplete: Doesn't test replay
}
```

### Testing Event Replay (Good)

```rust
#[test]
fn test_add_item_and_replay() {
    let mut cart = AggregateRoot::<ShoppingCart>::new(EntityId::new());
    cart.add_item(EntityId::new(), "Coffee", 1299, 2).unwrap();

    // Test replay
    let events = cart.pending_events().to_vec();
    let mut replayed = AggregateRoot::<ShoppingCart>::new(EntityId::new());
    for event in &events {
        replayed.apply_unchecked(event);
    }

    assert_eq!(replayed.item_count(), cart.item_count());
}
```

## Resources

- [Getting Started Guide](getting-started.md)
- [Architecture Overview](architecture.md)
- [Examples](../crates/event-sauce/examples/)
- [proptest Documentation](https://docs.rs/proptest/)
