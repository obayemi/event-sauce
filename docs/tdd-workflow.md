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
    let mut cart = ShoppingCart::new(CartId::new());

    // This will fail because ShoppingCart doesn't exist yet
    cart.add_item(ProductId::new(), "Coffee", 1299, 2).unwrap();

    assert_eq!(cart.item_count(), 1);
}
```

### GREEN: Make It Pass

Write the **minimum** code to pass the test.

```rust
pub struct ShoppingCart {
    id: CartId,
    items: Vec<CartItem>,
}

impl ShoppingCart {
    pub fn new(id: CartId) -> Self {
        Self { id, items: vec![] }
    }

    pub fn add_item(&mut self, id: ProductId, name: &str, price: i64, qty: u32) -> Result<()> {
        self.items.push(CartItem { id, name: name.to_string(), price, quantity: qty });
        Ok(())
    }

    pub fn item_count(&self) -> usize {
        self.items.len()
    }
}
```

### REFACTOR: Improve Design

Now improve the code while keeping tests green:

```rust
// Extract event
pub enum CartEvent {
    ItemAdded {
        product_id: ProductId,
        name: String,
        price: i64,
        quantity: u32,
    }
}

// Use event sourcing
impl ShoppingCart {
    pub fn add_item(&mut self, id: ProductId, name: &str, price: i64, qty: u32) -> Result<()> {
        let event = CartEvent::ItemAdded {
            product_id: id,
            name: name.to_string(),
            price,
            quantity: qty,
        };

        self.apply(&event);
        self.pending_events.push(event);
        Ok(())
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
    let mut cart = ShoppingCart::new(CartId::new());
    let product_id = ProductId::new();
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
    let mut cart = ShoppingCart::new(CartId::new());

    cart.add_item(ProductId::new(), "Coffee", 1299, 2).unwrap();

    // Verify the event
    let events = cart.pending_events();
    assert_eq!(events.len(), 1);

    match &events[0] {
        CartEvent::ItemAdded { name, quantity, .. } => {
            assert_eq!(name, "Coffee");
            assert_eq!(*quantity, 2);
        }
        _ => panic!("Wrong event type"),
    }
}
```

### Pattern 3: State Reconstruction

Test that events rebuild state correctly:

```rust
#[test]
fn test_events_reconstruct_state() {
    let cart_id = CartId::new();
    let product_id = ProductId::new();

    // Build up events
    let mut cart = ShoppingCart::new(cart_id);
    cart.add_item(product_id, "Coffee", 1299, 2).unwrap();
    cart.add_item(product_id, "Coffee", 1299, 1).unwrap();
    cart.remove_item(product_id, 1).unwrap();

    // Replay events
    let events = cart.pending_events().clone();
    let mut replayed = ShoppingCart::new(cart_id);

    for event in events {
        replayed.apply(&event);
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
    let mut cart = ShoppingCart::new(CartId::new());

    let result = cart.add_item(ProductId::new(), "Coffee", 1299, 0);

    assert!(result.is_err());
    assert_eq!(cart.item_count(), 0); // State unchanged
}

#[test]
fn test_cannot_remove_more_than_available() {
    let mut cart = ShoppingCart::new(CartId::new());
    let product_id = ProductId::new();

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
    let mut cart = ShoppingCart::new(CartId::new());

    for op in operations {
        let _ = cart.apply_operation(op);
    }

    assert!(cart.total() >= 0);
}

#[proptest]
fn test_version_always_increases(operations: Vec<CartOperation>) {
    let mut cart = ShoppingCart::new(CartId::new());
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
    let mut cart = ShoppingCart::new(CartId::new());
    let product = ProductId::new();

    cart.add_item(product, "Coffee", 1299, 2).unwrap();

    assert_eq!(cart.item_count(), 1);
    assert_eq!(cart.item_quantity(product), Some(2));
}
```

**GREEN - Minimal implementation:**

```rust
#[derive(Aggregate)]
pub struct ShoppingCart {
    #[aggregate_id]
    id: CartId,
    items: HashMap<ProductId, CartItem>,
    #[aggregate_version]
    version: i64,
    #[aggregate_events]
    pending_events: Vec<CartEvent>,
}

#[derive(Event)]
pub enum CartEvent {
    ItemAdded { product_id: ProductId, name: String, price: i64, quantity: u32 },
}

impl ShoppingCart {
    pub fn add_item(&mut self, id: ProductId, name: &str, price: i64, qty: u32) -> Result<()> {
        let event = CartEvent::ItemAdded {
            product_id: id,
            name: name.to_string(),
            price,
            quantity: qty,
        };

        self.apply(&event);
        self.pending_events.push(event);
        Ok(())
    }

    pub fn item_count(&self) -> usize {
        self.items.len()
    }

    pub fn item_quantity(&self, id: ProductId) -> Option<u32> {
        self.items.get(&id).map(|item| item.quantity)
    }

    fn apply(&mut self, event: &CartEvent) {
        match event {
            CartEvent::ItemAdded { product_id, name, price, quantity } => {
                self.items.insert(*product_id, CartItem {
                    name: name.clone(),
                    price: *price,
                    quantity: *quantity,
                });
                self.version += 1;
            }
        }
    }
}
```

### Iteration 2: Add Same Item Twice

**RED:**

```rust
#[test]
fn test_add_same_item_increases_quantity() {
    let mut cart = ShoppingCart::new(CartId::new());
    let product = ProductId::new();

    cart.add_item(product, "Coffee", 1299, 2).unwrap();
    cart.add_item(product, "Coffee", 1299, 1).unwrap();

    assert_eq!(cart.item_count(), 1);
    assert_eq!(cart.item_quantity(product), Some(3));
}
```

**GREEN:**

```rust
fn apply(&mut self, event: &CartEvent) {
    match event {
        CartEvent::ItemAdded { product_id, name, price, quantity } => {
            self.items.entry(*product_id)
                .and_modify(|item| item.quantity += quantity)
                .or_insert(CartItem {
                    name: name.clone(),
                    price: *price,
                    quantity: *quantity,
                });
            self.version += 1;
        }
    }
}
```

### Iteration 3: Validate Quantity

**RED:**

```rust
#[test]
fn test_cannot_add_zero_quantity() {
    let mut cart = ShoppingCart::new(CartId::new());

    let result = cart.add_item(ProductId::new(), "Coffee", 1299, 0);

    assert!(matches!(result, Err(CartError::InvalidQuantity(0))));
}
```

**GREEN:**

```rust
pub fn add_item(&mut self, id: ProductId, name: &str, price: i64, qty: u32) -> Result<(), CartError> {
    if qty == 0 {
        return Err(CartError::InvalidQuantity(qty));
    }

    // ... rest of implementation
}
```

### Iteration 4: Remove Item

**RED:**

```rust
#[test]
fn test_remove_item_decreases_quantity() {
    let mut cart = ShoppingCart::new(CartId::new());
    let product = ProductId::new();

    cart.add_item(product, "Coffee", 1299, 3).unwrap();
    cart.remove_item(product, 1).unwrap();

    assert_eq!(cart.item_quantity(product), Some(2));
}
```

**GREEN:**

```rust
#[derive(Event)]
pub enum CartEvent {
    ItemAdded { ... },
    ItemRemoved { product_id: ProductId, quantity: u32 },
}

pub fn remove_item(&mut self, id: ProductId, qty: u32) -> Result<(), CartError> {
    let current = self.items.get(&id)
        .ok_or(CartError::ItemNotFound(id))?;

    if current.quantity < qty {
        return Err(CartError::InsufficientQuantity {
            available: current.quantity,
            requested: qty,
        });
    }

    let event = CartEvent::ItemRemoved { product_id: id, quantity: qty };
    self.apply(&event);
    self.pending_events.push(event);
    Ok(())
}
```

## Coverage and Quality

### Generate Coverage Reports

```bash
# Install cargo-llvm-cov
cargo install cargo-llvm-cov

# Generate HTML report
cargo llvm-cov --workspace --html
open target/llvm-cov/html/index.html

# Generate LCOV for CI
cargo llvm-cov --workspace --lcov --output-path coverage.lcov

# Check coverage percentage
cargo llvm-cov --workspace --summary-only
```

### Aim for 100% Coverage

```bash
# This project enforces 100% coverage
cargo llvm-cov --workspace --fail-under-lines 100
```

### Run Clippy

```bash
# Zero warnings policy
cargo clippy --workspace -- -D warnings
```

### Run Tests

```bash
# All tests
cargo test --workspace

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

### ❌ Testing Implementation Details

```rust
#[test]
fn test_internal_hashmap_size() {
    let cart = ShoppingCart::new(CartId::new());
    assert_eq!(cart.items.len(), 0); // BAD: Testing internal structure
}
```

### ✅ Testing Behavior

```rust
#[test]
fn test_new_cart_is_empty() {
    let cart = ShoppingCart::new(CartId::new());
    assert_eq!(cart.item_count(), 0); // GOOD: Testing public API
}
```

### ❌ Not Testing Event Replay

```rust
#[test]
fn test_add_item() {
    let mut cart = ShoppingCart::new(CartId::new());
    cart.add_item(ProductId::new(), "Coffee", 1299, 2).unwrap();
    assert_eq!(cart.item_count(), 1); // Incomplete: Doesn't test replay
}
```

### ✅ Testing Event Replay

```rust
#[test]
fn test_add_item_and_replay() {
    let mut cart = ShoppingCart::new(CartId::new());
    cart.add_item(ProductId::new(), "Coffee", 1299, 2).unwrap();

    // Test replay
    let events = cart.pending_events().clone();
    let mut replayed = ShoppingCart::new(cart.id());
    for event in events {
        replayed.apply(&event);
    }

    assert_eq!(replayed.item_count(), cart.item_count());
}
```

## Resources

- [Getting Started Guide](getting-started.md)
- [Architecture Overview](architecture.md)
- [Examples](../examples/)
- [proptest Documentation](https://docs.rs/proptest/)
