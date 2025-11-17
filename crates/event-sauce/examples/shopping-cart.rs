//! Shopping Cart Example
//!
//! This example demonstrates a complex aggregate with AggregateState:
//! - Multiple operations (add, remove, clear, checkout)
//! - Business rules (inventory, pricing, checkout state)
//! - Complex state (HashMap of items)
//! - Calculations (totals)
//!
//! Run with: cargo run -p event-sauce --example shopping-cart --features "memory,macros"

use chrono::Utc;
use event_sauce_core::{Aggregate, AggregateError, AggregateId, ApplyEvent};
use event_sauce_macros::{AggregateState, Event as DeriveEvent};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt;
use uuid::Uuid;

// ============================================================================
// Domain Model
// ============================================================================

/// Unique identifier for a shopping cart
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
struct CartId(Uuid);

impl CartId {
    fn new() -> Self {
        Self(Uuid::new_v4())
    }
}

impl Default for CartId {
    fn default() -> Self {
        Self(Uuid::nil())
    }
}

impl fmt::Display for CartId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Cart-{}", self.0)
    }
}

impl AggregateId for CartId {}

/// Unique identifier for a product
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
struct ProductId(Uuid);

impl ProductId {
    fn new() -> Self {
        Self(Uuid::new_v4())
    }
}

/// Shopping cart item
#[derive(Debug, Clone, Serialize, Deserialize)]
struct CartItem {
    product_id: ProductId,
    name: String,
    price: i64, // Price in cents
    quantity: u32,
}

impl CartItem {
    fn subtotal(&self) -> i64 {
        self.price * i64::from(self.quantity)
    }
}

// ============================================================================
// Event Structs - Separated event definitions
// ============================================================================

/// Event: Shopping cart was created
#[derive(Debug, Clone, Serialize, Deserialize)]
struct CartCreatedEvent {
    cart_id: String,
    customer_id: String,
    timestamp: chrono::DateTime<Utc>,
}

/// Event: Item was added to cart
#[derive(Debug, Clone, Serialize, Deserialize)]
struct CartItemAddedEvent {
    product_id: ProductId,
    name: String,
    price: i64,
    quantity: u32,
    timestamp: chrono::DateTime<Utc>,
}

/// Event: Item was removed from cart
#[derive(Debug, Clone, Serialize, Deserialize)]
struct CartItemRemovedEvent {
    product_id: ProductId,
    quantity: u32,
    timestamp: chrono::DateTime<Utc>,
}

/// Event: Cart was cleared
#[derive(Debug, Clone, Serialize, Deserialize)]
struct CartClearedEvent {
    timestamp: chrono::DateTime<Utc>,
}

/// Event: Cart was checked out
#[derive(Debug, Clone, Serialize, Deserialize)]
struct CartCheckedOutEvent {
    total: i64,
    timestamp: chrono::DateTime<Utc>,
}

/// Domain events wrapping separated event structs
#[derive(DeriveEvent, Debug, Clone, Serialize, Deserialize)]
#[event(version = 1, type_prefix = "Cart")]
enum CartEvent {
    Created(CartCreatedEvent),
    ItemAdded(CartItemAddedEvent),
    ItemRemoved(CartItemRemovedEvent),
    Cleared(CartClearedEvent),
    CheckedOut(CartCheckedOutEvent),
}

/// Domain errors
#[derive(Debug, thiserror::Error)]
enum CartError {
    #[error("Invalid quantity: {0} (must be greater than 0)")]
    InvalidQuantity(u32),

    #[error("Invalid price: {0} (must be greater than 0)")]
    InvalidPrice(i64),

    #[error("Product not found in cart: {0:?}")]
    ProductNotFound(ProductId),

    #[error("Insufficient quantity: available={available}, requested={requested}")]
    InsufficientQuantity { available: u32, requested: u32 },

    #[error("Cart is empty")]
    EmptyCart,

    #[error("Cart already checked out")]
    AlreadyCheckedOut,
}

impl AggregateError for CartError {}

// ============================================================================
// ApplyEvent Implementations - for GENERATED ShoppingCartAggregate
// ============================================================================

impl ApplyEvent<ShoppingCartAggregate, CartError> for CartCreatedEvent {
    fn apply(&self, cart: &mut ShoppingCartAggregate) {
        cart.customer_id = self.customer_id.clone();
    }
}

impl ApplyEvent<ShoppingCartAggregate, CartError> for CartItemAddedEvent {
    fn validate(&self, cart: &ShoppingCartAggregate) -> Result<(), CartError> {
        if cart.checked_out {
            return Err(CartError::AlreadyCheckedOut);
        }
        if self.quantity == 0 {
            return Err(CartError::InvalidQuantity(self.quantity));
        }
        if self.price <= 0 {
            return Err(CartError::InvalidPrice(self.price));
        }
        Ok(())
    }

    fn apply(&self, cart: &mut ShoppingCartAggregate) {
        cart.items
            .entry(self.product_id)
            .and_modify(|item| item.quantity += self.quantity)
            .or_insert(CartItem {
                product_id: self.product_id,
                name: self.name.clone(),
                price: self.price,
                quantity: self.quantity,
            });
    }
}

impl ApplyEvent<ShoppingCartAggregate, CartError> for CartItemRemovedEvent {
    fn validate(&self, cart: &ShoppingCartAggregate) -> Result<(), CartError> {
        if cart.checked_out {
            return Err(CartError::AlreadyCheckedOut);
        }
        let item = cart
            .items
            .get(&self.product_id)
            .ok_or(CartError::ProductNotFound(self.product_id))?;
        if item.quantity < self.quantity {
            return Err(CartError::InsufficientQuantity {
                available: item.quantity,
                requested: self.quantity,
            });
        }
        Ok(())
    }

    fn apply(&self, cart: &mut ShoppingCartAggregate) {
        if let Some(item) = cart.items.get_mut(&self.product_id) {
            if item.quantity <= self.quantity {
                cart.items.remove(&self.product_id);
            } else {
                item.quantity -= self.quantity;
            }
        }
    }
}

impl ApplyEvent<ShoppingCartAggregate, CartError> for CartClearedEvent {
    fn validate(&self, cart: &ShoppingCartAggregate) -> Result<(), CartError> {
        if cart.checked_out {
            return Err(CartError::AlreadyCheckedOut);
        }
        Ok(())
    }

    fn apply(&self, cart: &mut ShoppingCartAggregate) {
        cart.items.clear();
    }
}

impl ApplyEvent<ShoppingCartAggregate, CartError> for CartCheckedOutEvent {
    fn validate(&self, cart: &ShoppingCartAggregate) -> Result<(), CartError> {
        if cart.checked_out {
            return Err(CartError::AlreadyCheckedOut);
        }
        if cart.items.is_empty() {
            return Err(CartError::EmptyCart);
        }
        Ok(())
    }

    fn apply(&self, cart: &mut ShoppingCartAggregate) {
        cart.checked_out = true;
    }
}

// ============================================================================
// Shopping Cart State - Business Logic Only
// ============================================================================

/// Shopping cart state - contains only business data
#[derive(AggregateState, Debug, Clone, Default)]
#[aggregate(id = "CartId", event = "CartEvent", error = "CartError")]
struct ShoppingCartState {
    #[aggregate_id]
    id: CartId,
    customer_id: String,
    items: HashMap<ProductId, CartItem>,
    checked_out: bool,
}

impl ShoppingCartState {
    fn new(id: CartId) -> Self {
        Self {
            id,
            customer_id: String::new(),
            items: HashMap::new(),
            checked_out: false,
        }
    }

    /// Apply event to update state
    fn apply_event(&mut self, event: &CartEvent) {
        match event {
            CartEvent::Created(e) => {
                self.customer_id = e.customer_id.clone();
            }
            CartEvent::ItemAdded(e) => {
                self.items
                    .entry(e.product_id)
                    .and_modify(|item| item.quantity += e.quantity)
                    .or_insert(CartItem {
                        product_id: e.product_id,
                        name: e.name.clone(),
                        price: e.price,
                        quantity: e.quantity,
                    });
            }
            CartEvent::ItemRemoved(e) => {
                if let Some(item) = self.items.get_mut(&e.product_id) {
                    if item.quantity <= e.quantity {
                        self.items.remove(&e.product_id);
                    } else {
                        item.quantity -= e.quantity;
                    }
                }
            }
            CartEvent::Cleared(_) => {
                self.items.clear();
            }
            CartEvent::CheckedOut(_) => {
                self.checked_out = true;
            }
        }
    }
}

// ============================================================================
// Command Methods - Implemented on generated ShoppingCartAggregate
// ============================================================================

impl ShoppingCartAggregate {
    /// Create a new shopping cart
    fn create(id: CartId, customer_id: String) -> Self {
        let mut cart = Self::from_state(ShoppingCartState::new(id));

        cart.apply(CartCreatedEvent {
            cart_id: id.to_string(),
            customer_id,
            timestamp: Utc::now(),
        });

        cart
    }

    /// Add an item to the cart
    fn add_item(
        &mut self,
        product_id: ProductId,
        name: String,
        price: i64,
        quantity: u32,
    ) -> Result<(), CartError> {
        let event = CartItemAddedEvent {
            product_id,
            name,
            price,
            quantity,
            timestamp: Utc::now(),
        };
        event.validate(self)?;
        self.apply(event);
        Ok(())
    }

    /// Remove a quantity of an item
    fn remove_item(&mut self, product_id: ProductId, quantity: u32) -> Result<(), CartError> {
        let event = CartItemRemovedEvent {
            product_id,
            quantity,
            timestamp: Utc::now(),
        };
        event.validate(self)?;
        self.apply(event);
        Ok(())
    }

    /// Clear all items
    #[allow(dead_code)]
    fn clear(&mut self) -> Result<(), CartError> {
        let event = CartClearedEvent {
            timestamp: Utc::now(),
        };
        event.validate(self)?;
        self.apply(event);
        Ok(())
    }

    /// Checkout the cart
    fn checkout(&mut self) -> Result<(), CartError> {
        let total = self.total();
        let event = CartCheckedOutEvent {
            total,
            timestamp: Utc::now(),
        };
        event.validate(self)?;
        self.apply(event);
        Ok(())
    }

    /// Calculate total
    fn total(&self) -> i64 {
        self.items.values().map(|item| item.subtotal()).sum()
    }

    /// Get item count
    fn item_count(&self) -> usize {
        self.items.len()
    }

    /// Get total quantity
    fn total_quantity(&self) -> u32 {
        self.items.values().map(|item| item.quantity).sum()
    }
}

// ============================================================================
// Main Example
// ============================================================================

fn main() -> Result<(), CartError> {
    println!("\n{}", "=".repeat(70));
    println!("🛒 Shopping Cart - Complex Aggregate with AggregateState");
    println!("{}\n", "=".repeat(70));

    // Create cart
    let cart_id = CartId::new();
    let mut cart = ShoppingCartAggregate::create(cart_id, "customer-123".to_string());
    println!("✓ Created cart: {}", cart_id);
    println!("  Customer: {}", cart.customer_id);

    // Add items
    println!("\n📝 Adding items...");
    let product1 = ProductId::new();
    let product2 = ProductId::new();

    cart.add_item(product1, "Laptop".to_string(), 99900, 1)?;
    println!("✓ Added: Laptop x1 @ ${:.2}", 999.00);

    cart.add_item(product2, "Mouse".to_string(), 2500, 2)?;
    println!("✓ Added: Mouse x2 @ ${:.2}", 25.00);

    println!("\n📊 Cart Summary:");
    println!("  Items: {}", cart.item_count());
    println!("  Total quantity: {}", cart.total_quantity());
    println!("  Total: ${:.2}", cart.total() as f64 / 100.0);
    println!("  Pending events: {}", cart.pending_events().len());

    // Remove item
    println!("\n📝 Removing 1 mouse...");
    cart.remove_item(product2, 1)?;
    println!("✓ Removed: Mouse x1");
    println!("  New total: ${:.2}", cart.total() as f64 / 100.0);

    // Checkout
    println!("\n📝 Checking out...");
    cart.checkout()?;
    println!("✓ Cart checked out!");
    println!("  Final total: ${:.2}", cart.total() as f64 / 100.0);
    println!("  Total events: {}", cart.pending_events().len());

    // Test error handling
    println!("\n📝 Testing business rules...");
    match cart.add_item(product1, "Laptop".to_string(), 99900, 1) {
        Err(CartError::AlreadyCheckedOut) => {
            println!("✓ Correctly rejected adding to checked-out cart");
        }
        _ => println!("❌ Should have rejected!"),
    }

    println!("\n{}", "=".repeat(70));
    println!("✅ Example completed!");
    println!("\nKey Takeaways:");
    println!("  • AggregateState works seamlessly with complex state (HashMap)");
    println!("  • ~45% less boilerplate vs manual aggregate");
    println!("  • Clear separation: business logic vs infrastructure");
    println!("  • Validation rules enforced in ApplyEvent implementations");
    println!("{}", "=".repeat(70));
    println!();

    Ok(())
}
