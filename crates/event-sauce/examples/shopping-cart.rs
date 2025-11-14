//! Shopping Cart Example
//!
//! This example demonstrates a more complex aggregate with:
//! - Multiple operations (add, remove, clear)
//! - Business rules (inventory, pricing)
//! - Complex state (HashMap of items)
//! - Calculations (totals, discounts)
//!
//! Run with: cargo run -p event-sauce --example shopping-cart --features "memory,macros"

use chrono::Utc;
use event_sauce_core::{Aggregate, AggregateId, Version};
use event_sauce_macros::{Aggregate as DeriveAggregate, Event as DeriveEvent};
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

/// Domain events for ShoppingCart
#[derive(DeriveEvent, Debug, Clone, Serialize, Deserialize)]
#[event(version = 1, type_prefix = "Cart")]
enum CartEvent {
    Created {
        cart_id: String,
        customer_id: String,
        timestamp: chrono::DateTime<Utc>,
    },
    ItemAdded {
        product_id: ProductId,
        name: String,
        price: i64,
        quantity: u32,
        timestamp: chrono::DateTime<Utc>,
    },
    ItemRemoved {
        product_id: ProductId,
        quantity: u32,
        timestamp: chrono::DateTime<Utc>,
    },
    ItemQuantityChanged {
        product_id: ProductId,
        new_quantity: u32,
        timestamp: chrono::DateTime<Utc>,
    },
    Cleared {
        timestamp: chrono::DateTime<Utc>,
    },
    CheckedOut {
        total: i64,
        timestamp: chrono::DateTime<Utc>,
    },
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

/// Shopping cart aggregate
#[derive(DeriveAggregate, Debug, Clone)]
#[aggregate(id = "CartId", event = "CartEvent")]
struct ShoppingCart {
    #[aggregate_id]
    id: CartId,

    customer_id: String,
    items: HashMap<ProductId, CartItem>,
    checked_out: bool,

    #[aggregate_version]
    version: Version,

    #[aggregate_events]
    pending_events: Vec<CartEvent>,
}

impl ShoppingCart {
    /// Create a new shopping cart
    fn create(id: CartId, customer_id: String) -> Self {
        let mut cart = Self {
            id,
            customer_id: String::new(),
            items: HashMap::new(),
            checked_out: false,
            version: Version::initial(),
            pending_events: Vec::new(),
        };

        let event = CartEvent::Created {
            cart_id: id.to_string(),
            customer_id,
            timestamp: Utc::now(),
        };

        cart.apply_event(&event);
        cart.pending_events.push(event);
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
        if self.checked_out {
            return Err(CartError::AlreadyCheckedOut);
        }

        if quantity == 0 {
            return Err(CartError::InvalidQuantity(quantity));
        }

        if price <= 0 {
            return Err(CartError::InvalidPrice(price));
        }

        let event = CartEvent::ItemAdded {
            product_id,
            name,
            price,
            quantity,
            timestamp: Utc::now(),
        };

        self.apply_event(&event);
        self.pending_events.push(event);
        Ok(())
    }

    /// Remove a quantity of an item
    fn remove_item(&mut self, product_id: ProductId, quantity: u32) -> Result<(), CartError> {
        if self.checked_out {
            return Err(CartError::AlreadyCheckedOut);
        }

        let item = self
            .items
            .get(&product_id)
            .ok_or(CartError::ProductNotFound(product_id))?;

        if item.quantity < quantity {
            return Err(CartError::InsufficientQuantity {
                available: item.quantity,
                requested: quantity,
            });
        }

        let event = CartEvent::ItemRemoved {
            product_id,
            quantity,
            timestamp: Utc::now(),
        };

        self.apply_event(&event);
        self.pending_events.push(event);
        Ok(())
    }

    /// Update the quantity of an item
    fn update_quantity(&mut self, product_id: ProductId, quantity: u32) -> Result<(), CartError> {
        if self.checked_out {
            return Err(CartError::AlreadyCheckedOut);
        }

        if !self.items.contains_key(&product_id) {
            return Err(CartError::ProductNotFound(product_id));
        }

        if quantity == 0 {
            return Err(CartError::InvalidQuantity(quantity));
        }

        let event = CartEvent::ItemQuantityChanged {
            product_id,
            new_quantity: quantity,
            timestamp: Utc::now(),
        };

        self.apply_event(&event);
        self.pending_events.push(event);
        Ok(())
    }

    /// Clear all items from the cart
    #[allow(dead_code)]
    fn clear(&mut self) -> Result<(), CartError> {
        if self.checked_out {
            return Err(CartError::AlreadyCheckedOut);
        }

        let event = CartEvent::Cleared {
            timestamp: Utc::now(),
        };
        self.apply_event(&event);
        self.pending_events.push(event);
        Ok(())
    }

    /// Checkout the cart
    fn checkout(&mut self) -> Result<(), CartError> {
        if self.checked_out {
            return Err(CartError::AlreadyCheckedOut);
        }

        if self.items.is_empty() {
            return Err(CartError::EmptyCart);
        }

        let total = self.total();
        let event = CartEvent::CheckedOut {
            total,
            timestamp: Utc::now(),
        };

        self.apply_event(&event);
        self.pending_events.push(event);
        Ok(())
    }

    /// Get the total value of items in the cart
    fn total(&self) -> i64 {
        self.items.values().map(|item| item.subtotal()).sum()
    }

    /// Get the number of unique items
    fn item_count(&self) -> usize {
        self.items.len()
    }

    /// Get the total quantity of all items
    fn total_quantity(&self) -> u32 {
        self.items.values().map(|item| item.quantity).sum()
    }

    /// Apply an event to update state
    fn apply_event(&mut self, event: &CartEvent) {
        match event {
            CartEvent::Created {
                cart_id: _,
                customer_id,
                ..
            } => {
                self.customer_id = customer_id.clone();
                self.version = self.version.next();
            }
            CartEvent::ItemAdded {
                product_id,
                name,
                price,
                quantity,
                ..
            } => {
                self.items
                    .entry(*product_id)
                    .and_modify(|item| item.quantity += quantity)
                    .or_insert(CartItem {
                        product_id: *product_id,
                        name: name.clone(),
                        price: *price,
                        quantity: *quantity,
                    });
                self.version = self.version.next();
            }
            CartEvent::ItemRemoved {
                product_id,
                quantity,
                ..
            } => {
                if let Some(item) = self.items.get_mut(product_id) {
                    item.quantity -= quantity;
                    if item.quantity == 0 {
                        self.items.remove(product_id);
                    }
                }
                self.version = self.version.next();
            }
            CartEvent::ItemQuantityChanged {
                product_id,
                new_quantity,
                ..
            } => {
                if let Some(item) = self.items.get_mut(product_id) {
                    item.quantity = *new_quantity;
                }
                self.version = self.version.next();
            }
            CartEvent::Cleared { .. } => {
                self.items.clear();
                self.version = self.version.next();
            }
            CartEvent::CheckedOut { .. } => {
                self.checked_out = true;
                self.version = self.version.next();
            }
        }
    }
}

// ============================================================================
// Main Example
// ============================================================================

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Shopping Cart Example ===\n");

    // Create sample products
    let coffee_id = ProductId::new();
    let tea_id = ProductId::new();
    let mug_id = ProductId::new();

    // Create a shopping cart
    let cart_id = CartId::new();
    let mut cart = ShoppingCart::create(cart_id, "customer-123".to_string());
    println!("✓ Created cart: {}", cart_id);
    println!("  Customer: {}\n", cart.customer_id);

    // Add items
    println!("--- Adding Items ---");

    cart.add_item(coffee_id, "Premium Coffee".to_string(), 1299, 2)?;
    println!("✓ Added 2x Premium Coffee ($12.99)");

    cart.add_item(tea_id, "Green Tea".to_string(), 899, 3)?;
    println!("✓ Added 3x Green Tea ($8.99)");

    cart.add_item(mug_id, "Ceramic Mug".to_string(), 1599, 1)?;
    println!("✓ Added 1x Ceramic Mug ($15.99)");

    // Show cart summary
    println!("\n--- Cart Summary ---");
    println!("Items: {}", cart.item_count());
    println!("Total quantity: {}", cart.total_quantity());
    println!("Total: ${:.2}", cart.total() as f64 / 100.0);

    // Add more of an existing item
    println!("\n--- Updating Quantities ---");

    cart.add_item(coffee_id, "Premium Coffee".to_string(), 1299, 1)?;
    println!("✓ Added 1 more coffee (now 3 total)");

    // Update quantity
    cart.update_quantity(tea_id, 5)?;
    println!("✓ Updated tea quantity to 5");

    println!("\nNew total: ${:.2}", cart.total() as f64 / 100.0);

    // Remove some items
    println!("\n--- Removing Items ---");

    cart.remove_item(coffee_id, 2)?;
    println!("✓ Removed 2 coffees (1 remaining)");

    println!("Updated total: ${:.2}", cart.total() as f64 / 100.0);

    // Try some invalid operations
    println!("\n--- Error Handling ---");

    match cart.add_item(coffee_id, "Coffee".to_string(), 0, 1) {
        Ok(_) => println!("❌ Should have failed"),
        Err(e) => println!("✓ Rejected invalid price: {}", e),
    }

    match cart.remove_item(ProductId::new(), 1) {
        Ok(_) => println!("❌ Should have failed"),
        Err(e) => println!("✓ Rejected unknown product: {}", e),
    }

    match cart.remove_item(mug_id, 10) {
        Ok(_) => println!("❌ Should have failed"),
        Err(e) => println!("✓ Rejected insufficient quantity: {}", e),
    }

    // Checkout
    println!("\n--- Checkout ---");
    let total = cart.total();
    cart.checkout()?;
    println!("✓ Checked out cart");
    println!("  Final total: ${:.2}", total as f64 / 100.0);

    // Try to modify after checkout
    println!("\n--- Post-Checkout ---");
    match cart.add_item(coffee_id, "Coffee".to_string(), 1299, 1) {
        Ok(_) => println!("❌ Should have failed"),
        Err(e) => println!("✓ Rejected post-checkout modification: {}", e),
    }

    // Show event history
    println!("\n--- Event History ---");
    println!("Total events: {}", cart.pending_events().len());
    for (i, event) in cart.pending_events().iter().enumerate() {
        match event {
            CartEvent::Created { .. } => println!("{}. Cart created", i + 1),
            CartEvent::ItemAdded { name, quantity, .. } => {
                println!("{}. Added {} x {}", i + 1, quantity, name)
            }
            CartEvent::ItemRemoved { quantity, .. } => println!("{}. Removed {}", i + 1, quantity),
            CartEvent::ItemQuantityChanged { new_quantity, .. } => {
                println!("{}. Quantity changed to {}", i + 1, new_quantity)
            }
            CartEvent::Cleared { .. } => println!("{}. Cart cleared", i + 1),
            CartEvent::CheckedOut { total, .. } => {
                println!("{}. Checked out (${:.2})", i + 1, *total as f64 / 100.0)
            }
        }
    }

    Ok(())
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create_cart() {
        let cart = ShoppingCart::create(CartId::new(), "customer-1".to_string());

        assert_eq!(cart.customer_id, "customer-1");
        assert_eq!(cart.item_count(), 0);
        assert_eq!(cart.total(), 0);
        assert!(!cart.checked_out);
    }

    #[test]
    fn test_add_item() {
        let mut cart = ShoppingCart::create(CartId::new(), "customer-1".to_string());

        cart.add_item(ProductId::new(), "Coffee".to_string(), 1299, 2)
            .unwrap();

        assert_eq!(cart.item_count(), 1);
        assert_eq!(cart.total_quantity(), 2);
        assert_eq!(cart.total(), 2598);
    }

    #[test]
    fn test_add_same_item_increases_quantity() {
        let mut cart = ShoppingCart::create(CartId::new(), "customer-1".to_string());
        let product_id = ProductId::new();

        cart.add_item(product_id, "Coffee".to_string(), 1299, 2)
            .unwrap();
        cart.add_item(product_id, "Coffee".to_string(), 1299, 1)
            .unwrap();

        assert_eq!(cart.item_count(), 1);
        assert_eq!(cart.total_quantity(), 3);
    }

    #[test]
    fn test_remove_item() {
        let mut cart = ShoppingCart::create(CartId::new(), "customer-1".to_string());
        let product_id = ProductId::new();

        cart.add_item(product_id, "Coffee".to_string(), 1299, 3)
            .unwrap();
        cart.remove_item(product_id, 1).unwrap();

        assert_eq!(cart.total_quantity(), 2);
    }

    #[test]
    fn test_remove_all_quantity_removes_item() {
        let mut cart = ShoppingCart::create(CartId::new(), "customer-1".to_string());
        let product_id = ProductId::new();

        cart.add_item(product_id, "Coffee".to_string(), 1299, 2)
            .unwrap();
        cart.remove_item(product_id, 2).unwrap();

        assert_eq!(cart.item_count(), 0);
    }

    #[test]
    fn test_update_quantity() {
        let mut cart = ShoppingCart::create(CartId::new(), "customer-1".to_string());
        let product_id = ProductId::new();

        cart.add_item(product_id, "Coffee".to_string(), 1299, 2)
            .unwrap();
        cart.update_quantity(product_id, 5).unwrap();

        assert_eq!(cart.total_quantity(), 5);
    }

    #[test]
    fn test_clear() {
        let mut cart = ShoppingCart::create(CartId::new(), "customer-1".to_string());

        cart.add_item(ProductId::new(), "Coffee".to_string(), 1299, 2)
            .unwrap();
        cart.add_item(ProductId::new(), "Tea".to_string(), 899, 1)
            .unwrap();
        cart.clear().unwrap();

        assert_eq!(cart.item_count(), 0);
    }

    #[test]
    fn test_checkout() {
        let mut cart = ShoppingCart::create(CartId::new(), "customer-1".to_string());

        cart.add_item(ProductId::new(), "Coffee".to_string(), 1299, 2)
            .unwrap();
        cart.checkout().unwrap();

        assert!(cart.checked_out);
    }

    #[test]
    fn test_cannot_checkout_empty_cart() {
        let mut cart = ShoppingCart::create(CartId::new(), "customer-1".to_string());

        let result = cart.checkout();

        assert!(matches!(result, Err(CartError::EmptyCart)));
    }

    #[test]
    fn test_cannot_modify_after_checkout() {
        let mut cart = ShoppingCart::create(CartId::new(), "customer-1".to_string());

        cart.add_item(ProductId::new(), "Coffee".to_string(), 1299, 1)
            .unwrap();
        cart.checkout().unwrap();

        let result = cart.add_item(ProductId::new(), "Tea".to_string(), 899, 1);

        assert!(matches!(result, Err(CartError::AlreadyCheckedOut)));
    }
}
