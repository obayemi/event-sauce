//! # Unit of Work Example — One Atomic Commit Across Persistence Styles
//!
//! An event-sourced `Order` is placed against a state-stored `Product`. The
//! command loads the product, removes stock, creates the order, and never
//! says how either is persisted: `UnitOfWorkStore::execute` commits both in
//! one atomic write, or neither when the command is refused.
//!
//! Run with:
//! ```bash
//! cargo run --example unit-of-work --features unit-of-work
//! ```

use std::sync::Arc;

use async_trait::async_trait;
use event_sauce::memory::{InMemoryEventStore, InMemoryStateStore, InMemoryTransactionalStore};
use event_sauce::{
    aggregate_command, define_events, Aggregate, AggregateError, Entity, EntityId, EventStore,
    Repository, RetryPolicy, StateStore, TransactionalStore, UnitOfWork, UnitOfWorkCommand,
    UnitOfWorkError, UnitOfWorkStore,
};
use serde::{Deserialize, Serialize};

#[derive(Debug, thiserror::Error)]
enum ProductError {
    #[error("only {available} in stock")]
    InsufficientStock { available: u32 },
}

impl AggregateError for ProductError {}

#[derive(Debug, Serialize, Deserialize)]
struct Product {
    id: EntityId,
    price: i64,
    stock: u32,
}

impl Entity for Product {
    fn entity_id(&self) -> EntityId {
        self.id
    }
}

impl Aggregate for Product {
    type Event = ProductEvent;
    type Error = ProductError;
    type DeletedState = Self;
}

define_events! {
    enum ProductEvent for Product {
        Listed {
            price: i64,
            stock: u32,
        }
        @init
        => |id, event| {
            Product { id, price: event.price, stock: event.stock }
        },

        StockRemoved {
            quantity: u32,
        }
        @validate |product, event| {
            if product.stock < event.quantity {
                return Err(ProductError::InsufficientStock { available: product.stock });
            }
            return Ok(());
        }
        => |product, event| {
            product.stock -= event.quantity;
        },
    }
}

aggregate_command! {
    impl Product {
        @clock @init fn list(price: i64, stock: u32) -> ListedEvent { price, stock };
        @clock fn remove_stock(quantity: u32) -> StockRemovedEvent { quantity };
    }
}

#[derive(Debug, thiserror::Error)]
enum OrderError {}

impl AggregateError for OrderError {}

#[derive(Debug, Serialize, Deserialize)]
struct Order {
    id: EntityId,
    total: i64,
}

impl Entity for Order {
    fn entity_id(&self) -> EntityId {
        self.id
    }
}

impl Aggregate for Order {
    type Event = OrderEvent;
    type Error = OrderError;
    type DeletedState = Self;
}

define_events! {
    enum OrderEvent for Order {
        Placed {
            total: i64,
        }
        @init
        => |id, event| {
            Order { id, total: event.total }
        },

        Paid {}
        => |_order, _event| {},
    }
}

aggregate_command! {
    impl Order {
        @clock @init fn place(total: i64) -> PlacedEvent { total };
        @clock fn pay() -> PaidEvent { };
    }
}

struct ListProduct {
    id: EntityId,
    price: i64,
    stock: u32,
}

#[async_trait]
impl<T: TransactionalStore + 'static> UnitOfWorkCommand<T> for ListProduct {
    async fn handle(&self, uow: &UnitOfWork<'_, T>, _ctx: &()) -> Result<(), UnitOfWorkError> {
        uow.create::<Product>(self.id, |root| root.list(self.price, self.stock))?;
        Ok(())
    }
}

struct PlaceOrder {
    order_id: EntityId,
    product_id: EntityId,
    quantity: u32,
}

#[async_trait]
impl<T: TransactionalStore + 'static> UnitOfWorkCommand<T> for PlaceOrder {
    async fn handle(&self, uow: &UnitOfWork<'_, T>, _ctx: &()) -> Result<(), UnitOfWorkError> {
        let mut product = uow.load::<Product>(self.product_id).await?;
        product.remove_stock(self.quantity)?;
        let total = product.price * i64::from(self.quantity);
        let mut order = uow.create::<Order>(self.order_id, |root| root.place(total))?;
        order.pay()?;
        Ok(())
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let events = Arc::new(InMemoryEventStore::new());
    let states = Arc::new(InMemoryStateStore::new());
    let stores = InMemoryTransactionalStore::new(Arc::clone(&events), Arc::clone(&states));
    let store = UnitOfWorkStore::new(stores)
        .event_sourced::<Order>()
        .state_stored::<Product>();
    let retry = RetryPolicy::default();

    let product_id = EntityId::new();
    let listing = ListProduct {
        id: product_id,
        price: 250,
        stock: 5,
    };
    store.execute(&listing, &(), retry.clone()).await?;

    let order_id = EntityId::new();
    let place = PlaceOrder {
        order_id,
        product_id,
        quantity: 2,
    };
    store.execute(&place, &(), retry.clone()).await?;
    println!(
        "order total {}, stock left {}",
        events.repository::<Order>().load(order_id).await?.total,
        states.repository::<Product>().load(product_id).await?.stock,
    );

    let too_many = PlaceOrder {
        order_id: EntityId::new(),
        product_id,
        quantity: 10,
    };
    let refusal = store.execute(&too_many, &(), retry).await.unwrap_err();
    println!(
        "refused: {refusal}; stock still {}",
        states.repository::<Product>().load(product_id).await?.stock,
    );

    Ok(())
}
