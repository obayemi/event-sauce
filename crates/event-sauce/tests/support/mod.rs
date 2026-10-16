//! Shared domain for the unit-of-work system tests: an event-sourced `Order`
//! priced from a state-stored `Product`, and the commands that span both.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use event_sauce::{
    aggregate_command, define_events, Aggregate, AggregateError, Entity, EntityId, RetryPolicy,
    TransactionalStore, UnitOfWork, UnitOfWorkCommand, UnitOfWorkError, UnitOfWorkStore,
};
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, thiserror::Error)]
pub enum ProductError {
    #[error("only {available} in stock")]
    InsufficientStock { available: u32 },
}

impl AggregateError for ProductError {}

#[derive(Debug, Serialize, Deserialize)]
pub struct Product {
    pub id: EntityId,
    pub name: String,
    pub price: i64,
    pub stock: u32,
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
    pub enum ProductEvent for Product {
        Listed {
            name: String,
            price: i64,
            stock: u32,
        }
        @init
        => |id, event| {
            Product {
                id,
                name: event.name.clone(),
                price: event.price,
                stock: event.stock,
            }
        },

        Repriced {
            price: i64,
        }
        => |product, event| {
            product.price = event.price;
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

        StockAdded {
            quantity: u32,
        }
        => |product, event| {
            product.stock += event.quantity;
        },
    }
}

aggregate_command! {
    impl Product {
        @clock @init fn list(name: String, price: i64, stock: u32) -> ListedEvent { name, price, stock };
        @clock fn reprice(price: i64) -> RepricedEvent { price };
        @clock fn remove_stock(quantity: u32) -> StockRemovedEvent { quantity };
        @clock fn add_stock(quantity: u32) -> StockAddedEvent { quantity };
    }
}

#[derive(Debug, thiserror::Error)]
pub enum OrderError {}

impl AggregateError for OrderError {}

#[derive(Debug, Serialize, Deserialize)]
pub struct Order {
    pub id: EntityId,
    pub product_id: EntityId,
    pub quantity: u32,
    pub unit_price: i64,
    pub confirmed: bool,
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
    pub enum OrderEvent for Order {
        Placed {
            product_id: EntityId,
            quantity: u32,
            unit_price: i64,
        }
        @init
        => |id, event| {
            Order {
                id,
                product_id: event.product_id,
                quantity: event.quantity,
                unit_price: event.unit_price,
                confirmed: false,
            }
        },

        Confirmed {}
        => |order, _event| {
            order.confirmed = true;
        },
    }
}

aggregate_command! {
    impl Order {
        @clock @init fn place(product_id: EntityId, quantity: u32, unit_price: i64) -> PlacedEvent {
            product_id, quantity, unit_price
        };
        @clock fn confirm() -> ConfirmedEvent { };
    }
}

fn open_order<T: TransactionalStore + 'static>(
    uow: &UnitOfWork<'_, T>,
    order_id: EntityId,
    product_id: EntityId,
    quantity: u32,
    unit_price: i64,
) -> Result<(), UnitOfWorkError> {
    let mut order = uow.create::<Order>(order_id, |root| {
        root.place(product_id, quantity, unit_price)
    })?;
    order.confirm()?;
    Ok(())
}

pub struct ListProduct {
    pub id: EntityId,
    pub price: i64,
    pub stock: u32,
}

#[async_trait]
impl<T: TransactionalStore + 'static> UnitOfWorkCommand<T> for ListProduct {
    async fn handle(&self, uow: &UnitOfWork<'_, T>, _ctx: &()) -> Result<(), UnitOfWorkError> {
        uow.create::<Product>(self.id, |root| {
            root.list("widget".into(), self.price, self.stock)
        })?;
        Ok(())
    }
}

pub struct Reprice {
    pub product_id: EntityId,
    pub price: i64,
}

#[async_trait]
impl<T: TransactionalStore + 'static> UnitOfWorkCommand<T> for Reprice {
    async fn handle(&self, uow: &UnitOfWork<'_, T>, _ctx: &()) -> Result<(), UnitOfWorkError> {
        uow.load::<Product>(self.product_id)
            .await?
            .reprice(self.price)?;
        Ok(())
    }
}

pub struct PlaceOrder {
    pub order_id: EntityId,
    pub product_id: EntityId,
    pub quantity: u32,
}

#[async_trait]
impl<T: TransactionalStore + 'static> UnitOfWorkCommand<T> for PlaceOrder {
    async fn handle(&self, uow: &UnitOfWork<'_, T>, _ctx: &()) -> Result<(), UnitOfWorkError> {
        let mut product = uow.load::<Product>(self.product_id).await?;
        product.remove_stock(self.quantity)?;
        let unit_price = product.price;
        drop(product);
        open_order(
            uow,
            self.order_id,
            self.product_id,
            self.quantity,
            unit_price,
        )
    }
}

pub struct RivalRepricing<T: TransactionalStore> {
    pub store: Arc<UnitOfWorkStore<T>>,
    pub price: i64,
    pub fired: AtomicBool,
}

pub struct QuoteOrder<T: TransactionalStore> {
    pub order_id: EntityId,
    pub product_id: EntityId,
    pub quantity: u32,
    pub rival: Option<RivalRepricing<T>>,
}

#[async_trait]
impl<T: TransactionalStore + 'static> UnitOfWorkCommand<T> for QuoteOrder<T> {
    async fn handle(&self, uow: &UnitOfWork<'_, T>, _ctx: &()) -> Result<(), UnitOfWorkError> {
        let unit_price = uow.load::<Product>(self.product_id).await?.price;
        if let Some(rival) = &self.rival {
            if !rival.fired.swap(true, Ordering::SeqCst) {
                let reprice = Reprice {
                    product_id: self.product_id,
                    price: rival.price,
                };
                rival
                    .store
                    .execute(&reprice, &(), RetryPolicy::none())
                    .await?;
            }
        }
        open_order(
            uow,
            self.order_id,
            self.product_id,
            self.quantity,
            unit_price,
        )
    }
}

pub struct TransferStock {
    pub from: EntityId,
    pub to: EntityId,
    pub quantity: u32,
}

#[async_trait]
impl<T: TransactionalStore + 'static> UnitOfWorkCommand<T> for TransferStock {
    async fn handle(&self, uow: &UnitOfWork<'_, T>, _ctx: &()) -> Result<(), UnitOfWorkError> {
        let mut from = uow.load::<Product>(self.from).await?;
        let mut to = uow.load::<Product>(self.to).await?;
        from.remove_stock(self.quantity)?;
        to.add_stock(self.quantity)?;
        Ok(())
    }
}

pub fn register<T: TransactionalStore + 'static>(stores: T) -> UnitOfWorkStore<T> {
    UnitOfWorkStore::new(stores)
        .event_sourced::<Order>()
        .state_stored::<Product>()
}
