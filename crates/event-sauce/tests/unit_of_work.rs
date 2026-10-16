//! System tests driving a unit of work that mixes an event-sourced `Order`
//! and a state-stored `Product` through the facade, on the in-memory backend.

mod support;

use std::sync::atomic::AtomicBool;
use std::sync::Arc;

use event_sauce::memory::{InMemoryEventStore, InMemoryStateStore, InMemoryTransactionalStore};
use event_sauce::{
    EntityId, EventStore, Repository, RetryPolicy, StateStore, UnitOfWorkError, UnitOfWorkStore,
};
use support::{
    register, ListProduct, Order, PlaceOrder, Product, ProductError, QuoteOrder, RivalRepricing,
    TransferStock,
};

struct World {
    uow: Arc<UnitOfWorkStore<InMemoryTransactionalStore>>,
    events: Arc<InMemoryEventStore>,
    states: Arc<InMemoryStateStore>,
}

impl World {
    fn new() -> Self {
        let events = Arc::new(InMemoryEventStore::new());
        let states = Arc::new(InMemoryStateStore::new());
        let stores = InMemoryTransactionalStore::new(Arc::clone(&events), Arc::clone(&states));
        Self {
            uow: Arc::new(register(stores)),
            events,
            states,
        }
    }

    async fn list_product(&self, price: i64, stock: u32) -> EntityId {
        let id = EntityId::new();
        let listing = ListProduct { id, price, stock };
        self.uow
            .execute(&listing, &(), RetryPolicy::none())
            .await
            .unwrap();
        id
    }

    async fn product(&self, id: EntityId) -> Product {
        let root = self.states.repository::<Product>().load(id).await.unwrap();
        Product {
            id,
            name: root.name.clone(),
            price: root.price,
            stock: root.stock,
        }
    }

    async fn order_exists(&self, id: EntityId) -> bool {
        self.events.repository::<Order>().exists(id).await.unwrap()
    }
}

#[tokio::test]
async fn a_command_commits_the_order_and_the_product_together() {
    let world = World::new();
    let product_id = world.list_product(250, 10).await;
    let order_id = EntityId::new();
    let place = PlaceOrder {
        order_id,
        product_id,
        quantity: 3,
    };

    let executed = world
        .uow
        .execute(&place, &(), RetryPolicy::default())
        .await
        .unwrap();

    assert_eq!(executed.attempts.get(), 1);
    assert_eq!(world.product(product_id).await.stock, 7);
    let order = world
        .events
        .repository::<Order>()
        .load(order_id)
        .await
        .unwrap();
    assert_eq!((order.quantity, order.unit_price), (3, 250));
    assert!(order.confirmed);
}

#[tokio::test]
async fn a_refused_command_writes_nothing() {
    let world = World::new();
    let product_id = world.list_product(250, 2).await;
    let order_id = EntityId::new();
    let place = PlaceOrder {
        order_id,
        product_id,
        quantity: 3,
    };

    let error = world
        .uow
        .execute(&place, &(), RetryPolicy::default())
        .await
        .unwrap_err();

    assert!(matches!(error, UnitOfWorkError::Domain(_)));
    assert!(error
        .to_string()
        .contains(&ProductError::InsufficientStock { available: 2 }.to_string()));
    assert_eq!(world.product(product_id).await.stock, 2);
    assert!(!world.order_exists(order_id).await);
}

#[tokio::test]
async fn a_concurrent_change_to_a_read_only_product_retries_with_the_fresh_price() {
    let world = World::new();
    let product_id = world.list_product(250, 10).await;
    let order_id = EntityId::new();
    let quote = QuoteOrder {
        order_id,
        product_id,
        quantity: 1,
        rival: Some(RivalRepricing {
            store: Arc::clone(&world.uow),
            price: 300,
            fired: AtomicBool::new(false),
        }),
    };

    let executed = world
        .uow
        .execute(&quote, &(), RetryPolicy::default())
        .await
        .unwrap();

    assert_eq!(executed.attempts.get(), 2);
    let order = world
        .events
        .repository::<Order>()
        .load(order_id)
        .await
        .unwrap();
    assert_eq!(order.unit_price, 300);
}

#[tokio::test]
async fn transferring_stock_to_the_same_product_is_already_checked_out() {
    let world = World::new();
    let product_id = world.list_product(250, 10).await;
    let transfer = TransferStock {
        from: product_id,
        to: product_id,
        quantity: 1,
    };

    let error = world
        .uow
        .execute(&transfer, &(), RetryPolicy::default())
        .await
        .unwrap_err();

    assert!(matches!(error, UnitOfWorkError::AlreadyCheckedOut { id, .. } if id == product_id));
    assert_eq!(world.product(product_id).await.stock, 10);
}
