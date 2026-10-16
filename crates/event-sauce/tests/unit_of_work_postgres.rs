//! System tests driving a unit of work across an event-sourced `Order` and a
//! state-stored `Product` on one `PostgreSQL` database.

mod support;

use std::sync::atomic::AtomicBool;
use std::sync::Arc;

use event_sauce::postgres::{PostgresEventStore, PostgresStateStore, PostgresTransactionalStore};
use event_sauce::{
    EntityId, EventStore, Repository, RetryPolicy, StateStore, UnitOfWorkError, UnitOfWorkStore,
};
use sqlx::postgres::PgPoolOptions;
use support::{register, Order, PlaceOrder, Product, QuoteOrder, RivalRepricing, TransferStock};
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use testcontainers_modules::testcontainers::{ContainerAsync, ImageExt};

struct World {
    _container: ContainerAsync<Postgres>,
    uow: Arc<UnitOfWorkStore<PostgresTransactionalStore>>,
    events: Arc<PostgresEventStore>,
    states: Arc<PostgresStateStore>,
}

impl World {
    async fn start() -> Self {
        let container = Postgres::default()
            .with_tag("16-alpine")
            .start()
            .await
            .unwrap();
        let host = container.get_host().await.unwrap();
        let port = container.get_host_port_ipv4(5432).await.unwrap();
        let pool = PgPoolOptions::new()
            .connect(&format!(
                "postgresql://postgres:postgres@{host}:{port}/postgres"
            ))
            .await
            .unwrap();
        let events = Arc::new(PostgresEventStore::new(pool.clone()));
        let states = Arc::new(PostgresStateStore::new(pool));
        events.migrate().await.unwrap();
        states.migrate().await.unwrap();
        let stores =
            PostgresTransactionalStore::new(Arc::clone(&events), Arc::clone(&states)).unwrap();
        Self {
            _container: container,
            uow: Arc::new(register(stores)),
            events,
            states,
        }
    }

    async fn list_product(&self, price: i64, stock: u32) -> EntityId {
        support::list_product(&self.uow, price, stock).await
    }

    async fn stock(&self, id: EntityId) -> u32 {
        self.states
            .repository::<Product>()
            .load(id)
            .await
            .unwrap()
            .stock
    }
}

#[tokio::test]
async fn a_command_commits_the_order_and_the_products_together() {
    let world = World::start().await;
    let product_id = world.list_product(250, 10).await;
    let spare_id = world.list_product(250, 0).await;
    let order_id = EntityId::new();
    let place = PlaceOrder {
        order_id,
        product_id,
        quantity: 3,
    };

    world
        .uow
        .execute(&place, &(), RetryPolicy::default())
        .await
        .unwrap();
    let transfer = TransferStock {
        from: product_id,
        to: spare_id,
        quantity: 2,
    };
    world
        .uow
        .execute(&transfer, &(), RetryPolicy::default())
        .await
        .unwrap();
    let to_self = TransferStock {
        from: product_id,
        to: product_id,
        quantity: 1,
    };
    let error = world
        .uow
        .execute(&to_self, &(), RetryPolicy::default())
        .await
        .unwrap_err();

    assert!(matches!(error, UnitOfWorkError::AlreadyCheckedOut { .. }));
    assert_eq!(world.stock(product_id).await, 5);
    assert_eq!(world.stock(spare_id).await, 2);
    let order = world
        .events
        .repository::<Order>()
        .load(order_id)
        .await
        .unwrap();
    assert_eq!(
        (order.quantity, order.unit_price, order.confirmed),
        (3, 250, true)
    );
}

#[tokio::test]
async fn a_concurrent_change_to_a_read_only_product_retries_with_the_fresh_price() {
    let world = World::start().await;
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
