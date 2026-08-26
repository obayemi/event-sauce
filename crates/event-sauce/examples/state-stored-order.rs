//! # State-Stored Order Example — One Codebase, Two Persistence Styles
//!
//! Demonstrates the core promise of the state-store split: the SAME aggregate,
//! events, commands, and application code run unchanged against
//! an event-sourced store and a state-stored one. Only the composition root
//! differs.
//!
//! The state-stored run additionally registers an **in-transaction
//! projection**: a read model updated inside the same save "transaction",
//! with read-your-writes consistency and rollback-on-failure.
//!
//! Run with:
//! ```bash
//! cargo run --example state-stored-order
//! ```

use std::sync::Arc;

use event_sauce_core::{
    command_handler, define_events, Aggregate, AggregateError, DefaultEntity, Entity, EntityId,
    EventEnvelope, EventFilter, EventStore, Repository, StateProjection, StateStore,
};
use event_sauce_memory::{InMemoryEventStore, InMemoryProjectionContext, InMemoryStateStore};
use serde::{Deserialize, Serialize};

// ============================================================================
// Domain layer — written once, persistence-agnostic
// ============================================================================

#[derive(Debug, thiserror::Error)]
enum OrderError {
    #[error("order already completed")]
    AlreadyCompleted,
    #[error("cannot complete an empty order")]
    EmptyOrder,
}

impl AggregateError for OrderError {}

#[derive(Debug, Serialize, Deserialize)]
struct Order {
    id: EntityId,
    items: Vec<String>,
    total: i64,
    completed: bool,
}

impl Entity for Order {
    fn new(id: EntityId) -> Self {
        Self {
            id,
            items: Vec::new(),
            total: 0,
            completed: false,
        }
    }
    fn entity_id(&self) -> EntityId {
        self.id
    }
}

impl DefaultEntity for Order {}

impl Aggregate for Order {
    type Event = OrderEvent;
    type Error = OrderError;
    type DeletedState = Self;
}

define_events! {
    enum OrderEvent for Order {
        ItemAdded {
            item_id: String,
            price: i64,
        }
        @validate |order, _event| {
            if order.completed {
                return Err(OrderError::AlreadyCompleted);
            }
            return Ok(());
        }
        => |order, event| {
            order.items.push(event.item_id.clone());
            order.total += event.price;
        },

        Completed {}
        @validate |order, _event| {
            if order.completed {
                return Err(OrderError::AlreadyCompleted);
            }
            if order.items.is_empty() {
                return Err(OrderError::EmptyOrder);
            }
            return Ok(());
        }
        => |order, _event| {
            order.completed = true;
        },
    }
}

command_handler! {
    impl Order {
        fn add_item(item_id: String, price: i64) -> ItemAddedEvent { item_id, price };
        fn complete() -> CompletedEvent { };
    }
}

// ============================================================================
// Application layer — generic over the persistence style
// ============================================================================

async fn place_order<R: Repository<Order>>(repo: &R) -> event_sauce_core::Result<EntityId> {
    let mut order = repo.create();
    order.add_item("laptop".into(), 120_000)?;
    order.add_item("mouse".into(), 4_500)?;
    repo.save(&mut order).await?;

    let total = repo
        .modify(order.entity_id(), |order| {
            order.complete()?;
            Ok(order.total)
        })
        .await?;

    println!(
        "  order {} completed: {} items, total {} cents (version {})",
        order.entity_id(),
        repo.load(order.entity_id()).await?.items.len(),
        total,
        repo.get_version(order.entity_id()).await?.as_i64(),
    );
    Ok(order.entity_id())
}

// ============================================================================
// A state-stored read model, maintained in-transaction
// ============================================================================

struct CompletedOrderCount;

#[async_trait::async_trait]
impl StateProjection<InMemoryProjectionContext> for CompletedOrderCount {
    fn name(&self) -> &str {
        "completed-order-count"
    }

    fn filter(&self) -> EventFilter {
        EventFilter::by_event::<CompletedEvent>()
    }

    async fn project(
        &self,
        ctx: &mut InMemoryProjectionContext,
        events: &[EventEnvelope],
    ) -> event_sauce_core::Result<()> {
        let count = ctx
            .get("completed")
            .and_then(serde_json::Value::as_u64)
            .unwrap_or(0);
        ctx.insert("completed", serde_json::json!(count + events.len() as u64));
        Ok(())
    }
}

// ============================================================================
// Composition roots — the ONLY place the persistence style differs
// ============================================================================

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    println!("event-sourced run (events are the source of truth):");
    let event_store = Arc::new(InMemoryEventStore::builder().build());
    let repo = event_store.repository::<Order>();
    place_order(&repo).await?;

    println!("state-stored run (current state is the source of truth):");
    let state_store = Arc::new(
        InMemoryStateStore::builder()
            .with_projection(Arc::new(CompletedOrderCount))
            .build(),
    );
    let repo = state_store.repository::<Order>();
    place_order(&repo).await?;
    place_order(&repo).await?;

    println!(
        "  read model (updated in-transaction): {} completed orders",
        state_store
            .projection_state("completed")
            .await
            .and_then(|v| v.as_u64())
            .unwrap_or(0),
    );

    Ok(())
}
