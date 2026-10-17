//! Account aggregate shared by the event context system tests.

use event_sauce::memory::InMemoryEventStore;
use event_sauce::prelude::*;
use event_sauce::{AggregateVersion, EventMetadata, StreamId};
use futures::StreamExt;
use serde::{Deserialize, Serialize};

#[aggregate_error(aggregate = "Account")]
#[derive(Debug, thiserror::Error)]
pub enum AccountError {}

#[derive(Debug, Serialize, Deserialize)]
pub struct Account {
    id: EntityId,
    pub balance: i64,
}

impl Entity for Account {
    fn entity_id(&self) -> EntityId {
        self.id
    }
}

impl Aggregate for Account {
    type Event = AccountEvent;
    type Error = AccountError;
    type DeletedState = Self;
}

define_events! {
    pub enum AccountEvent for Account {
        Opened {}
        @init
        => |id, _event| {
            Account { id, balance: 0 }
        },

        Deposited {
            amount: i64,
        }
        => |account, event| {
            account.balance += event.amount;
        },
    }
}

aggregate_command! {
    impl Account {
        @clock @init fn open() -> OpenedEvent { };
        @clock fn deposit(amount: i64) -> DepositedEvent { amount };
    }
}

/// Loads the stored envelopes of an account and returns their metadata.
pub async fn stored_metadata(
    store: &InMemoryEventStore,
    id: EntityId,
) -> Vec<Option<EventMetadata>> {
    store
        .load_stream(
            StreamId::new("Account", id.as_uuid()),
            AggregateVersion::initial(),
        )
        .await
        .unwrap()
        .map(|envelope| envelope.unwrap().metadata)
        .collect()
        .await
}
