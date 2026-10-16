//! System test: events applied inside an ambient event context carry its
//! values into the stored envelopes, across `.await` points.

use std::sync::Arc;

use event_sauce::memory::InMemoryEventStore;
use event_sauce::prelude::*;
use event_sauce::{AggregateVersion, EventMetadata, StreamId};
use futures::StreamExt;
use serde::{Deserialize, Serialize};
use serde_json::json;

#[aggregate_error(aggregate = "Account")]
#[derive(Debug, thiserror::Error)]
enum AccountError {}

#[derive(Debug, Serialize, Deserialize)]
struct Account {
    id: EntityId,
    balance: i64,
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
    enum AccountEvent for Account {
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

async fn stored_metadata(store: &InMemoryEventStore, id: EntityId) -> Vec<EventMetadata> {
    let stream = store
        .load_stream(
            StreamId::new("Account", id.as_uuid()),
            AggregateVersion::initial(),
        )
        .await
        .unwrap();
    stream
        .map(|envelope| envelope.unwrap().metadata.unwrap())
        .collect()
        .await
}

#[tokio::test]
async fn events_applied_in_a_scope_are_stored_with_its_context() {
    let store = Arc::new(InMemoryEventStore::builder().build());
    let repo = store.repository::<Account>();
    let context = EventContext::root()
        .tenant("acme")
        .correlation_id("req-1")
        .with("region", "eu");

    let id = async {
        let mut account = Account::open().unwrap();
        tokio::task::yield_now().await;
        account.deposit(5).unwrap();
        repo.save(&mut account).await.unwrap();
        account.entity_id()
    }
    .with_event_context(context)
    .await;

    let metadata = stored_metadata(&store, id).await;
    assert_eq!(metadata.len(), 2);
    for metadata in metadata {
        assert_eq!(metadata.tenant.as_deref(), Some("acme"));
        assert_eq!(metadata.correlation_id.as_deref(), Some("req-1"));
        assert_eq!(metadata.correlation_path, ["req-1"]);
        assert_eq!(metadata.additional, Some(json!({"region": "eu"})));
    }
    assert_eq!(repo.load(id).await.unwrap().balance, 5);
}
