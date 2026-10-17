//! System test: events applied inside an ambient event context carry its
//! values into the stored envelopes, across `.await` points.

use std::sync::Arc;

use event_sauce::memory::InMemoryEventStore;
use event_sauce::prelude::*;
use serde_json::json;

mod account_fixture;
use account_fixture::{stored_metadata, Account, AccountCommands};

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
        let metadata = metadata.expect("event carries context metadata");
        assert_eq!(metadata.tenant.as_deref(), Some("acme"));
        assert_eq!(metadata.correlation_id.as_deref(), Some("req-1"));
        assert_eq!(metadata.correlation_path, ["req-1"]);
        assert_eq!(
            metadata.additional.map(serde_json::Value::Object),
            Some(json!({"region": "eu"}))
        );
    }
    assert_eq!(repo.load(id).await.unwrap().balance, 5);
}
