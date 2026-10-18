//! System tests driving `Repository::execute` against a fully encrypted
//! aggregate on the in-memory event store.

use event_sauce::memory::InMemoryEventStore;
use event_sauce::{
    aggregate_command, define_events, Aggregate, AggregateError, AggregateVersion, Command,
    CommandError, Entity, EntityId, EventStore, Repository, RetryPolicy, Root, StreamId,
};
use futures::StreamExt;
use serde::{Deserialize, Serialize};

#[derive(Debug, thiserror::Error)]
#[error("vault error")]
struct VaultError;

impl AggregateError for VaultError {}

#[derive(Debug, Serialize, Deserialize)]
struct Vault {
    id: EntityId,
    secret: String,
}

impl Entity for Vault {
    fn entity_id(&self) -> EntityId {
        self.id
    }
}

impl Aggregate for Vault {
    type Event = VaultEvent;
    type Error = VaultError;
    type DeletedState = Self;

    fn is_encrypted() -> bool {
        true
    }
}

define_events! {
    enum VaultEvent for Vault {
        Sealed {
            secret: String,
        }
        @init
        => |id, event| {
            Vault { id, secret: event.secret.clone() }
        },

        Rotated {
            secret: String,
        }
        => |vault, event| {
            vault.secret = event.secret.clone();
        },
    }
}

aggregate_command! {
    impl Vault {
        @clock @init fn seal(secret: String) -> SealedEvent { secret };
    }
}

struct Seal(&'static str);

impl Command<Vault> for Seal {
    fn handle(
        &self,
        root: Root<Vault>,
        _ctx: &(),
    ) -> Result<Root<Vault>, CommandError<VaultError>> {
        Ok(root.missing()?.seal(self.0.to_owned())?.into())
    }
}

#[tokio::test]
async fn a_create_command_on_a_fresh_fully_encrypted_aggregate_stores_ciphertext() {
    let store = std::sync::Arc::new(InMemoryEventStore::new());
    let repo = store.repository::<Vault>();
    let id = EntityId::new();

    repo.execute(id, &Seal("hunter2"), &(), RetryPolicy::default())
        .await
        .unwrap();

    let stream = store
        .load_stream(
            StreamId::new("Vault", id.as_uuid()),
            AggregateVersion::initial(),
        )
        .await
        .unwrap();
    futures::pin_mut!(stream);
    let envelope = stream.next().await.unwrap().unwrap();
    assert!(event_sauce::crypto::is_encrypted(&envelope.event_data));
    assert_eq!(repo.load(id).await.unwrap().entity().secret, "hunter2");
}
