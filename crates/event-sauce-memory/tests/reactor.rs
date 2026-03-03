//! Integration tests for the reactor system with the in-memory event store.
//!
//! Tests cross-aggregate event reactions, causation tracking,
//! cascading reactions with depth limits, and the `reactor!` macro.

use event_sauce_core::{
    command_handler, define_events, reactor, Aggregate, AggregateError, AggregateRoot, Entity,
    EntityId, EventEnvelope, EventFilter, EventStore, ReactorContext, ReactorRunner, Repository,
};
use event_sauce_memory::InMemoryEventStore;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

// ============================================================================
// User aggregate
// ============================================================================

#[derive(Debug, thiserror::Error)]
enum UserError {
    #[error("User already deactivated")]
    AlreadyDeactivated,
}

impl AggregateError for UserError {}

#[derive(Debug, Serialize, Deserialize)]
struct User {
    id: EntityId,
    name: String,
    active: bool,
}

impl Entity for User {
    fn new(id: EntityId) -> Self {
        Self {
            id,
            name: String::new(),
            active: true,
        }
    }
    fn entity_id(&self) -> EntityId {
        self.id
    }
}

impl event_sauce_core::DefaultEntity for User {}

impl Aggregate for User {
    type Event = UserEvent;
    type Error = UserError;
    type DeletedState = Self;
}

define_events! {
    enum UserEvent for User {
        Registered {
            name: String,
        } => |user, event| {
            user.name = event.name.clone();
        },

        Kicked {
            group_id: EntityId,
            reason: String,
        }
        @validate |agg, _event| {
            if !agg.active {
                return Err(UserError::AlreadyDeactivated);
            }
            return Ok(());
        }
        => |user, event| {
            let _ = event;
            user.active = false;
        },
    }
}

command_handler! {
    impl User {
        fn register(name: String) -> RegisteredEvent { name };
        fn kick(group_id: EntityId, reason: String) -> KickedEvent { group_id, reason };
    }
}

// ============================================================================
// Group aggregate
// ============================================================================

#[derive(Debug, thiserror::Error)]
enum GroupError {
    #[error("Member not found")]
    MemberNotFound,
}

impl AggregateError for GroupError {}

#[derive(Debug, Serialize, Deserialize)]
struct Group {
    id: EntityId,
    name: String,
    members: Vec<EntityId>,
}

impl Entity for Group {
    fn new(id: EntityId) -> Self {
        Self {
            id,
            name: String::new(),
            members: Vec::new(),
        }
    }
    fn entity_id(&self) -> EntityId {
        self.id
    }
}

impl event_sauce_core::DefaultEntity for Group {}

impl Aggregate for Group {
    type Event = GroupEvent;
    type Error = GroupError;
    type DeletedState = Self;
}

define_events! {
    enum GroupEvent for Group {
        Created {
            name: String,
        } => |group, event| {
            group.name = event.name.clone();
        },

        MemberAdded {
            member_id: EntityId,
        } => |group, event| {
            group.members.push(event.member_id);
        },

        MemberRemoved {
            member_id: EntityId,
            reason: String,
        }
        @validate |agg, event| {
            if !agg.members.contains(&event.member_id) {
                return Err(GroupError::MemberNotFound);
            }
            return Ok(());
        }
        => |group, event| {
            group.members.retain(|m| *m != event.member_id);
        },
    }
}

command_handler! {
    impl Group {
        fn create(name: String) -> CreatedEvent { name };
        fn add_member(member_id: EntityId) -> MemberAddedEvent { member_id };
        fn remove_member(member_id: EntityId, reason: String)
            -> MemberRemovedEvent { member_id, reason };
    }
}

// ============================================================================
// Notification aggregate (for cascading test)
// ============================================================================

#[derive(Debug, thiserror::Error)]
#[error("notification error")]
struct NotificationError;

impl AggregateError for NotificationError {}

#[derive(Debug, Serialize, Deserialize)]
struct Notification {
    id: EntityId,
    message: String,
}

impl Entity for Notification {
    fn new(id: EntityId) -> Self {
        Self {
            id,
            message: String::new(),
        }
    }
    fn entity_id(&self) -> EntityId {
        self.id
    }
}

impl event_sauce_core::DefaultEntity for Notification {}

impl Aggregate for Notification {
    type Event = NotificationEvent;
    type Error = NotificationError;
    type DeletedState = Self;
}

define_events! {
    enum NotificationEvent for Notification {
        Sent {
            message: String,
        } => |n, event| {
            n.message = event.message.clone();
        },
    }
}

command_handler! {
    impl Notification {
        fn send_notification(message: String) -> SentEvent { message };
    }
}

// ============================================================================
// Reactors
// ============================================================================

// Reactor: when a user is kicked, remove them from the group.
// The kicked user's ID comes from the event envelope's aggregate_id.
reactor! {
    /// Removes kicked users from their groups.
    KickUserReactor {
        on KickedEvent |event, ctx| {
            let user_id = EntityId::from(ctx.source_event().aggregate_id);
            let mut group = ctx.load_as::<Group>(event.group_id).await?;
            group.remove_member(user_id, event.reason.clone())
                .map_err(|e| event_sauce_core::Error::invalid_state(format!("{e}")))?;
            ctx.commit(&mut group).await?;
            Ok(())
        },
    }
}

// ============================================================================
// Tests
// ============================================================================

fn create_store() -> Arc<InMemoryEventStore> {
    Arc::new(InMemoryEventStore::new())
}

#[tokio::test]
async fn test_basic_reactor_cross_aggregate_reaction() {
    let store = create_store();

    // Create a group with a member
    let group_id = EntityId::new();
    let user_id = EntityId::new();

    let mut group = AggregateRoot::<Group>::new(group_id);
    group.create("Test Group".to_string()).unwrap();
    group.add_member(user_id).unwrap();
    store.commit(&mut group).await.unwrap();

    // Kick the user — produces a KickedEvent
    let mut user = AggregateRoot::<User>::new(user_id);
    user.register("Alice".to_string()).unwrap();
    user.kick(group_id, "bad behavior".to_string()).unwrap();
    store.commit(&mut user).await.unwrap();

    // Run the reactor
    let runner = ReactorRunner::new(Arc::clone(&store)).register(Arc::new(KickUserReactor));

    let processed = runner.process_pending().await.unwrap();
    assert!(processed > 0);

    // Verify the group no longer has the member
    let repo: Repository<InMemoryEventStore, Group> = Repository::new(Arc::clone(&store));
    let loaded_group = repo.load(group_id).await.unwrap();
    assert!(loaded_group.members.is_empty());
}

#[tokio::test]
async fn test_causation_tracking() {
    let store = create_store();

    // Create group with member
    let group_id = EntityId::new();
    let user_id = EntityId::new();

    let mut group = AggregateRoot::<Group>::new(group_id);
    group.create("Test Group".to_string()).unwrap();
    group.add_member(user_id).unwrap();
    store.commit(&mut group).await.unwrap();

    // Kick user
    let mut user = AggregateRoot::<User>::new(user_id);
    user.register("Bob".to_string()).unwrap();
    user.kick(group_id, "rules violation".to_string()).unwrap();
    store.commit(&mut user).await.unwrap();

    // Process with reactor
    let runner = ReactorRunner::new(Arc::clone(&store)).register(Arc::new(KickUserReactor));
    runner.process_pending().await.unwrap();

    // Find events to check causation
    use futures::StreamExt;
    let stream = store
        .stream_all(event_sauce_core::Position::start())
        .await
        .unwrap();
    futures::pin_mut!(stream);

    let mut kick_event = None;
    let mut removal_event = None;
    while let Some(Ok(envelope)) = stream.next().await {
        if envelope.event_type == "User.Kicked" {
            kick_event = Some(envelope);
        } else if envelope.event_type == "Group.MemberRemoved" {
            removal_event = Some(envelope);
        }
    }

    let kick = kick_event.expect("Should have kick event");
    let removal = removal_event.expect("Should have removal event");

    // Verify causation metadata
    let meta = removal
        .metadata
        .as_ref()
        .expect("Reactor should inject metadata");

    assert_eq!(meta.causation_id, Some(kick.id));
    assert_eq!(meta.correlation_id, Some(kick.id));
    assert_eq!(meta.causation_chain, vec![kick.id]);
}

#[tokio::test]
async fn test_cascading_reactions() {
    let store = create_store();

    // Create group with member
    let group_id = EntityId::new();
    let user_id = EntityId::new();

    let mut group = AggregateRoot::<Group>::new(group_id);
    group.create("Cascade Group".to_string()).unwrap();
    group.add_member(user_id).unwrap();
    store.commit(&mut group).await.unwrap();

    // Cascading reactor: MemberRemoved → create Notification
    reactor! {
        CascadeNotifyReactor {
            on MemberRemovedEvent |event, ctx| {
                let id = EntityId::new();
                let mut notification = AggregateRoot::<Notification>::new(id);
                let msg = format!("Member {} removed: {}", event.member_id, event.reason);
                notification.send_notification(msg)
                    .map_err(|e| event_sauce_core::Error::invalid_state(format!("{e}")))?;
                ctx.commit(&mut notification).await?;
                Ok(())
            },
        }
    }

    // Kick user
    let mut user = AggregateRoot::<User>::new(user_id);
    user.register("Charlie".to_string()).unwrap();
    user.kick(group_id, "cascade test".to_string()).unwrap();
    store.commit(&mut user).await.unwrap();

    // Register both reactors: Kick → MemberRemoved → Notification
    let runner = ReactorRunner::new(Arc::clone(&store))
        .register(Arc::new(KickUserReactor))
        .register(Arc::new(CascadeNotifyReactor));

    runner.process_pending().await.unwrap();

    // Verify cascade: Notification was created
    use futures::StreamExt;
    let stream = store
        .stream_all(event_sauce_core::Position::start())
        .await
        .unwrap();
    futures::pin_mut!(stream);

    let mut kick_event = None;
    let mut removal_event = None;
    let mut notification_event = None;

    while let Some(Ok(envelope)) = stream.next().await {
        if envelope.event_type == "User.Kicked" {
            kick_event = Some(envelope);
        } else if envelope.event_type == "Group.MemberRemoved" {
            removal_event = Some(envelope);
        } else if envelope.event_type == "Notification.Sent" {
            notification_event = Some(envelope);
        }
    }

    let kick = kick_event.expect("Should have kick event");
    let removal = removal_event.expect("Should have removal event");
    let notification = notification_event.expect("Should have notification event");

    // Verify causation chain grows through cascade
    let removal_meta = removal.metadata.as_ref().unwrap();
    assert_eq!(removal_meta.causation_id, Some(kick.id));
    assert_eq!(removal_meta.causation_chain.len(), 1);

    let notification_meta = notification.metadata.as_ref().unwrap();
    assert_eq!(notification_meta.causation_id, Some(removal.id));
    assert_eq!(notification_meta.correlation_id, Some(kick.id));
    assert_eq!(notification_meta.causation_chain.len(), 2);
    assert_eq!(notification_meta.causation_chain[0], kick.id);
    assert_eq!(notification_meta.causation_chain[1], removal.id);
}

#[tokio::test]
async fn test_cascade_depth_limit() {
    let store = create_store();

    // Self-referencing reactor that would loop forever:
    // User.Registered → create another User → Registered event → ...
    reactor! {
        InfiniteLoopReactor {
            on RegisteredEvent |_event, ctx| {
                let id = EntityId::new();
                let mut user = AggregateRoot::<User>::new(id);
                user.register("loop".to_string())
                    .map_err(|e| event_sauce_core::Error::invalid_state(format!("{e}")))?;
                ctx.commit(&mut user).await?;
                Ok(())
            },
        }
    }

    // Create initial user
    let mut user = AggregateRoot::<User>::new(EntityId::new());
    user.register("seed".to_string()).unwrap();
    store.commit(&mut user).await.unwrap();

    // Set low cascade depth to stop the loop
    let runner = ReactorRunner::new(Arc::clone(&store))
        .with_max_cascade_depth(3)
        .register(Arc::new(InfiniteLoopReactor));

    let result = runner.process_pending().await;
    assert!(result.is_err());
    assert!(result.unwrap_err().is_cascade_depth_exceeded());
}

#[tokio::test]
async fn test_multiple_reactors_same_event() {
    let store = create_store();

    let counter1 = Arc::new(std::sync::Mutex::new(0_usize));
    let counter2 = Arc::new(std::sync::Mutex::new(0_usize));

    struct CountReactor1(Arc<std::sync::Mutex<usize>>);
    struct CountReactor2(Arc<std::sync::Mutex<usize>>);

    #[async_trait::async_trait]
    impl<S: EventStore + 'static> event_sauce_core::Reactor<S> for CountReactor1 {
        fn name(&self) -> &'static str {
            "CountReactor1"
        }
        fn event_filter(&self) -> EventFilter {
            EventFilter::by_event_type("User.Registered")
        }
        async fn handle(
            &self,
            _event: &EventEnvelope,
            _ctx: &ReactorContext<S>,
        ) -> event_sauce_core::Result<()> {
            *self.0.lock().unwrap() += 1;
            Ok(())
        }
    }

    #[async_trait::async_trait]
    impl<S: EventStore + 'static> event_sauce_core::Reactor<S> for CountReactor2 {
        fn name(&self) -> &'static str {
            "CountReactor2"
        }
        fn event_filter(&self) -> EventFilter {
            EventFilter::by_event_type("User.Registered")
        }
        async fn handle(
            &self,
            _event: &EventEnvelope,
            _ctx: &ReactorContext<S>,
        ) -> event_sauce_core::Result<()> {
            *self.0.lock().unwrap() += 1;
            Ok(())
        }
    }

    // Create a user
    let mut user = AggregateRoot::<User>::new(EntityId::new());
    user.register("Dave".to_string()).unwrap();
    store.commit(&mut user).await.unwrap();

    let runner = ReactorRunner::new(Arc::clone(&store))
        .register(Arc::new(CountReactor1(Arc::clone(&counter1))))
        .register(Arc::new(CountReactor2(Arc::clone(&counter2))));

    runner.process_pending().await.unwrap();

    // Both reactors should have processed the Registered event
    assert!(*counter1.lock().unwrap() > 0);
    assert!(*counter2.lock().unwrap() > 0);
}

#[tokio::test]
async fn test_process_pending_one_shot() {
    let store = create_store();

    // Create multiple users
    for i in 0..5 {
        let mut user = AggregateRoot::<User>::new(EntityId::new());
        user.register(format!("User{i}")).unwrap();
        store.commit(&mut user).await.unwrap();
    }

    let count = Arc::new(std::sync::Mutex::new(0_usize));

    struct OneShot(Arc<std::sync::Mutex<usize>>);

    #[async_trait::async_trait]
    impl<S: EventStore + 'static> event_sauce_core::Reactor<S> for OneShot {
        fn name(&self) -> &'static str {
            "OneShot"
        }
        fn event_filter(&self) -> EventFilter {
            EventFilter::by_event_type("User.Registered")
        }
        async fn handle(
            &self,
            _event: &EventEnvelope,
            _ctx: &ReactorContext<S>,
        ) -> event_sauce_core::Result<()> {
            *self.0.lock().unwrap() += 1;
            Ok(())
        }
    }

    let runner =
        ReactorRunner::new(Arc::clone(&store)).register(Arc::new(OneShot(Arc::clone(&count))));

    let processed = runner.process_pending().await.unwrap();
    assert_eq!(processed, 5);
    assert_eq!(*count.lock().unwrap(), 5);
}

#[tokio::test]
async fn test_no_matching_events() {
    let store = create_store();

    // Create users (Registered events)
    let mut user = AggregateRoot::<User>::new(EntityId::new());
    user.register("Eve".to_string()).unwrap();
    store.commit(&mut user).await.unwrap();

    // Register a reactor that only matches Group.Created events
    reactor! {
        GroupOnlyReactor {
            on CreatedEvent |_event, _ctx| {
                Ok(())
            },
        }
    }

    let runner = ReactorRunner::new(Arc::clone(&store)).register(Arc::new(GroupOnlyReactor));

    let processed = runner.process_pending().await.unwrap();
    assert_eq!(processed, 0);
}

#[tokio::test]
async fn test_reactor_macro_integration_full_flow() {
    let store = create_store();

    // Set up group and user
    let group_id = EntityId::new();
    let user_id = EntityId::new();

    let mut group = AggregateRoot::<Group>::new(group_id);
    group.create("Macro Test".to_string()).unwrap();
    group.add_member(user_id).unwrap();
    store.commit(&mut group).await.unwrap();

    let mut user = AggregateRoot::<User>::new(user_id);
    user.register("Frank".to_string()).unwrap();
    user.kick(group_id, "macro test".to_string()).unwrap();
    store.commit(&mut user).await.unwrap();

    // Use macro-defined reactor
    let runner = ReactorRunner::new(Arc::clone(&store)).register(Arc::new(KickUserReactor));

    runner.process_pending().await.unwrap();

    // Verify the reaction happened
    let repo: Repository<InMemoryEventStore, Group> = Repository::new(Arc::clone(&store));
    let loaded = repo.load(group_id).await.unwrap();
    assert!(loaded.members.is_empty());
}

#[tokio::test]
async fn test_process_single_event() {
    let store = create_store();

    let count = Arc::new(std::sync::Mutex::new(0_usize));

    struct SingleReactor(Arc<std::sync::Mutex<usize>>);

    #[async_trait::async_trait]
    impl<S: EventStore + 'static> event_sauce_core::Reactor<S> for SingleReactor {
        fn name(&self) -> &'static str {
            "SingleReactor"
        }
        fn event_filter(&self) -> EventFilter {
            EventFilter::by_event_type("User.Registered")
        }
        async fn handle(
            &self,
            _event: &EventEnvelope,
            _ctx: &ReactorContext<S>,
        ) -> event_sauce_core::Result<()> {
            *self.0.lock().unwrap() += 1;
            Ok(())
        }
    }

    let runner = ReactorRunner::new(Arc::clone(&store))
        .register(Arc::new(SingleReactor(Arc::clone(&count))));

    // Process a single event directly
    let envelope = EventEnvelope::new(
        uuid::Uuid::new_v4(),
        uuid::Uuid::new_v4(),
        "User",
        "User.Registered".to_string(),
        event_sauce_core::EventVersion::new(1),
        serde_json::json!({"name": "test", "timestamp": "2025-01-01T00:00:00Z"}),
    );

    let handled = runner.process_event(&envelope).await.unwrap();
    assert_eq!(handled, 1);
    assert_eq!(*count.lock().unwrap(), 1);
}

#[tokio::test]
async fn test_correlation_id_defaults_to_source_id() {
    let store = create_store();

    // Create group with member
    let group_id = EntityId::new();
    let user_id = EntityId::new();

    let mut group = AggregateRoot::<Group>::new(group_id);
    group.create("Correlation Group".to_string()).unwrap();
    group.add_member(user_id).unwrap();
    store.commit(&mut group).await.unwrap();

    // Kick user (no explicit correlation_id on source event)
    let mut user = AggregateRoot::<User>::new(user_id);
    user.register("Correlate".to_string()).unwrap();
    user.kick(group_id, "correlation test".to_string()).unwrap();
    store.commit(&mut user).await.unwrap();

    // Process
    let runner = ReactorRunner::new(Arc::clone(&store)).register(Arc::new(KickUserReactor));
    runner.process_pending().await.unwrap();

    // Find events
    use futures::StreamExt;
    let stream = store
        .stream_all(event_sauce_core::Position::start())
        .await
        .unwrap();
    futures::pin_mut!(stream);

    let mut kick_event = None;
    let mut removal_event = None;
    while let Some(Ok(envelope)) = stream.next().await {
        if envelope.event_type == "User.Kicked" {
            kick_event = Some(envelope);
        } else if envelope.event_type == "Group.MemberRemoved" {
            removal_event = Some(envelope);
        }
    }

    let kick = kick_event.expect("Should have kick event");
    let removal = removal_event.expect("Should have removal event");

    // Without parent correlation_id, correlation_id defaults to source event's id
    let meta = removal.metadata.as_ref().unwrap();
    assert_eq!(meta.correlation_id, Some(kick.id));
    assert_eq!(meta.causation_id, Some(kick.id));
}
