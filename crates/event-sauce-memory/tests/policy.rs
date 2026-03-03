//! Integration tests for the policy system with the in-memory event store.
//!
//! Tests cross-aggregate event reactions, causation tracking,
//! cascading reactions with depth limits, checkpoint-based resumption,
//! error handling strategies, and the `policy!` macro.

use event_sauce_core::{
    command_handler, define_events, policy, Aggregate, AggregateError, AggregateRoot,
    CheckpointStore, Entity, EntityId, EventEnvelope, EventFilter, EventStore, OnError,
    OnRetryExhausted, PolicyContext, PolicyRunner, Repository, RetryConfig, RetryLimit,
};
use event_sauce_memory::{InMemoryCheckpointStore, InMemoryEventStore};
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
// Policies
// ============================================================================

// Policy: when a user is kicked, remove them from the group.
// The kicked user's ID comes from the event envelope's aggregate_id.
policy! {
    /// Removes kicked users from their groups.
    KickUserPolicy {
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
    let cp = Arc::new(InMemoryCheckpointStore::new());
    Arc::new(InMemoryEventStore::builder().checkpoint_store(cp).build())
}

fn checkpoint_store(store: &Arc<InMemoryEventStore>) -> Arc<dyn CheckpointStore> {
    store.checkpoint_store().unwrap()
}

/// Seeds a policy's checkpoint to start so it processes all events.
/// Call this BEFORE adding events to the store.
async fn seed_checkpoint(cp: &Arc<dyn CheckpointStore>, policy_name: &str) {
    cp.save_checkpoint(policy_name, event_sauce_core::Position::start())
        .await
        .unwrap();
}

#[tokio::test]
async fn test_basic_policy_cross_aggregate_reaction() {
    let store = create_store();
    let cp = checkpoint_store(&store);
    seed_checkpoint(&cp, "KickUserPolicy").await;

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

    // Run the policy
    let runner = PolicyRunner::new(Arc::clone(&store), cp).register(Arc::new(KickUserPolicy));

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
    let cp = checkpoint_store(&store);
    seed_checkpoint(&cp, "KickUserPolicy").await;

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

    // Process with policy
    let runner = PolicyRunner::new(Arc::clone(&store), cp).register(Arc::new(KickUserPolicy));
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
        .expect("Policy should inject metadata");

    assert_eq!(meta.causation_id, Some(kick.id));
    assert_eq!(meta.correlation_id, Some(kick.id));
    assert_eq!(meta.causation_chain, vec![kick.id]);
}

#[tokio::test]
async fn test_cascading_reactions() {
    let store = create_store();
    let cp = checkpoint_store(&store);
    seed_checkpoint(&cp, "KickUserPolicy").await;
    seed_checkpoint(&cp, "CascadeNotifyPolicy").await;

    // Create group with member
    let group_id = EntityId::new();
    let user_id = EntityId::new();

    let mut group = AggregateRoot::<Group>::new(group_id);
    group.create("Cascade Group".to_string()).unwrap();
    group.add_member(user_id).unwrap();
    store.commit(&mut group).await.unwrap();

    // Cascading policy: MemberRemoved → create Notification
    policy! {
        CascadeNotifyPolicy {
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

    // Register both policies: Kick → MemberRemoved → Notification
    let runner = PolicyRunner::new(Arc::clone(&store), cp)
        .register(Arc::new(KickUserPolicy))
        .register(Arc::new(CascadeNotifyPolicy));

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
    let cp = checkpoint_store(&store);
    seed_checkpoint(&cp, "InfiniteLoopPolicy").await;

    // Self-referencing policy that would loop forever:
    // User.Registered → create another User → Registered event → ...
    policy! {
        InfiniteLoopPolicy {
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
    let runner = PolicyRunner::new(Arc::clone(&store), cp)
        .with_max_cascade_depth(3)
        .register(Arc::new(InfiniteLoopPolicy));

    let result = runner.process_pending().await;
    assert!(result.is_err());
    assert!(result.unwrap_err().is_cascade_depth_exceeded());
}

#[tokio::test]
async fn test_multiple_policies_same_event() {
    let store = create_store();
    let cp = checkpoint_store(&store);
    seed_checkpoint(&cp, "CountPolicy1").await;
    seed_checkpoint(&cp, "CountPolicy2").await;

    let counter1 = Arc::new(std::sync::Mutex::new(0_usize));
    let counter2 = Arc::new(std::sync::Mutex::new(0_usize));

    struct CountPolicy1(Arc<std::sync::Mutex<usize>>);
    struct CountPolicy2(Arc<std::sync::Mutex<usize>>);

    #[async_trait::async_trait]
    impl<S: EventStore + 'static> event_sauce_core::Policy<S> for CountPolicy1 {
        fn name(&self) -> &'static str {
            "CountPolicy1"
        }
        fn event_filter(&self) -> EventFilter {
            EventFilter::by_event_type("User.Registered")
        }
        async fn handle(
            &self,
            _event: &EventEnvelope,
            _ctx: &PolicyContext<S>,
        ) -> event_sauce_core::Result<()> {
            *self.0.lock().unwrap() += 1;
            Ok(())
        }
    }

    #[async_trait::async_trait]
    impl<S: EventStore + 'static> event_sauce_core::Policy<S> for CountPolicy2 {
        fn name(&self) -> &'static str {
            "CountPolicy2"
        }
        fn event_filter(&self) -> EventFilter {
            EventFilter::by_event_type("User.Registered")
        }
        async fn handle(
            &self,
            _event: &EventEnvelope,
            _ctx: &PolicyContext<S>,
        ) -> event_sauce_core::Result<()> {
            *self.0.lock().unwrap() += 1;
            Ok(())
        }
    }

    // Create a user
    let mut user = AggregateRoot::<User>::new(EntityId::new());
    user.register("Dave".to_string()).unwrap();
    store.commit(&mut user).await.unwrap();

    let runner = PolicyRunner::new(Arc::clone(&store), cp)
        .register(Arc::new(CountPolicy1(Arc::clone(&counter1))))
        .register(Arc::new(CountPolicy2(Arc::clone(&counter2))));

    runner.process_pending().await.unwrap();

    // Both policies should have processed the Registered event
    assert!(*counter1.lock().unwrap() > 0);
    assert!(*counter2.lock().unwrap() > 0);
}

#[tokio::test]
async fn test_process_pending_one_shot() {
    let store = create_store();
    let cp = checkpoint_store(&store);
    seed_checkpoint(&cp, "OneShot").await;

    // Create multiple users
    for i in 0..5 {
        let mut user = AggregateRoot::<User>::new(EntityId::new());
        user.register(format!("User{i}")).unwrap();
        store.commit(&mut user).await.unwrap();
    }

    let count = Arc::new(std::sync::Mutex::new(0_usize));

    struct OneShot(Arc<std::sync::Mutex<usize>>);

    #[async_trait::async_trait]
    impl<S: EventStore + 'static> event_sauce_core::Policy<S> for OneShot {
        fn name(&self) -> &'static str {
            "OneShot"
        }
        fn event_filter(&self) -> EventFilter {
            EventFilter::by_event_type("User.Registered")
        }
        async fn handle(
            &self,
            _event: &EventEnvelope,
            _ctx: &PolicyContext<S>,
        ) -> event_sauce_core::Result<()> {
            *self.0.lock().unwrap() += 1;
            Ok(())
        }
    }

    let runner =
        PolicyRunner::new(Arc::clone(&store), cp).register(Arc::new(OneShot(Arc::clone(&count))));

    let processed = runner.process_pending().await.unwrap();
    assert_eq!(processed, 5);
    assert_eq!(*count.lock().unwrap(), 5);
}

#[tokio::test]
async fn test_no_matching_events() {
    let store = create_store();
    let cp = checkpoint_store(&store);
    seed_checkpoint(&cp, "GroupOnlyPolicy").await;

    // Create users (Registered events)
    let mut user = AggregateRoot::<User>::new(EntityId::new());
    user.register("Eve".to_string()).unwrap();
    store.commit(&mut user).await.unwrap();

    // Register a policy that only matches Group.Created events
    policy! {
        GroupOnlyPolicy {
            on CreatedEvent |_event, _ctx| {
                Ok(())
            },
        }
    }

    let runner = PolicyRunner::new(Arc::clone(&store), cp).register(Arc::new(GroupOnlyPolicy));

    let processed = runner.process_pending().await.unwrap();
    assert_eq!(processed, 0);
}

#[tokio::test]
async fn test_policy_macro_integration_full_flow() {
    let store = create_store();
    let cp = checkpoint_store(&store);
    seed_checkpoint(&cp, "KickUserPolicy").await;

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

    // Use macro-defined policy
    let runner = PolicyRunner::new(Arc::clone(&store), cp).register(Arc::new(KickUserPolicy));

    runner.process_pending().await.unwrap();

    // Verify the reaction happened
    let repo: Repository<InMemoryEventStore, Group> = Repository::new(Arc::clone(&store));
    let loaded = repo.load(group_id).await.unwrap();
    assert!(loaded.members.is_empty());
}

#[tokio::test]
async fn test_process_single_event() {
    let store = create_store();
    let cp = checkpoint_store(&store);

    let count = Arc::new(std::sync::Mutex::new(0_usize));

    struct SinglePolicy(Arc<std::sync::Mutex<usize>>);

    #[async_trait::async_trait]
    impl<S: EventStore + 'static> event_sauce_core::Policy<S> for SinglePolicy {
        fn name(&self) -> &'static str {
            "SinglePolicy"
        }
        fn event_filter(&self) -> EventFilter {
            EventFilter::by_event_type("User.Registered")
        }
        async fn handle(
            &self,
            _event: &EventEnvelope,
            _ctx: &PolicyContext<S>,
        ) -> event_sauce_core::Result<()> {
            *self.0.lock().unwrap() += 1;
            Ok(())
        }
    }

    let runner = PolicyRunner::new(Arc::clone(&store), cp)
        .register(Arc::new(SinglePolicy(Arc::clone(&count))));

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
    let cp = checkpoint_store(&store);
    seed_checkpoint(&cp, "KickUserPolicy").await;

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
    let runner = PolicyRunner::new(Arc::clone(&store), cp).register(Arc::new(KickUserPolicy));
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

// ============================================================================
// Checkpoint tests
// ============================================================================

#[tokio::test]
async fn test_policy_runner_saves_checkpoints_after_processing() {
    let store = create_store();
    let cp = checkpoint_store(&store);
    seed_checkpoint(&cp, "CheckpointPolicy").await;

    let mut user = AggregateRoot::<User>::new(EntityId::new());
    user.register("Test".to_string()).unwrap();
    store.commit(&mut user).await.unwrap();

    let count = Arc::new(std::sync::Mutex::new(0_usize));

    struct CheckpointPolicy(Arc<std::sync::Mutex<usize>>);

    #[async_trait::async_trait]
    impl<S: EventStore + 'static> event_sauce_core::Policy<S> for CheckpointPolicy {
        fn name(&self) -> &'static str {
            "CheckpointPolicy"
        }
        fn event_filter(&self) -> EventFilter {
            EventFilter::all()
        }
        async fn handle(
            &self,
            _event: &EventEnvelope,
            _ctx: &PolicyContext<S>,
        ) -> event_sauce_core::Result<()> {
            *self.0.lock().unwrap() += 1;
            Ok(())
        }
    }

    let runner = PolicyRunner::new(Arc::clone(&store), Arc::clone(&cp))
        .register(Arc::new(CheckpointPolicy(Arc::clone(&count))));

    runner.process_pending().await.unwrap();

    // Checkpoint should be saved
    let saved = cp.load_checkpoint("CheckpointPolicy").await.unwrap();
    assert!(saved.is_some(), "Checkpoint should be persisted");
}

#[tokio::test]
async fn test_policy_runner_resumes_from_checkpoint() {
    let store = create_store();
    let cp = checkpoint_store(&store);
    seed_checkpoint(&cp, "ResumePolicy").await;

    let mut user = AggregateRoot::<User>::new(EntityId::new());
    user.register("First".to_string()).unwrap();
    store.commit(&mut user).await.unwrap();

    let count = Arc::new(std::sync::Mutex::new(0_usize));

    struct ResumePolicy(Arc<std::sync::Mutex<usize>>);

    #[async_trait::async_trait]
    impl<S: EventStore + 'static> event_sauce_core::Policy<S> for ResumePolicy {
        fn name(&self) -> &'static str {
            "ResumePolicy"
        }
        fn event_filter(&self) -> EventFilter {
            EventFilter::all()
        }
        async fn handle(
            &self,
            _event: &EventEnvelope,
            _ctx: &PolicyContext<S>,
        ) -> event_sauce_core::Result<()> {
            *self.0.lock().unwrap() += 1;
            Ok(())
        }
    }

    let policy: Arc<dyn event_sauce_core::Policy<InMemoryEventStore>> =
        Arc::new(ResumePolicy(Arc::clone(&count)));

    // First run
    let runner =
        PolicyRunner::new(Arc::clone(&store), Arc::clone(&cp)).register(Arc::clone(&policy));
    let processed = runner.process_pending().await.unwrap();
    assert!(processed > 0);

    let count_after_first = *count.lock().unwrap();

    // Second run — should process 0 events (checkpoint skips old ones)
    let runner2 = PolicyRunner::new(Arc::clone(&store), Arc::clone(&cp)).register(policy);
    let processed2 = runner2.process_pending().await.unwrap();
    assert_eq!(processed2, 0, "Second run should process no events");
    assert_eq!(
        *count.lock().unwrap(),
        count_after_first,
        "Counter should not increase"
    );
}

#[tokio::test]
async fn test_policy_runner_no_duplicate_cascades_on_restart() {
    let store = create_store();
    let cp = checkpoint_store(&store);
    seed_checkpoint(&cp, "KickUserPolicy").await;

    // Create group with member
    let group_id = EntityId::new();
    let user_id = EntityId::new();

    let mut group = AggregateRoot::<Group>::new(group_id);
    group.create("NoDup Group".to_string()).unwrap();
    group.add_member(user_id).unwrap();
    store.commit(&mut group).await.unwrap();

    let mut user = AggregateRoot::<User>::new(user_id);
    user.register("NoDup".to_string()).unwrap();
    user.kick(group_id, "test".to_string()).unwrap();
    store.commit(&mut user).await.unwrap();

    // First run — produces cascade events
    let runner =
        PolicyRunner::new(Arc::clone(&store), Arc::clone(&cp)).register(Arc::new(KickUserPolicy));
    let processed1 = runner.process_pending().await.unwrap();
    assert!(processed1 > 0);

    // Count MemberRemoved events
    use futures::StreamExt;
    async fn count_member_removed(store: &Arc<InMemoryEventStore>) -> usize {
        let stream = store
            .stream_all(event_sauce_core::Position::start())
            .await
            .unwrap();
        futures::pin_mut!(stream);
        let mut count = 0;
        while let Some(Ok(envelope)) = stream.next().await {
            if envelope.event_type == "Group.MemberRemoved" {
                count += 1;
            }
        }
        count
    }

    let count_before = count_member_removed(&store).await;

    // Second run — should NOT produce duplicate cascade events
    let runner2 =
        PolicyRunner::new(Arc::clone(&store), Arc::clone(&cp)).register(Arc::new(KickUserPolicy));
    let processed2 = runner2.process_pending().await.unwrap();
    assert_eq!(processed2, 0, "Second run should process no events");

    let count_after = count_member_removed(&store).await;
    assert_eq!(
        count_before, count_after,
        "No duplicate MemberRemoved events"
    );
}

#[tokio::test]
async fn test_new_policy_skips_existing_events() {
    let store = create_store();
    let cp = checkpoint_store(&store);

    // Create some events first
    for _ in 0..3 {
        let mut user = AggregateRoot::<User>::new(EntityId::new());
        user.register("Existing".to_string()).unwrap();
        store.commit(&mut user).await.unwrap();
    }

    let count = Arc::new(std::sync::Mutex::new(0_usize));

    struct NewPolicy(Arc<std::sync::Mutex<usize>>);

    #[async_trait::async_trait]
    impl<S: EventStore + 'static> event_sauce_core::Policy<S> for NewPolicy {
        fn name(&self) -> &'static str {
            "NewPolicy"
        }
        fn event_filter(&self) -> EventFilter {
            EventFilter::all()
        }
        async fn handle(
            &self,
            _event: &EventEnvelope,
            _ctx: &PolicyContext<S>,
        ) -> event_sauce_core::Result<()> {
            *self.0.lock().unwrap() += 1;
            Ok(())
        }
    }

    // New policy with no checkpoint should skip all existing events
    let runner = PolicyRunner::new(Arc::clone(&store), Arc::clone(&cp))
        .register(Arc::new(NewPolicy(Arc::clone(&count))));

    let processed = runner.process_pending().await.unwrap();
    assert_eq!(processed, 0, "New policy should skip existing events");
    assert_eq!(*count.lock().unwrap(), 0, "Handler should not be called");

    // Now add a new event — the policy should process it
    let mut user = AggregateRoot::<User>::new(EntityId::new());
    user.register("New".to_string()).unwrap();
    store.commit(&mut user).await.unwrap();

    let runner2 =
        PolicyRunner::new(Arc::clone(&store), cp).register(Arc::new(NewPolicy(Arc::clone(&count))));

    let processed2 = runner2.process_pending().await.unwrap();
    assert_eq!(processed2, 1, "Should process only the new event");
    assert_eq!(*count.lock().unwrap(), 1);
}

#[tokio::test]
async fn test_cascade_with_checkpoints() {
    let store = create_store();
    let cp = checkpoint_store(&store);
    seed_checkpoint(&cp, "KickUserPolicy").await;
    seed_checkpoint(&cp, "CascadeNotifyCP").await;

    // Create group with member
    let group_id = EntityId::new();
    let user_id = EntityId::new();

    let mut group = AggregateRoot::<Group>::new(group_id);
    group.create("Cascade CP Group".to_string()).unwrap();
    group.add_member(user_id).unwrap();
    store.commit(&mut group).await.unwrap();

    policy! {
        CascadeNotifyCP {
            on MemberRemovedEvent |event, ctx| {
                let id = EntityId::new();
                let mut n = AggregateRoot::<Notification>::new(id);
                let msg = format!("Removed: {}", event.member_id);
                n.send_notification(msg)
                    .map_err(|e| event_sauce_core::Error::invalid_state(format!("{e}")))?;
                ctx.commit(&mut n).await?;
                Ok(())
            },
        }
    }

    let mut user = AggregateRoot::<User>::new(user_id);
    user.register("CascadeCP".to_string()).unwrap();
    user.kick(group_id, "cascade-cp test".to_string()).unwrap();
    store.commit(&mut user).await.unwrap();

    let runner = PolicyRunner::new(Arc::clone(&store), Arc::clone(&cp))
        .register(Arc::new(KickUserPolicy))
        .register(Arc::new(CascadeNotifyCP));

    let processed = runner.process_pending().await.unwrap();
    assert!(
        processed >= 2,
        "Both kick→remove and remove→notify should fire"
    );

    // Second run should process 0
    let runner2 = PolicyRunner::new(Arc::clone(&store), cp)
        .register(Arc::new(KickUserPolicy))
        .register(Arc::new(CascadeNotifyCP));

    let processed2 = runner2.process_pending().await.unwrap();
    assert_eq!(processed2, 0);
}

// ============================================================================
// Error handling tests
// ============================================================================

#[tokio::test]
async fn test_on_error_fail_aborts_processing() {
    let store = create_store();
    let cp = checkpoint_store(&store);
    seed_checkpoint(&cp, "FailingPolicy").await;

    struct FailingPolicy;

    #[async_trait::async_trait]
    impl<S: EventStore + 'static> event_sauce_core::Policy<S> for FailingPolicy {
        fn name(&self) -> &'static str {
            "FailingPolicy"
        }
        fn event_filter(&self) -> EventFilter {
            EventFilter::all()
        }
        async fn handle(
            &self,
            _event: &EventEnvelope,
            _ctx: &PolicyContext<S>,
        ) -> event_sauce_core::Result<()> {
            Err(event_sauce_core::Error::custom("handler error"))
        }
    }

    let mut user = AggregateRoot::<User>::new(EntityId::new());
    user.register("Fail".to_string()).unwrap();
    store.commit(&mut user).await.unwrap();

    // Default is OnError::Fail
    let runner = PolicyRunner::new(Arc::clone(&store), cp).register(Arc::new(FailingPolicy));

    let result = runner.process_pending().await;
    assert!(result.is_err(), "Should abort on error with OnError::Fail");
}

#[tokio::test]
async fn test_on_error_fail_does_not_advance_checkpoint() {
    let store = create_store();
    let cp = checkpoint_store(&store);
    seed_checkpoint(&cp, "FailCheckpointPolicy").await;

    struct FailCheckpointPolicy;

    #[async_trait::async_trait]
    impl<S: EventStore + 'static> event_sauce_core::Policy<S> for FailCheckpointPolicy {
        fn name(&self) -> &'static str {
            "FailCheckpointPolicy"
        }
        fn event_filter(&self) -> EventFilter {
            EventFilter::all()
        }
        async fn handle(
            &self,
            _event: &EventEnvelope,
            _ctx: &PolicyContext<S>,
        ) -> event_sauce_core::Result<()> {
            Err(event_sauce_core::Error::custom("fail"))
        }
    }

    let mut user = AggregateRoot::<User>::new(EntityId::new());
    user.register("FailCP".to_string()).unwrap();
    store.commit(&mut user).await.unwrap();

    let runner = PolicyRunner::new(Arc::clone(&store), Arc::clone(&cp))
        .register(Arc::new(FailCheckpointPolicy));

    let _ = runner.process_pending().await;

    // Checkpoint should NOT advance past start (error aborted before save)
    let saved = cp.load_checkpoint("FailCheckpointPolicy").await.unwrap();
    assert_eq!(
        saved,
        Some(event_sauce_core::Position::start()),
        "Checkpoint should not advance on failure"
    );
}

#[tokio::test]
async fn test_on_error_skip_continues_processing() {
    let store = create_store();
    let cp = checkpoint_store(&store);
    seed_checkpoint(&cp, "SkipPolicy").await;

    let count = Arc::new(std::sync::Mutex::new(0_usize));

    struct SkipPolicy(Arc<std::sync::Mutex<usize>>);

    #[async_trait::async_trait]
    impl<S: EventStore + 'static> event_sauce_core::Policy<S> for SkipPolicy {
        fn name(&self) -> &'static str {
            "SkipPolicy"
        }
        fn event_filter(&self) -> EventFilter {
            EventFilter::all()
        }
        async fn handle(
            &self,
            event: &EventEnvelope,
            _ctx: &PolicyContext<S>,
        ) -> event_sauce_core::Result<()> {
            *self.0.lock().unwrap() += 1;
            // Fail on the first event (Registered), succeed on second (Kicked)
            if event.event_type == "User.Registered" {
                return Err(event_sauce_core::Error::custom("skip me"));
            }
            Ok(())
        }
    }

    let mut user = AggregateRoot::<User>::new(EntityId::new());
    user.register("Skip".to_string()).unwrap();
    store.commit(&mut user).await.unwrap();

    // Add a second event that will succeed
    let mut user2 = AggregateRoot::<User>::new(EntityId::new());
    user2.register("Skip2".to_string()).unwrap();
    store.commit(&mut user2).await.unwrap();

    let runner = PolicyRunner::new(Arc::clone(&store), cp)
        .on_error(OnError::Skip)
        .register(Arc::new(SkipPolicy(Arc::clone(&count))));

    let result = runner.process_pending().await;
    assert!(result.is_ok(), "Should not abort with OnError::Skip");

    // Handler was called for both events (first failed, second also failed since both are Registered)
    assert_eq!(
        *count.lock().unwrap(),
        2,
        "Handler should be called for each event"
    );
}

#[tokio::test]
async fn test_on_error_skip_advances_checkpoint() {
    let store = create_store();
    let cp = checkpoint_store(&store);
    seed_checkpoint(&cp, "SkipCPPolicy").await;

    struct SkipCPPolicy;

    #[async_trait::async_trait]
    impl<S: EventStore + 'static> event_sauce_core::Policy<S> for SkipCPPolicy {
        fn name(&self) -> &'static str {
            "SkipCPPolicy"
        }
        fn event_filter(&self) -> EventFilter {
            EventFilter::all()
        }
        async fn handle(
            &self,
            _event: &EventEnvelope,
            _ctx: &PolicyContext<S>,
        ) -> event_sauce_core::Result<()> {
            Err(event_sauce_core::Error::custom("skip"))
        }
    }

    let mut user = AggregateRoot::<User>::new(EntityId::new());
    user.register("SkipCP".to_string()).unwrap();
    store.commit(&mut user).await.unwrap();

    let runner = PolicyRunner::new(Arc::clone(&store), Arc::clone(&cp))
        .on_error(OnError::Skip)
        .register(Arc::new(SkipCPPolicy));

    runner.process_pending().await.unwrap();

    // Checkpoint should be saved even though handler failed
    let saved = cp.load_checkpoint("SkipCPPolicy").await.unwrap();
    assert!(saved.is_some(), "Checkpoint should advance on skip");
}

#[tokio::test]
async fn test_on_error_retry_succeeds_after_transient_failure() {
    let store = create_store();
    let cp = checkpoint_store(&store);
    seed_checkpoint(&cp, "TransientPolicy").await;

    let attempt_count = Arc::new(std::sync::Mutex::new(0_usize));

    struct TransientPolicy(Arc<std::sync::Mutex<usize>>);

    #[async_trait::async_trait]
    impl<S: EventStore + 'static> event_sauce_core::Policy<S> for TransientPolicy {
        fn name(&self) -> &'static str {
            "TransientPolicy"
        }
        fn event_filter(&self) -> EventFilter {
            EventFilter::all()
        }
        async fn handle(
            &self,
            _event: &EventEnvelope,
            _ctx: &PolicyContext<S>,
        ) -> event_sauce_core::Result<()> {
            let mut count = self.0.lock().unwrap();
            *count += 1;
            if *count <= 1 {
                // First attempt fails
                return Err(event_sauce_core::Error::custom("transient"));
            }
            // Retry succeeds
            Ok(())
        }
    }

    let mut user = AggregateRoot::<User>::new(EntityId::new());
    user.register("Retry".to_string()).unwrap();
    store.commit(&mut user).await.unwrap();

    let retry_config = RetryConfig {
        base_delay: std::time::Duration::from_millis(1),
        max_delay: std::time::Duration::from_millis(10),
        limit: RetryLimit::MaxRetries(3),
        on_exhausted: OnRetryExhausted::Fail,
    };

    let runner = PolicyRunner::new(Arc::clone(&store), cp)
        .on_error(OnError::Retry(retry_config))
        .register(Arc::new(TransientPolicy(Arc::clone(&attempt_count))));

    let result = runner.process_pending().await;
    assert!(result.is_ok(), "Should succeed after retry");
    assert!(
        *attempt_count.lock().unwrap() >= 2,
        "Should have retried at least once"
    );
}

#[tokio::test]
async fn test_on_error_retry_max_retries_exhausted_fail() {
    let store = create_store();
    let cp = checkpoint_store(&store);
    seed_checkpoint(&cp, "AlwaysFailPolicy").await;

    struct AlwaysFailPolicy;

    #[async_trait::async_trait]
    impl<S: EventStore + 'static> event_sauce_core::Policy<S> for AlwaysFailPolicy {
        fn name(&self) -> &'static str {
            "AlwaysFailPolicy"
        }
        fn event_filter(&self) -> EventFilter {
            EventFilter::all()
        }
        async fn handle(
            &self,
            _event: &EventEnvelope,
            _ctx: &PolicyContext<S>,
        ) -> event_sauce_core::Result<()> {
            Err(event_sauce_core::Error::custom("always fail"))
        }
    }

    let mut user = AggregateRoot::<User>::new(EntityId::new());
    user.register("ExhaustFail".to_string()).unwrap();
    store.commit(&mut user).await.unwrap();

    let retry_config = RetryConfig {
        base_delay: std::time::Duration::from_millis(1),
        max_delay: std::time::Duration::from_millis(10),
        limit: RetryLimit::MaxRetries(2),
        on_exhausted: OnRetryExhausted::Fail,
    };

    let runner = PolicyRunner::new(Arc::clone(&store), cp)
        .on_error(OnError::Retry(retry_config))
        .register(Arc::new(AlwaysFailPolicy));

    let result = runner.process_pending().await;
    assert!(
        result.is_err(),
        "Should fail when retries exhausted with Fail"
    );
}

#[tokio::test]
async fn test_on_error_retry_max_retries_exhausted_skip() {
    let store = create_store();
    let cp = checkpoint_store(&store);
    seed_checkpoint(&cp, "AlwaysFailSkipPolicy").await;

    struct AlwaysFailSkipPolicy;

    #[async_trait::async_trait]
    impl<S: EventStore + 'static> event_sauce_core::Policy<S> for AlwaysFailSkipPolicy {
        fn name(&self) -> &'static str {
            "AlwaysFailSkipPolicy"
        }
        fn event_filter(&self) -> EventFilter {
            EventFilter::all()
        }
        async fn handle(
            &self,
            _event: &EventEnvelope,
            _ctx: &PolicyContext<S>,
        ) -> event_sauce_core::Result<()> {
            Err(event_sauce_core::Error::custom("always fail"))
        }
    }

    let mut user = AggregateRoot::<User>::new(EntityId::new());
    user.register("ExhaustSkip".to_string()).unwrap();
    store.commit(&mut user).await.unwrap();

    let retry_config = RetryConfig {
        base_delay: std::time::Duration::from_millis(1),
        max_delay: std::time::Duration::from_millis(10),
        limit: RetryLimit::MaxRetries(2),
        on_exhausted: OnRetryExhausted::Skip,
    };

    let runner = PolicyRunner::new(Arc::clone(&store), Arc::clone(&cp))
        .on_error(OnError::Retry(retry_config))
        .register(Arc::new(AlwaysFailSkipPolicy));

    let result = runner.process_pending().await;
    assert!(
        result.is_ok(),
        "Should skip when retries exhausted with Skip"
    );

    // Checkpoint should be saved (event was skipped)
    let saved = cp.load_checkpoint("AlwaysFailSkipPolicy").await.unwrap();
    assert!(saved.is_some(), "Checkpoint should advance on skip");
}

#[tokio::test]
async fn test_on_error_retry_does_not_advance_checkpoint_on_fail() {
    let store = create_store();
    let cp = checkpoint_store(&store);
    seed_checkpoint(&cp, "RetryFailCPPolicy").await;

    struct RetryFailCPPolicy;

    #[async_trait::async_trait]
    impl<S: EventStore + 'static> event_sauce_core::Policy<S> for RetryFailCPPolicy {
        fn name(&self) -> &'static str {
            "RetryFailCPPolicy"
        }
        fn event_filter(&self) -> EventFilter {
            EventFilter::all()
        }
        async fn handle(
            &self,
            _event: &EventEnvelope,
            _ctx: &PolicyContext<S>,
        ) -> event_sauce_core::Result<()> {
            Err(event_sauce_core::Error::custom("fail"))
        }
    }

    let mut user = AggregateRoot::<User>::new(EntityId::new());
    user.register("RetryFailCP".to_string()).unwrap();
    store.commit(&mut user).await.unwrap();

    let retry_config = RetryConfig {
        base_delay: std::time::Duration::from_millis(1),
        max_delay: std::time::Duration::from_millis(10),
        limit: RetryLimit::MaxRetries(1),
        on_exhausted: OnRetryExhausted::Fail,
    };

    let runner = PolicyRunner::new(Arc::clone(&store), Arc::clone(&cp))
        .on_error(OnError::Retry(retry_config))
        .register(Arc::new(RetryFailCPPolicy));

    let _ = runner.process_pending().await;

    let saved = cp.load_checkpoint("RetryFailCPPolicy").await.unwrap();
    assert_eq!(
        saved,
        Some(event_sauce_core::Position::start()),
        "Checkpoint should not advance on retry+fail"
    );
}

#[tokio::test]
async fn test_on_error_retry_indefinite_eventually_succeeds() {
    let store = create_store();
    let cp = checkpoint_store(&store);
    seed_checkpoint(&cp, "IndefinitePolicy").await;

    let attempt_count = Arc::new(std::sync::Mutex::new(0_usize));

    struct IndefinitePolicy(Arc<std::sync::Mutex<usize>>);

    #[async_trait::async_trait]
    impl<S: EventStore + 'static> event_sauce_core::Policy<S> for IndefinitePolicy {
        fn name(&self) -> &'static str {
            "IndefinitePolicy"
        }
        fn event_filter(&self) -> EventFilter {
            EventFilter::all()
        }
        async fn handle(
            &self,
            _event: &EventEnvelope,
            _ctx: &PolicyContext<S>,
        ) -> event_sauce_core::Result<()> {
            let mut count = self.0.lock().unwrap();
            *count += 1;
            if *count <= 3 {
                return Err(event_sauce_core::Error::custom("not yet"));
            }
            Ok(())
        }
    }

    let mut user = AggregateRoot::<User>::new(EntityId::new());
    user.register("Indefinite".to_string()).unwrap();
    store.commit(&mut user).await.unwrap();

    let retry_config = RetryConfig {
        base_delay: std::time::Duration::from_millis(1),
        max_delay: std::time::Duration::from_millis(10),
        limit: RetryLimit::Indefinite,
        on_exhausted: OnRetryExhausted::Fail, // irrelevant for Indefinite
    };

    let runner = PolicyRunner::new(Arc::clone(&store), cp)
        .on_error(OnError::Retry(retry_config))
        .register(Arc::new(IndefinitePolicy(Arc::clone(&attempt_count))));

    let result = runner.process_pending().await;
    assert!(result.is_ok(), "Should eventually succeed");
    assert!(
        *attempt_count.lock().unwrap() >= 4,
        "Should have tried at least 4 times"
    );
}

#[tokio::test]
async fn test_on_error_retry_max_duration_exhausted() {
    let store = create_store();
    let cp = checkpoint_store(&store);
    seed_checkpoint(&cp, "DurationFailPolicy").await;

    struct DurationFailPolicy;

    #[async_trait::async_trait]
    impl<S: EventStore + 'static> event_sauce_core::Policy<S> for DurationFailPolicy {
        fn name(&self) -> &'static str {
            "DurationFailPolicy"
        }
        fn event_filter(&self) -> EventFilter {
            EventFilter::all()
        }
        async fn handle(
            &self,
            _event: &EventEnvelope,
            _ctx: &PolicyContext<S>,
        ) -> event_sauce_core::Result<()> {
            Err(event_sauce_core::Error::custom("always fail"))
        }
    }

    let mut user = AggregateRoot::<User>::new(EntityId::new());
    user.register("Duration".to_string()).unwrap();
    store.commit(&mut user).await.unwrap();

    let retry_config = RetryConfig {
        base_delay: std::time::Duration::from_millis(10),
        max_delay: std::time::Duration::from_millis(50),
        limit: RetryLimit::MaxDuration(std::time::Duration::from_millis(30)),
        on_exhausted: OnRetryExhausted::Fail,
    };

    let runner = PolicyRunner::new(Arc::clone(&store), cp)
        .on_error(OnError::Retry(retry_config))
        .register(Arc::new(DurationFailPolicy));

    let result = runner.process_pending().await;
    assert!(result.is_err(), "Should fail when duration exhausted");
}

// ============================================================================
// Convenience method tests
// ============================================================================

#[tokio::test]
async fn test_policy_runner_convenience_method() {
    let store = create_store();

    let runner = store.policy_runner().unwrap();
    assert!(runner.policies().is_empty());
}

#[tokio::test]
async fn test_policy_runner_convenience_method_without_checkpoint_store_errors() {
    // Create store without checkpoint store
    let store = Arc::new(InMemoryEventStore::new());

    let result = store.policy_runner();
    assert!(result.is_err());
}
