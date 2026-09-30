//! The [`policy!`](crate::policy) declarative macro for defining a `Policy`
//! from a set of `on EventStruct |event, ctx| { .. }` handlers.

/// Define a policy that handles events by issuing commands on other aggregates.
///
/// This macro generates a struct implementing [`Policy<S>`](crate::policy::Policy)
/// that routes events to handler closures based on event type. Each handler receives
/// a deserialized event struct and a [`PolicyContext`](crate::policy::PolicyContext)
/// for loading and committing aggregates with automatic causation tracking.
///
/// # Syntax
///
/// ```ignore
/// policy! {
///     /// Optional doc comment
///     PolicyName {
///         on EventStruct |event, ctx| {
///             // Handle the event
///         },
///         on AnotherEventStruct |event, ctx| {
///             // Handle another event
///         },
///     }
/// }
/// ```
///
/// Each `EventStruct` must implement [`EventType`](crate::EventType) (which provides
/// the event type string for filtering) and [`serde::de::DeserializeOwned`] (for
/// deserialization from the event envelope).
///
/// When using [`define_events!`](crate::define_events), each variant `Foo` generates
/// a `FooEvent` struct that implements both traits automatically.
///
/// # Generated Code
///
/// For `policy! { MyPolicy { on FooEvent |e, ctx| { ... }, on BarEvent |e, ctx| { ... } } }`:
///
/// 1. `struct MyPolicy;`
/// 2. `impl<S: EventStore + 'static> Policy<S> for MyPolicy` with:
///    - `name()` → `"MyPolicy"`
///    - `event_filter()` → `EventFilter::any_of_event_types([FooEvent::EVENT_TYPE, BarEvent::EVENT_TYPE])`
///    - `handle()` → match on `event_type`, deserialize `event_data` to the struct, call handler
///
/// # Examples
///
/// ```ignore
/// use event_sauce::policy;
///
/// policy! {
///     /// Reacts to user kicks by removing them from groups.
///     KickUserPolicy {
///         on KickedUserEvent |event, ctx| {
///             let mut group = ctx.load(event.group_id).await?;
///             group.remove_user(event.user_id, "kicked")?;
///             ctx.commit(&mut group).await?;
///         },
///     }
/// }
///
/// // Register with a runner:
/// let runner = PolicyRunner::new(store, checkpoint_store)
///     .register(Arc::new(KickUserPolicy));
/// ```
#[cfg(feature = "event-sourcing")]
#[macro_export]
macro_rules! policy {
    // Entry point: with doc comments
    (
        $(#[$meta:meta])*
        $vis:vis $name:ident {
            $(
                on $event_struct:ty |$event:ident, $ctx:ident| $handler:block
            ),+ $(,)?
        }
    ) => {
        $(#[$meta])*
        $vis struct $name;

        #[$crate::__private::async_trait]
        impl<S: $crate::EventStore + 'static> $crate::policy::Policy<S> for $name {
            fn name(&self) -> &str {
                stringify!($name)
            }

            fn event_filter(&self) -> $crate::EventFilter {
                $crate::EventFilter::any_of_event_types([
                    $(<$event_struct as $crate::EventType>::EVENT_TYPE),+
                ])
            }

            async fn handle(
                &self,
                event: &$crate::EventEnvelope,
                ctx: &$crate::policy::PolicyContext<S>,
            ) -> $crate::Result<()> {
                $(
                    if event.event_type == <$event_struct as $crate::EventType>::EVENT_TYPE {
                        let $event: $event_struct = $crate::__private::serde_json::from_value(event.event_data.clone())?;
                        let $ctx = ctx;
                        return $handler;
                    }
                )+

                Ok(())
            }
        }
    };
}

#[cfg(test)]
mod tests {
    use super::super::context::PolicyContext;
    use super::super::runner::PolicyRunner;
    use super::super::test_support::{test_checkpoint_store, test_envelope};
    use super::super::Policy;
    use crate::test_fixtures::{MockEventStore, SimpleTestEntity, SimpleTestEvent};
    use crate::{AggregateRoot, EventEnvelope, EventFilter, EventStore, Result};
    use std::sync::Arc;

    #[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
    struct FooCreatedEvent {
        pub name: String,
    }

    impl crate::EventType for FooCreatedEvent {
        const EVENT_TYPE: &'static str = "Foo.Created";
    }

    #[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
    struct FooUpdatedEvent {
        pub value: i32,
    }

    impl crate::EventType for FooUpdatedEvent {
        const EVENT_TYPE: &'static str = "Foo.Updated";
    }

    #[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
    struct BarDeletedEvent {
        pub reason: String,
    }

    impl crate::EventType for BarDeletedEvent {
        const EVENT_TYPE: &'static str = "Bar.Deleted";
    }

    crate::policy! {
        TestPolicy {
            on FooCreatedEvent |_event, _ctx| {
                Ok(())
            },
        }
    }

    #[test]
    fn test_policy_macro_generates_struct() {
        let policy: &dyn Policy<MockEventStore> = &TestPolicy;
        assert_eq!(policy.name(), "TestPolicy");
    }

    #[test]
    fn test_policy_macro_name() {
        let policy: &dyn Policy<MockEventStore> = &TestPolicy;
        assert_eq!(policy.name(), "TestPolicy");
    }

    #[test]
    fn test_policy_macro_event_filter_single() {
        let policy: &dyn Policy<MockEventStore> = &TestPolicy;
        let filter = policy.event_filter();

        let matching = test_envelope("Foo.Created", "Foo");
        let non_matching = test_envelope("Foo.Updated", "Foo");

        assert!(filter.matches(&matching));
        assert!(!filter.matches(&non_matching));
    }

    crate::policy! {
        MultiEventPolicy {
            on FooCreatedEvent |_event, _ctx| {
                Ok(())
            },
            on FooUpdatedEvent |_event, _ctx| {
                Ok(())
            },
            on BarDeletedEvent |_event, _ctx| {
                Ok(())
            },
        }
    }

    #[test]
    fn test_policy_macro_event_filter_multiple() {
        let policy: &dyn Policy<MockEventStore> = &MultiEventPolicy;
        let filter = policy.event_filter();

        assert!(filter.matches(&test_envelope("Foo.Created", "Foo")));
        assert!(filter.matches(&test_envelope("Foo.Updated", "Foo")));
        assert!(filter.matches(&test_envelope("Bar.Deleted", "Bar")));
        assert!(!filter.matches(&test_envelope("Other.Event", "Other")));
    }

    struct CapturingPolicy(Arc<std::sync::Mutex<Option<String>>>);

    #[async_trait::async_trait]
    impl<S: EventStore + 'static> Policy<S> for CapturingPolicy {
        fn name(&self) -> &'static str {
            "CapturingPolicy"
        }
        fn event_filter(&self) -> EventFilter {
            EventFilter::by_event_type("Foo.Created")
        }
        async fn handle(&self, event: &EventEnvelope, _ctx: &PolicyContext<S>) -> Result<()> {
            let foo: FooCreatedEvent = serde_json::from_value(event.event_data.clone())?;
            *self.0.lock().unwrap() = Some(foo.name);
            Ok(())
        }
    }

    #[tokio::test]
    async fn test_policy_macro_handle_deserializes_event() {
        let store = Arc::new(MockEventStore::new());
        let received = Arc::new(std::sync::Mutex::new(None::<String>));

        let policy = CapturingPolicy(Arc::clone(&received));
        let mut envelope = test_envelope("Foo.Created", "Foo");
        envelope.event_data = serde_json::json!({"name": "hello"});

        let ctx = PolicyContext::new(Arc::clone(&store), envelope.clone(), 10);
        policy.handle(&envelope, &ctx).await.unwrap();

        assert_eq!(*received.lock().unwrap(), Some("hello".to_string()));
    }

    #[tokio::test]
    async fn test_policy_macro_handle_routes_to_correct_handler() {
        let store = Arc::new(MockEventStore::new());

        let mut envelope = test_envelope("Foo.Created", "Foo");
        envelope.event_data = serde_json::json!({"name": "test_name"});

        let ctx = PolicyContext::new(Arc::clone(&store), envelope.clone(), 10);

        let result =
            <TestPolicy as Policy<MockEventStore>>::handle(&TestPolicy, &envelope, &ctx).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_policy_macro_handle_deserialization_error() {
        let store = Arc::new(MockEventStore::new());

        let mut envelope = test_envelope("Foo.Created", "Foo");
        envelope.event_data = serde_json::json!({"wrong_field": 42});

        let ctx = PolicyContext::new(Arc::clone(&store), envelope.clone(), 10);

        let result =
            <TestPolicy as Policy<MockEventStore>>::handle(&TestPolicy, &envelope, &ctx).await;
        assert!(result.is_err());
        assert!(result.unwrap_err().is_serialization());
    }

    #[tokio::test]
    async fn test_policy_macro_handle_unmatched_event() {
        let store = Arc::new(MockEventStore::new());

        let envelope = test_envelope("Unknown.Event", "Unknown");
        let ctx = PolicyContext::new(Arc::clone(&store), envelope.clone(), 10);

        let result =
            <TestPolicy as Policy<MockEventStore>>::handle(&TestPolicy, &envelope, &ctx).await;
        assert!(result.is_ok());
    }

    crate::policy! {
        /// A documented policy for testing.
        DocPolicy {
            on FooCreatedEvent |_event, _ctx| {
                Ok(())
            },
        }
    }

    #[test]
    fn test_policy_macro_with_doc_comment() {
        let policy: &dyn Policy<MockEventStore> = &DocPolicy;
        assert_eq!(policy.name(), "DocPolicy");
    }

    crate::policy! {
        pub PubPolicy {
            on FooCreatedEvent |_event, _ctx| {
                Ok(())
            },
        }
    }

    #[test]
    fn test_policy_macro_with_visibility() {
        let policy: &dyn Policy<MockEventStore> = &PubPolicy;
        assert_eq!(policy.name(), "PubPolicy");
    }

    #[test]
    fn test_policy_macro_with_runner() {
        let store = Arc::new(MockEventStore::new());
        let cp = test_checkpoint_store();
        let runner = PolicyRunner::new(store, cp).register(Arc::new(TestPolicy));
        assert_eq!(runner.policies().len(), 1);
        assert_eq!(runner.policies()[0].name(), "TestPolicy");
    }

    #[tokio::test]
    async fn test_policy_macro_process_event_via_runner() {
        let store = Arc::new(MockEventStore::new());
        let cp = test_checkpoint_store();
        let runner = PolicyRunner::new(Arc::clone(&store), cp).register(Arc::new(TestPolicy));

        let mut envelope = test_envelope("Foo.Created", "Foo");
        envelope.event_data = serde_json::json!({"name": "via_runner"});

        let handled = runner.process_event(&envelope).await.unwrap();
        assert_eq!(handled, 1);
    }

    crate::policy! {
        FieldAccessPolicy {
            on FooCreatedEvent |event, _ctx| {
                assert_eq!(event.name, "expected_name");
                Ok(())
            },
        }
    }

    #[tokio::test]
    async fn test_policy_macro_handler_accesses_event_fields() {
        let store = Arc::new(MockEventStore::new());

        let mut envelope = test_envelope("Foo.Created", "Foo");
        envelope.event_data = serde_json::json!({"name": "expected_name"});

        let ctx = PolicyContext::new(Arc::clone(&store), envelope.clone(), 10);

        let result = <FieldAccessPolicy as Policy<MockEventStore>>::handle(
            &FieldAccessPolicy,
            &envelope,
            &ctx,
        )
        .await;
        assert!(result.is_ok());
    }

    crate::policy! {
        CtxAccessPolicy {
            on FooCreatedEvent |_event, ctx| {
                assert_eq!(ctx.cascade_depth(), 0);
                Ok(())
            },
        }
    }

    #[tokio::test]
    async fn test_policy_macro_handler_accesses_ctx() {
        let store = Arc::new(MockEventStore::new());

        let mut envelope = test_envelope("Foo.Created", "Foo");
        envelope.event_data = serde_json::json!({"name": "test"});

        let ctx = PolicyContext::new(Arc::clone(&store), envelope.clone(), 10);

        let result =
            <CtxAccessPolicy as Policy<MockEventStore>>::handle(&CtxAccessPolicy, &envelope, &ctx)
                .await;
        assert!(result.is_ok());
    }

    crate::policy! {
        AsyncPolicy {
            on FooCreatedEvent |_event, ctx| {
                let id = crate::EntityId::new();
                let mut agg = AggregateRoot::<SimpleTestEntity>::new(id);
                agg.apply(SimpleTestEvent::Created { value: 42 })
                    .map_err(|e| crate::Error::invalid_state(format!("{e:?}")))?;
                ctx.commit(&mut agg).await?;
                Ok(())
            },
        }
    }

    #[tokio::test]
    async fn test_policy_macro_async_handler() {
        let store = Arc::new(MockEventStore::new());

        let mut envelope = test_envelope("Foo.Created", "Foo");
        envelope.event_data = serde_json::json!({"name": "hello"});

        let ctx = PolicyContext::new(Arc::clone(&store), envelope.clone(), 10);

        let result =
            <AsyncPolicy as Policy<MockEventStore>>::handle(&AsyncPolicy, &envelope, &ctx).await;
        assert!(result.is_ok());
        ctx.flush().await.unwrap();

        let stored = store.get_events();
        assert_eq!(stored.len(), 1);

        let meta = stored[0].metadata.as_ref().expect("Should have metadata");
        assert_eq!(meta.causation_id, Some(envelope.id));
    }

    crate::policy! {
        RoutingPolicy {
            on FooCreatedEvent |_event, ctx| {
                let id = crate::EntityId::new();
                let mut agg = AggregateRoot::<SimpleTestEntity>::new(id);
                agg.apply(SimpleTestEvent::Created { value: 99 })
                    .map_err(|e| crate::Error::invalid_state(format!("{e:?}")))?;
                ctx.commit(&mut agg).await?;
                Ok(())
            },
            on FooUpdatedEvent |event, ctx| {
                let id = crate::EntityId::new();
                let mut agg = AggregateRoot::<SimpleTestEntity>::new(id);
                let val = event.value * 10;
                agg.apply(SimpleTestEvent::Created { value: val })
                    .map_err(|e| crate::Error::invalid_state(format!("{e:?}")))?;
                ctx.commit(&mut agg).await?;
                Ok(())
            },
        }
    }

    #[tokio::test]
    async fn test_policy_macro_routes_to_correct_async_handler() {
        let store = Arc::new(MockEventStore::new());

        let mut envelope = test_envelope("Foo.Updated", "Foo");
        envelope.event_data = serde_json::json!({"value": 7});

        let ctx = PolicyContext::new(Arc::clone(&store), envelope.clone(), 10);

        let result =
            <RoutingPolicy as Policy<MockEventStore>>::handle(&RoutingPolicy, &envelope, &ctx)
                .await;
        assert!(result.is_ok());
        ctx.flush().await.unwrap();

        let stored = store.get_events();
        assert_eq!(stored.len(), 1);

        let data: serde_json::Value = stored[0].event_data.clone();
        assert_eq!(data["Created"]["value"], 70);
    }
}
