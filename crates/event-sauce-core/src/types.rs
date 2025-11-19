//! Common type aliases used throughout the event-sauce-core library.
//!
//! This module provides convenient type aliases for commonly used complex types,
//! improving readability and maintainability of the codebase.

use std::sync::Arc;

use crate::CheckpointStore;

/// Arc-wrapped checkpoint store for shared ownership across threads.
///
/// This type alias is commonly used when sharing a checkpoint store
/// between subscriptions and other components.
///
/// # Examples
///
/// ```ignore
/// use event_sauce_core::CheckpointStoreRef;
/// use std::sync::Arc;
///
/// let store: CheckpointStoreRef = Arc::new(my_checkpoint_store);
/// ```
pub type CheckpointStoreRef = Arc<dyn CheckpointStore>;

/// Arc-wrapped event store for shared ownership across threads.
///
/// This generic type alias wraps any event store implementation in an `Arc`,
/// allowing safe sharing between threads and tasks.
///
/// # Examples
///
/// ```ignore
/// use event_sauce_core::EventStoreRef;
/// use std::sync::Arc;
///
/// let store: EventStoreRef<PostgresEventStore> = Arc::new(postgres_store);
/// ```
pub type EventStoreRef<S> = Arc<S>;
