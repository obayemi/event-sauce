//! Common identifier and alias types shared by both persistence styles.
//!
//! This module holds the types every store implementation speaks —
//! [`StreamId`], [`Position`] — plus convenient type aliases for commonly
//! used shared trait objects.

#[cfg(feature = "event-sourcing")]
use std::sync::Arc;

use uuid::Uuid;

use crate::AggregateType;

#[cfg(feature = "event-sourcing")]
use crate::{CheckpointStore, CryptoKeyStore, CryptoProvider};

/// Stream ID uniquely identifying an aggregate's persistence stream.
///
/// Combines aggregate type and aggregate ID to create a unique identifier —
/// the event stream key in event-sourced mode, the state row key in
/// state-stored mode.
///
/// # Examples
///
/// ```
/// use event_sauce_core::StreamId;
/// use uuid::Uuid;
///
/// let stream_id = StreamId::new("User", Uuid::new_v4());
/// assert_eq!(stream_id.aggregate_type(), "User");
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct StreamId {
    aggregate_type: AggregateType,
    aggregate_id: Uuid,
}

impl StreamId {
    /// Creates a new stream ID.
    ///
    /// # Examples
    ///
    /// ```
    /// use event_sauce_core::StreamId;
    /// use uuid::Uuid;
    ///
    /// let stream_id = StreamId::new("Order", Uuid::new_v4());
    /// ```
    #[must_use]
    pub fn new(aggregate_type: impl Into<AggregateType>, aggregate_id: Uuid) -> Self {
        Self {
            aggregate_type: aggregate_type.into(),
            aggregate_id,
        }
    }

    /// Returns the aggregate type.
    #[must_use]
    pub fn aggregate_type(&self) -> &AggregateType {
        &self.aggregate_type
    }

    /// Returns the aggregate ID.
    #[must_use]
    pub fn aggregate_id(&self) -> Uuid {
        self.aggregate_id
    }
}

impl std::fmt::Display for StreamId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}:{}", self.aggregate_type, self.aggregate_id)
    }
}

/// An opaque, store-issued, strictly-monotonic position in the global event log.
///
/// A position is a token the store assigns to each appended event in insertion
/// order — for the `PostgreSQL` backend it is the `BIGSERIAL` `events.id`. Treat
/// it as opaque: do not assume positions are dense (gaps can appear when an
/// append is rolled back, e.g. a unique-violation conflict), and do not
/// reconstruct it by counting events. To resume processing, checkpoint the
/// [`Position`] of the last entry you handled, then call `stream_all` on the
/// event store with it; the store yields only entries whose position is
/// strictly greater.
///
/// [`Position::start`] is the position before any event — passing it streams the
/// whole log.
///
/// # Examples
///
/// ```
/// use event_sauce_core::Position;
///
/// let pos = Position::from(100);
/// assert_eq!(pos.as_i64(), 100);
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Position(i64);

impl Position {
    /// Creates a new position.
    #[must_use]
    pub const fn new(value: i64) -> Self {
        Self(value)
    }

    /// Returns the position as an i64.
    #[must_use]
    pub const fn as_i64(self) -> i64 {
        self.0
    }

    /// Returns the start position (0).
    #[must_use]
    pub const fn start() -> Self {
        Self(0)
    }
}

impl From<i64> for Position {
    fn from(value: i64) -> Self {
        Self(value)
    }
}

impl From<Position> for i64 {
    fn from(pos: Position) -> Self {
        pos.0
    }
}

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
#[cfg(feature = "event-sourcing")]
pub type CheckpointStoreRef = Arc<dyn CheckpointStore>;

/// Arc-wrapped crypto key store for shared ownership across threads.
#[cfg(feature = "event-sourcing")]
pub type CryptoKeyStoreRef = Arc<dyn CryptoKeyStore>;

/// Arc-wrapped crypto provider for shared ownership across threads.
#[cfg(feature = "event-sourcing")]
pub type CryptoProviderRef = Arc<dyn CryptoProvider>;
