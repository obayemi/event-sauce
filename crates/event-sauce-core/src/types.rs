//! Common type aliases used throughout the event-sauce-core library.
//!
//! This module provides convenient type aliases for commonly used complex types,
//! improving readability and maintainability of the codebase.

use std::sync::Arc;

use crate::{CheckpointStore, CryptoKeyStore, CryptoProvider};

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

/// Arc-wrapped crypto key store for shared ownership across threads.
pub type CryptoKeyStoreRef = Arc<dyn CryptoKeyStore>;

/// Arc-wrapped crypto provider for shared ownership across threads.
pub type CryptoProviderRef = Arc<dyn CryptoProvider>;
