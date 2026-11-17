//! A user aggregate and its stored form, for the unit tests of the caches.

use event_sauce_core::{
    define_events, Aggregate, AggregateError, AggregateRoot, AggregateVersion, Entity, EntityId,
    StoredVersion,
};
use serde::{Deserialize, Serialize};

use crate::test_support::SecondFactorCredentials;
use crate::{Identity, UserAggregate};

/// A user aggregate with just enough state to tell two versions apart.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CacheUser {
    pub id: EntityId,
    pub identity: Identity<SecondFactorCredentials>,
    pub name: String,
}

impl Entity for CacheUser {
    fn entity_id(&self) -> EntityId {
        self.id
    }
}

#[derive(Debug, thiserror::Error)]
#[error("cache user error")]
pub struct CacheUserError;

impl AggregateError for CacheUserError {}

impl Aggregate for CacheUser {
    type Event = CacheUserEvent;
    type Error = CacheUserError;
    type DeletedState = Self;
}

impl UserAggregate for CacheUser {
    type Credentials = SecondFactorCredentials;

    fn identity(&self) -> &Identity<SecondFactorCredentials> {
        &self.identity
    }
}

define_events! {
    pub enum CacheUserEvent for CacheUser {
        Renamed {
            name: String,
        } => |user, event| {
            user.name.clone_from(&event.name);
        },
    }
}

/// A stored user at `version`, as a repository would load it.
pub(crate) fn cache_user_at(id: EntityId, name: &str, version: i64) -> AggregateRoot<CacheUser> {
    let version = StoredVersion::try_from(AggregateVersion::new(version))
        .expect("a stored version is positive");
    AggregateRoot::restore(
        version,
        CacheUser {
            id,
            identity: Identity::Anonymous,
            name: name.to_owned(),
        },
    )
    .expect("a user restores from itself")
}
