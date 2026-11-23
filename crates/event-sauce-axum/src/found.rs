//! [`Found`]: the aggregate a request's path names, loaded once and read-only.

use std::future::Future;
use std::marker::PhantomData;
use std::ops::Deref;
use std::sync::Arc;

use axum::extract::rejection::RawPathParamsRejection;
use axum::extract::{FromRef, FromRequestParts, RawPathParams};
use axum::response::{IntoResponse, Response};
use event_sauce_core::{Aggregate, AggregateRoot, EntityId, Error, EventStore, Repository};
use http::request::Parts;
use http::{Extensions, StatusCode};
use uuid::Uuid;

use crate::{diagnostics, DeletedStatus, Store, StoreError};

#[cfg(test)]
mod tests;

/// An aggregate that a path parameter names.
///
/// Implement it on the aggregate to say which parameter of the route holds
/// its id, and how a deleted one is answered. The defaults suit a flat
/// `/notes/{id}` route, where an empty `impl PathAggregate for Note {}` is
/// enough; a nested `/groups/{group_id}/activities/{activity_id}` route sets
/// `PARAM` on each aggregate:
///
/// ```
/// use event_sauce_axum::{DeletedStatus, PathAggregate};
/// use event_sauce_core::{define_events, Aggregate, AggregateError, Entity, EntityId};
/// use serde::{Deserialize, Serialize};
///
/// # #[derive(Debug, thiserror::Error)]
/// # #[error("group error")]
/// # struct GroupError;
/// # impl AggregateError for GroupError {}
/// #[derive(Debug, Serialize, Deserialize)]
/// struct Group {
///     id: EntityId,
/// }
/// # impl Entity for Group { fn entity_id(&self) -> EntityId { self.id } }
/// # impl Aggregate for Group {
/// #     type Event = GroupEvent;
/// #     type Error = GroupError;
/// #     type DeletedState = Self;
/// # }
/// # define_events! {
/// #     enum GroupEvent for Group {
/// #         Founded {} @init => |id, _event| Group { id },
/// #         Renamed {} => |_group, _event| {},
/// #     }
/// # }
///
/// impl PathAggregate for Group {
///     const PARAM: &'static str = "group_id";
///     const DELETED: DeletedStatus = DeletedStatus::Gone;
/// }
///
/// assert_eq!(Group::PARAM, "group_id");
/// assert_eq!(Group::DELETED, DeletedStatus::Gone);
/// ```
pub trait PathAggregate:
    Aggregate<DeletedState: serde::Serialize + serde::de::DeserializeOwned> + 'static
{
    /// The path parameter that holds the id, `"id"` by default.
    const PARAM: &'static str = "id";
    /// How a deleted aggregate is answered, `404` by default.
    const DELETED: DeletedStatus = DeletedStatus::NotFound;
}

/// Why [`Found`] refused the request.
#[derive(Debug, thiserror::Error)]
pub enum FoundRejection {
    /// The route has no such path parameter, a wiring mistake: answered
    /// with `500 Internal Server Error`.
    #[error("the route has no path parameter `{0}`")]
    MissingParam(&'static str),
    /// The parameter is not an aggregate id: answered with `400 Bad Request`.
    #[error("the path parameter is not an aggregate id")]
    BadId,
    /// The store could not give the aggregate, answered as [`StoreError`]
    /// does.
    #[error(transparent)]
    Store(#[from] StoreError),
}

impl FoundRejection {
    /// The status the rejection is answered with.
    #[must_use]
    pub fn status(&self) -> StatusCode {
        match self {
            Self::MissingParam(_) => StatusCode::INTERNAL_SERVER_ERROR,
            Self::BadId => StatusCode::BAD_REQUEST,
            Self::Store(store) => store.status(),
        }
    }
}

impl IntoResponse for FoundRejection {
    fn into_response(self) -> Response {
        match self {
            Self::MissingParam(_) => diagnostics::internal_error(&self),
            Self::BadId => (self.status(), "bad id").into_response(),
            Self::Store(store) => store.into_response(),
        }
    }
}

/// The aggregate the path of the request names, loaded once for the request.
///
/// The id is read from the path parameter [`PathAggregate::PARAM`] and the
/// aggregate is loaded from the [`Store<S>`] of the application state. It
/// derefs to its [`AggregateRoot`] and is read only: there is no way to
/// commit through it, writes go through [`Repo`](crate::Repo), which reloads
/// and retries on conflicts.
///
/// The loaded aggregate is kept in the request, so any number of extractors
/// that ask for the same aggregate in the same store load it once.
///
/// | Failure | Status |
/// |---|---|
/// | the parameter is not an id | `400` |
/// | the route has no such parameter | `500` |
/// | missing | `404` |
/// | deleted | `404`, or what [`PathAggregate::DELETED`] says |
/// | the store failed | `500` |
///
/// ```
/// use axum::extract::FromRef;
/// use event_sauce_axum::{Found, PathAggregate, Store};
/// use event_sauce_core::Aggregate;
/// use event_sauce_memory::InMemoryEventStore;
///
/// #[derive(Clone, FromRef)]
/// struct AppState {
///     store: Store<InMemoryEventStore>,
/// }
///
/// async fn show<A>(aggregate: Found<A, InMemoryEventStore>) -> String
/// where
///     A: PathAggregate,
/// {
///     aggregate.entity_id().to_string()
/// }
/// ```
pub struct Found<A: Aggregate, S> {
    root: Arc<AggregateRoot<A>>,
    store: PhantomData<fn() -> S>,
}

impl<A: Aggregate, S> Found<A, S> {
    pub(crate) fn new(root: Arc<AggregateRoot<A>>) -> Self {
        Self {
            root,
            store: PhantomData,
        }
    }

    /// The shared aggregate, for APIs that take an [`Arc`].
    #[must_use]
    pub fn arc(&self) -> &Arc<AggregateRoot<A>> {
        &self.root
    }

    /// Takes the shared aggregate out.
    #[must_use]
    pub fn into_arc(self) -> Arc<AggregateRoot<A>> {
        self.root
    }
}

impl<A: Aggregate, S> Deref for Found<A, S> {
    type Target = AggregateRoot<A>;

    fn deref(&self) -> &Self::Target {
        &self.root
    }
}

struct Cached<A: Aggregate, S> {
    id: EntityId,
    root: Arc<AggregateRoot<A>>,
    store: PhantomData<fn() -> S>,
}

impl<A: Aggregate, S> Clone for Cached<A, S> {
    fn clone(&self) -> Self {
        Self {
            id: self.id,
            root: Arc::clone(&self.root),
            store: PhantomData,
        }
    }
}

fn path_id<'a>(
    params: impl IntoIterator<Item = (&'a str, &'a str)>,
    name: &'static str,
) -> Result<EntityId, FoundRejection> {
    let (_, value) = params
        .into_iter()
        .find(|(key, _)| *key == name)
        .ok_or(FoundRejection::MissingParam(name))?;
    Uuid::parse_str(value)
        .map(EntityId::from)
        .map_err(|_| FoundRejection::BadId)
}

async fn load_once<A, S, F, Fut>(
    extensions: &mut Extensions,
    id: EntityId,
    load: F,
) -> Result<Arc<AggregateRoot<A>>, Error>
where
    A: Aggregate + 'static,
    S: 'static,
    F: FnOnce() -> Fut,
    Fut: Future<Output = Result<AggregateRoot<A>, Error>>,
{
    if let Some(cached) = extensions
        .get::<Cached<A, S>>()
        .filter(|cached| cached.id == id)
    {
        return Ok(Arc::clone(&cached.root));
    }
    let root = Arc::new(load().await?);
    extensions.insert(Cached::<A, S> {
        id,
        root: Arc::clone(&root),
        store: PhantomData,
    });
    Ok(root)
}

pub(crate) async fn load_path<St, A, S>(
    parts: &mut Parts,
    state: &St,
    deleted: DeletedStatus,
) -> Result<Arc<AggregateRoot<A>>, FoundRejection>
where
    St: Send + Sync,
    Store<S>: FromRef<St>,
    S: EventStore + 'static,
    A: PathAggregate,
{
    let params = RawPathParams::from_request_parts(parts, state)
        .await
        .map_err(|rejection| match rejection {
            RawPathParamsRejection::InvalidUtf8InPathParam(_) => FoundRejection::BadId,
            _ => FoundRejection::MissingParam(A::PARAM),
        })?;
    let id = path_id(&params, A::PARAM)?;
    let repository = Store::<S>::from_ref(state).arc().repository::<A>();
    load_once::<A, S, _, _>(&mut parts.extensions, id, || repository.load(id))
        .await
        .map_err(|error| StoreError::from(error).deleted_as(deleted).into())
}

impl<St, A, S> FromRequestParts<St> for Found<A, S>
where
    St: Send + Sync,
    Store<S>: FromRef<St>,
    S: EventStore + 'static,
    A: PathAggregate,
{
    type Rejection = FoundRejection;

    async fn from_request_parts(parts: &mut Parts, state: &St) -> Result<Self, FoundRejection> {
        load_path::<St, A, S>(parts, state, A::DELETED)
            .await
            .map(Self::new)
    }
}
