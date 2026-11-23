//! [`Access`] and [`Authorized`]: the aggregate a path names, which the
//! current user may reach.

use std::marker::PhantomData;
use std::ops::Deref;
use std::sync::Arc;

use axum::extract::{FromRef, FromRequestParts};
use axum::response::{IntoResponse, Response};
use event_sauce_auth::UserAggregate;
use event_sauce_core::{Aggregate, AggregateRoot, EventStore};
use http::request::Parts;
use http::StatusCode;

use crate::found::load_path;
use crate::login::auth_session::session_user;
use crate::{AuthRejection, DeletedStatus, Found, FoundRejection, PathAggregate, Store};

#[cfg(test)]
mod tests;

/// How a user who may not reach an aggregate is answered.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum DeniedStatus {
    /// `404 Not Found`, so a caller cannot tell an aggregate it may not
    /// reach from one that does not exist.
    #[default]
    NotFound,
    /// `403 Forbidden`.
    Forbidden,
}

/// Who may reach an aggregate of type `A`.
///
/// A policy is a marker type, never built: the check is the associated
/// function [`allows`](Self::allows), a function of the aggregate and the
/// logged-in user only. A check that needs a query or a second aggregate is
/// the application's own extractor, built from [`Found`] and
/// [`CurrentUser`](crate::CurrentUser).
pub trait Access<A: Aggregate> {
    /// The user aggregate the policy is checked for.
    type User: UserAggregate;

    /// How a user who does not pass is answered, `404` by default.
    const DENIED: DeniedStatus = DeniedStatus::NotFound;

    /// Whether an anonymous user is refused with `403` before
    /// [`allows`](Self::allows) runs, `false` by default.
    const REGISTERED: bool = false;

    /// Whether `user` may reach `aggregate`.
    fn allows(aggregate: &AggregateRoot<A>, user: &AggregateRoot<Self::User>) -> bool;
}

/// Why [`Authorized`] refused the request.
#[derive(Debug, thiserror::Error)]
pub enum AccessRejection {
    /// Nobody is logged in, or the user is anonymous where the policy wants
    /// a registered one.
    #[error(transparent)]
    Auth(#[from] AuthRejection),
    /// The aggregate could not be found.
    #[error(transparent)]
    Found(#[from] FoundRejection),
    /// The user may not reach the aggregate.
    #[error("access denied")]
    Denied(DeniedStatus),
}

impl AccessRejection {
    /// The status the rejection is answered with.
    #[must_use]
    pub fn status(&self) -> StatusCode {
        match self {
            Self::Auth(auth) => auth.status(),
            Self::Found(found) => found.status(),
            Self::Denied(DeniedStatus::NotFound) => StatusCode::NOT_FOUND,
            Self::Denied(DeniedStatus::Forbidden) => StatusCode::FORBIDDEN,
        }
    }
}

impl IntoResponse for AccessRejection {
    fn into_response(self) -> Response {
        match self {
            Self::Auth(auth) => auth.into_response(),
            Self::Found(found) => found.into_response(),
            Self::Denied(DeniedStatus::NotFound) => (self.status(), "not found").into_response(),
            Self::Denied(DeniedStatus::Forbidden) => (self.status(), "forbidden").into_response(),
        }
    }
}

fn admit<A, P>(
    user: Option<Arc<AggregateRoot<P::User>>>,
) -> Result<Arc<AggregateRoot<P::User>>, AccessRejection>
where
    A: Aggregate,
    P: Access<A>,
{
    let user = user.ok_or(AuthRejection::Unauthenticated)?;
    if P::REGISTERED && user.identity().credentials().is_none() {
        return Err(AuthRejection::Forbidden.into());
    }
    Ok(user)
}

fn deleted_status<A, P>(of_type: DeletedStatus) -> DeletedStatus
where
    A: Aggregate,
    P: Access<A>,
{
    match P::DENIED {
        DeniedStatus::NotFound => DeletedStatus::NotFound,
        DeniedStatus::Forbidden => of_type,
    }
}

fn permit<A, P>(
    aggregate: &AggregateRoot<A>,
    user: &AggregateRoot<P::User>,
) -> Result<(), AccessRejection>
where
    A: Aggregate,
    P: Access<A>,
{
    if P::allows(aggregate, user) {
        Ok(())
    } else {
        Err(AccessRejection::Denied(P::DENIED))
    }
}

/// The aggregate the path names, which the current user may reach under the
/// policy `P`.
///
/// It checks, in this order, and answers the first failure:
///
/// | Check | Failure | Status |
/// |---|---|---|
/// | somebody is logged in | nobody | `401` |
/// | [`Access::REGISTERED`] and the user is registered | anonymous | `403` |
/// | the path parameter of [`PathAggregate::PARAM`] | missing, not an id | `500`, `400` |
/// | the aggregate loads | missing, deleted | `404`, or [`PathAggregate::DELETED`] for a deleted one when the policy denies with `403` |
/// | [`Access::allows`] | no | [`Access::DENIED`]: `404` or `403` |
///
/// Nothing is loaded for a request without a user, so ids cannot be probed
/// anonymously. A policy that hides existence also hides deletions: a
/// deleted aggregate is `404` whatever [`PathAggregate::DELETED`] says, as
/// the policy cannot be shown it.
///
/// The handler cannot run unless the user, the load and the check passed,
/// so there is no guard layer to order. Like [`Found`], it is read only and
/// shares the aggregate loaded for the request; writes go through
/// [`Repo`](crate::Repo).
///
/// ```
/// use axum::extract::FromRef;
/// use event_sauce_axum::{Access, Authorized, PathAggregate, Store};
/// use event_sauce_memory::InMemoryEventStore;
///
/// #[derive(Clone, FromRef)]
/// struct AppState {
///     store: Store<InMemoryEventStore>,
/// }
///
/// async fn show<A, P>(aggregate: Authorized<A, P, InMemoryEventStore>) -> String
/// where
///     A: PathAggregate + serde::Serialize + serde::de::DeserializeOwned,
///     A::DeletedState: serde::Serialize + serde::de::DeserializeOwned,
///     A::Event: serde::Serialize + serde::de::DeserializeOwned,
///     P: Access<A>,
/// {
///     aggregate.entity_id().to_string()
/// }
/// ```
pub struct Authorized<A: Aggregate, P: Access<A>, S> {
    found: Found<A, S>,
    user: Arc<AggregateRoot<P::User>>,
    policy: PhantomData<fn() -> P>,
}

impl<A: Aggregate, P: Access<A>, S> Authorized<A, P, S> {
    /// The user the check passed for.
    #[must_use]
    pub fn user(&self) -> &Arc<AggregateRoot<P::User>> {
        &self.user
    }

    /// The aggregate, as [`Found`] would give it.
    #[must_use]
    pub fn found(&self) -> &Found<A, S> {
        &self.found
    }
}

impl<A: Aggregate, P: Access<A>, S> Deref for Authorized<A, P, S> {
    type Target = AggregateRoot<A>;

    fn deref(&self) -> &Self::Target {
        &self.found
    }
}

impl<St, A, P, S> FromRequestParts<St> for Authorized<A, P, S>
where
    St: Send + Sync,
    Store<S>: FromRef<St>,
    S: EventStore + 'static,
    A: PathAggregate + serde::Serialize + serde::de::DeserializeOwned,
    A::DeletedState: serde::Serialize + serde::de::DeserializeOwned,
    A::Event: serde::Serialize + serde::de::DeserializeOwned,
    P: Access<A>,
{
    type Rejection = AccessRejection;

    async fn from_request_parts(parts: &mut Parts, state: &St) -> Result<Self, AccessRejection> {
        let user = admit::<A, P>(session_user::<P::User>(parts)?)?;
        let root = load_path::<St, A, S>(parts, state, deleted_status::<A, P>(A::DELETED)).await?;
        permit::<A, P>(&root, &user)?;
        Ok(Self {
            found: Found::new(root),
            user,
            policy: PhantomData,
        })
    }
}
