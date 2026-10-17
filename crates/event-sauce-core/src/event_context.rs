//! Ambient event context: values that follow the current task or thread and
//! are read when events are applied.
//!
//! An [`EventContext`] works like a `tracing` span: [`EventContext::in_scope`]
//! (or the [`EnteredEventContext`] guard) makes it the active context for the
//! current thread, and contexts nest. Marker types [`Root`] and [`Nested`]
//! decide what a context may carry: only a [`Root`] context can set a tenant,
//! and a [`Root`] context inherits nothing from the contexts around it.
//!
//! # Examples
//!
//! ```
//! use event_sauce_core::EventContext;
//!
//! EventContext::root().tenant("acme").correlation_id("req-1").in_scope(|| {
//!     EventContext::new().with("region", "eu").in_scope(|| {
//!         // events applied here see tenant, correlation id and region
//!     });
//! });
//! ```

mod future;
mod scopes;
mod snapshot;
mod stack;
mod value;

use stack::{Frame, StackFrame};
use std::sync::Arc;

pub use future::{WithEventContext, WithEventContextExt};
pub(crate) use scopes::execution_context;
#[cfg(feature = "event-sourcing")]
pub(crate) use scopes::reaction_context;
#[cfg(test)]
pub(crate) use snapshot::ContextSnapshot;
pub(crate) use snapshot::{current_snapshot, has_correlation_id};
pub use stack::EnteredEventContext;
pub use value::{display, DisplayValue};

/// Kind of an [`EventContext`]: [`Root`] or [`Nested`]. Sealed.
pub trait Scope: sealed::Sealed {}

mod sealed {
    #[derive(Debug, Clone)]
    pub enum Boundary {
        Root { tenant: Option<String> },
        Nested,
    }

    pub trait Sealed {
        fn into_boundary(self) -> Boundary;
    }
}

use sealed::Boundary;

/// Scope of an [`EventContext`] that starts a fresh context: it inherits
/// nothing from the contexts around it and may carry a tenant.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Root {
    tenant: Option<String>,
}

/// Scope of an [`EventContext`] layered on top of the active context.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Nested;

impl Scope for Root {}
impl Scope for Nested {}

impl sealed::Sealed for Root {
    fn into_boundary(self) -> Boundary {
        Boundary::Root {
            tenant: self.tenant,
        }
    }
}

impl sealed::Sealed for Nested {
    fn into_boundary(self) -> Boundary {
        Boundary::Nested
    }
}

/// A set of ambient values attached to the events applied while it is active.
///
/// Build one with [`EventContext::new`] (layered on the active context) or
/// [`EventContext::root`] (a fresh start that may carry a tenant), then
/// activate it with [`in_scope`](Self::in_scope),
/// [`entered`](Self::entered), or, across `.await` points,
/// [`with_event_context`](WithEventContextExt::with_event_context).
///
/// Resolution walks the active contexts from the outermost to the innermost,
/// starting at the last [`Root`] one:
///
/// - the correlation id is the first one set, so the id a request arrived
///   with is never overwritten by deeper layers,
/// - the tenant and the command are the innermost ones set,
/// - additional values merge key by key, the innermost winning.
///
/// Spawned tasks do not inherit the context of the task that spawned them:
/// carry it over with [`EventContext::current`].
///
/// ```
/// use event_sauce_core::{EventContext, WithEventContextExt};
///
/// # #[tokio::main(flavor = "current_thread")]
/// # async fn main() {
/// let work = async { /* applies events */ };
/// tokio::spawn(work.with_event_context(EventContext::current()))
///     .await
///     .unwrap();
/// # }
/// ```
///
/// A tenant can only be set on a [`Root`] context:
///
/// ```compile_fail
/// use event_sauce_core::EventContext;
///
/// let _ = EventContext::new().tenant("acme");
/// ```
#[derive(Debug, Clone)]
pub struct EventContext<K: Scope = Nested> {
    frame: Frame,
    scope: K,
}

impl<K: Scope> EventContext<K> {
    fn from_parts(frame: Frame, scope: K) -> Self {
        Self { frame, scope }
    }

    pub(crate) fn into_frame(self) -> Arc<StackFrame> {
        Arc::new(StackFrame {
            boundary: sealed::Sealed::into_boundary(self.scope),
            frame: self.frame,
        })
    }

    /// Sets the correlation id.
    ///
    /// # Examples
    ///
    /// ```
    /// use event_sauce_core::EventContext;
    ///
    /// let ctx = EventContext::new().correlation_id("req-1");
    /// ctx.in_scope(|| ());
    /// ```
    #[must_use]
    pub fn correlation_id(mut self, id: impl Into<String>) -> Self {
        self.frame.correlation_id = Some(id.into());
        self
    }

    /// Adds an additional value under `key`, replacing one set on this context.
    ///
    /// # Examples
    ///
    /// ```
    /// use event_sauce_core::EventContext;
    ///
    /// let ctx = EventContext::new().with("region", "eu").with("attempt", 2_u32);
    /// ctx.in_scope(|| ());
    /// ```
    #[must_use]
    pub fn with(mut self, key: impl Into<String>, value: impl Into<serde_json::Value>) -> Self {
        self.frame.additional.insert(key.into(), value.into());
        self
    }

    #[must_use]
    pub(crate) fn command(mut self, name: impl Into<String>) -> Self {
        self.frame.command = Some(name.into());
        self
    }

    /// Makes this context the active one until the returned guard is dropped.
    ///
    /// Like `tracing::Span::enter`, the guard is bound to the current thread
    /// (it is not `Send`) and must not be held across an `.await`: the task
    /// may resume on another thread, leaving this one with a stale context.
    /// Use [`with_event_context`](WithEventContextExt::with_event_context)
    /// for async code. Leaking the guard with `std::mem::forget` leaves the
    /// context active on this thread for good.
    ///
    /// ```
    /// use event_sauce_core::EventContext;
    ///
    /// let guard = EventContext::new().with("k", 1).entered();
    /// drop(guard);
    /// ```
    ///
    /// The guard cannot cross threads:
    ///
    /// ```compile_fail
    /// use event_sauce_core::EventContext;
    ///
    /// let guard = EventContext::new().entered();
    /// std::thread::spawn(move || drop(guard));
    /// ```
    #[must_use = "the context is only active while the guard is alive"]
    pub fn entered(self) -> EnteredEventContext {
        EnteredEventContext::enter(self.into_frame())
    }

    /// Runs `f` with this context active, restoring the previous one
    /// afterwards, also when `f` panics.
    ///
    /// # Examples
    ///
    /// ```
    /// use event_sauce_core::EventContext;
    ///
    /// let answer = EventContext::new().with("k", 1).in_scope(|| 42);
    /// assert_eq!(answer, 42);
    /// ```
    pub fn in_scope<R>(self, f: impl FnOnce() -> R) -> R {
        let _guard = self.entered();
        f()
    }
}

impl EventContext<Nested> {
    /// Creates an empty context layered on the active one.
    ///
    /// # Examples
    ///
    /// ```
    /// use event_sauce_core::EventContext;
    ///
    /// EventContext::new().with("k", 1).in_scope(|| ());
    /// ```
    #[must_use]
    pub fn new() -> Self {
        Self::from_parts(Frame::default(), Nested)
    }
}

impl Default for EventContext<Nested> {
    fn default() -> Self {
        Self::new()
    }
}

impl EventContext<Root> {
    /// Creates an empty context that starts afresh: while it is active, the
    /// correlation id, tenant, command and additional values of every context
    /// outside it are ignored.
    ///
    /// # Examples
    ///
    /// ```
    /// use event_sauce_core::EventContext;
    ///
    /// EventContext::root().tenant("acme").in_scope(|| ());
    /// ```
    #[must_use]
    pub fn root() -> Self {
        Self::from_parts(Frame::default(), Root::default())
    }

    /// Sets the tenant. Only a root context can carry one.
    ///
    /// # Examples
    ///
    /// ```
    /// use event_sauce_core::EventContext;
    ///
    /// EventContext::root().tenant("acme").in_scope(|| ());
    /// ```
    #[must_use]
    pub fn tenant(mut self, tenant: impl Into<String>) -> Self {
        self.scope.tenant = Some(tenant.into());
        self
    }

    /// Snapshots the effective context of the current thread as a root
    /// context, to hand to a spawned task (see
    /// [`with_event_context`](WithEventContextExt::with_event_context)).
    ///
    /// The correlation path is carried over whole, so contexts nested in the
    /// snapshot extend it.
    ///
    /// # Examples
    ///
    /// ```
    /// use event_sauce_core::EventContext;
    ///
    /// let snapshot = EventContext::new().correlation_id("req-1").in_scope(EventContext::current);
    /// snapshot.in_scope(|| ());
    /// ```
    #[must_use]
    pub fn current() -> Self {
        let snapshot = current_snapshot().unwrap_or_default();
        let tenant = snapshot.tenant.clone();
        Self::from_parts(snapshot.into(), Root { tenant })
    }
}

#[cfg(test)]
mod tests;
