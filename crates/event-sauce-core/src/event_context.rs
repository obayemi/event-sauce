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
mod value;

use serde_json::{Map, Value};
use std::cell::RefCell;
use std::marker::PhantomData;
use std::sync::Arc;

pub use future::{WithEventContext, WithEventContextExt};
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

#[derive(Debug)]
pub(crate) struct StackFrame {
    boundary: Boundary,
    frame: Frame,
}

#[derive(Debug, Clone, Default)]
struct Frame {
    correlation_id: Option<String>,
    correlation_path: Vec<String>,
    command: Option<String>,
    additional: Map<String, Value>,
}

thread_local! {
    static STACK: RefCell<Vec<Arc<StackFrame>>> = const { RefCell::new(Vec::new()) };
}

fn push(frame: Arc<StackFrame>) -> usize {
    STACK.with(|stack| {
        let mut stack = stack.borrow_mut();
        let depth = stack.len();
        stack.push(frame);
        depth
    })
}

fn truncate(depth: usize) {
    let _ = STACK.try_with(|stack| stack.borrow_mut().truncate(depth));
}

fn effective<R>(read: impl FnOnce(&[Arc<StackFrame>]) -> R) -> R {
    STACK.with(|stack| {
        let stack = stack.borrow();
        let start = stack
            .iter()
            .rposition(|f| matches!(f.boundary, Boundary::Root { .. }))
            .unwrap_or(0);
        read(&stack[start..])
    })
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

impl<K: Scope> EventContext<K> {
    pub(crate) fn into_frame(self) -> Arc<StackFrame> {
        Arc::new(StackFrame {
            boundary: sealed::Sealed::into_boundary(self.scope),
            frame: self.frame,
        })
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
        let frame = Frame {
            correlation_id: None,
            correlation_path: snapshot.correlation_path,
            command: snapshot.command,
            additional: match snapshot.additional {
                Some(Value::Object(map)) => map,
                _ => Map::new(),
            },
        };
        Self::from_parts(
            frame,
            Root {
                tenant: snapshot.tenant,
            },
        )
    }
}

impl EventContext<Root> {
    #[cfg(feature = "event-sourcing")]
    pub(crate) fn from_metadata(metadata: &crate::EventMetadata) -> Self {
        let correlation_path = if metadata.correlation_path.is_empty() {
            metadata.correlation_id.iter().cloned().collect()
        } else {
            metadata.correlation_path.clone()
        };
        let frame = Frame {
            correlation_path,
            ..Frame::default()
        };
        Self::from_parts(
            frame,
            Root {
                tenant: metadata.tenant.clone(),
            },
        )
    }
}

/// Guard returned by [`EventContext::entered`]: the context stays active until
/// it is dropped.
///
/// It is not `Send`: it must be dropped on the thread that created it.
#[derive(Debug)]
pub struct EnteredEventContext {
    depth: usize,
    not_send: PhantomData<*const ()>,
}

impl EnteredEventContext {
    fn enter(frame: Arc<StackFrame>) -> Self {
        Self {
            depth: push(frame),
            not_send: PhantomData,
        }
    }
}

impl Drop for EnteredEventContext {
    fn drop(&mut self) {
        truncate(self.depth);
    }
}

#[derive(Debug, Clone, Default, PartialEq)]
pub(crate) struct ContextSnapshot {
    pub tenant: Option<String>,
    pub correlation_id: Option<String>,
    pub correlation_path: Vec<String>,
    pub command: Option<String>,
    pub additional: Option<Value>,
}

impl From<ContextSnapshot> for crate::EventMetadata {
    fn from(snapshot: ContextSnapshot) -> Self {
        Self {
            correlation_id: snapshot.correlation_id,
            tenant: snapshot.tenant,
            command: snapshot.command,
            correlation_path: snapshot.correlation_path,
            additional: snapshot.additional,
            ..Self::new()
        }
    }
}

pub(crate) fn current_snapshot() -> Option<ContextSnapshot> {
    let snapshot = effective(|frames| {
        let mut correlation_path: Vec<String> = Vec::new();
        for id in frames
            .iter()
            .map(|f| &f.frame)
            .flat_map(|f| f.correlation_path.iter().chain(f.correlation_id.iter()))
        {
            if correlation_path.last() != Some(id) {
                correlation_path.push(id.clone());
            }
        }
        let mut additional = Map::new();
        for frame in frames {
            additional.extend(frame.frame.additional.clone());
        }
        ContextSnapshot {
            tenant: match frames.first().map(|f| &f.boundary) {
                Some(Boundary::Root { tenant }) => tenant.clone(),
                _ => None,
            },
            correlation_id: correlation_path.first().cloned(),
            correlation_path,
            command: frames.iter().rev().find_map(|f| f.frame.command.clone()),
            additional: (!additional.is_empty()).then_some(Value::Object(additional)),
        }
    });
    (snapshot != ContextSnapshot::default()).then_some(snapshot)
}

pub(crate) fn has_correlation_id() -> bool {
    effective(|frames| {
        frames
            .iter()
            .any(|f| f.frame.correlation_id.is_some() || !f.frame.correlation_path.is_empty())
    })
}

/// The context a command execution runs in: it names the command when it
/// has a name and starts a correlation id unless one is already active.
pub(crate) fn execution_context(command: Option<&str>) -> EventContext {
    let mut context = EventContext::new();
    if let Some(name) = command {
        context = context.command(name);
    }
    if !has_correlation_id() {
        context = context.correlation_id(uuid::Uuid::new_v4().to_string());
    }
    context
}

#[cfg(test)]
mod tests;
