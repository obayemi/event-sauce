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
pub use value::{display, ContextValue, DisplayValue};

/// Marker of an [`EventContext`] that starts a fresh context: it inherits
/// nothing from the contexts around it and may carry a tenant.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Root;

/// Marker of an [`EventContext`] layered on top of the active context.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Nested;

#[derive(Debug, Clone, Default)]
struct Frame {
    root: bool,
    tenant: Option<String>,
    correlation_id: Option<String>,
    correlation_path: Vec<String>,
    command: Option<String>,
    additional: Map<String, Value>,
}

thread_local! {
    static STACK: RefCell<Vec<Arc<Frame>>> = const { RefCell::new(Vec::new()) };
}

fn push(frame: Arc<Frame>) -> usize {
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

fn effective<R>(read: impl FnOnce(&[Arc<Frame>]) -> R) -> R {
    STACK.with(|stack| {
        let stack = stack.borrow();
        let start = stack.iter().rposition(|frame| frame.root).unwrap_or(0);
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
pub struct EventContext<K = Nested> {
    frame: Frame,
    kind: PhantomData<K>,
}

impl<K> EventContext<K> {
    fn from_frame(frame: Frame) -> Self {
        Self {
            frame,
            kind: PhantomData,
        }
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
    pub fn with(mut self, key: impl Into<String>, value: impl ContextValue) -> Self {
        self.frame.additional.insert(key.into(), value.to_json());
        self
    }

    /// Sets the name of the command being executed. Set by command
    /// execution, not meant to be called directly.
    #[doc(hidden)]
    #[must_use]
    pub fn command(mut self, name: impl Into<String>) -> Self {
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
        EnteredEventContext::enter(Arc::new(self.frame))
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

impl<K> EventContext<K> {
    fn into_frame(self) -> Arc<Frame> {
        Arc::new(self.frame)
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
        Self::from_frame(Frame::default())
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
        Self::from_frame(Frame {
            root: true,
            ..Frame::default()
        })
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
        self.frame.tenant = Some(tenant.into());
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
        Self::from_frame(Frame {
            root: true,
            tenant: snapshot.tenant,
            correlation_id: None,
            correlation_path: snapshot.correlation_path,
            command: snapshot.command,
            additional: match snapshot.additional {
                Some(Value::Object(map)) => map,
                _ => Map::new(),
            },
        })
    }
}

impl EventContext<Root> {
    #[cfg(test)]
    pub(crate) fn from_metadata(metadata: &crate::EventMetadata) -> Self {
        let correlation_path = if metadata.correlation_path.is_empty() {
            metadata.correlation_id.iter().cloned().collect()
        } else {
            metadata.correlation_path.clone()
        };
        Self::from_frame(Frame {
            root: true,
            tenant: metadata.tenant.clone(),
            correlation_path,
            ..Frame::default()
        })
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
    fn enter(frame: Arc<Frame>) -> Self {
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

pub(crate) fn current_snapshot() -> Option<ContextSnapshot> {
    let snapshot = effective(|frames| {
        let mut correlation_path: Vec<String> = Vec::new();
        for id in frames
            .iter()
            .flat_map(|f| f.correlation_path.iter().chain(f.correlation_id.iter()))
        {
            if correlation_path.last() != Some(id) {
                correlation_path.push(id.clone());
            }
        }
        let mut additional = Map::new();
        for frame in frames {
            additional.extend(frame.additional.clone());
        }
        ContextSnapshot {
            tenant: frames.iter().rev().find_map(|f| f.tenant.clone()),
            correlation_id: correlation_path.first().cloned(),
            correlation_path,
            command: frames.iter().rev().find_map(|f| f.command.clone()),
            additional: (!additional.is_empty()).then_some(Value::Object(additional)),
        }
    });
    (snapshot != ContextSnapshot::default()).then_some(snapshot)
}

/// Whether the effective context already carries a correlation id. Used by
/// command execution, not meant to be called directly.
#[doc(hidden)]
#[must_use]
pub fn has_correlation_id() -> bool {
    effective(|frames| {
        frames
            .iter()
            .any(|f| f.correlation_id.is_some() || !f.correlation_path.is_empty())
    })
}

#[cfg(test)]
mod tests;
