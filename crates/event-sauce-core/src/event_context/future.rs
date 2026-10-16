use super::{EnteredEventContext, EventContext, Frame};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

/// Future returned by [`WithEventContextExt::with_event_context`].
///
/// It activates its context for the duration of every poll, so the context
/// follows the future across `.await` points and threads. The wrapped future
/// is boxed, which keeps this wrapper `Unpin`.
#[derive(Debug)]
pub struct WithEventContext<F> {
    inner: Pin<Box<F>>,
    frame: Arc<Frame>,
}

impl<F: Future> Future for WithEventContext<F> {
    type Output = F::Output;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let _guard = EnteredEventContext::enter(Arc::clone(&self.frame));
        self.inner.as_mut().poll(cx)
    }
}

/// Runs any future inside an [`EventContext`].
///
/// A thread-bound guard ([`EventContext::entered`]) cannot be held across an
/// `.await`, and a spawned task starts with no context at all: wrap the
/// future instead. To keep the context of the spawning task, hand over a
/// snapshot with [`EventContext::current`].
///
/// # Examples
///
/// ```
/// use event_sauce_core::{EventContext, WithEventContextExt};
///
/// # #[tokio::main(flavor = "current_thread")]
/// # async fn main() {
/// let work = async { /* applies events */ };
/// let ctx = EventContext::root().tenant("acme").correlation_id("req-1");
/// tokio::spawn(work.with_event_context(ctx)).await.unwrap();
/// # }
/// ```
pub trait WithEventContextExt: Future + Sized {
    /// Wraps the future so `ctx` is active on every poll, wherever the task
    /// runs.
    fn with_event_context<K>(self, ctx: EventContext<K>) -> WithEventContext<Self> {
        WithEventContext {
            inner: Box::pin(self),
            frame: ctx.into_frame(),
        }
    }
}

impl<F: Future> WithEventContextExt for F {}

#[cfg(test)]
mod tests;
