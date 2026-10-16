use super::*;
use crate::event_context::{current_snapshot, ContextSnapshot};
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::pin::pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::{Context, Poll, Waker};

struct YieldOnce(bool);

impl Future for YieldOnce {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        if self.0 {
            Poll::Ready(())
        } else {
            self.0 = true;
            cx.waker().wake_by_ref();
            Poll::Pending
        }
    }
}

fn correlation() -> Option<String> {
    current_snapshot().and_then(|s| s.correlation_id)
}

fn assert_send<T: Send>() {}

#[tokio::test]
async fn the_context_is_visible_before_and_after_a_yield_point() {
    async {
        assert_eq!(correlation().as_deref(), Some("req"));
        YieldOnce(false).await;
        assert_eq!(correlation().as_deref(), Some("req"));
    }
    .with_event_context(EventContext::new().correlation_id("req"))
    .await;
    assert_eq!(current_snapshot(), None);
}

#[test]
fn the_wrapped_future_output_is_returned() {
    let mut fut = pin!(async { 5 }.with_event_context(EventContext::new()));
    let mut cx = Context::from_waker(Waker::noop());
    assert_eq!(fut.as_mut().poll(&mut cx), Poll::Ready(5));
}

#[test]
fn interleaved_futures_never_see_each_others_frames() {
    let observe = |id: &'static str| {
        async move {
            for _ in 0..3 {
                assert_eq!(correlation().as_deref(), Some(id));
                YieldOnce(false).await;
            }
            id
        }
        .with_event_context(EventContext::root().correlation_id(id))
    };
    let mut a = pin!(observe("a"));
    let mut b = pin!(observe("b"));
    let mut cx = Context::from_waker(Waker::noop());
    let (mut done_a, mut done_b) = (None, None);
    while done_a.is_none() || done_b.is_none() {
        if done_a.is_none() {
            if let Poll::Ready(v) = a.as_mut().poll(&mut cx) {
                done_a = Some(v);
            }
        }
        assert_eq!(current_snapshot(), None);
        if done_b.is_none() {
            if let Poll::Ready(v) = b.as_mut().poll(&mut cx) {
                done_b = Some(v);
            }
        }
        assert_eq!(current_snapshot(), None);
    }
    assert_eq!((done_a, done_b), (Some("a"), Some("b")));
}

#[test]
fn nesting_inside_an_active_scope_layers_on_it() {
    EventContext::new().correlation_id("outer").in_scope(|| {
        let mut fut = pin!(async {
            assert_eq!(
                current_snapshot().map(|s: ContextSnapshot| s.correlation_path),
                Some(vec!["outer".to_string(), "inner".to_string()])
            );
        }
        .with_event_context(EventContext::new().correlation_id("inner")));
        let mut cx = Context::from_waker(Waker::noop());
        assert_eq!(fut.as_mut().poll(&mut cx), Poll::Ready(()));
    });
}

#[test]
fn the_stack_is_restored_after_a_panic_in_poll() {
    let polled = AtomicBool::new(false);
    let mut fut = pin!(async {
        polled.store(true, Ordering::SeqCst);
        panic!("boom");
    }
    .with_event_context(EventContext::new().correlation_id("req")));
    let mut cx = Context::from_waker(Waker::noop());
    let outcome = catch_unwind(AssertUnwindSafe(|| fut.as_mut().poll(&mut cx)));
    assert!(outcome.is_err());
    assert!(polled.load(Ordering::SeqCst));
    assert_eq!(current_snapshot(), None);
}

#[test]
fn the_wrapper_is_send_and_unpin_when_the_future_is_send() {
    fn assert_unpin<T: Unpin>() {}
    assert_send::<WithEventContext<std::future::Ready<u8>>>();
    assert_unpin::<WithEventContext<std::future::Ready<u8>>>();
}

#[tokio::test]
async fn a_spawned_task_only_sees_the_context_it_is_given() {
    let handle = EventContext::new().correlation_id("req").in_scope(|| {
        tokio::spawn(async { correlation() }.with_event_context(EventContext::current()))
    });
    assert_eq!(handle.await.unwrap().as_deref(), Some("req"));
    let bare = EventContext::new()
        .correlation_id("req")
        .in_scope(|| tokio::spawn(async { correlation() }));
    assert_eq!(bare.await.unwrap(), None);
}
