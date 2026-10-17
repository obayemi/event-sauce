use super::sealed::Boundary;
use serde_json::{Map, Value};
use std::cell::RefCell;
use std::marker::PhantomData;
use std::sync::Arc;

#[derive(Debug)]
pub(crate) struct StackFrame {
    pub(super) boundary: Boundary,
    pub(super) frame: Frame,
}

#[derive(Debug, Clone, Default)]
pub(super) struct Frame {
    pub(super) correlation_id: Option<String>,
    pub(super) correlation_path: Vec<String>,
    pub(super) command: Option<String>,
    pub(super) additional: Map<String, Value>,
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

pub(super) fn effective<R>(read: impl FnOnce(&[Arc<StackFrame>]) -> R) -> R {
    STACK.with(|stack| {
        let stack = stack.borrow();
        let start = stack
            .iter()
            .rposition(|f| matches!(f.boundary, Boundary::Root { .. }))
            .unwrap_or(0);
        read(&stack[start..])
    })
}

/// Guard returned by [`EventContext::entered`](super::EventContext::entered):
/// the context stays active until it is dropped.
///
/// It is not `Send`: it must be dropped on the thread that created it.
#[derive(Debug)]
pub struct EnteredEventContext {
    depth: usize,
    not_send: PhantomData<*const ()>,
}

impl EnteredEventContext {
    pub(super) fn enter(frame: Arc<StackFrame>) -> Self {
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
