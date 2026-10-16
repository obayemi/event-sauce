//! Ambient event context: values that follow the current task or thread and
//! are read when events are applied.

mod value;

pub use value::{display, ContextValue, DisplayValue};
