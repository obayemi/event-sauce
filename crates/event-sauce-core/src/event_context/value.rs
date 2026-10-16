use serde_json::Value;
use std::fmt::Display;

/// Adapter storing any [`Display`] type in an event context as a string.
///
/// Build it with [`display`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DisplayValue<T>(T);

/// Wraps a [`Display`] value so it can be stored in an event context.
///
/// # Examples
///
/// ```
/// use event_sauce_core::display;
/// use serde_json::{json, Value};
///
/// let value: Value = display(std::net::Ipv4Addr::LOCALHOST).into();
/// assert_eq!(value, json!("127.0.0.1"));
/// ```
pub fn display<T: Display>(value: T) -> DisplayValue<T> {
    DisplayValue(value)
}

impl<T: Display> From<DisplayValue<T>> for Value {
    fn from(value: DisplayValue<T>) -> Self {
        Self::String(value.0.to_string())
    }
}

#[cfg(test)]
mod tests;
