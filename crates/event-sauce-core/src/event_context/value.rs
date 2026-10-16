use chrono::{DateTime, SecondsFormat, Utc};
use serde_json::Value;
use std::borrow::Cow;
use std::fmt::Display;
use uuid::Uuid;

/// A value that can be stored in an event context.
///
/// Conversion is infallible: a context is ambient metadata, and attaching a
/// value to it must never be able to fail a command. There is deliberately no
/// blanket implementation over [`serde::Serialize`], which is fallible; wrap a
/// [`Display`] type with [`display`] or build a [`serde_json::Value`] yourself.
///
/// # Examples
///
/// ```
/// use event_sauce_core::ContextValue;
/// use serde_json::json;
///
/// assert_eq!(42_u32.to_json(), json!(42));
/// assert_eq!("eu-west".to_json(), json!("eu-west"));
/// assert_eq!(Some(true).to_json(), json!(true));
/// assert_eq!(vec![1_u8, 2].to_json(), json!([1, 2]));
/// ```
pub trait ContextValue {
    /// Converts the value to JSON.
    ///
    /// # Examples
    ///
    /// ```
    /// use event_sauce_core::ContextValue;
    ///
    /// assert_eq!(f64::NAN.to_json(), serde_json::Value::Null);
    /// ```
    fn to_json(&self) -> Value;
}

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
/// use event_sauce_core::{display, ContextValue};
/// use serde_json::json;
///
/// assert_eq!(display(std::net::Ipv4Addr::LOCALHOST).to_json(), json!("127.0.0.1"));
/// ```
pub fn display<T: Display>(value: T) -> DisplayValue<T> {
    DisplayValue(value)
}

impl<T: Display> ContextValue for DisplayValue<T> {
    fn to_json(&self) -> Value {
        Value::String(self.0.to_string())
    }
}

impl<T: ContextValue + ?Sized> ContextValue for &T {
    fn to_json(&self) -> Value {
        (**self).to_json()
    }
}

macro_rules! json_number {
    ($($ty:ty),*) => {$(
        impl ContextValue for $ty {
            fn to_json(&self) -> Value {
                Value::from(*self)
            }
        }
    )*};
}

json_number!(bool, i8, i16, i32, i64, isize, u8, u16, u32, u64, usize, f32, f64);

impl ContextValue for i128 {
    fn to_json(&self) -> Value {
        i64::try_from(*self).map_or_else(|_| Value::String(self.to_string()), Value::from)
    }
}

impl ContextValue for u128 {
    fn to_json(&self) -> Value {
        u64::try_from(*self).map_or_else(|_| Value::String(self.to_string()), Value::from)
    }
}

impl ContextValue for char {
    fn to_json(&self) -> Value {
        Value::String(self.to_string())
    }
}

impl ContextValue for str {
    fn to_json(&self) -> Value {
        Value::String(self.to_owned())
    }
}

impl ContextValue for String {
    fn to_json(&self) -> Value {
        self.as_str().to_json()
    }
}

impl ContextValue for Cow<'_, str> {
    fn to_json(&self) -> Value {
        self.as_ref().to_json()
    }
}

impl ContextValue for Uuid {
    fn to_json(&self) -> Value {
        Value::String(self.hyphenated().to_string())
    }
}

impl ContextValue for DateTime<Utc> {
    fn to_json(&self) -> Value {
        Value::String(self.to_rfc3339_opts(SecondsFormat::AutoSi, false))
    }
}

impl ContextValue for Value {
    fn to_json(&self) -> Value {
        self.clone()
    }
}

impl<T: ContextValue> ContextValue for Option<T> {
    fn to_json(&self) -> Value {
        self.as_ref().map_or(Value::Null, T::to_json)
    }
}

impl<T: ContextValue> ContextValue for [T] {
    fn to_json(&self) -> Value {
        Value::Array(self.iter().map(T::to_json).collect())
    }
}

impl<T: ContextValue> ContextValue for Vec<T> {
    fn to_json(&self) -> Value {
        self.as_slice().to_json()
    }
}

impl<T: ContextValue, const N: usize> ContextValue for [T; N] {
    fn to_json(&self) -> Value {
        self.as_slice().to_json()
    }
}

#[cfg(test)]
mod tests;
