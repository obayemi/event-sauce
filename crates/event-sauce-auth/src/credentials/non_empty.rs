//! The [`NonEmpty`] set.

use serde::{Deserialize, Serialize};

/// Why a [`NonEmpty`] could not be built.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("a non-empty set cannot be empty")]
pub struct EmptyError;

/// An ordered set of distinct items that is never empty.
///
/// Insertion order is kept and duplicates are refused, so the set can be
/// serialized as a plain sequence and read back unchanged.
///
/// # Examples
///
/// ```
/// use event_sauce_auth::NonEmpty;
///
/// let mut items = NonEmpty::new("a");
/// assert!(items.push("b"));
/// assert!(!items.push("a"));
/// assert_eq!(items.as_slice(), &["a", "b"]);
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "Vec<T>", into = "Vec<T>")]
pub struct NonEmpty<T: PartialEq + Clone>(Vec<T>);

impl<T: PartialEq + Clone> NonEmpty<T> {
    /// A set holding only `first`.
    #[must_use]
    pub fn new(first: T) -> Self {
        Self(vec![first])
    }

    /// Appends `item` unless the set already holds it; reports whether it
    /// was added.
    pub fn push(&mut self, item: T) -> bool {
        let absent = !self.contains(&item);
        if absent {
            self.0.push(item);
        }
        absent
    }

    /// Whether the set holds `item`.
    #[must_use]
    pub fn contains(&self, item: &T) -> bool {
        self.0.contains(item)
    }

    /// The items, in insertion order.
    #[must_use]
    pub fn as_slice(&self) -> &[T] {
        &self.0
    }

    /// Iterates over the items in insertion order.
    pub fn iter(&self) -> std::slice::Iter<'_, T> {
        self.0.iter()
    }

    /// The items for which `keep` is true, or `None` when none is left.
    #[must_use]
    pub fn retained(&self, mut keep: impl FnMut(&T) -> bool) -> Option<Self> {
        let kept: Vec<T> = self.0.iter().filter(|item| keep(item)).cloned().collect();
        (!kept.is_empty()).then_some(Self(kept))
    }
}

impl<T: PartialEq + Clone> TryFrom<Vec<T>> for NonEmpty<T> {
    type Error = EmptyError;

    /// Builds a set from `items`, dropping repeats after the first
    /// occurrence.
    fn try_from(items: Vec<T>) -> Result<Self, EmptyError> {
        let mut items = items.into_iter();
        let mut set = Self::new(items.next().ok_or(EmptyError)?);
        for item in items {
            set.push(item);
        }
        Ok(set)
    }
}

impl<T: PartialEq + Clone> From<NonEmpty<T>> for Vec<T> {
    fn from(set: NonEmpty<T>) -> Self {
        set.0
    }
}

impl<'a, T: PartialEq + Clone> IntoIterator for &'a NonEmpty<T> {
    type Item = &'a T;
    type IntoIter = std::slice::Iter<'a, T>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

#[cfg(test)]
mod tests;
