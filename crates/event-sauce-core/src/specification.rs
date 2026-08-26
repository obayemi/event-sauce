//! Specification pattern for composable, reusable business rule validation.
//!
//! The specification pattern enables defining named business rules as objects
//! that can be combined (AND/OR/NOT) and plugged into the validation system
//! declaratively.
//!
//! # Examples
//!
//! ```
//! use event_sauce_core::specification::{Specification, SpecificationError};
//!
//! struct IsPositive;
//!
//! impl Specification<i64> for IsPositive {
//!     fn is_satisfied_by(&self, candidate: &i64) -> bool {
//!         *candidate > 0
//!     }
//!
//!     fn error_message(&self, candidate: &i64) -> String {
//!         format!("Value must be positive, got {candidate}")
//!     }
//! }
//!
//! let spec = IsPositive;
//! assert!(spec.is_satisfied_by(&42));
//! assert!(spec.check(&42).is_ok());
//! assert!(spec.check(&-1).is_err());
//! ```

use std::fmt;
use std::marker::PhantomData;
use std::ops;

/// Generic error type for specification failures, parameterized on the model type.
///
/// The type parameter `T` connects the error to its originating specification's
/// target type, enabling `From` conversion into aggregate error types.
///
/// # Examples
///
/// ```
/// use event_sauce_core::specification::SpecificationError;
///
/// let error: SpecificationError<String> = SpecificationError::new("Value too short".to_string());
/// assert_eq!(error.to_string(), "Value too short");
/// ```
pub struct SpecificationError<T: ?Sized> {
    /// The human-readable error message describing the specification failure.
    pub message: String,
    _marker: PhantomData<fn() -> T>,
}

impl<T: ?Sized> SpecificationError<T> {
    /// Creates a new specification error with the given message.
    #[must_use]
    pub fn new(message: String) -> Self {
        Self {
            message,
            _marker: PhantomData,
        }
    }
}

impl<T: ?Sized> fmt::Debug for SpecificationError<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SpecificationError")
            .field("message", &self.message)
            .finish()
    }
}

impl<T: ?Sized> fmt::Display for SpecificationError<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}

impl<T: ?Sized> std::error::Error for SpecificationError<T> {}

/// Trait for composable business rule specifications.
///
/// A specification encapsulates a single business rule that can be checked
/// against a candidate value. Specifications can be combined using boolean
/// combinators (`and`, `or`, `not`).
///
/// # Examples
///
/// ```
/// use event_sauce_core::specification::Specification;
///
/// struct MinBalance { min: i64 }
///
/// impl Specification<i64> for MinBalance {
///     fn is_satisfied_by(&self, balance: &i64) -> bool {
///         *balance >= self.min
///     }
///
///     fn error_message(&self, balance: &i64) -> String {
///         format!("Balance {} is below minimum {}", balance, self.min)
///     }
/// }
///
/// let spec = MinBalance { min: 100 };
/// assert!(spec.check(&200).is_ok());
/// assert!(spec.check(&50).is_err());
/// ```
pub trait Specification<T: ?Sized>: Send + Sync {
    /// Returns `true` if the candidate satisfies this specification.
    fn is_satisfied_by(&self, candidate: &T) -> bool;

    /// Returns a human-readable error message when the specification is not satisfied.
    fn error_message(&self, candidate: &T) -> String;

    /// Checks the candidate against this specification, returning a `SpecificationError` on failure.
    ///
    /// This is the primary method for integrating with aggregate error types that
    /// implement `From<SpecificationError<T>>`.
    ///
    /// # Errors
    ///
    /// Returns `SpecificationError<T>` if the candidate does not satisfy this specification.
    fn check(&self, candidate: &T) -> Result<(), SpecificationError<T>> {
        if self.is_satisfied_by(candidate) {
            Ok(())
        } else {
            Err(SpecificationError::new(self.error_message(candidate)))
        }
    }

    /// Validates the candidate and maps the error using a custom function.
    ///
    /// Use this when the aggregate error type does not implement `From<SpecificationError<T>>`,
    /// or when you need a specific error variant.
    ///
    /// # Errors
    ///
    /// Returns the error produced by `error_fn` if the candidate does not satisfy this specification.
    ///
    /// # Examples
    ///
    /// ```
    /// use event_sauce_core::specification::Specification;
    ///
    /// struct IsPositive;
    ///
    /// impl Specification<i64> for IsPositive {
    ///     fn is_satisfied_by(&self, val: &i64) -> bool { *val > 0 }
    ///     fn error_message(&self, val: &i64) -> String {
    ///         format!("Value must be positive, got {val}")
    ///     }
    /// }
    ///
    /// let result: Result<(), String> = IsPositive.validate_or(&-5, |msg| msg);
    /// assert!(result.is_err());
    /// ```
    fn validate_or<E>(&self, candidate: &T, error_fn: impl FnOnce(String) -> E) -> Result<(), E> {
        if self.is_satisfied_by(candidate) {
            Ok(())
        } else {
            Err(error_fn(self.error_message(candidate)))
        }
    }

    /// Combines this specification with another using logical AND.
    ///
    /// Both specifications must be satisfied for the combined specification to pass.
    fn and<S: Specification<T>>(self, other: S) -> And<Self, S>
    where
        Self: Sized,
    {
        And {
            left: self,
            right: other,
        }
    }

    /// Combines this specification with another using logical OR.
    ///
    /// At least one specification must be satisfied for the combined specification to pass.
    fn or<S: Specification<T>>(self, other: S) -> Or<Self, S>
    where
        Self: Sized,
    {
        Or {
            left: self,
            right: other,
        }
    }

    /// Negates this specification.
    ///
    /// The negated specification is satisfied when the original is not.
    fn not(self) -> Not<Self>
    where
        Self: Sized,
    {
        Not { inner: self }
    }

    /// Wraps this specification in [`Spec`] to enable operator syntax
    /// for manually-defined specifications.
    ///
    /// Specifications created with `spec!` or `#[specification]` already
    /// support operators directly — this method is only needed for specs
    /// that implement the trait manually.
    fn spec(self) -> Spec<Self>
    where
        Self: Sized,
    {
        Spec::__new(self)
    }
}

/// Logical AND combinator for two specifications.
///
/// Both the left and right specifications must be satisfied.
pub struct And<L, R> {
    left: L,
    right: R,
}

impl<L, R> And<L, R> {
    /// Constructs an `And` combinator. Used by macro-generated code.
    #[doc(hidden)]
    #[inline]
    #[must_use]
    pub fn __new(left: L, right: R) -> Self {
        Self { left, right }
    }
}

impl<L, R, T> Specification<T> for And<L, R>
where
    L: Specification<T>,
    R: Specification<T>,
    T: ?Sized,
{
    fn is_satisfied_by(&self, candidate: &T) -> bool {
        self.left.is_satisfied_by(candidate) && self.right.is_satisfied_by(candidate)
    }

    fn error_message(&self, candidate: &T) -> String {
        let left_msg = self.left.error_message(candidate);
        let right_msg = self.right.error_message(candidate);
        format!("{left_msg} AND {right_msg}")
    }
}

/// Logical OR combinator for two specifications.
///
/// At least one of the left or right specifications must be satisfied.
pub struct Or<L, R> {
    left: L,
    right: R,
}

impl<L, R> Or<L, R> {
    /// Constructs an `Or` combinator. Used by macro-generated code.
    #[doc(hidden)]
    #[inline]
    #[must_use]
    pub fn __new(left: L, right: R) -> Self {
        Self { left, right }
    }
}

impl<L, R, T> Specification<T> for Or<L, R>
where
    L: Specification<T>,
    R: Specification<T>,
    T: ?Sized,
{
    fn is_satisfied_by(&self, candidate: &T) -> bool {
        self.left.is_satisfied_by(candidate) || self.right.is_satisfied_by(candidate)
    }

    fn error_message(&self, candidate: &T) -> String {
        let left_msg = self.left.error_message(candidate);
        let right_msg = self.right.error_message(candidate);
        format!("{left_msg} OR {right_msg}")
    }
}

/// Logical NOT combinator for a specification.
///
/// Satisfied when the inner specification is *not* satisfied.
pub struct Not<S> {
    inner: S,
}

impl<S> Not<S> {
    /// Constructs a `Not` combinator. Used by macro-generated code.
    #[doc(hidden)]
    #[inline]
    #[must_use]
    pub fn __new(inner: S) -> Self {
        Self { inner }
    }
}

impl<S, T> Specification<T> for Not<S>
where
    S: Specification<T>,
    T: ?Sized,
{
    fn is_satisfied_by(&self, candidate: &T) -> bool {
        !self.inner.is_satisfied_by(candidate)
    }

    fn error_message(&self, candidate: &T) -> String {
        let inner_msg = self.inner.error_message(candidate);
        format!("NOT ({inner_msg})")
    }
}

/// A specification built from closures.
///
/// Created via the `spec!` macro's inline form or directly.
///
/// # Examples
///
/// ```
/// use event_sauce_core::specification::{FnSpec, Specification};
///
/// let spec = FnSpec::new(
///     |val: &i64| *val > 0,
///     |val: &i64| format!("Must be positive, got {val}"),
/// );
/// assert!(spec.is_satisfied_by(&42));
/// assert!(!spec.is_satisfied_by(&-1));
/// ```
pub struct FnSpec<T: ?Sized, F, M> {
    predicate: F,
    message_fn: M,
    _marker: PhantomData<fn() -> T>,
}

impl<T, F, M> FnSpec<T, F, M>
where
    F: Fn(&T) -> bool + Send + Sync,
    M: Fn(&T) -> String + Send + Sync,
    T: ?Sized,
{
    /// Creates a new closure-based specification.
    pub fn new(predicate: F, message_fn: M) -> Self {
        Self {
            predicate,
            message_fn,
            _marker: PhantomData,
        }
    }
}

impl<T, F, M> Specification<T> for FnSpec<T, F, M>
where
    F: Fn(&T) -> bool + Send + Sync,
    M: Fn(&T) -> String + Send + Sync,
    T: ?Sized,
{
    fn is_satisfied_by(&self, candidate: &T) -> bool {
        (self.predicate)(candidate)
    }

    fn error_message(&self, candidate: &T) -> String {
        (self.message_fn)(candidate)
    }
}

/// Wrapper enabling operator syntax (`&`, `|`, `!`) for specifications.
///
/// Automatically generated by the `spec!` and `#[specification]` macros.
/// Supports chaining: the result of an operator expression is itself a `Spec`,
/// so further operators work without extra wrapping.
///
/// # Examples
///
/// ```
/// use event_sauce_core::{spec, Specification};
///
/// spec!(IsPositive for i64, "Must be positive", |val| { *val > 0 });
/// spec!(IsEven for i64, "Must be even", |val| { *val % 2 == 0 });
/// spec!(LessThan100 for i64, "Must be < 100", |val| { *val < 100 });
///
/// // Operator chaining: (IsPositive | IsEven) & LessThan100
/// let spec = (IsPositive | IsEven) & LessThan100;
/// assert!(spec.is_satisfied_by(&42));  // positive, even, < 100
/// assert!(spec.is_satisfied_by(&3));   // positive (OR branch), < 100
/// assert!(!spec.is_satisfied_by(&-3)); // neither positive nor even
/// assert!(!spec.is_satisfied_by(&102)); // >= 100
/// ```
pub struct Spec<S> {
    inner: S,
}

impl<S> Spec<S> {
    /// Constructs a `Spec` wrapper. Used by macro-generated code.
    #[doc(hidden)]
    #[inline]
    #[must_use]
    pub fn __new(inner: S) -> Self {
        Self { inner }
    }
}

impl<S, T> Specification<T> for Spec<S>
where
    S: Specification<T>,
    T: ?Sized,
{
    fn is_satisfied_by(&self, candidate: &T) -> bool {
        self.inner.is_satisfied_by(candidate)
    }

    fn error_message(&self, candidate: &T) -> String {
        self.inner.error_message(candidate)
    }
}

impl<L, R> ops::BitAnd<R> for Spec<L> {
    type Output = Spec<And<L, R>>;

    fn bitand(self, rhs: R) -> Self::Output {
        Spec {
            inner: And {
                left: self.inner,
                right: rhs,
            },
        }
    }
}

impl<L, R> ops::BitOr<R> for Spec<L> {
    type Output = Spec<Or<L, R>>;

    fn bitor(self, rhs: R) -> Self::Output {
        Spec {
            inner: Or {
                left: self.inner,
                right: rhs,
            },
        }
    }
}

impl<S> ops::Not for Spec<S> {
    type Output = Spec<Not<S>>;

    fn not(self) -> Self::Output {
        Spec {
            inner: Not { inner: self.inner },
        }
    }
}

#[cfg(test)]
#[allow(clippy::unnecessary_wraps)]
mod tests {
    use super::*;

    // ===== SpecificationError Tests =====

    #[test]
    fn test_specification_error_new() {
        let error: SpecificationError<i32> = SpecificationError::new("test error".to_string());
        assert_eq!(error.message, "test error");
    }

    #[test]
    fn test_specification_error_display() {
        let error: SpecificationError<i32> =
            SpecificationError::new("something failed".to_string());
        assert_eq!(error.to_string(), "something failed");
    }

    #[test]
    fn test_specification_error_is_error() {
        fn assert_error<T: std::error::Error>() {}
        assert_error::<SpecificationError<i32>>();
    }

    #[test]
    fn test_specification_error_unsized_type() {
        let error: SpecificationError<str> = SpecificationError::new("unsized test".to_string());
        assert_eq!(error.to_string(), "unsized test");
    }

    // ===== Simple Specification Implementation =====

    struct IsPositive;

    impl Specification<i64> for IsPositive {
        fn is_satisfied_by(&self, candidate: &i64) -> bool {
            *candidate > 0
        }

        fn error_message(&self, candidate: &i64) -> String {
            format!("Value must be positive, got {candidate}")
        }
    }

    struct IsEven;

    impl Specification<i64> for IsEven {
        fn is_satisfied_by(&self, candidate: &i64) -> bool {
            *candidate % 2 == 0
        }

        fn error_message(&self, candidate: &i64) -> String {
            format!("Value must be even, got {candidate}")
        }
    }

    struct LessThan {
        max: i64,
    }

    impl Specification<i64> for LessThan {
        fn is_satisfied_by(&self, candidate: &i64) -> bool {
            *candidate < self.max
        }

        fn error_message(&self, candidate: &i64) -> String {
            format!("Value {candidate} must be less than {}", self.max)
        }
    }

    // ===== Specification Trait Tests =====

    #[test]
    fn test_specification_is_satisfied_by() {
        let spec = IsPositive;
        assert!(spec.is_satisfied_by(&42));
        assert!(!spec.is_satisfied_by(&-1));
        assert!(!spec.is_satisfied_by(&0));
    }

    #[test]
    fn test_specification_error_message() {
        let spec = IsPositive;
        assert_eq!(spec.error_message(&-5), "Value must be positive, got -5");
    }

    #[test]
    fn test_specification_check_success() {
        let spec = IsPositive;
        assert!(spec.check(&42).is_ok());
    }

    #[test]
    fn test_specification_check_failure() {
        let spec = IsPositive;
        let result = spec.check(&-1);
        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err().message,
            "Value must be positive, got -1"
        );
    }

    #[test]
    fn test_specification_validate_or_success() {
        let spec = IsPositive;
        let result: Result<(), String> = spec.validate_or(&42, |msg| msg);
        assert!(result.is_ok());
    }

    #[test]
    fn test_specification_validate_or_failure() {
        let spec = IsPositive;
        let result: Result<(), String> = spec.validate_or(&-5, |msg| msg);
        assert!(result.is_err());
        assert_eq!(result.unwrap_err(), "Value must be positive, got -5");
    }

    #[test]
    fn test_specification_validate_or_custom_error() {
        #[derive(Debug)]
        struct CustomError(String);

        let spec = IsPositive;
        let result: Result<(), CustomError> = spec.validate_or(&-1, CustomError);
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().0, "Value must be positive, got -1");
    }

    // ===== AND Combinator Tests =====

    #[test]
    fn test_and_both_satisfied() {
        let spec = IsPositive.and(IsEven);
        assert!(spec.is_satisfied_by(&42));
    }

    #[test]
    fn test_and_left_unsatisfied() {
        let spec = IsPositive.and(IsEven);
        assert!(!spec.is_satisfied_by(&-2));
    }

    #[test]
    fn test_and_right_unsatisfied() {
        let spec = IsPositive.and(IsEven);
        assert!(!spec.is_satisfied_by(&3));
    }

    #[test]
    fn test_and_neither_satisfied() {
        let spec = IsPositive.and(IsEven);
        assert!(!spec.is_satisfied_by(&-3));
    }

    #[test]
    fn test_and_error_message() {
        let spec = IsPositive.and(IsEven);
        let msg = spec.error_message(&-3);
        assert_eq!(
            msg,
            "Value must be positive, got -3 AND Value must be even, got -3"
        );
    }

    #[test]
    fn test_and_check_success() {
        let spec = IsPositive.and(IsEven);
        assert!(spec.check(&42).is_ok());
    }

    #[test]
    fn test_and_check_failure() {
        let spec = IsPositive.and(IsEven);
        assert!(spec.check(&3).is_err());
    }

    // ===== OR Combinator Tests =====

    #[test]
    fn test_or_both_satisfied() {
        let spec = IsPositive.or(IsEven);
        assert!(spec.is_satisfied_by(&42));
    }

    #[test]
    fn test_or_left_satisfied() {
        let spec = IsPositive.or(IsEven);
        assert!(spec.is_satisfied_by(&3));
    }

    #[test]
    fn test_or_right_satisfied() {
        let spec = IsPositive.or(IsEven);
        assert!(spec.is_satisfied_by(&-2));
    }

    #[test]
    fn test_or_neither_satisfied() {
        let spec = IsPositive.or(IsEven);
        assert!(!spec.is_satisfied_by(&-3));
    }

    #[test]
    fn test_or_error_message() {
        let spec = IsPositive.or(IsEven);
        let msg = spec.error_message(&-3);
        assert_eq!(
            msg,
            "Value must be positive, got -3 OR Value must be even, got -3"
        );
    }

    #[test]
    fn test_or_check_success() {
        let spec = IsPositive.or(IsEven);
        assert!(spec.check(&3).is_ok());
    }

    #[test]
    fn test_or_check_failure() {
        let spec = IsPositive.or(IsEven);
        assert!(spec.check(&-3).is_err());
    }

    // ===== NOT Combinator Tests =====

    #[test]
    fn test_not_satisfied_when_inner_unsatisfied() {
        let spec = IsPositive.not();
        assert!(spec.is_satisfied_by(&-1));
    }

    #[test]
    fn test_not_unsatisfied_when_inner_satisfied() {
        let spec = IsPositive.not();
        assert!(!spec.is_satisfied_by(&42));
    }

    #[test]
    fn test_not_error_message() {
        let spec = IsPositive.not();
        let msg = spec.error_message(&42);
        assert_eq!(msg, "NOT (Value must be positive, got 42)");
    }

    #[test]
    fn test_not_check_success() {
        let spec = IsPositive.not();
        assert!(spec.check(&-1).is_ok());
    }

    #[test]
    fn test_not_check_failure() {
        let spec = IsPositive.not();
        assert!(spec.check(&42).is_err());
    }

    // ===== Complex Combinator Composition Tests =====

    #[test]
    fn test_and_or_composition() {
        // (positive AND even) OR less_than_0
        let spec = IsPositive.and(IsEven).or(LessThan { max: 0 });

        assert!(spec.is_satisfied_by(&42)); // positive and even
        assert!(spec.is_satisfied_by(&-5)); // less than 0
        assert!(!spec.is_satisfied_by(&3)); // positive but odd, not < 0
    }

    #[test]
    fn test_not_and_composition() {
        // NOT positive AND even = negative-or-zero AND even
        let spec = IsPositive.not().and(IsEven);
        assert!(spec.is_satisfied_by(&-2)); // not positive, even
        assert!(spec.is_satisfied_by(&0)); // not positive, even
        assert!(!spec.is_satisfied_by(&2)); // positive
        assert!(!spec.is_satisfied_by(&-3)); // odd
    }

    #[test]
    fn test_double_negation() {
        let spec = IsPositive.not().not();
        assert!(spec.is_satisfied_by(&42));
        assert!(!spec.is_satisfied_by(&-1));
    }

    #[test]
    fn test_triple_and_chain() {
        let spec = IsPositive.and(IsEven).and(LessThan { max: 100 });
        assert!(spec.is_satisfied_by(&42));
        assert!(!spec.is_satisfied_by(&102)); // > 100
        assert!(!spec.is_satisfied_by(&3)); // odd
        assert!(!spec.is_satisfied_by(&-2)); // negative
    }

    // ===== FnSpec Tests =====

    #[test]
    fn test_fn_spec_satisfied() {
        let spec = FnSpec::new(
            |val: &i64| *val > 0,
            |val: &i64| format!("Must be positive, got {val}"),
        );
        assert!(spec.is_satisfied_by(&42));
    }

    #[test]
    fn test_fn_spec_unsatisfied() {
        let spec = FnSpec::new(
            |val: &i64| *val > 0,
            |val: &i64| format!("Must be positive, got {val}"),
        );
        assert!(!spec.is_satisfied_by(&-1));
    }

    #[test]
    fn test_fn_spec_error_message() {
        let spec = FnSpec::new(
            |val: &i64| *val > 0,
            |val: &i64| format!("Must be positive, got {val}"),
        );
        assert_eq!(spec.error_message(&-5), "Must be positive, got -5");
    }

    #[test]
    fn test_fn_spec_check() {
        let spec = FnSpec::new(
            |val: &i64| *val > 0,
            |val: &i64| format!("Must be positive, got {val}"),
        );
        assert!(spec.check(&42).is_ok());
        assert!(spec.check(&-1).is_err());
    }

    #[test]
    fn test_fn_spec_composable_with_and() {
        let positive = FnSpec::new(
            |val: &i64| *val > 0,
            |val: &i64| format!("Must be positive, got {val}"),
        );
        let even = FnSpec::new(
            |val: &i64| *val % 2 == 0,
            |val: &i64| format!("Must be even, got {val}"),
        );
        let spec = positive.and(even);
        assert!(spec.is_satisfied_by(&42));
        assert!(!spec.is_satisfied_by(&3));
    }

    #[test]
    fn test_fn_spec_composable_with_or() {
        let positive = FnSpec::new(
            |val: &i64| *val > 0,
            |val: &i64| format!("Must be positive, got {val}"),
        );
        let even = FnSpec::new(
            |val: &i64| *val % 2 == 0,
            |val: &i64| format!("Must be even, got {val}"),
        );
        let spec = positive.or(even);
        assert!(spec.is_satisfied_by(&3));
        assert!(spec.is_satisfied_by(&-2));
        assert!(!spec.is_satisfied_by(&-3));
    }

    #[test]
    fn test_fn_spec_composable_with_not() {
        let positive = FnSpec::new(
            |val: &i64| *val > 0,
            |val: &i64| format!("Must be positive, got {val}"),
        );
        let spec = positive.not();
        assert!(spec.is_satisfied_by(&-1));
        assert!(!spec.is_satisfied_by(&42));
    }

    // ===== Specification with struct fields =====

    struct HasMinBalance {
        min: i64,
    }

    impl Specification<i64> for HasMinBalance {
        fn is_satisfied_by(&self, balance: &i64) -> bool {
            *balance >= self.min
        }

        fn error_message(&self, balance: &i64) -> String {
            format!(
                "Insufficient balance: balance={balance}, required={}",
                self.min
            )
        }
    }

    #[test]
    fn test_parameterized_spec_satisfied() {
        let spec = HasMinBalance { min: 100 };
        assert!(spec.is_satisfied_by(&200));
    }

    #[test]
    fn test_parameterized_spec_unsatisfied() {
        let spec = HasMinBalance { min: 100 };
        assert!(!spec.is_satisfied_by(&50));
    }

    #[test]
    fn test_parameterized_spec_error_message() {
        let spec = HasMinBalance { min: 100 };
        assert_eq!(
            spec.error_message(&50),
            "Insufficient balance: balance=50, required=100"
        );
    }

    #[test]
    fn test_parameterized_spec_composable() {
        let spec = HasMinBalance { min: 100 }.and(LessThan { max: 10000 });
        assert!(spec.is_satisfied_by(&500));
        assert!(!spec.is_satisfied_by(&50));
        assert!(!spec.is_satisfied_by(&20000));
    }

    // ===== Integration with From conversion pattern =====

    #[test]
    fn test_specification_error_with_from_conversion() {
        #[derive(Debug)]
        enum AccountError {
            SpecFailed(SpecificationError<i64>),
        }

        impl From<SpecificationError<i64>> for AccountError {
            fn from(e: SpecificationError<i64>) -> Self {
                AccountError::SpecFailed(e)
            }
        }

        fn validate_balance(balance: i64) -> Result<(), AccountError> {
            let spec = HasMinBalance { min: 100 };
            spec.check(&balance)?;
            Ok(())
        }

        assert!(validate_balance(200).is_ok());
        let err = validate_balance(50).unwrap_err();
        match err {
            AccountError::SpecFailed(e) => {
                assert_eq!(e.message, "Insufficient balance: balance=50, required=100");
            }
        }
    }

    // ===== validate_or with complex error types =====

    #[test]
    fn test_validate_or_with_enum_error() {
        #[derive(Debug, PartialEq)]
        enum OrderError {
            InsufficientFunds(String),
        }

        let spec = HasMinBalance { min: 100 };
        let result: Result<(), OrderError> = spec.validate_or(&50, OrderError::InsufficientFunds);

        assert_eq!(
            result.unwrap_err(),
            OrderError::InsufficientFunds(
                "Insufficient balance: balance=50, required=100".to_string()
            )
        );
    }

    // ===== Specification with complex types =====

    #[derive(Debug)]
    struct Account {
        balance: i64,
        status: &'static str,
    }

    struct AccountIsActive;

    impl Specification<Account> for AccountIsActive {
        fn is_satisfied_by(&self, account: &Account) -> bool {
            account.status == "active"
        }

        fn error_message(&self, account: &Account) -> String {
            format!("Account must be active, got {}", account.status)
        }
    }

    struct AccountHasFunds {
        amount: i64,
    }

    impl Specification<Account> for AccountHasFunds {
        fn is_satisfied_by(&self, account: &Account) -> bool {
            account.balance >= self.amount
        }

        fn error_message(&self, account: &Account) -> String {
            format!(
                "Insufficient funds: balance={}, requested={}",
                account.balance, self.amount
            )
        }
    }

    #[test]
    fn test_complex_type_single_spec() {
        let account = Account {
            balance: 500,
            status: "active",
        };
        let spec = AccountIsActive;
        assert!(spec.check(&account).is_ok());
    }

    #[test]
    fn test_complex_type_combined_specs() {
        let account = Account {
            balance: 500,
            status: "active",
        };
        let spec = AccountIsActive.and(AccountHasFunds { amount: 200 });
        assert!(spec.check(&account).is_ok());
    }

    #[test]
    fn test_complex_type_combined_specs_failure() {
        let account = Account {
            balance: 50,
            status: "active",
        };
        let spec = AccountIsActive.and(AccountHasFunds { amount: 200 });
        let result = spec.check(&account);
        assert!(result.is_err());
    }

    #[test]
    fn test_complex_type_inactive_account() {
        let account = Account {
            balance: 500,
            status: "frozen",
        };
        let spec = AccountIsActive;
        let result = spec.check(&account);
        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err().message,
            "Account must be active, got frozen"
        );
    }

    #[test]
    fn test_complex_type_or_combinator() {
        // Active OR has enough funds (weird rule but tests composition)
        let account = Account {
            balance: 1000,
            status: "frozen",
        };
        let spec = AccountIsActive.or(AccountHasFunds { amount: 500 });
        assert!(spec.check(&account).is_ok());
    }

    // ===== Operator Syntax Tests =====

    #[test]
    fn test_spec_wrapper_delegates_is_satisfied_by() {
        let spec = IsPositive.spec();
        assert!(spec.is_satisfied_by(&42));
        assert!(!spec.is_satisfied_by(&-1));
    }

    #[test]
    fn test_spec_wrapper_delegates_error_message() {
        let spec = IsPositive.spec();
        assert_eq!(spec.error_message(&-5), "Value must be positive, got -5");
    }

    #[test]
    fn test_spec_wrapper_delegates_check() {
        let spec = IsPositive.spec();
        assert!(spec.check(&42).is_ok());
        assert!(spec.check(&-1).is_err());
    }

    #[test]
    fn test_bitand_operator() {
        let spec = IsPositive.spec() & IsEven;
        assert!(spec.is_satisfied_by(&42));
        assert!(!spec.is_satisfied_by(&3)); // odd
        assert!(!spec.is_satisfied_by(&-2)); // negative
    }

    #[test]
    fn test_bitor_operator() {
        let spec = IsPositive.spec() | IsEven;
        assert!(spec.is_satisfied_by(&42)); // both
        assert!(spec.is_satisfied_by(&3)); // positive only
        assert!(spec.is_satisfied_by(&-2)); // even only
        assert!(!spec.is_satisfied_by(&-3)); // neither
    }

    #[test]
    fn test_not_operator() {
        let spec = !IsPositive.spec();
        assert!(spec.is_satisfied_by(&-1));
        assert!(!spec.is_satisfied_by(&42));
    }

    #[test]
    fn test_operator_chaining_or_then_and() {
        // (IsPositive | IsEven) & LessThan { max: 100 }
        let spec = (IsPositive.spec() | IsEven) & LessThan { max: 100 };
        assert!(spec.is_satisfied_by(&42)); // positive, even, < 100
        assert!(spec.is_satisfied_by(&3)); // positive, < 100
        assert!(spec.is_satisfied_by(&-2)); // even, < 100
        assert!(!spec.is_satisfied_by(&-3)); // neither positive nor even
        assert!(!spec.is_satisfied_by(&102)); // positive but >= 100
    }

    #[test]
    fn test_operator_chaining_and_then_or() {
        // (IsPositive & IsEven) | LessThan { max: 0 }
        let spec = (IsPositive.spec() & IsEven) | LessThan { max: 0 };
        assert!(spec.is_satisfied_by(&42)); // positive and even
        assert!(spec.is_satisfied_by(&-5)); // less than 0
        assert!(!spec.is_satisfied_by(&3)); // positive but odd, not < 0
    }

    #[test]
    fn test_operator_triple_or() {
        // IsPositive | IsEven | LessThan { max: -10 }
        let spec = IsPositive.spec() | IsEven | LessThan { max: -10 };
        assert!(spec.is_satisfied_by(&1)); // positive
        assert!(spec.is_satisfied_by(&-2)); // even
        assert!(spec.is_satisfied_by(&-11)); // < -10
        assert!(!spec.is_satisfied_by(&-3)); // none
    }

    #[test]
    fn test_operator_not_with_and() {
        // !IsPositive & IsEven
        let spec = !IsPositive.spec() & IsEven;
        assert!(spec.is_satisfied_by(&-2)); // not positive, even
        assert!(spec.is_satisfied_by(&0)); // not positive, even
        assert!(!spec.is_satisfied_by(&2)); // positive
        assert!(!spec.is_satisfied_by(&-3)); // odd
    }

    #[test]
    fn test_operator_check_returns_error() {
        let spec = IsPositive.spec() & IsEven;
        let result = spec.check(&3);
        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err().message,
            "Value must be positive, got 3 AND Value must be even, got 3"
        );
    }

    #[test]
    fn test_operator_with_parameterized_specs() {
        let spec = HasMinBalance { min: 100 }.spec() & LessThan { max: 10000 };
        assert!(spec.is_satisfied_by(&500));
        assert!(!spec.is_satisfied_by(&50)); // below min
        assert!(!spec.is_satisfied_by(&20000)); // above max
    }

    #[test]
    fn test_operator_with_complex_types() {
        let account = Account {
            balance: 500,
            status: "active",
        };
        let spec = AccountIsActive.spec() & AccountHasFunds { amount: 200 };
        assert!(spec.check(&account).is_ok());
    }

    #[test]
    fn test_operator_result_is_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<Spec<IsPositive>>();
        assert_send_sync::<Spec<And<IsPositive, IsEven>>>();
    }

    #[test]
    fn test_operator_with_fn_spec() {
        let positive = FnSpec::new(
            |val: &i64| *val > 0,
            |val: &i64| format!("Must be positive, got {val}"),
        );
        let even = FnSpec::new(
            |val: &i64| *val % 2 == 0,
            |val: &i64| format!("Must be even, got {val}"),
        );
        let spec = positive.spec() | even;
        assert!(spec.is_satisfied_by(&3));
        assert!(spec.is_satisfied_by(&-2));
        assert!(!spec.is_satisfied_by(&-3));
    }

    #[test]
    fn test_operator_double_negation() {
        let spec = !(!IsPositive.spec());
        assert!(spec.is_satisfied_by(&42));
        assert!(!spec.is_satisfied_by(&-1));
    }

    #[test]
    fn test_operator_validate_or() {
        let spec = IsPositive.spec() & IsEven;
        let result: Result<(), String> = spec.validate_or(&-3, |msg| msg);
        assert!(result.is_err());
    }
}
