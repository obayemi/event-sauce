//! Apply event trait for event application logic.
//!
//! Provides the `ApplyEvent` trait that allows events to define their own
//! application logic, making events self-contained and reducing boilerplate.

use crate::AggregateError;

/// Trait for applying events to aggregates.
///
/// This trait allows each event type to define how it should be applied
/// to an aggregate, promoting encapsulation and reducing the need for
/// large match statements in aggregate code.
///
/// # Type Parameters
///
/// - `A`: The aggregate type this event applies to
/// - `E`: The error type (must implement `AggregateError`)
///
/// # Pattern
///
/// Events should implement this trait to define:
/// 1. How to validate the event can be applied (optional, via `validate`)
/// 2. How to apply the event to update aggregate state (via `apply`)
///
/// The `validate` method has a default implementation that always succeeds,
/// making validation optional. Only implement it when your event needs
/// validation logic.
///
/// # Examples
///
/// ## Simple Event Without Validation
///
/// ```
/// use event_sauce_core::{ApplyEvent, AggregateError};
/// use thiserror::Error;
///
/// #[derive(Debug, Error)]
/// #[error("Counter error")]
/// enum CounterError {}
/// impl AggregateError for CounterError {}
///
/// struct Counter {
///     value: i32,
/// }
///
/// struct Incremented {
///     amount: i32,
/// }
///
/// impl ApplyEvent<Counter, CounterError> for Incremented {
///     fn apply(&self, counter: &mut Counter) {
///         counter.value += self.amount;
///     }
/// }
/// ```
///
/// ## Event With Validation
///
/// ```
/// use event_sauce_core::{ApplyEvent, AggregateError};
/// use thiserror::Error;
///
/// #[derive(Debug, Error)]
/// enum BankAccountError {
///     #[error("Insufficient funds")]
///     InsufficientFunds,
///     #[error("Account closed")]
///     AccountClosed,
/// }
/// impl AggregateError for BankAccountError {}
///
/// #[derive(Debug, PartialEq)]
/// enum AccountStatus {
///     Active,
///     Closed,
/// }
///
/// struct BankAccount {
///     balance: i64,
///     status: AccountStatus,
/// }
///
/// struct MoneyWithdrawn {
///     amount: i64,
/// }
///
/// impl ApplyEvent<BankAccount, BankAccountError> for MoneyWithdrawn {
///     fn validate(&self, account: &BankAccount) -> Result<(), BankAccountError> {
///         if account.status == AccountStatus::Closed {
///             return Err(BankAccountError::AccountClosed);
///         }
///         if account.balance < self.amount {
///             return Err(BankAccountError::InsufficientFunds);
///         }
///         Ok(())
///     }
///
///     fn apply(&self, account: &mut BankAccount) {
///         account.balance -= self.amount;
///     }
/// }
/// ```
///
/// ## Usage in Aggregates
///
/// ```
/// use event_sauce_core::{ApplyEvent, AggregateError};
/// use thiserror::Error;
///
/// #[derive(Debug, Error)]
/// #[error("Counter error")]
/// enum CounterError {}
/// impl AggregateError for CounterError {}
///
/// struct Counter {
///     value: i32,
/// }
///
/// struct Incremented {
///     amount: i32,
/// }
///
/// impl ApplyEvent<Counter, CounterError> for Incremented {
///     fn apply(&self, counter: &mut Counter) {
///         counter.value += self.amount;
///     }
/// }
///
/// impl Counter {
///     fn increment(&mut self, amount: i32) -> Result<(), CounterError> {
///         let event = Incremented { amount };
///
///         // Validate before applying
///         event.validate(self)?;
///
///         // Apply the event
///         event.apply(self);
///
///         Ok(())
///     }
/// }
/// ```
pub trait ApplyEvent<A, E: AggregateError> {
    /// Validates that the event can be applied to the aggregate.
    ///
    /// This method checks business rules and invariants without modifying
    /// the aggregate's state. If validation fails, it returns an error.
    ///
    /// The default implementation always succeeds, making validation optional.
    /// Only override this method when your event requires validation.
    ///
    /// # Arguments
    ///
    /// * `aggregate` - Immutable reference to the aggregate to validate against
    ///
    /// # Returns
    ///
    /// * `Ok(())` if the event can be applied
    /// * `Err(E)` if validation fails
    ///
    /// # Examples
    ///
    /// ```
    /// use event_sauce_core::{ApplyEvent, AggregateError};
    /// use thiserror::Error;
    ///
    /// #[derive(Debug, Error)]
    /// enum CounterError {
    ///     #[error("Invalid amount: {0}")]
    ///     InvalidAmount(i32),
    /// }
    /// impl AggregateError for CounterError {}
    ///
    /// struct Counter {
    ///     value: i32,
    /// }
    ///
    /// struct Incremented {
    ///     amount: i32,
    /// }
    ///
    /// impl ApplyEvent<Counter, CounterError> for Incremented {
    ///     fn validate(&self, _counter: &Counter) -> Result<(), CounterError> {
    ///         if self.amount <= 0 {
    ///             return Err(CounterError::InvalidAmount(self.amount));
    ///         }
    ///         Ok(())
    ///     }
    ///
    ///     fn apply(&self, counter: &mut Counter) {
    ///         counter.value += self.amount;
    ///     }
    /// }
    /// ```
    fn validate(&self, _aggregate: &A) -> Result<(), E> {
        Ok(())
    }

    /// Applies the event to the aggregate, updating its state.
    ///
    /// This method performs the actual state transformation. It assumes
    /// that validation has already been performed (or is not needed).
    ///
    /// This method should be pure and deterministic - given the same
    /// aggregate state and event, it should always produce the same result.
    ///
    /// # Arguments
    ///
    /// * `aggregate` - Mutable reference to the aggregate to update
    ///
    /// # Examples
    ///
    /// ```
    /// use event_sauce_core::{ApplyEvent, AggregateError};
    /// use thiserror::Error;
    ///
    /// #[derive(Debug, Error)]
    /// #[error("Counter error")]
    /// enum CounterError {}
    /// impl AggregateError for CounterError {}
    ///
    /// struct Counter {
    ///     value: i32,
    /// }
    ///
    /// struct Incremented {
    ///     amount: i32,
    /// }
    ///
    /// impl ApplyEvent<Counter, CounterError> for Incremented {
    ///     fn apply(&self, counter: &mut Counter) {
    ///         counter.value += self.amount;
    ///     }
    /// }
    ///
    /// let mut counter = Counter { value: 0 };
    /// let event = Incremented { amount: 5 };
    /// event.apply(&mut counter);
    /// assert_eq!(counter.value, 5);
    /// ```
    fn apply(&self, aggregate: &mut A);
}

#[cfg(test)]
mod tests {
    use super::*;
    use thiserror::Error;

    #[derive(Debug, Error)]
    #[error("Test error: {0}")]
    struct TestError(String);

    impl AggregateError for TestError {}

    struct TestAggregate {
        value: i32,
        status: Status,
    }

    #[derive(Debug, PartialEq)]
    enum Status {
        Active,
        Inactive,
    }

    struct SimpleEvent {
        amount: i32,
    }

    impl ApplyEvent<TestAggregate, TestError> for SimpleEvent {
        fn apply(&self, aggregate: &mut TestAggregate) {
            aggregate.value += self.amount;
        }
    }

    struct ValidatedEvent {
        amount: i32,
    }

    impl ApplyEvent<TestAggregate, TestError> for ValidatedEvent {
        fn validate(&self, aggregate: &TestAggregate) -> Result<(), TestError> {
            if aggregate.status == Status::Inactive {
                return Err(TestError("Aggregate is inactive".to_string()));
            }
            if self.amount < 0 {
                return Err(TestError("Amount cannot be negative".to_string()));
            }
            Ok(())
        }

        fn apply(&self, aggregate: &mut TestAggregate) {
            aggregate.value += self.amount;
        }
    }

    #[test]
    fn test_apply_event_without_validation() {
        let mut aggregate = TestAggregate {
            value: 10,
            status: Status::Active,
        };
        let event = SimpleEvent { amount: 5 };

        event.apply(&mut aggregate);

        assert_eq!(aggregate.value, 15);
    }

    #[test]
    fn test_apply_event_default_validation_succeeds() {
        let aggregate = TestAggregate {
            value: 10,
            status: Status::Active,
        };
        let event = SimpleEvent { amount: 5 };

        let result = event.validate(&aggregate);

        assert!(result.is_ok());
    }

    #[test]
    fn test_apply_event_with_successful_validation() {
        let mut aggregate = TestAggregate {
            value: 10,
            status: Status::Active,
        };
        let event = ValidatedEvent { amount: 5 };

        assert!(event.validate(&aggregate).is_ok());

        event.apply(&mut aggregate);

        assert_eq!(aggregate.value, 15);
    }

    #[test]
    fn test_apply_event_validation_fails_on_inactive_status() {
        let aggregate = TestAggregate {
            value: 10,
            status: Status::Inactive,
        };
        let event = ValidatedEvent { amount: 5 };

        let result = event.validate(&aggregate);

        assert!(result.is_err());
        assert_eq!(result.unwrap_err().to_string(), "Test error: Aggregate is inactive");
    }

    #[test]
    fn test_apply_event_validation_fails_on_negative_amount() {
        let aggregate = TestAggregate {
            value: 10,
            status: Status::Active,
        };
        let event = ValidatedEvent { amount: -5 };

        let result = event.validate(&aggregate);

        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err().to_string(),
            "Test error: Amount cannot be negative"
        );
    }

    #[test]
    fn test_apply_event_apply_without_validation() {
        // Apply should work even if validation would fail
        // (for event replay scenarios)
        let mut aggregate = TestAggregate {
            value: 10,
            status: Status::Inactive,
        };
        let event = ValidatedEvent { amount: 5 };

        // Validation would fail
        assert!(event.validate(&aggregate).is_err());

        // But apply still works (for replay)
        event.apply(&mut aggregate);

        assert_eq!(aggregate.value, 15);
    }

    #[test]
    fn test_apply_event_multiple_applications() {
        let mut aggregate = TestAggregate {
            value: 0,
            status: Status::Active,
        };

        let event1 = SimpleEvent { amount: 5 };
        let event2 = SimpleEvent { amount: 10 };
        let event3 = SimpleEvent { amount: 3 };

        event1.apply(&mut aggregate);
        event2.apply(&mut aggregate);
        event3.apply(&mut aggregate);

        assert_eq!(aggregate.value, 18);
    }

    #[test]
    fn test_apply_event_is_deterministic() {
        let event = SimpleEvent { amount: 5 };

        let mut aggregate1 = TestAggregate {
            value: 10,
            status: Status::Active,
        };
        event.apply(&mut aggregate1);

        let mut aggregate2 = TestAggregate {
            value: 10,
            status: Status::Active,
        };
        event.apply(&mut aggregate2);

        assert_eq!(aggregate1.value, aggregate2.value);
    }
}
