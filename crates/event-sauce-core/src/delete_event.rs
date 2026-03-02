//! Delete event trait for aggregate termination.
//!
//! The `DeleteEvent` trait is the counterpart to `ApplyEvent` for events
//! that terminate an aggregate. While `ApplyEvent` mutates an existing
//! aggregate, `DeleteEvent` consumes it and produces a `DeletedState`.

use crate::Aggregate;

/// Trait for events that delete/terminate an aggregate.
///
/// This is used with the type-state pattern: an initialized aggregate
/// (`AggregateRoot<A>`) transitions to a deleted aggregate
/// (`DeletedAggregateRoot<A>`) by applying a delete event.
///
/// # Lifecycle
///
/// 1. `validate_delete()` — Pre-condition check (aggregate + event data)
/// 2. `delete()` — Consumes the aggregate and produces `DeletedState`
/// 3. `post_validate_delete()` — Invariant check on the deleted state
///
/// # Examples
///
/// ```ignore
/// use event_sauce_core::{Aggregate, DeleteEvent};
///
/// struct AccountClosedEvent {
///     reason: String,
/// }
///
/// impl DeleteEvent<Account> for AccountClosedEvent {
///     fn validate_delete(&self, account: &Account) -> Result<(), AccountError> {
///         if account.balance != 0 {
///             return Err(AccountError::NonZeroBalance);
///         }
///         Ok(())
///     }
///
///     fn delete(&self, mut account: Account) -> Account {
///         account.email.clear(); // PII cleanup
///         account
///     }
/// }
/// ```
pub trait DeleteEvent<A: Aggregate> {
    /// Pre-condition validation (aggregate state + event data).
    ///
    /// # Errors
    ///
    /// Returns an error if the aggregate cannot be deleted.
    fn validate_delete(&self, _aggregate: &A) -> Result<(), A::Error> {
        Ok(())
    }

    /// Consumes the aggregate and produces the deleted state.
    ///
    /// The default implementation uses the `Into<DeletedState>` conversion
    /// from the `Aggregate` supertrait bound. When `DeletedState = Self`,
    /// this is the identity conversion.
    fn delete(&self, aggregate: A) -> A::DeletedState {
        aggregate.into()
    }

    /// Post-condition validation on the deleted state.
    ///
    /// # Errors
    ///
    /// Returns an error if the deleted state violates invariants.
    fn post_validate_delete(&self, _state: &A::DeletedState) -> Result<(), A::Error> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{AggregateError, Entity, EntityId};

    #[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
    struct Account {
        id: EntityId,
        email: String,
        balance: i64,
    }

    impl Entity for Account {
        fn new(id: EntityId) -> Self {
            Self {
                id,
                email: String::new(),
                balance: 0,
            }
        }
        fn entity_id(&self) -> EntityId {
            self.id
        }
    }

    impl crate::DefaultEntity for Account {}

    #[derive(Debug, thiserror::Error)]
    enum AccountError {
        #[error("Non-zero balance")]
        NonZeroBalance,
    }

    impl AggregateError for AccountError {}

    #[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
    enum AccountEvent {
        Closed { reason: String },
    }

    impl crate::DomainEvent for AccountEvent {
        type Aggregate = Account;
        fn event_type(&self) -> &'static str {
            "Account.Closed"
        }
        fn event_version(&self) -> crate::EventVersion {
            crate::EventVersion::new(1)
        }
        fn occurred_at(&self) -> chrono::DateTime<chrono::Utc> {
            chrono::Utc::now()
        }
    }

    impl crate::EventApplicator<Account> for AccountEvent {
        fn dispatch(&self, _account: &mut Account) -> Result<(), AccountError> {
            Ok(())
        }
        fn dispatch_unchecked(&self, _account: &mut Account) {}
    }

    impl crate::Aggregate for Account {
        type Event = AccountEvent;
        type Error = AccountError;
        type DeletedState = Self;
    }

    struct ClosedEvent {
        reason: String,
    }

    impl DeleteEvent<Account> for ClosedEvent {
        fn validate_delete(&self, account: &Account) -> Result<(), AccountError> {
            if account.balance != 0 {
                return Err(AccountError::NonZeroBalance);
            }
            Ok(())
        }

        fn delete(&self, mut account: Account) -> Account {
            account.email = format!("closed: {}", self.reason);
            account
        }
    }

    #[test]
    fn test_delete_event_validate_succeeds() {
        let account = Account {
            id: EntityId::new(),
            email: "alice@example.com".to_string(),
            balance: 0,
        };
        let event = ClosedEvent {
            reason: "requested".to_string(),
        };
        assert!(event.validate_delete(&account).is_ok());
    }

    #[test]
    fn test_delete_event_validate_fails() {
        let account = Account {
            id: EntityId::new(),
            email: "alice@example.com".to_string(),
            balance: 100,
        };
        let event = ClosedEvent {
            reason: "requested".to_string(),
        };
        let result = event.validate_delete(&account);
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().to_string(), "Non-zero balance");
    }

    #[test]
    fn test_delete_event_transforms_state() {
        let account = Account {
            id: EntityId::new(),
            email: "alice@example.com".to_string(),
            balance: 0,
        };
        let event = ClosedEvent {
            reason: "gdpr".to_string(),
        };
        let deleted = event.delete(account);
        assert_eq!(deleted.email, "closed: gdpr");
    }

    #[test]
    fn test_delete_event_post_validate_default_succeeds() {
        let account = Account {
            id: EntityId::new(),
            email: String::new(),
            balance: 0,
        };
        let event = ClosedEvent {
            reason: "test".to_string(),
        };
        assert!(event.post_validate_delete(&account).is_ok());
    }

    // Test default delete() uses Into conversion
    struct DefaultDeleteEvent;

    impl DeleteEvent<Account> for DefaultDeleteEvent {}

    #[test]
    fn test_delete_event_default_uses_into() {
        let account = Account {
            id: EntityId::new(),
            email: "alice@example.com".to_string(),
            balance: 0,
        };
        let event = DefaultDeleteEvent;
        let deleted = event.delete(account);
        assert_eq!(deleted.email, "alice@example.com");
    }

    // Test with custom DeletedState
    #[derive(Debug)]
    struct DeletedAccount {
        id: EntityId,
    }

    #[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
    struct CustomAccount {
        id: EntityId,
        email: String,
    }

    impl Entity for CustomAccount {
        fn new(id: EntityId) -> Self {
            Self {
                id,
                email: String::new(),
            }
        }
        fn entity_id(&self) -> EntityId {
            self.id
        }
    }

    impl crate::DefaultEntity for CustomAccount {}

    impl From<CustomAccount> for DeletedAccount {
        fn from(account: CustomAccount) -> Self {
            DeletedAccount { id: account.id }
        }
    }

    #[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
    enum CustomAccountEvent {
        Closed,
    }

    impl crate::DomainEvent for CustomAccountEvent {
        type Aggregate = CustomAccount;
        fn event_type(&self) -> &'static str {
            "CustomAccount.Closed"
        }
        fn event_version(&self) -> crate::EventVersion {
            crate::EventVersion::new(1)
        }
        fn occurred_at(&self) -> chrono::DateTime<chrono::Utc> {
            chrono::Utc::now()
        }
    }

    #[derive(Debug, thiserror::Error)]
    #[error("custom account error")]
    struct CustomAccountError;

    impl AggregateError for CustomAccountError {}

    impl crate::EventApplicator<CustomAccount> for CustomAccountEvent {
        fn dispatch(&self, _account: &mut CustomAccount) -> Result<(), CustomAccountError> {
            Ok(())
        }
        fn dispatch_unchecked(&self, _account: &mut CustomAccount) {}
    }

    impl crate::Aggregate for CustomAccount {
        type Event = CustomAccountEvent;
        type Error = CustomAccountError;
        type DeletedState = DeletedAccount;
    }

    struct CustomDeleteEvent;

    impl DeleteEvent<CustomAccount> for CustomDeleteEvent {}

    #[test]
    fn test_delete_event_with_custom_deleted_state() {
        let account = CustomAccount {
            id: EntityId::new(),
            email: "alice@example.com".to_string(),
        };
        let id = account.id;
        let event = CustomDeleteEvent;
        let deleted = event.delete(account);
        assert_eq!(deleted.id, id);
    }
}
