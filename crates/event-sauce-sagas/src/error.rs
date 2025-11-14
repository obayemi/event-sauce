//! Error types for saga and process manager operations

use thiserror::Error;

/// Result type for saga operations
pub type Result<T> = std::result::Result<T, Error>;

/// Errors that can occur during saga execution
#[derive(Error, Debug)]
pub enum Error {
    /// Saga execution failed
    #[error("Saga execution failed: {0}")]
    ExecutionFailed(String),

    /// Compensation failed
    #[error("Compensation failed: {0}")]
    CompensationFailed(String),

    /// Process manager state transition invalid
    #[error("Invalid state transition from {from} to {to}: {reason}")]
    InvalidStateTransition {
        /// Current state
        from: String,
        /// Target state
        to: String,
        /// Reason for failure
        reason: String,
    },

    /// Command execution failed
    #[error("Command execution failed: {0}")]
    CommandFailed(String),

    /// Event store error
    #[error("Event store error: {0}")]
    EventStore(#[from] event_sauce_core::Error),

    /// Serialization error
    #[error("Serialization error: {0}")]
    Serialization(#[from] serde_json::Error),

    /// Timeout error
    #[error("Operation timed out: {0}")]
    Timeout(String),

    /// Generic error
    #[error("{0}")]
    Other(String),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_display() {
        let err = Error::ExecutionFailed("test error".into());
        assert_eq!(err.to_string(), "Saga execution failed: test error");

        let err = Error::InvalidStateTransition {
            from: "Started".into(),
            to: "Completed".into(),
            reason: "missing step".into(),
        };
        assert_eq!(
            err.to_string(),
            "Invalid state transition from Started to Completed: missing step"
        );
    }

    #[test]
    fn test_error_conversion() {
        let core_err = event_sauce_core::Error::ConcurrencyConflict {
            expected: 1.into(),
            actual: 2.into(),
        };
        let saga_err: Error = core_err.into();
        assert!(matches!(saga_err, Error::EventStore(_)));
    }
}
