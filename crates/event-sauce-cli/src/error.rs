//! Error types for the event-sauce CLI

use std::io;
use thiserror::Error;

/// Result type for CLI operations
pub type Result<T> = std::result::Result<T, CliError>;

/// CLI error types
#[derive(Error, Debug)]
pub enum CliError {
    /// IO error
    #[error("IO error: {0}")]
    Io(#[from] io::Error),

    /// Invalid project name
    #[error("Invalid project name: {0}")]
    InvalidProjectName(String),

    /// Invalid aggregate name
    #[error("Invalid aggregate name: {0} - must be PascalCase")]
    InvalidAggregateName(String),

    /// Invalid event name
    #[error("Invalid event name: {0} - must be PascalCase")]
    InvalidEventName(String),

    /// Invalid projection name
    #[error("Invalid projection name: {0} - must be PascalCase")]
    InvalidProjectionName(String),

    /// Project already exists
    #[error("Project directory already exists: {0}")]
    ProjectExists(String),

    /// Unsupported backend
    #[error("Unsupported backend: {0} - supported: postgres, memory")]
    UnsupportedBackend(String),

    /// Database error
    #[error("Database error: {0}")]
    #[allow(dead_code)]
    Database(String),

    /// Template error
    #[error("Template error: {0}")]
    #[allow(dead_code)]
    Template(String),

    /// Other error
    #[error("{0}")]
    Other(String),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_display() {
        let err = CliError::InvalidProjectName("123".to_string());
        assert_eq!(err.to_string(), "Invalid project name: 123");

        let err = CliError::InvalidAggregateName("bad_name".to_string());
        assert_eq!(
            err.to_string(),
            "Invalid aggregate name: bad_name - must be PascalCase"
        );

        let err = CliError::ProjectExists("/path/to/project".to_string());
        assert_eq!(
            err.to_string(),
            "Project directory already exists: /path/to/project"
        );
    }

    #[test]
    fn test_unsupported_backend_error() {
        let err = CliError::UnsupportedBackend("mysql".to_string());
        assert_eq!(
            err.to_string(),
            "Unsupported backend: mysql - supported: postgres, memory"
        );
    }

    #[test]
    fn test_io_error_conversion() {
        let io_err = io::Error::new(io::ErrorKind::NotFound, "file not found");
        let cli_err: CliError = io_err.into();
        assert!(cli_err.to_string().contains("IO error"));
    }
}
