//! Utility functions for commands

use crate::error::{CliError, Result};

/// Validate that a name is in `PascalCase`
pub fn validate_pascal_case(name: &str) -> Result<()> {
    if name.is_empty() {
        return Err(CliError::Other("Name cannot be empty".to_string()));
    }

    // Must start with uppercase letter
    if !name.chars().next().unwrap().is_uppercase() {
        return Err(CliError::Other(format!(
            "Name must start with uppercase letter: {name}"
        )));
    }

    // Must contain only alphanumeric characters
    if !name.chars().all(char::is_alphanumeric) {
        return Err(CliError::Other(format!(
            "Name must contain only alphanumeric characters: {name}"
        )));
    }

    Ok(())
}

/// Convert `PascalCase` to `snake_case`
pub fn to_snake_case(name: &str) -> String {
    let mut result = String::new();
    let mut prev_was_lowercase = false;

    for (i, ch) in name.chars().enumerate() {
        if ch.is_uppercase() {
            if i > 0 && prev_was_lowercase {
                result.push('_');
            }
            result.push(ch.to_ascii_lowercase());
            prev_was_lowercase = false;
        } else {
            result.push(ch);
            prev_was_lowercase = true;
        }
    }

    result
}

/// Validate project name (alphanumeric, hyphens, underscores)
pub fn validate_project_name(name: &str) -> Result<()> {
    if name.is_empty() {
        return Err(CliError::InvalidProjectName(
            "Project name cannot be empty".to_string(),
        ));
    }

    // Must start with alphanumeric
    if !name.chars().next().unwrap().is_alphanumeric() {
        return Err(CliError::InvalidProjectName(format!(
            "Project name must start with alphanumeric character: {name}"
        )));
    }

    // Can contain alphanumeric, hyphens, and underscores
    if !name
        .chars()
        .all(|c| c.is_alphanumeric() || c == '-' || c == '_')
    {
        return Err(CliError::InvalidProjectName(format!(
            "Project name can only contain alphanumeric characters, hyphens, and underscores: {name}"
        )));
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_validate_pascal_case_valid() {
        assert!(validate_pascal_case("Counter").is_ok());
        assert!(validate_pascal_case("BankAccount").is_ok());
        assert!(validate_pascal_case("OrderPlaced").is_ok());
    }

    #[test]
    fn test_validate_pascal_case_invalid() {
        assert!(validate_pascal_case("counter").is_err()); // lowercase start
        assert!(validate_pascal_case("bank_account").is_err()); // underscore
        assert!(validate_pascal_case("order-placed").is_err()); // hyphen
        assert!(validate_pascal_case("").is_err()); // empty
    }

    #[test]
    fn test_to_snake_case() {
        assert_eq!(to_snake_case("Counter"), "counter");
        assert_eq!(to_snake_case("BankAccount"), "bank_account");
        assert_eq!(to_snake_case("OrderPlaced"), "order_placed");
        assert_eq!(to_snake_case("HTTPServer"), "httpserver");
        assert_eq!(to_snake_case("UserID"), "user_id");
        assert_eq!(to_snake_case("API"), "api");
    }

    #[test]
    fn test_validate_project_name_valid() {
        assert!(validate_project_name("my-project").is_ok());
        assert!(validate_project_name("my_project").is_ok());
        assert!(validate_project_name("myproject").is_ok());
        assert!(validate_project_name("my-project-123").is_ok());
    }

    #[test]
    fn test_validate_project_name_invalid() {
        assert!(validate_project_name("").is_err()); // empty
        assert!(validate_project_name("-project").is_err()); // starts with hyphen
        assert!(validate_project_name("my project").is_err()); // space
        assert!(validate_project_name("my.project").is_err()); // dot
    }
}
