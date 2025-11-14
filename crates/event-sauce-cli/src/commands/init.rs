//! Initialize command - Create a new event-sauce project

use crate::error::{CliError, Result};
use console::style;
use indicatif::{ProgressBar, ProgressStyle};
use std::path::Path;
use tokio::fs;

/// Run the init command to create a new project
///
/// # Errors
///
/// Returns an error if:
/// - The project name is invalid
/// - The backend is unsupported
/// - The project directory already exists
/// - File system operations fail
///
/// # Panics
///
/// Panics if the progress bar template is invalid (this should never happen with the hardcoded template)
pub async fn run(name: &str, path: Option<&str>, backend: &str) -> Result<()> {
    super::utils::validate_project_name(name)?;

    // Validate backend
    if backend != "postgres" && backend != "memory" {
        return Err(CliError::UnsupportedBackend(backend.to_string()));
    }

    let project_path = path.unwrap_or(name);
    let project_dir = Path::new(project_path);

    // Check if directory already exists
    if project_dir.exists() {
        return Err(CliError::ProjectExists(project_path.to_string()));
    }

    println!(
        "{} {} project with {} backend...",
        style("Creating").green().bold(),
        style(name).cyan().bold(),
        style(backend).yellow()
    );

    let pb = ProgressBar::new(6);
    pb.set_style(
        ProgressStyle::default_bar()
            .template("{spinner:.green} [{bar:40.cyan/blue}] {pos}/{len} {msg}")
            .unwrap()
            .progress_chars("#>-"),
    );

    // Create project structure
    pb.set_message("Creating project structure");
    create_project_structure(project_dir, name, backend).await?;
    pb.inc(1);

    pb.set_message("Creating Cargo.toml");
    create_cargo_toml(project_dir, name, backend).await?;
    pb.inc(1);

    pb.set_message("Creating src/main.rs");
    create_main_rs(project_dir, name).await?;
    pb.inc(1);

    pb.set_message("Creating src/lib.rs");
    create_lib_rs(project_dir).await?;
    pb.inc(1);

    pb.set_message("Creating README.md");
    create_readme(project_dir, name, backend).await?;
    pb.inc(1);

    pb.set_message("Creating .gitignore");
    create_gitignore(project_dir).await?;
    pb.inc(1);

    pb.finish_with_message("Done!");

    println!();
    println!("{}", style("Project created successfully!").green().bold());
    println!();
    println!("Next steps:");
    println!("  cd {project_path}");
    println!("  cargo build");
    println!("  cargo test");
    if backend == "postgres" {
        println!();
        println!("Database setup:");
        println!("  export DATABASE_URL=postgres://user:password@localhost/dbname");
        println!("  event-sauce db init");
    }

    Ok(())
}

async fn create_project_structure(project_dir: &Path, _name: &str, _backend: &str) -> Result<()> {
    fs::create_dir_all(project_dir).await?;
    fs::create_dir_all(project_dir.join("src")).await?;
    fs::create_dir_all(project_dir.join("tests")).await?;
    fs::create_dir_all(project_dir.join("examples")).await?;
    Ok(())
}

async fn create_cargo_toml(project_dir: &Path, name: &str, backend: &str) -> Result<()> {
    let features = if backend == "postgres" {
        r#"default = ["postgres"]"#
    } else {
        r#"default = ["memory"]"#
    };

    let content = format!(
        "[package]
name = \"{name}\"
version = \"0.1.0\"
edition = \"2021\"

[dependencies]
event-sauce = {{ version = \"0.1\", features = [\"macros\", \"{backend}\"] }}
tokio = {{ version = \"1.48\", features = [\"full\"] }}
serde = {{ version = \"1.0\", features = [\"derive\"] }}
uuid = {{ version = \"1.18\", features = [\"v4\", \"serde\"] }}
anyhow = \"1.0\"
tracing = \"0.1\"
tracing-subscriber = {{ version = \"0.3\", features = [\"env-filter\"] }}

[dev-dependencies]
proptest = \"1.9\"

[features]
{features}
"
    );

    fs::write(project_dir.join("Cargo.toml"), content).await?;
    Ok(())
}

async fn create_main_rs(project_dir: &Path, name: &str) -> Result<()> {
    let content = format!(
        "//! {name} - Event Sourcing Application

use anyhow::Result;

#[tokio::main]
async fn main() -> Result<()> {{
    // Initialize tracing
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new(\"info\")),
        )
        .init();

    tracing::info!(\"Starting {name} application\");

    // Your application code here

    Ok(())
}}
"
    );

    fs::write(project_dir.join("src").join("main.rs"), content).await?;
    Ok(())
}

async fn create_lib_rs(project_dir: &Path) -> Result<()> {
    let content = r"//! Library code for your event-sourced application

#![deny(clippy::all)]
#![warn(clippy::pedantic)]

// Re-export event-sauce for convenience
pub use event_sauce::prelude::*;

// Your domain code goes here
";

    fs::write(project_dir.join("src").join("lib.rs"), content).await?;
    Ok(())
}

async fn create_readme(project_dir: &Path, name: &str, backend: &str) -> Result<()> {
    let db_section = if backend == "postgres" {
        r"
## Database Setup

This project uses PostgreSQL as the event store backend.

```bash
# Set database URL
export DATABASE_URL=postgres://user:password@localhost/dbname

# Initialize database schema
event-sauce db init
```
"
    } else {
        ""
    };

    let content = format!(
        "# {name}

Event-sourced application built with [event-sauce](https://github.com/yourusername/event-sauce).

## Getting Started

```bash
# Build the project
cargo build

# Run tests
cargo test

# Run the application
cargo run
```
{db_section}
## Project Structure

- `src/lib.rs` - Domain logic (aggregates, events)
- `src/main.rs` - Application entry point
- `tests/` - Integration tests
- `examples/` - Example usage

## Development

This project follows Test-Driven Development (TDD):

```bash
# Run tests with coverage
cargo llvm-cov --html
open target/llvm-cov/html/index.html

# Run clippy
cargo clippy -- -D warnings

# Format code
cargo fmt
```

## License

Licensed under MIT OR Apache-2.0
"
    );

    fs::write(project_dir.join("README.md"), content).await?;
    Ok(())
}

async fn create_gitignore(project_dir: &Path) -> Result<()> {
    let content = r"# Rust
/target/
**/*.rs.bk
*.pdb
Cargo.lock

# IDE
.vscode/
.idea/
*.swp
*.swo
*~

# OS
.DS_Store
Thumbs.db

# Environment
.env
.env.local

# Coverage
coverage/
*.profraw
*.profdata
lcov.info
";

    fs::write(project_dir.join(".gitignore"), content).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[tokio::test]
    async fn test_run_creates_project() {
        let temp_dir = TempDir::new().unwrap();
        let project_name = "test-project";
        let project_path = temp_dir.path().join(project_name);

        run(
            project_name,
            Some(project_path.to_str().unwrap()),
            "memory",
        )
        .await
        .unwrap();

        // Verify structure
        assert!(project_path.exists());
        assert!(project_path.join("src").exists());
        assert!(project_path.join("tests").exists());
        assert!(project_path.join("examples").exists());
        assert!(project_path.join("Cargo.toml").exists());
        assert!(project_path.join("src/main.rs").exists());
        assert!(project_path.join("src/lib.rs").exists());
        assert!(project_path.join("README.md").exists());
        assert!(project_path.join(".gitignore").exists());
    }

    #[tokio::test]
    async fn test_run_postgres_backend() {
        let temp_dir = TempDir::new().unwrap();
        let project_name = "postgres-project";
        let project_path = temp_dir.path().join(project_name);

        run(
            project_name,
            Some(project_path.to_str().unwrap()),
            "postgres",
        )
        .await
        .unwrap();

        // Verify Cargo.toml contains postgres feature
        let cargo_toml = fs::read_to_string(project_path.join("Cargo.toml"))
            .await
            .unwrap();
        assert!(cargo_toml.contains(r#"features = ["macros", "postgres"]"#));
    }

    #[tokio::test]
    async fn test_run_project_already_exists() {
        let temp_dir = TempDir::new().unwrap();
        let project_name = "existing-project";
        let project_path = temp_dir.path().join(project_name);

        // Create first time - should succeed
        run(
            project_name,
            Some(project_path.to_str().unwrap()),
            "memory",
        )
        .await
        .unwrap();

        // Create second time - should fail
        let result = run(
            project_name,
            Some(project_path.to_str().unwrap()),
            "memory",
        )
        .await;

        assert!(result.is_err());
        match result.unwrap_err() {
            CliError::ProjectExists(_) => {}
            _ => panic!("Expected ProjectExists error"),
        }
    }

    #[tokio::test]
    async fn test_run_invalid_project_name() {
        let temp_dir = TempDir::new().unwrap();
        let result = run("-invalid", Some(temp_dir.path().to_str().unwrap()), "memory").await;

        assert!(result.is_err());
        match result.unwrap_err() {
            CliError::InvalidProjectName(_) => {}
            _ => panic!("Expected InvalidProjectName error"),
        }
    }

    #[tokio::test]
    async fn test_run_unsupported_backend() {
        let temp_dir = TempDir::new().unwrap();
        let result = run(
            "test-project",
            Some(temp_dir.path().to_str().unwrap()),
            "mysql",
        )
        .await;

        assert!(result.is_err());
        match result.unwrap_err() {
            CliError::UnsupportedBackend(_) => {}
            _ => panic!("Expected UnsupportedBackend error"),
        }
    }
}
