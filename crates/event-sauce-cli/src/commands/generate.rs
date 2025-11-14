//! Generate command - Code generation for aggregates, events, and projections

use crate::error::{CliError, Result};
use console::style;
use std::path::Path;

/// Generate an aggregate boilerplate
///
/// # Errors
///
/// Returns an error if:
/// - The aggregate name is invalid (not `PascalCase`)
/// - File system operations fail
pub fn aggregate(name: &str, output: Option<&str>) -> Result<()> {
    super::utils::validate_pascal_case(name)
        .map_err(|_| CliError::InvalidAggregateName(name.to_string()))?;

    let snake_name = super::utils::to_snake_case(name);
    let default_path = format!("src/{snake_name}.rs");
    let output_path = output.unwrap_or(&default_path);

    println!(
        "{} aggregate {} to {}",
        style("Generating").green().bold(),
        style(name).cyan().bold(),
        style(output_path).yellow()
    );

    let content = generate_aggregate_code(name);

    // Sync write since this is quick
    std::fs::write(output_path, content)?;

    println!("{}", style("✓ Generated aggregate").green());
    println!();
    println!("Next steps:");
    println!("  1. Add your business logic methods");
    println!("  2. Define events in {name}Event enum");
    println!("  3. Implement the apply method");
    println!("  4. Write tests");

    Ok(())
}

/// Generate event types boilerplate
///
/// # Errors
///
/// Returns an error if:
/// - The event name is invalid (not `PascalCase`)
/// - The aggregate name is invalid (not `PascalCase`)
/// - File system operations fail
pub fn event(name: &str, aggregate: &str, output: Option<&str>) -> Result<()> {
    super::utils::validate_pascal_case(name)
        .map_err(|_| CliError::InvalidEventName(name.to_string()))?;
    super::utils::validate_pascal_case(aggregate)
        .map_err(|_| CliError::InvalidAggregateName(aggregate.to_string()))?;

    let snake_name = super::utils::to_snake_case(name);
    let default_path = format!("src/events/{snake_name}.rs");
    let output_path = output.unwrap_or(&default_path);

    // Create events directory if it doesn't exist
    if let Some(parent) = Path::new(output_path).parent() {
        std::fs::create_dir_all(parent)?;
    }

    println!(
        "{} event {} for aggregate {} to {}",
        style("Generating").green().bold(),
        style(name).cyan().bold(),
        style(aggregate).yellow(),
        style(output_path).yellow()
    );

    let content = generate_event_code(name, aggregate);

    std::fs::write(output_path, content)?;

    println!("{}", style("✓ Generated event").green());
    println!();
    println!("Next steps:");
    println!("  1. Add event variants with data");
    println!("  2. Update {aggregate} aggregate to use these events");
    println!("  3. Write tests");

    Ok(())
}

/// Generate projection boilerplate
///
/// # Errors
///
/// Returns an error if:
/// - The projection name is invalid (not `PascalCase`)
/// - File system operations fail
pub fn projection(name: &str, events: &str, output: Option<&str>) -> Result<()> {
    super::utils::validate_pascal_case(name)
        .map_err(|_| CliError::InvalidProjectionName(name.to_string()))?;

    let snake_name = super::utils::to_snake_case(name);
    let default_path = format!("src/projections/{snake_name}.rs");
    let output_path = output.unwrap_or(&default_path);

    // Create projections directory if it doesn't exist
    if let Some(parent) = Path::new(output_path).parent() {
        std::fs::create_dir_all(parent)?;
    }

    let event_list: Vec<&str> = events.split(',').map(str::trim).collect();

    println!(
        "{} projection {} to {}",
        style("Generating").green().bold(),
        style(name).cyan().bold(),
        style(output_path).yellow()
    );

    let content = generate_projection_code(name, &event_list);

    std::fs::write(output_path, content)?;

    println!("{}", style("✓ Generated projection").green());
    println!();
    println!("Next steps:");
    println!("  1. Implement handle_event for each event type");
    println!("  2. Define your projection state");
    println!("  3. Add persistence if needed");
    println!("  4. Write tests");

    Ok(())
}

fn generate_aggregate_code(name: &str) -> String {
    let snake_name = super::utils::to_snake_case(name);
    let id_type = format!("{name}Id");

    format!(
        "//! {name} aggregate

use event_sauce::prelude::*;
use serde::{{Deserialize, Serialize}};
use uuid::Uuid;

/// Unique identifier for {name}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct {id_type}(Uuid);

impl {id_type} {{
    /// Create a new ID
    pub fn new() -> Self {{
        Self(Uuid::new_v4())
    }}
}}

impl Default for {id_type} {{
    fn default() -> Self {{
        Self::new()
    }}
}}

impl std::fmt::Display for {id_type} {{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {{
        write!(f, \"{{}}\", self.0)
    }}
}}

/// {name} aggregate
#[derive(Aggregate, Debug, Clone)]
#[aggregate(id = \"{id_type}\", event = \"{name}Event\")]
pub struct {name} {{
    #[aggregate_id]
    id: {id_type},

    #[aggregate_version]
    version: i64,

    #[aggregate_events]
    pending_events: Vec<{name}Event>,

    // Add your state fields here
}}

/// Events for {name}
#[derive(Event, Debug, Clone, Serialize, Deserialize)]
#[event(aggregate = \"{name}\", version = 1)]
pub enum {name}Event {{
    // Add your event variants here
    // Example:
    // Created {{ name: String }},
    // Updated {{ value: i32 }},
}}

impl {name} {{
    /// Create a new {name} instance
    pub fn new(id: {id_type}) -> Self {{
        Self {{
            id,
            version: 0,
            pending_events: Vec::new(),
        }}
    }}

    // Add your command methods here
    // Example:
    // pub fn create(&mut self, name: String) -> Result<(), {name}Error> {{
    //     let event = {name}Event::Created {{ name }};
    //     self.apply(&event);
    //     self.pending_events.push(event);
    //     Ok(())
    // }}
}}

/// Error type for {name}
#[derive(Debug, thiserror::Error)]
pub enum {name}Error {{
    #[error(\"Invalid state\")]
    InvalidState,
}}

#[cfg(test)]
mod tests {{
    use super::*;

    #[test]
    fn test_{snake_name}_creation() {{
        let id = {id_type}::new();
        let {snake_name} = {name}::new(id);

        assert_eq!({snake_name}.id(), id);
        assert_eq!({snake_name}.version(), 0);
    }}
}}
"
    )
}

fn generate_event_code(name: &str, aggregate: &str) -> String {
    let snake_name = super::utils::to_snake_case(name);
    format!(
        "//! {name} events

use event_sauce::prelude::*;
use serde::{{Deserialize, Serialize}};

/// {name} event
#[derive(Event, Debug, Clone, Serialize, Deserialize)]
#[event(aggregate = \"{aggregate}\", version = 1)]
pub enum {name} {{
    // Add your event variants here
    // Example:
    // Created {{ id: Uuid, name: String }},
    // Updated {{ value: i32 }},
    // Deleted,
}}

#[cfg(test)]
mod tests {{
    use super::*;

    #[test]
    fn test_{snake_name}_serialization() {{
        // Add serialization tests here
    }}
}}
"
    )
}

fn generate_projection_code(name: &str, events: &[&str]) -> String {
    let event_handlers = events
        .iter()
        .map(|event| {
            format!(
                "        // {event}Event::SomeVariant {{ .. }} => {{
        //     // Handle {event} event
        // }},"
            )
        })
        .collect::<Vec<_>>()
        .join("\n");

    let snake_name = super::utils::to_snake_case(name);
    format!(
        "//! {name} projection

use event_sauce::prelude::*;
use serde::{{Deserialize, Serialize}};
use std::sync::Arc;
use tokio::sync::RwLock;

/// {name} projection state
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct {name}State {{
    // Add your projection state fields here
}}

/// {name} projection
pub struct {name} {{
    state: Arc<RwLock<{name}State>>,
}}

impl {name} {{
    /// Create a new projection
    pub fn new() -> Self {{
        Self {{
            state: Arc::new(RwLock::new({name}State::default())),
        }}
    }}

    /// Get a snapshot of the current state
    pub async fn state(&self) -> {name}State {{
        self.state.read().await.clone()
    }}
}}

impl Default for {name} {{
    fn default() -> Self {{
        Self::new()
    }}
}}

#[async_trait::async_trait]
impl Projection for {name} {{
    type Error = anyhow::Error;

    async fn handle_event(&mut self, event: &EventEnvelope) -> Result<(), Self::Error> {{
        // Parse and handle events
        // match event.event_type.as_str() {{
{event_handlers}
        //     _ => {{}}, // Ignore unknown events
        // }}

        Ok(())
    }}
}}

#[cfg(test)]
mod tests {{
    use super::*;

    #[tokio::test]
    async fn test_{snake_name}_creation() {{
        let projection = {name}::new();
        let state = projection.state().await;

        // Add assertions
    }}

    #[tokio::test]
    async fn test_{snake_name}_handles_events() {{
        let mut projection = {name}::new();

        // Create test event envelope
        // let envelope = EventEnvelope {{ ... }};
        // projection.handle_event(&envelope).await.unwrap();

        // Add assertions
    }}
}}
"
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_aggregate_generation() {
        let temp_dir = TempDir::new().unwrap();
        let output_path = temp_dir.path().join("counter.rs");

        aggregate("Counter", Some(output_path.to_str().unwrap())).unwrap();

        assert!(output_path.exists());
        let content = std::fs::read_to_string(&output_path).unwrap();
        assert!(content.contains("pub struct Counter"));
        assert!(content.contains("pub enum CounterEvent"));
        assert!(content.contains("#[derive(Aggregate"));
    }

    #[test]
    fn test_aggregate_invalid_name() {
        let result = aggregate("invalid_name", None);
        assert!(result.is_err());
        match result.unwrap_err() {
            CliError::InvalidAggregateName(_) => {}
            _ => panic!("Expected InvalidAggregateName error"),
        }
    }

    #[test]
    fn test_event_generation() {
        let temp_dir = TempDir::new().unwrap();
        let output_path = temp_dir.path().join("order_placed.rs");

        event(
            "OrderPlaced",
            "Order",
            Some(output_path.to_str().unwrap()),
        )
        .unwrap();

        assert!(output_path.exists());
        let content = std::fs::read_to_string(&output_path).unwrap();
        assert!(content.contains("pub enum OrderPlaced"));
        assert!(content.contains(r#"aggregate = "Order""#));
    }

    #[test]
    fn test_event_invalid_name() {
        let result = event("invalid_event", "Order", None);
        assert!(result.is_err());
    }

    #[test]
    fn test_projection_generation() {
        let temp_dir = TempDir::new().unwrap();
        let output_path = temp_dir.path().join("user_list.rs");

        projection(
            "UserList",
            "UserCreated,UserUpdated",
            Some(output_path.to_str().unwrap()),
        )
        .unwrap();

        assert!(output_path.exists());
        let content = std::fs::read_to_string(&output_path).unwrap();
        assert!(content.contains("pub struct UserList"));
        assert!(content.contains("impl Projection for UserList"));
    }

    #[test]
    fn test_projection_invalid_name() {
        let result = projection("invalid-projection", "Event1", None);
        assert!(result.is_err());
    }
}
