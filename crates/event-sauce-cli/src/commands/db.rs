//! Database command - Initialize database schema

use crate::error::{CliError, Result};
use console::style;

/// Initialize database schema
///
/// # Errors
///
/// Returns an error if:
/// - The backend is unsupported (not postgres or memory)
pub fn init(backend: &str, _url: Option<&str>) -> Result<()> {
    if backend != "postgres" && backend != "memory" {
        return Err(CliError::UnsupportedBackend(backend.to_string()));
    }

    if backend == "memory" {
        println!(
            "{} {}",
            style("ℹ").blue().bold(),
            style("Memory backend doesn't require database initialization").yellow()
        );
        return Ok(());
    }

    println!(
        "{} database schema for {}...",
        style("Initializing").green().bold(),
        style(backend).cyan().bold()
    );

    // For now, just print the SQL schema that needs to be run
    // In the future, this could actually connect and run migrations
    print_postgres_schema();

    println!();
    println!("{}", style("✓ Schema displayed").green());
    println!();
    println!("To initialize your database, run the SQL above or use:");
    println!("  psql $DATABASE_URL < schema.sql");
    println!();
    println!("Or save to a file:");
    println!("  event-sauce db init --backend postgres > schema.sql");

    Ok(())
}

fn print_postgres_schema() {
    let schema = r"
-- event-sauce PostgreSQL Schema
-- This schema supports event sourcing with event streams, projections, and sagas

-- Events table - stores all domain events
CREATE TABLE IF NOT EXISTS events (
    event_id UUID PRIMARY KEY,
    stream_id VARCHAR(255) NOT NULL,
    stream_type VARCHAR(100) NOT NULL,
    event_type VARCHAR(100) NOT NULL,
    event_version INTEGER NOT NULL,
    aggregate_version BIGINT NOT NULL,
    data JSONB NOT NULL,
    metadata JSONB NOT NULL DEFAULT '{}',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Indexes for efficient querying
CREATE INDEX IF NOT EXISTS idx_events_stream
    ON events(stream_id, aggregate_version);

CREATE INDEX IF NOT EXISTS idx_events_stream_type
    ON events(stream_type, created_at);

CREATE INDEX IF NOT EXISTS idx_events_type
    ON events(event_type, created_at);

CREATE INDEX IF NOT EXISTS idx_events_created_at
    ON events(created_at DESC);

-- Unique constraint for optimistic concurrency control
CREATE UNIQUE INDEX IF NOT EXISTS idx_events_stream_version
    ON events(stream_id, aggregate_version);

-- Snapshots table - stores aggregate snapshots for performance
CREATE TABLE IF NOT EXISTS snapshots (
    stream_id VARCHAR(255) NOT NULL,
    aggregate_version BIGINT NOT NULL,
    data JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (stream_id, aggregate_version)
);

CREATE INDEX IF NOT EXISTS idx_snapshots_stream
    ON snapshots(stream_id, aggregate_version DESC);

-- Projections checkpoints - tracks projection progress
CREATE TABLE IF NOT EXISTS projection_checkpoints (
    projection_name VARCHAR(255) PRIMARY KEY,
    last_event_id UUID,
    last_processed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    metadata JSONB NOT NULL DEFAULT '{}'
);

-- Sagas state - tracks saga execution state (optional)
CREATE TABLE IF NOT EXISTS saga_state (
    saga_id UUID PRIMARY KEY,
    saga_type VARCHAR(100) NOT NULL,
    state VARCHAR(50) NOT NULL,
    data JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_saga_state_type
    ON saga_state(saga_type, state);

-- Event notifications for pub/sub (PostgreSQL specific)
CREATE OR REPLACE FUNCTION notify_event() RETURNS TRIGGER AS $$
BEGIN
    PERFORM pg_notify(
        'event_sauce_events',
        json_build_object(
            'stream_id', NEW.stream_id,
            'stream_type', NEW.stream_type,
            'event_type', NEW.event_type,
            'event_id', NEW.event_id
        )::text
    );
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER event_notification
    AFTER INSERT ON events
    FOR EACH ROW
    EXECUTE FUNCTION notify_event();

-- Grant permissions (adjust as needed)
-- GRANT ALL ON events TO your_app_user;
-- GRANT ALL ON snapshots TO your_app_user;
-- GRANT ALL ON projection_checkpoints TO your_app_user;
-- GRANT ALL ON saga_state TO your_app_user;
";

    println!("{schema}");
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_init_postgres() {
        // Should succeed and print schema
        let result = init("postgres", None);
        assert!(result.is_ok());
    }

    #[test]
    fn test_init_memory() {
        // Should succeed with info message
        let result = init("memory", None);
        assert!(result.is_ok());
    }

    #[test]
    fn test_init_unsupported_backend() {
        let result = init("mysql", None);
        assert!(result.is_err());
        match result.unwrap_err() {
            CliError::UnsupportedBackend(_) => {}
            _ => panic!("Expected UnsupportedBackend error"),
        }
    }

    #[test]
    fn test_postgres_schema_generation() {
        // Just verify it doesn't panic
        print_postgres_schema();
    }
}
