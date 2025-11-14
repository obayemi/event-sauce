//! # event-sauce CLI
//!
//! Command-line tool for managing event-sauce projects.
//!
//! ## Commands
//!
//! - `init` - Initialize a new event-sauce project
//! - `generate` - Generate code (aggregates, events, projections)
//! - `db` - Database management commands

#![deny(clippy::all)]
#![warn(clippy::pedantic)]

mod commands;
mod error;

use clap::{Parser, Subcommand};
use error::Result;

/// event-sauce CLI - Event sourcing toolkit for Rust
#[derive(Parser, Debug)]
#[command(
    name = "event-sauce",
    author,
    version,
    about = "Event sourcing toolkit for Rust",
    long_about = "A professional event sourcing library with CLI tools for project scaffolding, code generation, and database management."
)]
struct Cli {
    /// Subcommand to run
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand, Debug)]
enum Commands {
    /// Initialize a new event-sauce project
    Init {
        /// Project name
        name: String,

        /// Project directory (defaults to project name)
        #[arg(short, long)]
        path: Option<String>,

        /// Database backend to use
        #[arg(short, long, default_value = "postgres")]
        backend: String,
    },

    /// Generate code (aggregates, events, projections)
    #[command(subcommand, alias = "gen")]
    Generate(GenerateCommand),

    /// Database management commands
    #[command(subcommand)]
    Db(DbCommand),
}

#[derive(Subcommand, Debug)]
enum GenerateCommand {
    /// Generate an aggregate
    Aggregate {
        /// Aggregate name (`PascalCase`)
        name: String,

        /// Output file path (optional, defaults to `src/<snake_case>.rs`)
        #[arg(short, long)]
        output: Option<String>,
    },

    /// Generate event types
    Event {
        /// Event name (`PascalCase`)
        name: String,

        /// Associated aggregate name
        #[arg(short, long)]
        aggregate: String,

        /// Output file path (optional)
        #[arg(short, long)]
        output: Option<String>,
    },

    /// Generate a projection
    Projection {
        /// Projection name (`PascalCase`)
        name: String,

        /// Event types to handle (comma-separated)
        #[arg(short, long)]
        events: String,

        /// Output file path (optional)
        #[arg(short, long)]
        output: Option<String>,
    },
}

#[derive(Subcommand, Debug)]
enum DbCommand {
    /// Initialize database schema
    Init {
        /// Database backend
        #[arg(short, long, default_value = "postgres")]
        backend: String,

        /// Database URL (optional, reads from `DATABASE_URL` env)
        #[arg(short, long)]
        url: Option<String>,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize tracing for better error reporting
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let cli = Cli::parse();

    match cli.command {
        Commands::Init { name, path, backend } => {
            commands::init::run(&name, path.as_deref(), &backend).await?;
        }
        Commands::Generate(gen_cmd) => match gen_cmd {
            GenerateCommand::Aggregate { name, output } => {
                commands::generate::aggregate(&name, output.as_deref())?;
            }
            GenerateCommand::Event {
                name,
                aggregate,
                output,
            } => {
                commands::generate::event(&name, &aggregate, output.as_deref())?;
            }
            GenerateCommand::Projection {
                name,
                events,
                output,
            } => {
                commands::generate::projection(&name, &events, output.as_deref())?;
            }
        },
        Commands::Db(db_cmd) => match db_cmd {
            DbCommand::Init { backend, url } => {
                commands::db::init(&backend, url.as_deref())?;
            }
        },
    }

    Ok(())
}
