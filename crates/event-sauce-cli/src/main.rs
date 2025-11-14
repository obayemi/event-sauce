//! # event-sauce CLI
//!
//! Command-line tool for managing event-sauce projects.

#![deny(clippy::all)]
#![warn(clippy::pedantic)]

use clap::Parser;

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
struct Cli {
    /// Subcommand to run
    #[command(subcommand)]
    command: Option<Commands>,
}

#[derive(clap::Subcommand)]
enum Commands {
    /// Initialize a new event-sauce project
    Init {
        /// Project name
        name: String,
    },
}

fn main() {
    let _cli = Cli::parse();
    println!("event-sauce CLI - Coming soon!");
}
