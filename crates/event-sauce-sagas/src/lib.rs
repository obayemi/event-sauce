//! # event-sauce-sagas
//!
//! Saga and process manager patterns for long-running workflows in event-sourced systems.
//!
//! This crate provides two coordination patterns:
//!
//! ## Saga Pattern (Choreography)
//!
//! Sagas provide decentralized coordination where each step reacts to events:
//!
//! ```
//! use event_sauce_sagas::{Saga, Result};
//! use event_sauce_core::EventEnvelope;
//! use async_trait::async_trait;
//!
//! struct PaymentSaga {
//!     // saga state
//! }
//!
//! #[async_trait]
//! impl Saga for PaymentSaga {
//!     fn interested_in(&self, event: &EventEnvelope) -> bool {
//!         event.event_type.starts_with("Order")
//!     }
//!
//!     async fn handle(&mut self, event: &EventEnvelope) -> Result<()> {
//!         // React to event
//!         Ok(())
//!     }
//!
//!     async fn compensate(&mut self, event: &EventEnvelope) -> Result<()> {
//!         // Rollback changes
//!         Ok(())
//!     }
//! }
//! ```
//!
//! ## Process Manager Pattern (Orchestration)
//!
//! Process managers provide centralized coordination with explicit state:
//!
//! ```
//! use event_sauce_sagas::{ProcessManager, Command, Result};
//! use event_sauce_core::EventEnvelope;
//! use async_trait::async_trait;
//!
//! #[derive(Debug)]
//! enum OrderCommand {
//!     ProcessPayment,
//!     CreateShipment,
//! }
//!
//! impl Command for OrderCommand {}
//!
//! struct OrderFulfillmentProcess {
//!     state: String,
//!     completed: bool,
//! }
//!
//! #[async_trait]
//! impl ProcessManager for OrderFulfillmentProcess {
//!     type Command = OrderCommand;
//!
//!     async fn handle_event(&mut self, event: &EventEnvelope) -> Result<Vec<Self::Command>> {
//!         // Generate commands based on event
//!         Ok(vec![])
//!     }
//!
//!     fn is_complete(&self) -> bool {
//!         self.completed
//!     }
//! }
//! ```

#![deny(missing_docs)]
#![deny(clippy::all)]
#![warn(clippy::pedantic)]
#![allow(clippy::module_name_repetitions)]

mod error;
mod process_manager;
mod process_runner;
mod saga;
mod saga_runner;

pub use error::{Error, Result};
pub use process_manager::{Command, ProcessManager, ProcessState};
pub use process_runner::{CommandExecutor, ProcessRunner};
pub use saga::{Saga, SagaStep};
pub use saga_runner::SagaRunner;
