//! The contexts the framework installs around the code it calls.

use super::{has_correlation_id, EventContext};
#[cfg(feature = "event-sourcing")]
use super::{Frame, Root};
#[cfg(feature = "event-sourcing")]
use crate::EventMetadata;

/// The context a command execution runs in: it names the command when it
/// has a name and starts a correlation id unless one is already active.
pub(crate) fn execution_context(command: Option<&str>) -> EventContext {
    let mut context = EventContext::new();
    if let Some(name) = command {
        context = context.command(name);
    }
    if !has_correlation_id() {
        context = context.correlation_id(uuid::Uuid::new_v4().to_string());
    }
    context
}

/// The root context a policy reacts in: it inherits the tenant and the
/// correlation path of the reaction `metadata`, and names the policy as
/// command.
#[cfg(feature = "event-sourcing")]
pub(crate) fn reaction_context(metadata: &EventMetadata, policy: &str) -> EventContext<Root> {
    let correlation_path = if metadata.correlation_path.is_empty() {
        metadata.correlation_id.iter().cloned().collect()
    } else {
        metadata.correlation_path.clone()
    };
    let frame = Frame {
        correlation_path,
        ..Frame::default()
    };
    EventContext::from_parts(
        frame,
        Root {
            tenant: metadata.tenant.clone(),
        },
    )
    .command(policy)
}

#[cfg(test)]
mod tests;
