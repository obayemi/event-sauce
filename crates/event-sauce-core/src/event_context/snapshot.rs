use super::sealed::Boundary;
use super::stack::{effective, Frame};
use serde_json::{Map, Value};

#[derive(Debug, Clone, Default, PartialEq)]
pub(crate) struct ContextSnapshot {
    pub tenant: Option<String>,
    pub correlation_path: Vec<String>,
    pub command: Option<String>,
    pub additional: Map<String, Value>,
}

impl ContextSnapshot {
    #[cfg(test)]
    pub fn correlation_id(&self) -> Option<&str> {
        self.correlation_path.first().map(String::as_str)
    }
}

impl From<ContextSnapshot> for crate::EventMetadata {
    fn from(snapshot: ContextSnapshot) -> Self {
        Self {
            correlation_id: snapshot.correlation_path.first().cloned(),
            tenant: snapshot.tenant,
            command: snapshot.command,
            correlation_path: snapshot.correlation_path,
            additional: (!snapshot.additional.is_empty()).then_some(snapshot.additional),
            ..Self::new()
        }
    }
}

pub(crate) fn current_snapshot() -> Option<ContextSnapshot> {
    let snapshot = effective(|frames| {
        let mut correlation_path: Vec<String> = Vec::new();
        for id in frames
            .iter()
            .map(|f| &f.frame)
            .flat_map(|f| f.correlation_path.iter().chain(f.correlation_id.iter()))
        {
            if correlation_path.last() != Some(id) {
                correlation_path.push(id.clone());
            }
        }
        let mut additional = Map::new();
        for frame in frames {
            additional.extend(frame.frame.additional.clone());
        }
        ContextSnapshot {
            tenant: match frames.first().map(|f| &f.boundary) {
                Some(Boundary::Root { tenant }) => tenant.clone(),
                _ => None,
            },
            correlation_path,
            command: frames.iter().rev().find_map(|f| f.frame.command.clone()),
            additional,
        }
    });
    (snapshot != ContextSnapshot::default()).then_some(snapshot)
}

/// Merges the ambient context into `metadata`.
///
/// Explicit metadata wins field by field and the context fills the gaps;
/// outside any scope `metadata` is returned unchanged.
pub(crate) fn with_ambient(metadata: Option<crate::EventMetadata>) -> Option<crate::EventMetadata> {
    match (metadata, current_snapshot()) {
        (explicit, None) => explicit,
        (None, Some(ambient)) => Some(ambient.into()),
        (Some(mut explicit), Some(ambient)) => {
            explicit.fill_missing_from(&ambient.into());
            Some(explicit)
        }
    }
}

pub(crate) fn has_correlation_id() -> bool {
    effective(|frames| {
        frames
            .iter()
            .any(|f| f.frame.correlation_id.is_some() || !f.frame.correlation_path.is_empty())
    })
}

impl From<ContextSnapshot> for Frame {
    fn from(snapshot: ContextSnapshot) -> Self {
        Self {
            correlation_id: None,
            correlation_path: snapshot.correlation_path,
            command: snapshot.command,
            additional: snapshot.additional,
        }
    }
}
