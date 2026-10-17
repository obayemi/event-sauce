use super::sealed::Boundary;
use super::stack::{effective, Frame};
use crate::EventMetadata;
use serde_json::{Map, Value};

pub(crate) fn current_snapshot() -> Option<EventMetadata> {
    effective(|frames| {
        let mut correlation_path: Vec<String> = frames
            .iter()
            .flat_map(|f| {
                f.frame
                    .correlation_path
                    .iter()
                    .chain(&f.frame.correlation_id)
            })
            .cloned()
            .collect();
        correlation_path.dedup();
        let additional: Map<String, Value> = frames
            .iter()
            .flat_map(|f| f.frame.additional.clone())
            .collect();
        let tenant = match frames.first().map(|f| &f.boundary) {
            Some(Boundary::Root { tenant }) => tenant.clone(),
            _ => None,
        };
        let command = frames.iter().rev().find_map(|f| f.frame.command.clone());
        let is_empty = tenant.is_none()
            && command.is_none()
            && correlation_path.is_empty()
            && additional.is_empty();
        (!is_empty).then(|| EventMetadata {
            correlation_id: correlation_path.first().cloned(),
            tenant,
            command,
            correlation_path,
            additional: (!additional.is_empty()).then_some(additional),
            ..EventMetadata::new()
        })
    })
}

pub(crate) fn has_correlation_id() -> bool {
    effective(|frames| {
        frames
            .iter()
            .any(|f| f.frame.correlation_id.is_some() || !f.frame.correlation_path.is_empty())
    })
}

impl From<EventMetadata> for Frame {
    fn from(metadata: EventMetadata) -> Self {
        Self {
            correlation_id: None,
            correlation_path: metadata.correlation_path,
            command: metadata.command,
            additional: metadata.additional.unwrap_or_default(),
        }
    }
}
