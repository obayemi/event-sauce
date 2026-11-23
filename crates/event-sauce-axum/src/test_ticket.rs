//! An aggregate the unit tests of the path extractors share.

use event_sauce_core::{
    aggregate_command, define_events, Aggregate, AggregateError, AggregateRoot, Entity, EntityId,
};
use serde::{Deserialize, Serialize};

#[derive(Debug, thiserror::Error)]
#[error("ticket error")]
pub struct TicketError;

impl AggregateError for TicketError {}

#[derive(Debug, Serialize, Deserialize)]
pub struct Ticket {
    id: EntityId,
    pub owner: EntityId,
}

impl Entity for Ticket {
    fn entity_id(&self) -> EntityId {
        self.id
    }
}

impl Aggregate for Ticket {
    type Event = TicketEvent;
    type Error = TicketError;
    type DeletedState = Self;
}

define_events! {
    pub enum TicketEvent for Ticket {
        Opened {
            owner: EntityId,
        }
        @init
        => |id, event| Ticket { id, owner: event.owner },

        Touched {}
        => |_ticket, _event| {},
    }
}

aggregate_command! {
    impl Ticket {
        @clock @init fn open(owner: EntityId) -> OpenedEvent { owner };
    }
}

pub fn ticket_of(owner: EntityId) -> AggregateRoot<Ticket> {
    Ticket::open(owner).unwrap()
}

impl crate::PathAggregate for Ticket {}
