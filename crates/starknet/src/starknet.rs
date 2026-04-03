use crate::event::{EmittedEvent, Event};
use crate::Felt;
use starknet::core::types::Felt as SnFelt;
use starknet::core::types::{EmittedEvent as SnEmittedEvent, Event as SnEvent};

impl From<SnFelt> for Felt {
    fn from(value: SnFelt) -> Self {
        Self(value.to_bytes_be())
    }
}

impl From<&SnFelt> for Felt {
    fn from(value: &SnFelt) -> Self {
        Self(value.to_bytes_be())
    }
}

impl From<Felt> for SnFelt {
    fn from(value: Felt) -> Self {
        Self::from_bytes_be(&value.0)
    }
}
impl From<&Felt> for SnFelt {
    fn from(value: &Felt) -> Self {
        Self::from_bytes_be(&value.0)
    }
}

impl From<SnEvent> for Event {
    fn from(value: SnEvent) -> Self {
        Self {
            from_address: value.from_address.into(),
            keys: value.keys.into_iter().map(Into::into).collect(),
            data: value.data.into_iter().map(Into::into).collect(),
        }
    }
}

impl From<SnEmittedEvent> for EmittedEvent {
    fn from(value: SnEmittedEvent) -> Self {
        Self {
            from_address: value.from_address.into(),
            keys: value.keys.into_iter().map(Into::into).collect(),
            data: value.data.into_iter().map(Into::into).collect(),
            block_hash: value.block_hash.map(Into::into),
            block_number: value.block_number,
            transaction_hash: value.transaction_hash.into(),
        }
    }
}

impl From<Event> for SnEvent {
    fn from(value: Event) -> Self {
        Self {
            from_address: value.from_address.into(),
            keys: value.keys.into_iter().map(Into::into).collect(),
            data: value.data.into_iter().map(Into::into).collect(),
        }
    }
}

impl From<EmittedEvent> for SnEmittedEvent {
    fn from(value: EmittedEvent) -> Self {
        Self {
            from_address: value.from_address.into(),
            keys: value.keys.into_iter().map(Into::into).collect(),
            data: value.data.into_iter().map(Into::into).collect(),
            block_hash: value.block_hash.map(Into::into),
            block_number: value.block_number,
            transaction_hash: value.transaction_hash.into(),
        }
    }
}
