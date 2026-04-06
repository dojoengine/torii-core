use anyhow::Result;
use async_trait::async_trait;
use starknet::core::codec::Decode;
use starknet::core::types::{ByteArray, EmittedEvent, Felt, U256};
use starknet::core::utils::parse_cairo_short_string;
use starknet::macros::selector;
use std::any::Any;
use std::collections::HashMap;
use torii::etl::{Decoder, Envelope, TypedBody};

fn payload(event: &EmittedEvent) -> Vec<Felt> {
    event
        .keys
        .iter()
        .skip(1)
        .copied()
        .chain(event.data.iter().copied())
        .collect()
}

fn parse_u256(parts: &[Felt]) -> U256 {
    match parts {
        [] => U256::from(0u64),
        [single] => {
            let bytes = single.to_bytes_be();
            U256::from(u128::from_be_bytes(bytes[16..32].try_into().unwrap()))
        }
        [low, high, ..] => U256::from_words(
            u128::from_be_bytes(low.to_bytes_be()[16..32].try_into().unwrap()),
            u128::from_be_bytes(high.to_bytes_be()[16..32].try_into().unwrap()),
        ),
    }
}

fn parse_string(parts: &[Felt]) -> Option<String> {
    if parts.is_empty() {
        return None;
    }
    if parts.len() == 1 {
        return parse_cairo_short_string(&parts[0]).ok();
    }
    if let Ok(byte_array) = ByteArray::decode(parts) {
        return String::try_from(byte_array).ok();
    }
    parse_cairo_short_string(&parts[0]).ok()
}

macro_rules! typed_body {
    ($name:ident, $type_id:literal) => {
        impl TypedBody for $name {
            fn envelope_type_id(&self) -> torii::etl::envelope::TypeId {
                torii::etl::envelope::TypeId::new($type_id)
            }

            fn as_any(&self) -> &dyn Any {
                self
            }

            fn as_any_mut(&mut self) -> &mut dyn Any {
                self
            }
        }
    };
}

#[derive(Debug, Clone)]
pub struct ProposalCreated {
    pub governor: Felt,
    pub proposal_id: Felt,
    pub proposer: Felt,
    pub block_number: u64,
    pub transaction_hash: Felt,
}
typed_body!(ProposalCreated, "governance.proposal_created");

#[derive(Debug, Clone)]
pub struct ProposalQueued {
    pub governor: Felt,
    pub proposal_id: Felt,
    pub eta_seconds: Option<u64>,
    pub block_number: u64,
    pub transaction_hash: Felt,
}
typed_body!(ProposalQueued, "governance.proposal_queued");

#[derive(Debug, Clone)]
pub struct ProposalExecuted {
    pub governor: Felt,
    pub proposal_id: Felt,
    pub block_number: u64,
    pub transaction_hash: Felt,
}
typed_body!(ProposalExecuted, "governance.proposal_executed");

#[derive(Debug, Clone)]
pub struct ProposalCanceled {
    pub governor: Felt,
    pub proposal_id: Felt,
    pub block_number: u64,
    pub transaction_hash: Felt,
}
typed_body!(ProposalCanceled, "governance.proposal_canceled");

#[derive(Debug, Clone)]
pub struct VoteCast {
    pub governor: Felt,
    pub proposal_id: Felt,
    pub voter: Felt,
    pub support: u8,
    pub weight: U256,
    pub reason: Option<String>,
    pub block_number: u64,
    pub transaction_hash: Felt,
}
typed_body!(VoteCast, "governance.vote_cast");

#[derive(Debug, Clone)]
pub struct VoteCastWithParams {
    pub governor: Felt,
    pub proposal_id: Felt,
    pub voter: Felt,
    pub support: u8,
    pub weight: U256,
    pub reason: Option<String>,
    pub params: Vec<Felt>,
    pub block_number: u64,
    pub transaction_hash: Felt,
}
typed_body!(VoteCastWithParams, "governance.vote_cast_with_params");

#[derive(Debug, Clone)]
pub struct DelegateChanged {
    pub votes_token: Felt,
    pub delegator: Felt,
    pub from_delegate: Felt,
    pub to_delegate: Felt,
    pub block_number: u64,
    pub transaction_hash: Felt,
}
typed_body!(DelegateChanged, "governance.delegate_changed");

#[derive(Debug, Clone)]
pub struct DelegateVotesChanged {
    pub votes_token: Felt,
    pub delegate: Felt,
    pub previous_votes: U256,
    pub new_votes: U256,
    pub block_number: u64,
    pub transaction_hash: Felt,
}
typed_body!(DelegateVotesChanged, "governance.delegate_votes_changed");

pub struct GovernanceDecoder;

impl GovernanceDecoder {
    pub fn new() -> Self {
        Self
    }

    fn envelope(event: &EmittedEvent, kind: &str, body: Box<dyn TypedBody>) -> Envelope {
        let mut metadata = HashMap::new();
        metadata.insert("contract".to_string(), format!("{:#x}", event.from_address));
        metadata.insert(
            "block_number".to_string(),
            event.block_number.unwrap_or(0).to_string(),
        );
        metadata.insert(
            "tx_hash".to_string(),
            format!("{:#x}", event.transaction_hash),
        );
        Envelope::new(
            format!(
                "governance_{}_{}_{}",
                kind,
                event.block_number.unwrap_or(0),
                event.transaction_hash
            ),
            body,
            metadata,
        )
    }

    fn decode_proposal_created(event: &EmittedEvent) -> Option<Envelope> {
        let payload = payload(event);
        if payload.len() < 2 {
            return None;
        }
        Some(Self::envelope(
            event,
            "proposal_created",
            Box::new(ProposalCreated {
                governor: event.from_address,
                proposal_id: payload[0],
                proposer: payload[1],
                block_number: event.block_number.unwrap_or(0),
                transaction_hash: event.transaction_hash,
            }),
        ))
    }

    fn decode_proposal_queued(event: &EmittedEvent) -> Option<Envelope> {
        let payload = payload(event);
        let eta_seconds = payload.get(1).and_then(|value| (*value).try_into().ok());
        payload.first().copied().map(|proposal_id| {
            Self::envelope(
                event,
                "proposal_queued",
                Box::new(ProposalQueued {
                    governor: event.from_address,
                    proposal_id,
                    eta_seconds,
                    block_number: event.block_number.unwrap_or(0),
                    transaction_hash: event.transaction_hash,
                }),
            )
        })
    }

    fn decode_single_proposal(
        event: &EmittedEvent,
        kind: &str,
        canceled: bool,
    ) -> Option<Envelope> {
        let proposal_id = payload(event).first().copied()?;
        Some(Self::envelope(
            event,
            kind,
            if canceled {
                Box::new(ProposalCanceled {
                    governor: event.from_address,
                    proposal_id,
                    block_number: event.block_number.unwrap_or(0),
                    transaction_hash: event.transaction_hash,
                })
            } else {
                Box::new(ProposalExecuted {
                    governor: event.from_address,
                    proposal_id,
                    block_number: event.block_number.unwrap_or(0),
                    transaction_hash: event.transaction_hash,
                })
            },
        ))
    }

    fn decode_vote_cast(event: &EmittedEvent, with_params: bool) -> Option<Envelope> {
        let payload = payload(event);
        if payload.len() < 5 {
            return None;
        }
        let voter = payload[0];
        let proposal_id = payload[1];
        let support: u8 = payload[2].try_into().ok()?;
        let weight = parse_u256(&payload[3..5.min(payload.len())]);
        let tail = if payload.len() > 5 {
            &payload[5..]
        } else {
            &[]
        };
        if with_params {
            Some(Self::envelope(
                event,
                "vote_cast_with_params",
                Box::new(VoteCastWithParams {
                    governor: event.from_address,
                    proposal_id,
                    voter,
                    support,
                    weight,
                    reason: parse_string(tail),
                    params: tail.to_vec(),
                    block_number: event.block_number.unwrap_or(0),
                    transaction_hash: event.transaction_hash,
                }),
            ))
        } else {
            Some(Self::envelope(
                event,
                "vote_cast",
                Box::new(VoteCast {
                    governor: event.from_address,
                    proposal_id,
                    voter,
                    support,
                    weight,
                    reason: parse_string(tail),
                    block_number: event.block_number.unwrap_or(0),
                    transaction_hash: event.transaction_hash,
                }),
            ))
        }
    }

    fn decode_delegate_changed(event: &EmittedEvent) -> Option<Envelope> {
        let payload = payload(event);
        if payload.len() < 3 {
            return None;
        }
        Some(Self::envelope(
            event,
            "delegate_changed",
            Box::new(DelegateChanged {
                votes_token: event.from_address,
                delegator: payload[0],
                from_delegate: payload[1],
                to_delegate: payload[2],
                block_number: event.block_number.unwrap_or(0),
                transaction_hash: event.transaction_hash,
            }),
        ))
    }

    fn decode_delegate_votes_changed(event: &EmittedEvent) -> Option<Envelope> {
        let payload = payload(event);
        if payload.len() < 5 {
            return None;
        }
        Some(Self::envelope(
            event,
            "delegate_votes_changed",
            Box::new(DelegateVotesChanged {
                votes_token: event.from_address,
                delegate: payload[0],
                previous_votes: parse_u256(&payload[1..3]),
                new_votes: parse_u256(&payload[3..5]),
                block_number: event.block_number.unwrap_or(0),
                transaction_hash: event.transaction_hash,
            }),
        ))
    }
}

impl Default for GovernanceDecoder {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl Decoder for GovernanceDecoder {
    fn decoder_name(&self) -> &'static str {
        "governance"
    }

    async fn decode_event(&self, event: &EmittedEvent) -> Result<Vec<Envelope>> {
        if event.keys.is_empty() {
            return Ok(Vec::new());
        }
        let selector = event.keys[0];
        let envelope = if selector == selector!("ProposalCreated") {
            Self::decode_proposal_created(event)
        } else if selector == selector!("ProposalQueued") {
            Self::decode_proposal_queued(event)
        } else if selector == selector!("ProposalExecuted") {
            Self::decode_single_proposal(event, "proposal_executed", false)
        } else if selector == selector!("ProposalCanceled") {
            Self::decode_single_proposal(event, "proposal_canceled", true)
        } else if selector == selector!("VoteCast") {
            Self::decode_vote_cast(event, false)
        } else if selector == selector!("VoteCastWithParams") {
            Self::decode_vote_cast(event, true)
        } else if selector == selector!("DelegateChanged") {
            Self::decode_delegate_changed(event)
        } else if selector == selector!("DelegateVotesChanged") {
            Self::decode_delegate_votes_changed(event)
        } else {
            None
        };

        Ok(envelope.into_iter().collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn decodes_proposal_created() {
        let decoder = GovernanceDecoder::new();
        let event = EmittedEvent {
            from_address: Felt::from(1u64),
            keys: vec![selector!("ProposalCreated")],
            data: vec![Felt::from(2u64), Felt::from(3u64)],
            block_hash: None,
            block_number: Some(10),
            transaction_hash: Felt::from(4u64),
        };
        let envelopes = decoder.decode_event(&event).await.unwrap();
        let body = envelopes[0]
            .body
            .as_any()
            .downcast_ref::<ProposalCreated>()
            .unwrap();
        assert_eq!(body.proposal_id, Felt::from(2u64));
        assert_eq!(body.proposer, Felt::from(3u64));
    }

    #[tokio::test]
    async fn decodes_delegate_votes_changed() {
        let decoder = GovernanceDecoder::new();
        let event = EmittedEvent {
            from_address: Felt::from(9u64),
            keys: vec![selector!("DelegateVotesChanged")],
            data: vec![
                Felt::from(5u64),
                Felt::from(7u64),
                Felt::ZERO,
                Felt::from(8u64),
                Felt::ZERO,
            ],
            block_hash: None,
            block_number: Some(42),
            transaction_hash: Felt::from(11u64),
        };
        let envelopes = decoder.decode_event(&event).await.unwrap();
        let body = envelopes[0]
            .body
            .as_any()
            .downcast_ref::<DelegateVotesChanged>()
            .unwrap();
        assert_eq!(body.delegate, Felt::from(5u64));
        assert_eq!(body.previous_votes, U256::from(7u64));
        assert_eq!(body.new_votes, U256::from(8u64));
    }
}
