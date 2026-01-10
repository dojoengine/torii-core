//! ERC20 Transfer event decoder

use anyhow::Result;
use async_trait::async_trait;
use starknet::{core::types::{EmittedEvent, Felt}, macros::selector};
use std::any::Any;
use std::collections::HashMap;
use torii::etl::{Decoder, Envelope, TypedBody};

/// Transfer event from ERC20 token
#[derive(Debug, Clone)]
pub struct Transfer {
    pub from: Felt,
    pub to: Felt,
    pub amount: Felt,
    pub token: Felt,
    pub block_number: u64,
    pub transaction_hash: Felt,
}

impl TypedBody for Transfer {
    fn envelope_type_id(&self) -> torii::etl::envelope::TypeId {
        torii::etl::envelope::TypeId::new("erc20.transfer")
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

/// ERC20 Transfer event decoder
///
/// Decodes Transfer(from, to, value) events from ERC20 contracts.
/// The Transfer event signature:
/// - keys[0]: Transfer selector (sn_keccak("Transfer"))
/// - keys[1]: from address
/// - keys[2]: to address
/// - data[0]: amount low (u256 low bits)
/// - data[1]: amount high (u256 high bits)
pub struct Erc20Decoder;

impl Erc20Decoder {
    pub fn new() -> Self {
        Self
    }

    /// Transfer event selector
    fn transfer_selector() -> Felt {
        selector!("Transfer")
    }

    fn is_transfer_event(&self, event: &EmittedEvent) -> bool {
        // Check if this is a Transfer event
        if event.keys.is_empty() {
            return false;
        }

        event.keys[0] == Self::transfer_selector()
    }
}

#[async_trait]
impl Decoder for Erc20Decoder {
    fn decoder_name(&self) -> &str {
        "erc20"
    }

    async fn decode(&self, events: &[EmittedEvent]) -> Result<Vec<Envelope>> {
        let mut envelopes = Vec::new();

        for event in events {
            if !self.is_transfer_event(event) {
                continue;
            }

            // Some legacy ERC20 have no keys (except the selector), since starknet didn't have keys for events before.
            // So if an event transfer has 3 data fields, and only the selector in the keys, it's a legacy ERC20 (valid then).

            let from;
            let to;
            let amount;

            // TODO: this is ugly.. need something better...!
            if event.keys.len() == 1 && event.data.len() == 4 {
                // Legacy ERC20
                from = event.data[0];
                to = event.data[1];
                amount = event.data[2] + event.data[3] * Felt::from(2).pow(128u32);
            } else if event.keys.len() == 3 && event.data.len() == 2 {
                // Normal ERC20
                from = event.keys[1];
                to = event.keys[2];
                amount = event.data[0] + event.data[1] * Felt::from(2).pow(128u32);
            } else {
                tracing::warn!(
                    "Malformed Transfer event from contract {:?} -> {:?}",
                    event.from_address,
                    event
                );
                continue;
            }

            let transfer = Transfer {
                from,
                to,
                amount,
                token: event.from_address,
                block_number: event.block_number.unwrap_or(0),
                transaction_hash: event.transaction_hash,
            };

            // Create envelope with metadata
            let mut metadata = HashMap::new();
            metadata.insert("token".to_string(), format!("{:#x}", event.from_address));
            metadata.insert("block_number".to_string(), event.block_number.unwrap_or(0).to_string());
            metadata.insert("tx_hash".to_string(), format!("{:#x}", event.transaction_hash));

            let envelope_id = format!(
                "erc20_transfer_{}_{}",
                event.block_number.unwrap_or(0),
                format!("{:#x}", event.transaction_hash)
            );

            envelopes.push(Envelope::new(envelope_id, Box::new(transfer), metadata));
        }

        tracing::debug!(
            target: "torii_erc20::decoder",
            "Decoded {} Transfer events from {} input events",
            envelopes.len(),
            events.len()
        );

        Ok(envelopes)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_decode_transfer() {
        let decoder = Erc20Decoder::new();

        let event = EmittedEvent {
            from_address: Felt::from(0x123u64), // Token contract
            keys: vec![
                Erc20Decoder::transfer_selector(),
                Felt::from(0x1u64), // from
                Felt::from(0x2u64), // to
            ],
            data: vec![
                Felt::from(1000u64), // amount_low
                Felt::ZERO,          // amount_high
            ],
            block_hash: None,
            block_number: Some(100),
            transaction_hash: Felt::from(0xabcdu64),
        };

        let envelopes = decoder.decode(&[event]).await.unwrap();
        assert_eq!(envelopes.len(), 1);

        let transfer = envelopes[0]
            .body
            .as_any()
            .downcast_ref::<Transfer>()
            .unwrap();

        assert_eq!(transfer.from, Felt::from(0x1u64));
        assert_eq!(transfer.to, Felt::from(0x2u64));
        assert_eq!(transfer.amount, Felt::from(1000u64));
        assert_eq!(transfer.token, Felt::from(0x123u64));
    }
}
