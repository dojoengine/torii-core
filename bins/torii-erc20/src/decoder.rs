//! ERC20 Transfer event decoder

use anyhow::Result;
use async_trait::async_trait;
use starknet::core::types::{EmittedEvent, Felt, U256};
use starknet::macros::selector;
use std::any::Any;
use std::collections::HashMap;
use torii::etl::{Decoder, Envelope, TypedBody};

/// Transfer event from ERC20 token
#[derive(Debug, Clone)]
pub struct Transfer {
    pub from: Felt,
    pub to: Felt,
    /// Amount as U256 (256-bit), properly representing ERC20 token amounts
    pub amount: U256,
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

    async fn decode_event(&self, event: &EmittedEvent) -> Result<Vec<Envelope>> {
        if !self.is_transfer_event(event) {
            return Ok(Vec::new());
        }

        // Some legacy ERC20 have no keys (except the selector), since starknet didn't have keys for events before.
        // So if an event transfer has 3 data fields, and only the selector in the keys, it's a legacy ERC20 (valid then).

        let from;
        let to;
        let amount: U256;

        if event.keys.len() == 1 && event.data.len() == 4 {
            // Legacy ERC20: from, to, amount_low, amount_high in data
            from = event.data[0];
            to = event.data[1];
            // Build U256 from low (128 bits) and high (128 bits) parts
            let low: u128 = event.data[2].try_into().unwrap_or(0);
            let high: u128 = event.data[3].try_into().unwrap_or(0);
            amount = U256::from_words(low, high);
        } else if event.keys.len() == 3 && event.data.len() == 2 {
            // Normal ERC20: from, to in keys; amount_low, amount_high in data
            from = event.keys[1];
            to = event.keys[2];
            // Build U256 from low (128 bits) and high (128 bits) parts
            let low: u128 = event.data[0].try_into().unwrap_or(0);
            let high: u128 = event.data[1].try_into().unwrap_or(0);
            amount = U256::from_words(low, high);
        } else {
            tracing::warn!(
                "Malformed Transfer event from contract {:?} -> {:?}",
                event.from_address,
                event
            );
            return Ok(Vec::new());
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

        Ok(vec![Envelope::new(envelope_id, Box::new(transfer), metadata)])
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

        let envelopes = decoder.decode_event(&event).await.unwrap();
        assert_eq!(envelopes.len(), 1);

        let transfer = envelopes[0]
            .body
            .as_any()
            .downcast_ref::<Transfer>()
            .unwrap();

        assert_eq!(transfer.from, Felt::from(0x1u64));
        assert_eq!(transfer.to, Felt::from(0x2u64));
        assert_eq!(transfer.amount, U256::from(1000u64));
        assert_eq!(transfer.token, Felt::from(0x123u64));
    }
}
