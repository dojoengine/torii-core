//! ERC20 event decoder (Transfer + Approval)

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

/// Approval event from ERC20 token
#[derive(Debug, Clone)]
pub struct Approval {
    pub owner: Felt,
    pub spender: Felt,
    /// Amount as U256 (256-bit), properly representing ERC20 token amounts
    pub amount: U256,
    pub token: Felt,
    pub block_number: u64,
    pub transaction_hash: Felt,
}

impl TypedBody for Approval {
    fn envelope_type_id(&self) -> torii::etl::envelope::TypeId {
        torii::etl::envelope::TypeId::new("erc20.approval")
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

/// ERC20 event decoder
///
/// Decodes multiple ERC20 events:
/// - Transfer(from, to, value)
/// - Approval(owner, spender, value)
///
/// # Pattern for Multi-Event Decoders
///
/// This decoder showcases the recommended pattern for handling multiple event types:
/// 1. Check selector first (fast O(1) comparison)
/// 2. Dispatch to specific decode_X() method
/// 3. Each method returns Result<Option<Envelope>> (None if not interested/malformed)
/// 4. Main decode_event() collects results
///
/// This pattern scales cleanly to many event types without complex branching.
pub struct Erc20Decoder;

impl Erc20Decoder {
    pub fn new() -> Self {
        Self
    }

    /// Transfer event selector: sn_keccak("Transfer")
    fn transfer_selector() -> Felt {
        selector!("Transfer")
    }

    /// Approval event selector: sn_keccak("Approval")
    fn approval_selector() -> Felt {
        selector!("Approval")
    }

    /// Decode Transfer event into envelope
    ///
    /// Transfer event signatures (supports both modern and legacy):
    ///
    /// Modern ERC20:
    /// - keys[0]: Transfer selector
    /// - keys[1]: from address
    /// - keys[2]: to address
    /// - data[0]: amount_low (u128)
    /// - data[1]: amount_high (u128)
    ///
    /// Legacy ERC20 (pre-keys era):
    /// - keys[0]: Transfer selector
    /// - data[0]: from address
    /// - data[1]: to address
    /// - data[2]: amount_low (u128)
    /// - data[3]: amount_high (u128)
    ///
    /// Some old tokens are using `felt` as amount, and not `u256`, which reduce by one the number of values.
    /// Currently not handled, but may be added if some old tokens are required to be indexed.
    async fn decode_transfer(&self, event: &EmittedEvent) -> Result<Option<Envelope>> {
        let from;
        let to;
        let amount: U256;

        if event.keys.len() == 1 && event.data.len() == 4 {
            // Legacy ERC20: from, to, amount_low, amount_high in data
            from = event.data[0];
            to = event.data[1];
            let low: u128 = event.data[2].try_into().unwrap_or(0);
            let high: u128 = event.data[3].try_into().unwrap_or(0);
            amount = U256::from_words(low, high);
        } else if event.keys.len() == 3 && event.data.len() == 2 {
            // Modern ERC20: from, to in keys; amount_low, amount_high in data
            from = event.keys[1];
            to = event.keys[2];
            let low: u128 = event.data[0].try_into().unwrap_or(0);
            let high: u128 = event.data[1].try_into().unwrap_or(0);
            amount = U256::from_words(low, high);
        } else {
            tracing::warn!(
                "Malformed Transfer event from contract {:?} (keys: {}, data: {})",
                event.from_address,
                event.keys.len(),
                event.data.len()
            );
            return Ok(None);
        }

        let transfer = Transfer {
            from,
            to,
            amount,
            token: event.from_address,
            block_number: event.block_number.unwrap_or(0),
            transaction_hash: event.transaction_hash,
        };

        let mut metadata = HashMap::new();
        metadata.insert("token".to_string(), format!("{:#x}", event.from_address));
        metadata.insert(
            "block_number".to_string(),
            event.block_number.unwrap_or(0).to_string(),
        );
        metadata.insert(
            "tx_hash".to_string(),
            format!("{:#x}", event.transaction_hash),
        );

        let envelope_id = format!(
            "erc20_transfer_{}_{}",
            event.block_number.unwrap_or(0),
            format!("{:#x}", event.transaction_hash)
        );

        Ok(Some(Envelope::new(
            envelope_id,
            Box::new(transfer),
            metadata,
        )))
    }

    /// Decode Approval event into envelope
    ///
    /// Approval event signature:
    /// - keys[0]: Approval selector
    /// - keys[1]: owner address
    /// - keys[2]: spender address
    /// - data[0]: amount_low (u128)
    /// - data[1]: amount_high (u128)
    async fn decode_approval(&self, event: &EmittedEvent) -> Result<Option<Envelope>> {
        if event.keys.len() != 3 || event.data.len() != 2 {
            tracing::warn!(
                "Malformed Approval event from contract {:?} (keys: {}, data: {})",
                event.from_address,
                event.keys.len(),
                event.data.len()
            );
            return Ok(None);
        }

        let owner = event.keys[1];
        let spender = event.keys[2];
        let low: u128 = event.data[0].try_into().unwrap_or(0);
        let high: u128 = event.data[1].try_into().unwrap_or(0);
        let amount = U256::from_words(low, high);

        let approval = Approval {
            owner,
            spender,
            amount,
            token: event.from_address,
            block_number: event.block_number.unwrap_or(0),
            transaction_hash: event.transaction_hash,
        };

        let mut metadata = HashMap::new();
        metadata.insert("token".to_string(), format!("{:#x}", event.from_address));
        metadata.insert(
            "block_number".to_string(),
            event.block_number.unwrap_or(0).to_string(),
        );
        metadata.insert(
            "tx_hash".to_string(),
            format!("{:#x}", event.transaction_hash),
        );

        let envelope_id = format!(
            "erc20_approval_{}_{}",
            event.block_number.unwrap_or(0),
            format!("{:#x}", event.transaction_hash)
        );

        Ok(Some(Envelope::new(
            envelope_id,
            Box::new(approval),
            metadata,
        )))
    }
}

#[async_trait]
impl Decoder for Erc20Decoder {
    fn decoder_name(&self) -> &str {
        "erc20"
    }

    async fn decode_event(&self, event: &EmittedEvent) -> Result<Vec<Envelope>> {
        if event.keys.is_empty() {
            return Ok(Vec::new());
        }

        let selector = event.keys[0];

        if selector == Self::transfer_selector() {
            if let Some(envelope) = self.decode_transfer(event).await? {
                return Ok(vec![envelope]);
            }
        } else if selector == Self::approval_selector() {
            if let Some(envelope) = self.decode_approval(event).await? {
                return Ok(vec![envelope]);
            }
        }

        // TODO: maybe we need to log something to ensure we are not missing any events?
        // Or it may be because of the contract having other events than ERC20 (but includes some ERC20 events).
        Ok(Vec::new())
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

    #[tokio::test]
    async fn test_decode_approval() {
        let decoder = Erc20Decoder::new();

        let event = EmittedEvent {
            from_address: Felt::from(0x456u64), // Token contract
            keys: vec![
                Erc20Decoder::approval_selector(),
                Felt::from(0xau64), // owner
                Felt::from(0xbu64), // spender
            ],
            data: vec![
                Felt::from(5000u64), // amount_low
                Felt::ZERO,          // amount_high
            ],
            block_hash: None,
            block_number: Some(200),
            transaction_hash: Felt::from(0xdef0u64),
        };

        let envelopes = decoder.decode_event(&event).await.unwrap();
        assert_eq!(envelopes.len(), 1);

        let approval = envelopes[0]
            .body
            .as_any()
            .downcast_ref::<Approval>()
            .unwrap();

        assert_eq!(approval.owner, Felt::from(0xau64));
        assert_eq!(approval.spender, Felt::from(0xbu64));
        assert_eq!(approval.amount, U256::from(5000u64));
        assert_eq!(approval.token, Felt::from(0x456u64));
    }

    #[tokio::test]
    async fn test_decode_unknown_event() {
        let decoder = Erc20Decoder::new();

        // Event with unknown selector
        let event = EmittedEvent {
            from_address: Felt::from(0x789u64),
            keys: vec![
                Felt::from(0xdeadbeef_u64), // Unknown selector
            ],
            data: vec![],
            block_hash: None,
            block_number: Some(300),
            transaction_hash: Felt::from(0x1234u64),
        };

        let envelopes = decoder.decode_event(&event).await.unwrap();
        assert_eq!(envelopes.len(), 0); // Should return empty vec for unknown events
    }
}
