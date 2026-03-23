//! ERC20 event decoder (Transfer + Approval)

use anyhow::Result;
use async_trait::async_trait;
use starknet::core::types::{EmittedEvent, Felt, U256};
use starknet::macros::selector;
use std::any::Any;
use std::collections::HashMap;
use torii::etl::{Decoder, Envelope, TypedBody};
use torii::typed_body_impl;

pub const ERC20_APPROVAL_SELECTOR: Felt = selector!("Approval");
pub const ERC20_TRANSFER_SELECTOR: Felt = selector!("Transfer");

#[derive(Debug, Clone)]
pub enum Erc20Msg {
    Transfer(Transfer),
    Approval(Approval),
}

typed_body_impl!(Erc20Msg, "erc20");

impl From<Transfer> for Erc20Msg {
    fn from(value: Transfer) -> Self {
        Erc20Msg::Transfer(value)
    }
}

impl From<Approval> for Erc20Msg {
    fn from(value: Approval) -> Self {
        Erc20Msg::Approval(value)
    }
}

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

    /// Decode Transfer event into envelope
    ///
    /// Transfer event signatures (supports multiple formats):
    ///
    /// Modern ERC20 (standard):
    /// - keys[0]: Transfer selector
    /// - keys[1]: from address
    /// - keys[2]: to address
    /// - data[0]: amount_low (u128)
    /// - data[1]: amount_high (u128)
    ///
    /// All-in-keys format (some tokens put everything in keys):
    /// - keys[0]: Transfer selector
    /// - keys[1]: from address
    /// - keys[2]: to address
    /// - keys[3]: amount_low (u128)
    /// - keys[4]: amount_high (u128)
    /// - data: empty
    ///
    /// Legacy ERC20 (pre-keys era):
    /// - keys[0]: Transfer selector
    /// - data[0]: from address
    /// - data[1]: to address
    /// - data[2]: amount_low (u128)
    /// - data[3]: amount_high (u128)
    ///
    /// Felt-based variants use single felt for amount instead of U256.
    async fn decode_transfer(
        &self,
        from_address: &Felt,
        block_number: u64,
        transaction_hash: &Felt,
        keys: &[Felt],
        data: &[Felt],
    ) -> Result<Option<Transfer>> {
        let from;
        let to;
        let amount: U256;

        if keys.len() == 5 && data.is_empty() {
            // All-in-keys format: selector, from, to, amount_low, amount_high all in keys
            from = keys[1];
            to = keys[2];
            let low: u128 = keys[3].try_into().unwrap_or(0);
            let high: u128 = keys[4].try_into().unwrap_or(0);
            amount = U256::from_words(low, high);
            tracing::debug!(
                target: "torii_erc20::decoder",
                token = %format!("{:#x}", from_address),
                from = %format!("{:#x}", from),
                to = %format!("{:#x}", to),
                amount = %amount,
                "Decoded all-in-keys transfer (U256)"
            );
        } else if keys.len() == 4 && data.is_empty() {
            // All-in-keys format with felt amount: selector, from, to, amount in keys
            from = keys[1];
            to = keys[2];
            let amount_felt: u128 = keys[3].try_into().unwrap_or(0);
            amount = U256::from(amount_felt);
            tracing::debug!(
                target: "torii_erc20::decoder",
                token = %format!("{:#x}", from_address),
                from = %format!("{:#x}", from),
                to = %format!("{:#x}", to),
                amount = %amount,
                "Decoded all-in-keys transfer (felt)"
            );
        } else if keys.len() == 1 && data.len() == 4 {
            // Legacy ERC20: from, to, amount_low, amount_high in data
            from = data[0];
            to = data[1];
            let low: u128 = data[2].try_into().unwrap_or(0);
            let high: u128 = data[3].try_into().unwrap_or(0);
            amount = U256::from_words(low, high);
        } else if keys.len() == 3 && data.len() == 2 {
            // Modern ERC20: from, to in keys; amount_low, amount_high in data
            from = keys[1];
            to = keys[2];
            let low: u128 = data[0].try_into().unwrap_or(0);
            let high: u128 = data[1].try_into().unwrap_or(0);
            amount = U256::from_words(low, high);
        } else if keys.len() == 3 && data.len() == 1 {
            // Felt-based modern format: from, to in keys; amount as single felt (fits in u128)
            from = keys[1];
            to = keys[2];
            let amount_felt: u128 = data[0].try_into().unwrap_or(0);
            amount = U256::from(amount_felt);
            tracing::debug!(
                target: "torii_erc20::decoder",
                token = %format!("{:#x}", from_address),
                from = %format!("{:#x}", from),
                to = %format!("{:#x}", to),
                amount = %amount,
                "Decoded felt-based modern transfer"
            );
        } else if keys.len() == 1 && data.len() == 3 {
            // Felt-based legacy format: from, to, amount in data
            from = data[0];
            to = data[1];
            let amount_felt: u128 = data[2].try_into().unwrap_or(0);
            amount = U256::from(amount_felt);
            tracing::debug!(
                target: "torii_erc20::decoder",
                token = %format!("{:#x}", from_address),
                from = %format!("{:#x}", from),
                to = %format!("{:#x}", to),
                amount = %amount,
                "Decoded felt-based legacy transfer"
            );
        } else if keys.len() == 3 && data.is_empty() {
            // All-in-keys format with zero amount (or amount omitted): from, to in keys, no data
            from = keys[1];
            to = keys[2];
            amount = U256::from(0u64);
            tracing::debug!(
                target: "torii_erc20::decoder",
                token = %format!("{:#x}", from_address),
                from = %format!("{:#x}", from),
                to = %format!("{:#x}", to),
                "Decoded transfer with zero/omitted amount"
            );
        } else {
            tracing::warn!(
                target: "torii_erc20::decoder",
                token = %format!("{:#x}", from_address),
                tx_hash = %format!("{:#x}", transaction_hash),
                block_number = block_number.unwrap_or(0),
                keys_len = keys.len(),
                data_len = data.len(),
                "Malformed Transfer event"
            );
            return Ok(None);
        }

        let envelope_id = format!(
            "erc20_transfer_{}_{}",
            block_number,
            format!("{:#x}", transaction_hash)
        );

        Ok(Some(Transfer {
            from,
            to,
            amount,
            token: *from_address,
            block_number,
            transaction_hash: *transaction_hash,
        }))
    }

    /// Decode Approval event into envelope
    ///
    /// Approval event signatures (supports multiple formats):
    ///
    /// Modern ERC20 (standard):
    /// - keys[0]: Approval selector
    /// - keys[1]: owner address
    /// - keys[2]: spender address
    /// - data[0]: amount_low (u128)
    /// - data[1]: amount_high (u128)
    ///
    /// All-in-keys format (some tokens put everything in keys):
    /// - keys[0]: Approval selector
    /// - keys[1]: owner address
    /// - keys[2]: spender address
    /// - keys[3]: amount_low (u128)
    /// - keys[4]: amount_high (u128)
    /// - data: empty
    ///
    /// Legacy ERC20 (pre-keys era):
    /// - keys[0]: Approval selector
    /// - data[0]: owner address
    /// - data[1]: spender address
    /// - data[2]: amount_low (u128)
    /// - data[3]: amount_high (u128)
    ///
    /// Felt-based variants use single felt for amount instead of U256.
    async fn decode_approval(
        &self,
        from_address: &Felt,
        block_number: u64,
        transaction_hash: &Felt,
        keys: &[Felt],
        data: &[Felt],
    ) -> Result<Option<Approval>> {
        let owner;
        let spender;
        let amount: U256;

        if keys.len() == 5 && data.is_empty() {
            // All-in-keys format: selector, owner, spender, amount_low, amount_high all in keys
            owner = keys[1];
            spender = keys[2];
            let low: u128 = keys[3].try_into().unwrap_or(0);
            let high: u128 = keys[4].try_into().unwrap_or(0);
            amount = U256::from_words(low, high);
            tracing::debug!(
                target: "torii_erc20::decoder",
                token = %format!("{:#x}", from_address),
                owner = %format!("{:#x}", owner),
                spender = %format!("{:#x}", spender),
                amount = %amount,
                "Decoded all-in-keys approval (U256)"
            );
        } else if keys.len() == 4 && data.is_empty() {
            // All-in-keys format with felt amount: selector, owner, spender, amount in keys
            owner = keys[1];
            spender = keys[2];
            let amount_felt: u128 = keys[3].try_into().unwrap_or(0);
            amount = U256::from(amount_felt);
            tracing::debug!(
                target: "torii_erc20::decoder",
                token = %format!("{:#x}", from_address),
                owner = %format!("{:#x}", owner),
                spender = %format!("{:#x}", spender),
                amount = %amount,
                "Decoded all-in-keys approval (felt)"
            );
        } else if keys.len() == 1 && data.len() == 4 {
            // Legacy ERC20: owner, spender, amount_low, amount_high in data
            owner = data[0];
            spender = data[1];
            let low: u128 = data[2].try_into().unwrap_or(0);
            let high: u128 = data[3].try_into().unwrap_or(0);
            amount = U256::from_words(low, high);
        } else if keys.len() == 3 && data.len() == 2 {
            // Modern ERC20: owner, spender in keys; amount_low, amount_high in data
            owner = keys[1];
            spender = keys[2];
            let low: u128 = data[0].try_into().unwrap_or(0);
            let high: u128 = data[1].try_into().unwrap_or(0);
            amount = U256::from_words(low, high);
        } else if keys.len() == 3 && data.len() == 1 {
            // Felt-based modern format: owner, spender in keys; amount as single felt
            owner = keys[1];
            spender = keys[2];
            let amount_felt: u128 = data[0].try_into().unwrap_or(0);
            amount = U256::from(amount_felt);
            tracing::debug!(
                target: "torii_erc20::decoder",
                token = %format!("{:#x}", from_address),
                owner = %format!("{:#x}", owner),
                spender = %format!("{:#x}", spender),
                amount = %amount,
                "Decoded felt-based modern approval"
            );
        } else if keys.len() == 1 && data.len() == 3 {
            // Felt-based legacy format: owner, spender, amount in data
            owner = data[0];
            spender = data[1];
            let amount_felt: u128 = data[2].try_into().unwrap_or(0);
            amount = U256::from(amount_felt);
            tracing::debug!(
                target: "torii_erc20::decoder",
                token = %format!("{:#x}", from_address),
                owner = %format!("{:#x}", owner),
                spender = %format!("{:#x}", spender),
                amount = %amount,
                "Decoded felt-based legacy approval"
            );
        } else if keys.len() == 3 && data.is_empty() {
            // All-in-keys format with zero amount (or amount omitted)
            owner = keys[1];
            spender = keys[2];
            amount = U256::from(0u64);
            tracing::debug!(
                target: "torii_erc20::decoder",
                token = %format!("{:#x}", from_address),
                owner = %format!("{:#x}", owner),
                spender = %format!("{:#x}", spender),
                "Decoded approval with zero/omitted amount"
            );
        } else {
            tracing::warn!(
                target: "torii_erc20::decoder",
                token = %format!("{:#x}", from_address),
                tx_hash = %format!("{:#x}", transaction_hash),
                block_number = block_number.unwrap_or(0),
                keys_len = keys.len(),
                data_len = data.len(),
                "Malformed Approval event"
            );
            return Ok(None);
        }

        let envelope_id = format!(
            "erc20_approval_{}_{}",
            block_number,
            format!("{:#x}", transaction_hash)
        );

        Ok(Some(Approval {
            owner,
            spender,
            amount,
            token: *from_address,
            block_number,
            transaction_hash: *transaction_hash,
        }))
    }

    async fn decode_erc20_event(
        &self,
        from_address: &Felt,
        block_number: u64,
        transaction_hash: &Felt,
        keys: &[Felt],
        data: &[Felt],
    ) -> Result<Option<Erc20Msg>> {
        let selector = match keys.get(0) {
            Some(s) => *s,
            None => return Ok(None),
        };
        match selector {
            ERC20_TRANSFER_SELECTOR => self
                .decode_transfer(from_address, block_number, transaction_hash, keys, data)
                .await
                .map(|opt| opt.map(Into::into)),
            ERC20_APPROVAL_SELECTOR => self
                .decode_approval(from_address, block_number, transaction_hash, keys, data)
                .await
                .map(|opt| opt.map(Into::into)),
            _ => {
                tracing::trace!(
                    target: "torii_erc20::decoder",
                    token = %format!("{:#x}", from_address),
                    selector = %format!("{:#x}", selector),
                    keys_len = keys.len(),
                    data_len = data.len(),
                    block_number = block_number,
                    tx_hash = %format!("{:#x}", transaction_hash),
                    "Unhandled event selector"
                );
                Ok(None)
            }
        }
    }
}

impl Default for Erc20Decoder {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl Decoder for Erc20Decoder {
    fn decoder_name(&self) -> &'static str {
        "erc20"
    }

    async fn decode_event(
        &self,
        from_address: &Felt,
        block_number: u64,
        transaction_hash: &Felt,
        keys: &[Felt],
        data: &[Felt],
    ) -> Result<Vec<Box<dyn TypedBody>>> {
        match self
            .decode_erc20_event(from_address, block_number, transaction_hash, keys, data)
            .await?
        {
            Some(msg) => Ok(msg.into()),
            None => Ok(vec![]),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_decode_transfer() {
        let decoder = Erc20Decoder::new();

        let from_address = Felt::from(0x123u64); // Token contract
        let keys = vec![
            ERC20_TRANSFER_SELECTOR,
            Felt::from(0x1u64), // from
            Felt::from(0x2u64), // to
        ];
        let data = vec![
            Felt::from(1000u64), // amount_low
            Felt::ZERO,          // amount_high
        ];
        let block_number = 100;
        let transaction_hash = Felt::from(0xabcdu64);

        let msgs = decoder
            .decode_event(&from_address, block_number, &transaction_hash, &keys, &data)
            .await
            .unwrap();
        assert_eq!(msgs.len(), 1);

        let transfer = msgs[0].as_any().downcast_ref::<Erc20Msg>().unwrap();
        match transfer {
            Erc20Msg::Transfer(t) => {
                assert_eq!(t.from, Felt::from(0x1u64));
                assert_eq!(t.to, Felt::from(0x2u64));
                assert_eq!(t.amount, U256::from(1000u64));
                assert_eq!(t.token, Felt::from(0x123u64));
            }
            _ => panic!("Expected Transfer event"),
        }
    }

    #[tokio::test]
    async fn test_decode_approval() {
        let decoder = Erc20Decoder::new();

        let keys = vec![
            ERC20_APPROVAL_SELECTOR,
            Felt::from(0xau64), // owner
            Felt::from(0xbu64), // spender
        ];
        let data = vec![
            Felt::from(5000u64), // amount_low
            Felt::ZERO,          // amount_high
        ];
        let block_number = 200;
        let transaction_hash = Felt::from(0xdef0u64);

        let msgs = decoder
            .decode_event(&from_address, block_number, &transaction_hash, &keys, &data)
            .await
            .unwrap();
        assert_eq!(msgs.len(), 1);

        let approval = msgs[0].as_any().downcast_ref::<Erc20Msg>().unwrap();
        match approval {
            Erc20Msg::Approval(a) => {
                assert_eq!(a.owner, Felt::from(0xau64));
                assert_eq!(a.spender, Felt::from(0xbu64));
                assert_eq!(a.amount, U256::from(5000u64));
                assert_eq!(a.token, Felt::from(0x123u64));
            }
            _ => panic!("Expected Approval event"),
        }
    }

    #[tokio::test]
    async fn test_decode_unknown_event() {
        let decoder = Erc20Decoder::new();

        // Event with unknown selector
        let from_address = Felt::from(0x789u64);
        let keys = vec![
            Felt::from(0xdeadbeef_u64), // Unknown selector
        ];
        let data = vec![];
        let block_number = 300;
        let transaction_hash = Felt::from(0x1234u64);

        let msgs = decoder
            .decode_event(&from_address, block_number, &transaction_hash, &keys, &data)
            .await
            .unwrap();
        assert_eq!(msgs.len(), 0); // Should return empty vec for unknown events
    }

    #[tokio::test]
    async fn test_decode_felt_based_modern_transfer() {
        let decoder = Erc20Decoder::new();

        // Felt-based modern format: keys=3 (selector, from, to), data=1 (amount as felt)
        let from_address = Felt::from(0x123u64); // Token contract
        let keys = vec![
            ERC20_TRANSFER_SELECTOR,
            Felt::from(0x1u64), // from
            Felt::from(0x2u64), // to
        ];
        let data = vec![
            Felt::from(5000u64), // amount as single felt
        ];
        let block_number = 100;
        let transaction_hash = Felt::from(0xabcdu64);

        let msgs = decoder
            .decode_event(&from_address, block_number, &transaction_hash, &keys, &data)
            .await
            .unwrap();
        assert_eq!(msgs.len(), 1);

        let transfer = msgs[0].as_any().downcast_ref::<Erc20Msg>().unwrap();

        match transfer {
            Erc20Msg::Transfer(t) => {
                assert_eq!(t.from, Felt::from(0x1u64));
                assert_eq!(t.to, Felt::from(0x2u64));
                assert_eq!(t.amount, U256::from(5000u64));
                assert_eq!(t.token, Felt::from(0x123u64));
            }
            _ => panic!("Expected Transfer event"),
        }
    }

    #[tokio::test]
    async fn test_decode_felt_based_legacy_transfer() {
        let decoder = Erc20Decoder::new();

        // Felt-based legacy format: keys=1 (selector), data=3 (from, to, amount)
        let from_address = Felt::from(0x456u64); // Token contract
        let keys = vec![ERC20_TRANSFER_SELECTOR];
        let data = vec![
            Felt::from(0xau64),  // from
            Felt::from(0xbu64),  // to
            Felt::from(7500u64), // amount as single felt
        ];
        let block_number = 50;
        let transaction_hash = Felt::from(0xef01u64);

        let msgs = decoder
            .decode_event(&from_address, block_number, &transaction_hash, &keys, &data)
            .await
            .unwrap();
        assert_eq!(msgs.len(), 1);

        let transfer = msgs[0].as_any().downcast_ref::<Erc20Msg>().unwrap();

        match transfer {
            Erc20Msg::Transfer(t) => {
                assert_eq!(t.from, Felt::from(0xau64));
                assert_eq!(t.to, Felt::from(0xbu64));
                assert_eq!(t.amount, U256::from(7500u64));
                assert_eq!(t.token, Felt::from(0x456u64));
            }
            _ => panic!("Expected Transfer event"),
        }
    }

    #[tokio::test]
    async fn test_decode_legacy_approval() {
        let decoder = Erc20Decoder::new();

        // Legacy format: keys=1 (selector), data=4 (owner, spender, amount_low, amount_high)
        let from_address = Felt::from(0x789u64); // Token contract
        let keys = vec![ERC20_APPROVAL_SELECTOR];
        let data = vec![
            Felt::from(0xcu64),   // owner
            Felt::from(0xdu64),   // spender
            Felt::from(10000u64), // amount_low
            Felt::ZERO,           // amount_high
        ];
        let block_number = 150;
        let transaction_hash = Felt::from(0xabc1u64);

        let msgs = decoder
            .decode_event(&from_address, block_number, &transaction_hash, &keys, &data)
            .await
            .unwrap();
        assert_eq!(msgs.len(), 1);

        let approval = msgs[0].as_any().downcast_ref::<Erc20Msg>().unwrap();

        match approval {
            Erc20Msg::Approval(a) => {
                assert_eq!(a.owner, Felt::from(0xcu64));
                assert_eq!(a.spender, Felt::from(0xdu64));
                assert_eq!(a.amount, U256::from(10000u64));
                assert_eq!(a.token, Felt::from(0x789u64));
            }
            _ => panic!("Expected Approval event"),
        }
    }

    #[tokio::test]
    async fn test_decode_felt_based_modern_approval() {
        let decoder = Erc20Decoder::new();

        // Felt-based modern format: keys=3 (selector, owner, spender), data=1 (amount as felt)
        let from_address = Felt::from(0xaaau64); // Token contract
        let keys = vec![
            ERC20_APPROVAL_SELECTOR,
            Felt::from(0xeu64), // owner
            Felt::from(0xfu64), // spender
        ];
        let data = vec![
            Felt::from(8000u64), // amount as single felt
        ];
        let block_number = 175;
        let transaction_hash = Felt::from(0xdef2u64);

        let msgs = decoder
            .decode_event(&from_address, block_number, &transaction_hash, &keys, &data)
            .await
            .unwrap();
        assert_eq!(msgs.len(), 1);

        let approval = msgs[0].as_any().downcast_ref::<Erc20Msg>().unwrap();

        match approval {
            Erc20Msg::Approval(a) => {
                assert_eq!(a.owner, Felt::from(0xeu64));
                assert_eq!(a.spender, Felt::from(0xfu64));
                assert_eq!(a.amount, U256::from(8000u64));
                assert_eq!(a.token, Felt::from(0xaaau64));
            }
            _ => panic!("Expected Approval event"),
        }
    }

    #[tokio::test]
    async fn test_decode_felt_based_legacy_approval() {
        let decoder = Erc20Decoder::new();

        // Felt-based legacy format: keys=1 (selector), data=3 (owner, spender, amount)
        let from_address = Felt::from(0xbbbu64); // Token contract
        let keys = vec![ERC20_APPROVAL_SELECTOR];
        let data = vec![
            Felt::from(0x10u64),  // owner
            Felt::from(0x11u64),  // spender
            Felt::from(12000u64), // amount as single felt
        ];
        let block_number = 180;
        let transaction_hash = Felt::from(0xef03u64);

        let msgs = decoder
            .decode_event(&from_address, block_number, &transaction_hash, &keys, &data)
            .await
            .unwrap();
        assert_eq!(msgs.len(), 1);

        let approval = msgs[0].as_any().downcast_ref::<Erc20Msg>().unwrap();

        match approval {
            Erc20Msg::Approval(a) => {
                assert_eq!(a.owner, Felt::from(0x10u64));
                assert_eq!(a.spender, Felt::from(0x11u64));
                assert_eq!(a.amount, U256::from(12000u64));
                assert_eq!(a.token, Felt::from(0xbbbu64));
            }
            _ => panic!("Expected Approval event"),
        }
    }

    #[tokio::test]
    async fn test_decode_all_in_keys_transfer_u256() {
        let decoder = Erc20Decoder::new();

        // All-in-keys format: keys=5 (selector, from, to, amount_low, amount_high), data=empty
        let from_address = Felt::from(0x999u64); // Token contract
        let keys = vec![
            ERC20_TRANSFER_SELECTOR,
            Felt::from(0x20u64),  // from
            Felt::from(0x21u64),  // to
            Felt::from(50000u64), // amount_low
            Felt::ZERO,           // amount_high
        ];
        let data = vec![];
        let block_number = 500;
        let transaction_hash = Felt::from(0xfff1u64);

        let msgs = decoder
            .decode_event(&from_address, block_number, &transaction_hash, &keys, &data)
            .await
            .unwrap();
        assert_eq!(msgs.len(), 1);

        let transfer = msgs[0].as_any().downcast_ref::<Erc20Msg>().unwrap();

        match transfer {
            Erc20Msg::Transfer(t) => {
                assert_eq!(t.from, Felt::from(0x20u64));
                assert_eq!(t.to, Felt::from(0x21u64));
                assert_eq!(t.amount, U256::from(50000u64));
                assert_eq!(t.token, Felt::from(0x999u64));
            }
            _ => panic!("Expected Transfer event"),
        }
    }

    #[tokio::test]
    async fn test_decode_all_in_keys_transfer_felt() {
        let decoder = Erc20Decoder::new();

        // All-in-keys format with felt amount: keys=4 (selector, from, to, amount), data=empty
        let from_address = Felt::from(0xaaau64); // Token contract
        let keys = vec![
            ERC20_TRANSFER_SELECTOR,
            Felt::from(0x30u64),  // from
            Felt::from(0x31u64),  // to
            Felt::from(75000u64), // amount as felt
        ];
        let data = vec![];
        let block_number = 600;
        let transaction_hash = Felt::from(0xfff2u64);

        let msgs = decoder
            .decode_event(&from_address, block_number, &transaction_hash, &keys, &data)
            .await
            .unwrap();
        assert_eq!(msgs.len(), 1);

        let transfer = msgs[0].as_any().downcast_ref::<Erc20Msg>().unwrap();

        match transfer {
            Erc20Msg::Transfer(t) => {
                assert_eq!(t.from, Felt::from(0x30u64));
                assert_eq!(t.to, Felt::from(0x31u64));
                assert_eq!(t.amount, U256::from(75000u64));
                assert_eq!(t.token, Felt::from(0xaaau64));
            }
            _ => panic!("Expected Transfer event"),
        }
    }

    #[tokio::test]
    async fn test_decode_all_in_keys_approval_u256() {
        let decoder = Erc20Decoder::new();

        // All-in-keys format: keys=5 (selector, owner, spender, amount_low, amount_high), data=empty
        let from_address = Felt::from(0xcccu64); // Token contract
        let keys = vec![
            ERC20_APPROVAL_SELECTOR,
            Felt::from(0x40u64),   // owner
            Felt::from(0x41u64),   // spender
            Felt::from(100000u64), // amount_low
            Felt::ZERO,            // amount_high
        ];
        let data = vec![];
        let block_number = 700;
        let transaction_hash = Felt::from(0xfff3u64);

        let msgs = decoder
            .decode_event(&from_address, block_number, &transaction_hash, &keys, &data)
            .await
            .unwrap();
        assert_eq!(msgs.len(), 1);

        let approval = msgs[0].as_any().downcast_ref::<Erc20Msg>().unwrap();

        match approval {
            Erc20Msg::Approval(a) => {
                assert_eq!(a.owner, Felt::from(0x40u64));
                assert_eq!(a.spender, Felt::from(0x41u64));
                assert_eq!(a.amount, U256::from(100000u64));
                assert_eq!(a.token, Felt::from(0xcccu64));
            }
            _ => panic!("Expected Approval event"),
        }
    }

    #[tokio::test]
    async fn test_decode_all_in_keys_approval_felt() {
        let decoder = Erc20Decoder::new();

        // All-in-keys format with felt amount: keys=4 (selector, owner, spender, amount), data=empty
        let from_address = Felt::from(0xdddu64); // Token contract
        let keys = vec![
            ERC20_APPROVAL_SELECTOR,
            Felt::from(0x50u64),   // owner
            Felt::from(0x51u64),   // spender
            Felt::from(125000u64), // amount as felt
        ];
        let data = vec![];
        let block_number = 800;
        let transaction_hash = Felt::from(0xfff4u64);

        let msgs = decoder
            .decode_event(&from_address, block_number, &transaction_hash, &keys, &data)
            .await
            .unwrap();
        assert_eq!(msgs.len(), 1);

        let approval = msgs[0].as_any().downcast_ref::<Erc20Msg>().unwrap();

        match approval {
            Erc20Msg::Approval(a) => {
                assert_eq!(a.owner, Felt::from(0x50u64));
                assert_eq!(a.spender, Felt::from(0x51u64));
                assert_eq!(a.amount, U256::from(125000u64));
                assert_eq!(a.token, Felt::from(0xdddu64));
            }
            _ => panic!("Expected Approval event"),
        }
    }
}
