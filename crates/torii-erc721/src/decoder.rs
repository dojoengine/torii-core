//! ERC721 event decoder (Transfer, Approval, ApprovalForAll)

use anyhow::Result;
use async_trait::async_trait;
use starknet::core::types::{EmittedEvent, Felt, U256};
use starknet::macros::selector;
use std::any::Any;
use std::collections::HashMap;
use torii::etl::{Decoder, Envelope, TypedBody};
use torii::typed_body_impl;

pub const ERC721_TRANSFER_SELECTOR: Felt = selector!("Transfer");
pub const ERC721_APPROVAL_SELECTOR: Felt = selector!("Approval");
pub const ERC721_APPROVAL_FOR_ALL_SELECTOR: Felt = selector!("ApprovalForAll");
pub const ERC721_METADATA_UPDATE_SELECTOR: Felt = selector!("MetadataUpdate");
pub const ERC721_BATCH_METADATA_UPDATE_SELECTOR: Felt = selector!("BatchMetadataUpdate");

#[derive(Debug, Clone)]
pub enum Erc721Msg {
    Transfer(NftTransfer),
    Approval(NftApproval),
    ApprovalForAll(OperatorApproval),
    MetadataUpdate(MetadataUpdate),
    BatchMetadataUpdate(BatchMetadataUpdate),
}

typed_body_impl!(Erc721Msg, "erc721");

impl From<NftTransfer> for Erc721Msg {
    fn from(value: NftTransfer) -> Self {
        Erc721Msg::Transfer(value)
    }
}

impl From<NftApproval> for Erc721Msg {
    fn from(value: NftApproval) -> Self {
        Erc721Msg::Approval(value)
    }
}

impl From<OperatorApproval> for Erc721Msg {
    fn from(value: OperatorApproval) -> Self {
        Erc721Msg::ApprovalForAll(value)
    }
}

impl From<MetadataUpdate> for Erc721Msg {
    fn from(value: MetadataUpdate) -> Self {
        Erc721Msg::MetadataUpdate(value)
    }
}

impl From<BatchMetadataUpdate> for Erc721Msg {
    fn from(value: BatchMetadataUpdate) -> Self {
        Erc721Msg::BatchMetadataUpdate(value)
    }
}

/// Transfer event from ERC721 token
#[derive(Debug, Clone)]
pub struct NftTransfer {
    pub from: Felt,
    pub to: Felt,
    /// Token ID as U256 (256-bit)
    pub token_id: U256,
    pub token: Felt,
    pub block_number: u64,
    pub transaction_hash: Felt,
}

impl TypedBody for NftTransfer {
    fn envelope_type_id(&self) -> torii::etl::envelope::TypeId {
        torii::etl::envelope::TypeId::new("erc721.transfer")
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

/// Approval event from ERC721 token (single token approval)
#[derive(Debug, Clone)]
pub struct NftApproval {
    pub owner: Felt,
    pub approved: Felt,
    /// Token ID as U256 (256-bit)
    pub token_id: U256,
    pub token: Felt,
    pub block_number: u64,
    pub transaction_hash: Felt,
}

impl TypedBody for NftApproval {
    fn envelope_type_id(&self) -> torii::etl::envelope::TypeId {
        torii::etl::envelope::TypeId::new("erc721.approval")
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

/// ApprovalForAll event from ERC721 token (operator approval)
#[derive(Debug, Clone)]
pub struct OperatorApproval {
    pub owner: Felt,
    pub operator: Felt,
    pub approved: bool,
    pub token: Felt,
    pub block_number: u64,
    pub transaction_hash: Felt,
}

impl TypedBody for OperatorApproval {
    fn envelope_type_id(&self) -> torii::etl::envelope::TypeId {
        torii::etl::envelope::TypeId::new("erc721.approval_for_all")
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

/// MetadataUpdate event (EIP-4906) — single token
#[derive(Debug, Clone)]
pub struct MetadataUpdate {
    pub token: Felt,
    pub token_id: U256,
    pub block_number: u64,
    pub transaction_hash: Felt,
}

impl TypedBody for MetadataUpdate {
    fn envelope_type_id(&self) -> torii::etl::envelope::TypeId {
        torii::etl::envelope::TypeId::new("erc721.metadata_update")
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

/// BatchMetadataUpdate event (EIP-4906) — range of tokens
#[derive(Debug, Clone)]
pub struct BatchMetadataUpdate {
    pub token: Felt,
    pub from_token_id: U256,
    pub to_token_id: U256,
    pub block_number: u64,
    pub transaction_hash: Felt,
}

impl TypedBody for BatchMetadataUpdate {
    fn envelope_type_id(&self) -> torii::etl::envelope::TypeId {
        torii::etl::envelope::TypeId::new("erc721.batch_metadata_update")
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

/// ERC721 event decoder
///
/// Decodes multiple ERC721 events:
/// - Transfer(from, to, token_id)
/// - Approval(owner, approved, token_id)
/// - ApprovalForAll(owner, operator, approved)
/// - MetadataUpdate(token_id) — EIP-4906
/// - BatchMetadataUpdate(from_token_id, to_token_id) — EIP-4906
///
/// Supports both modern (keys) and legacy (data-only) formats from OpenZeppelin.
pub struct Erc721Decoder;

impl Erc721Decoder {
    pub fn new() -> Self {
        Self
    }

    /// Decode Transfer event into envelope
    ///
    /// Transfer event signatures (supports both modern and legacy):
    ///
    /// Modern ERC721:
    /// - keys[0]: Transfer selector
    /// - keys[1]: from address
    /// - keys[2]: to address
    /// - keys[3]: token_id_low (u128)
    /// - keys[4]: token_id_high (u128)
    ///
    /// Legacy ERC721 (all in data):
    /// - keys[0]: Transfer selector
    /// - data[0]: from address
    /// - data[1]: to address
    /// - data[2]: token_id_low (u128)
    /// - data[3]: token_id_high (u128)
    async fn decode_transfer(
        &self,
        from_address: &Felt,
        block_number: u64,
        transaction_hash: &Felt,
        keys: &[Felt],
        data: &[Felt],
    ) -> Result<Option> {
        let from;
        let to;
        let token_id: U256;

        if keys.len() == 5 && data.is_empty() {
            // Modern ERC721: from, to, token_id_low, token_id_high all in keys
            from = keys[1];
            to = keys[2];
            let low: u128 = keys[3].try_into().unwrap_or(0);
            let high: u128 = keys[4].try_into().unwrap_or(0);
            token_id = U256::from_words(low, high);
        } else if keys.len() == 1 && data.len() == 4 {
            // Legacy ERC721: from, to, token_id_low, token_id_high in data
            from = data[0];
            to = data[1];
            let low: u128 = data[2].try_into().unwrap_or(0);
            let high: u128 = data[3].try_into().unwrap_or(0);
            token_id = U256::from_words(low, high);
        } else if keys.len() == 4 && data.is_empty() {
            // Alternative modern format with single felt token_id
            from = keys[1];
            to = keys[2];
            let id_felt: u128 = keys[3].try_into().unwrap_or(0);
            token_id = U256::from(id_felt);
        } else if keys.len() == 1 && data.len() == 3 {
            // Alternative legacy format with single felt token_id
            from = data[0];
            to = data[1];
            let id_felt: u128 = data[2].try_into().unwrap_or(0);
            token_id = U256::from(id_felt);
        } else {
            tracing::warn!(
                target: "torii_erc721::decoder",
                token = %format!("{:#x}", from_address),
                tx_hash = %format!("{:#x}", transaction_hash),
                block_number = block_number,
                keys_len = keys.len(),
                data_len = data.len(),
                "Malformed ERC721 Transfer event"
            );
            return Ok(None);
        }

        Ok(Some(NftTransfer {
            from,
            to,
            token_id,
            token: *from_address,
            block_number: block_number,
            transaction_hash: *transaction_hash,
        }))
    }

    /// Decode Approval event into envelope
    ///
    /// Approval event signatures (supports both modern and legacy):
    ///
    /// Modern ERC721:
    /// - keys[0]: Approval selector
    /// - keys[1]: owner address
    /// - keys[2]: approved address
    /// - keys[3]: token_id_low (u128)
    /// - keys[4]: token_id_high (u128)
    ///
    /// Legacy ERC721:
    /// - keys[0]: Approval selector
    /// - data[0]: owner address
    /// - data[1]: approved address
    /// - data[2]: token_id_low (u128)
    /// - data[3]: token_id_high (u128)
    async fn decode_approval(
        &self,
        from_address: &Felt,
        block_number: u64,
        transaction_hash: &Felt,
        keys: &[Felt],
        data: &[Felt],
    ) -> Result<Option<Envelope>> {
        let owner;
        let approved;
        let token_id: U256;

        if keys.len() == 5 && data.is_empty() {
            // Modern ERC721: owner, approved, token_id_low, token_id_high all in keys
            owner = keys[1];
            approved = keys[2];
            let low: u128 = keys[3].try_into().unwrap_or(0);
            let high: u128 = keys[4].try_into().unwrap_or(0);
            token_id = U256::from_words(low, high);
        } else if keys.len() == 1 && data.len() == 4 {
            // Legacy ERC721: owner, approved, token_id_low, token_id_high in data
            owner = data[0];
            approved = data[1];
            let low: u128 = data[2].try_into().unwrap_or(0);
            let high: u128 = data[3].try_into().unwrap_or(0);
            token_id = U256::from_words(low, high);
        } else if keys.len() == 4 && data.is_empty() {
            // Alternative modern format with single felt token_id
            owner = keys[1];
            approved = keys[2];
            let id_felt: u128 = keys[3].try_into().unwrap_or(0);
            token_id = U256::from(id_felt);
        } else if keys.len() == 1 && data.len() == 3 {
            // Alternative legacy format with single felt token_id
            owner = data[0];
            approved = data[1];
            let id_felt: u128 = data[2].try_into().unwrap_or(0);
            token_id = U256::from(id_felt);
        } else {
            tracing::warn!(
                target: "torii_erc721::decoder",
                token = %format!("{:#x}", from_address),
                tx_hash = %format!("{:#x}", transaction_hash),
                block_number = block_number,
                keys_len = keys.len(),
                data_len = data.len(),
                "Malformed ERC721 Approval event"
            );
            return Ok(None);
        }

        let envelope_id = format!(
            "erc721_approval_{}_{}",
            block_number,
            format!("{:#x}", transaction_hash)
        );

        Ok(Some(NftApproval {
            owner,
            approved,
            token_id,
            token: *from_address,
            block_number,
            transaction_hash: *transaction_hash,
        }))
    }

    /// Decode ApprovalForAll event into envelope
    ///
    /// ApprovalForAll event signatures (supports both modern and legacy):
    ///
    /// Modern ERC721:
    /// - keys[0]: ApprovalForAll selector
    /// - keys[1]: owner address
    /// - keys[2]: operator address
    /// - data[0]: approved (bool as felt)
    ///
    /// Legacy ERC721:
    /// - keys[0]: ApprovalForAll selector
    /// - data[0]: owner address
    /// - data[1]: operator address
    /// - data[2]: approved (bool as felt)
    async fn decode_approval_for_all(
        &self,
        from_address: &Felt,
        block_number: u64,
        transaction_hash: &Felt,
        keys: &[Felt],
        data: &[Felt],
    ) -> Result<Option<Envelope>> {
        let owner;
        let operator;
        let approved: bool;

        if keys.len() == 3 && data.len() == 1 {
            // Modern ERC721: owner, operator in keys; approved in data
            owner = keys[1];
            operator = keys[2];
            approved = data[0] != Felt::ZERO;
        } else if keys.len() == 1 && data.len() == 3 {
            // Legacy ERC721: owner, operator, approved all in data
            owner = data[0];
            operator = data[1];
            approved = data[2] != Felt::ZERO;
        } else {
            tracing::warn!(
                target: "torii_erc721::decoder",
                token = %format!("{:#x}", from_address),
                tx_hash = %format!("{:#x}", transaction_hash),
                block_number = block_number,
                keys_len = keys.len(),
                data_len = data.len(),
                "Malformed ERC721 ApprovalForAll event"
            );
            return Ok(None);
        }

        Ok(Some(OperatorApproval {
            owner,
            operator,
            approved,
            token: *from_address,
            block_number,
            transaction_hash: *transaction_hash,
        }))
    }

    /// Decode MetadataUpdate event (EIP-4906)
    ///
    /// MetadataUpdate(uint256 tokenId):
    /// - keys[0]: selector
    /// - data[0]: token_id_low
    /// - data[1]: token_id_high
    /// OR:
    /// - keys[0]: selector
    /// - keys[1]: token_id_low
    /// - keys[2]: token_id_high
    async fn decode_metadata_update(
        &self,
        from_address: &Felt,
        block_number: u64,
        transaction_hash: &Felt,
        keys: &[Felt],
        data: &[Felt],
    ) -> Result<Option<Envelope>> {
        let token_id: U256;

        if data.len() >= 2 {
            let low: u128 = data[0].try_into().unwrap_or(0);
            let high: u128 = data[1].try_into().unwrap_or(0);
            token_id = U256::from_words(low, high);
        } else if keys.len() >= 3 {
            let low: u128 = keys[1].try_into().unwrap_or(0);
            let high: u128 = keys[2].try_into().unwrap_or(0);
            token_id = U256::from_words(low, high);
        } else if data.len() == 1 {
            let id: u128 = data[0].try_into().unwrap_or(0);
            token_id = U256::from(id);
        } else if keys.len() == 2 {
            let id: u128 = keys[1].try_into().unwrap_or(0);
            token_id = U256::from(id);
        } else {
            tracing::warn!(
                target: "torii_erc721::decoder",
                token = %format!("{:#x}", from_address),
                "Malformed MetadataUpdate event"
            );
            return Ok(None);
        }

        Ok(Some(MetadataUpdate {
            token: *from_address,
            token_id,
            block_number,
            transaction_hash: *transaction_hash,
        }))
    }

    /// Decode BatchMetadataUpdate event (EIP-4906)
    ///
    /// BatchMetadataUpdate(uint256 fromTokenId, uint256 toTokenId)
    async fn decode_batch_metadata_update(
        &self,
        from_address: &Felt,
        block_number: u64,
        transaction_hash: &Felt,
        keys: &[Felt],
        data: &[Felt],
    ) -> Result<Option<Envelope>> {
        let from_token_id: U256;
        let to_token_id: U256;

        if data.len() >= 4 {
            let low: u128 = data[0].try_into().unwrap_or(0);
            let high: u128 = data[1].try_into().unwrap_or(0);
            from_token_id = U256::from_words(low, high);
            let low: u128 = data[2].try_into().unwrap_or(0);
            let high: u128 = data[3].try_into().unwrap_or(0);
            to_token_id = U256::from_words(low, high);
        } else if keys.len() >= 5 {
            let low: u128 = keys[1].try_into().unwrap_or(0);
            let high: u128 = keys[2].try_into().unwrap_or(0);
            from_token_id = U256::from_words(low, high);
            let low: u128 = keys[3].try_into().unwrap_or(0);
            let high: u128 = keys[4].try_into().unwrap_or(0);
            to_token_id = U256::from_words(low, high);
        } else {
            tracing::warn!(
                target: "torii_erc721::decoder",
                token = %format!("{:#x}", from_address),
                "Malformed BatchMetadataUpdate event"
            );
            return Ok(None);
        }

        Ok(Some(BatchMetadataUpdate {
            token: *from_address,
            from_token_id,
            to_token_id,
            block_number,
            transaction_hash: *transaction_hash,
        }))
    }

    async fn decode_erc721_event(
        &self,
        from_address: &Felt,
        block_number: u64,
        transaction_hash: &Felt,
        keys: &[Felt],
        data: &[Felt],
    ) -> Result<Option<Erc721Msg>> {
        let selector = match keys.get(0) {
            Some(s) => *s,
            None => return Ok(None),
        };
        match selector {
            ERC721_TRANSFER_SELECTOR => self
                .decode_transfer(from_address, block_number, transaction_hash, keys, data)
                .await
                .map(|opt| opt.map(Into::into)),
            ERC721_APPROVAL_SELECTOR => self
                .decode_approval(from_address, block_number, transaction_hash, keys, data)
                .await
                .map(|opt| opt.map(Into::into)),
            ERC721_APPROVAL_FOR_ALL_SELECTOR => self
                .decode_approval_for_all(from_address, block_number, transaction_hash, keys, data)
                .await
                .map(|opt| opt.map(Into::into)),
            ERC721_METADATA_UPDATE_SELECTOR => self
                .decode_metadata_update(from_address, block_number, transaction_hash, keys, data)
                .await
                .map(|opt| opt.map(Into::into)),
            ERC721_BATCH_METADATA_UPDATE_SELECTOR => self
                .decode_batch_metadata_update(
                    from_address,
                    block_number,
                    transaction_hash,
                    keys,
                    data,
                )
                .await
                .map(|opt| opt.map(Into::into)),
            _ => {
                tracing::trace!(
                    target: "torii_erc721::decoder",
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

impl Default for Erc721Decoder {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl Decoder for Erc721Decoder {
    fn decoder_name(&self) -> &'static str {
        "erc721"
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
            .decode_erc721_event(from_address, block_number, transaction_hash, keys, data)
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
    async fn test_decode_modern_transfer() {
        let decoder = Erc721Decoder::new();

        // Modern format: all in keys
        let from_address = Felt::from(0x123u64);
        let keys = vec![
            ERC721_TRANSFER_SELECTOR,
            Felt::from(0x1u64), // from
            Felt::from(0x2u64), // to
            Felt::from(42u64),  // token_id_low
            Felt::ZERO,         // token_id_high
        ];
        let data = vec![];
        let block_number = 100;
        let transaction_hash = Felt::from(0xabcdu64);

        let msgs = decoder
            .decode_event(&from_address, block_number, &transaction_hash, &keys, &data)
            .await
            .unwrap();
        assert_eq!(msgs.len(), 1);

        let msg = msgs[0].as_any().downcast_ref::<Erc721Msg>().unwrap();
        match msg {
            Erc721Msg::Transfer(t) => {
                assert_eq!(t.from, Felt::from(0x1u64));
                assert_eq!(t.to, Felt::from(0x2u64));
                assert_eq!(t.token_id, U256::from(42u64));
                assert_eq!(t.token, Felt::from(0x123u64));
            }
            _ => panic!("Expected Transfer event"),
        }
    }

    #[tokio::test]
    async fn test_decode_legacy_transfer() {
        let decoder = Erc721Decoder::new();

        // Legacy format: all in data
        let from_address = Felt::from(0x456u64);
        let keys = vec![ERC721_TRANSFER_SELECTOR];
        let data = vec![
            Felt::from(0xau64), // from
            Felt::from(0xbu64), // to
            Felt::from(100u64), // token_id_low
            Felt::ZERO,         // token_id_high
        ];
        let block_number = 200;
        let transaction_hash = Felt::from(0xef01u64);

        let msgs = decoder
            .decode_event(&from_address, block_number, &transaction_hash, &keys, &data)
            .await
            .unwrap();
        assert_eq!(msgs.len(), 1);

        let msg = msgs[0].as_any().downcast_ref::<Erc721Msg>().unwrap();
        match msg {
            Erc721Msg::Transfer(t) => {
                assert_eq!(t.from, Felt::from(0xau64));
                assert_eq!(t.to, Felt::from(0xbu64));
                assert_eq!(t.token_id, U256::from(100u64));
            }
            _ => panic!("Expected Transfer event"),
        }
    }

    #[tokio::test]
    async fn test_decode_approval_for_all() {
        let decoder = Erc721Decoder::new();

        // Modern format
        let from_address = Felt::from(0x789u64);
        let keys = vec![
            ERC721_APPROVAL_FOR_ALL_SELECTOR,
            Felt::from(0xcu64), // owner
            Felt::from(0xdu64), // operator
        ];
        let data = vec![Felt::from(1u64)]; // approved = true
        let block_number = 300;
        let transaction_hash = Felt::from(0x2345u64);

        let msgs = decoder
            .decode_event(&from_address, block_number, &transaction_hash, &keys, &data)
            .await
            .unwrap();
        assert_eq!(msgs.len(), 1);

        let msg = msgs[0].as_any().downcast_ref::<Erc721Msg>().unwrap();
        match msg {
            Erc721Msg::ApprovalForAll(a) => {
                assert_eq!(a.owner, Felt::from(0xcu64));
                assert_eq!(a.operator, Felt::from(0xdu64));
                assert!(a.approved);
            }
            _ => panic!("Expected ApprovalForAll event"),
        }
    }
}
