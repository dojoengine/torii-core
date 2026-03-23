//! ERC1155 event decoder (TransferSingle, TransferBatch, ApprovalForAll, URI)

use anyhow::Result;
use async_trait::async_trait;
use starknet::core::codec::Decode;
use starknet::core::types::{ByteArray, Felt, U256};
use starknet::core::utils::parse_cairo_short_string;
use starknet::macros::selector;
use std::any::Any;
use torii::etl::{Decoder, TypedBody};
use torii::typed_body_impl;
use torii_common::bytes_to_u256;

pub const ERC1155_TRANSFER_SINGLE_SELECTOR: Felt = selector!("TransferSingle");
pub const ERC1155_TRANSFER_BATCH_SELECTOR: Felt = selector!("TransferBatch");
pub const ERC1155_APPROVAL_FOR_ALL_SELECTOR: Felt = selector!("ApprovalForAll");
pub const ERC1155_URI_SELECTOR: Felt = selector!("URI");

#[derive(Debug, Clone)]
pub enum Erc1155Msg {
    TransferSingle(TransferSingle),
    TransferBatch(TransferBatch),
    ApprovalForAll(OperatorApproval),
    UriUpdate(UriUpdate),
}

typed_body_impl!(Erc1155Msg, "erc1155");

impl From<TransferSingle> for Erc1155Msg {
    fn from(value: TransferSingle) -> Self {
        Erc1155Msg::TransferSingle(value)
    }
}

impl From<TransferBatch> for Erc1155Msg {
    fn from(value: TransferBatch) -> Self {
        Erc1155Msg::TransferBatch(value)
    }
}

impl From<OperatorApproval> for Erc1155Msg {
    fn from(value: OperatorApproval) -> Self {
        Erc1155Msg::ApprovalForAll(value)
    }
}

impl From<UriUpdate> for Erc1155Msg {
    fn from(value: UriUpdate) -> Self {
        Erc1155Msg::UriUpdate(value)
    }
}

/// TransferSingle event from ERC1155 token
#[derive(Debug, Clone)]
pub struct TransferSingle {
    pub operator: Felt,
    pub from: Felt,
    pub to: Felt,
    /// Token ID as U256 (256-bit)
    pub id: U256,
    /// Amount as U256 (256-bit)
    pub value: U256,
    pub token: Felt,
    pub block_number: u64,
    pub transaction_hash: Felt,
}

impl TypedBody for TransferSingle {
    fn envelope_type_id(&self) -> torii::etl::envelope::TypeId {
        torii::etl::envelope::TypeId::new("erc1155.transfer_single")
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

/// TransferBatch event from ERC1155 token (denormalized into individual transfers)
#[derive(Debug, Clone)]
pub struct TransferBatch {
    pub operator: Felt,
    pub from: Felt,
    pub to: Felt,
    /// Token ID for this specific transfer in the batch
    pub id: U256,
    /// Amount for this specific transfer in the batch
    pub value: U256,
    /// Index in the original batch
    pub batch_index: u32,
    pub token: Felt,
    pub block_number: u64,
    pub transaction_hash: Felt,
}

impl TypedBody for TransferBatch {
    fn envelope_type_id(&self) -> torii::etl::envelope::TypeId {
        torii::etl::envelope::TypeId::new("erc1155.transfer_batch")
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

/// ApprovalForAll event from ERC1155 token
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
        torii::etl::envelope::TypeId::new("erc1155.approval_for_all")
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

/// URI event from ERC1155 token
#[derive(Debug, Clone)]
pub struct UriUpdate {
    pub token: Felt,
    pub token_id: U256,
    pub uri: String,
    pub block_number: u64,
    pub transaction_hash: Felt,
}

impl TypedBody for UriUpdate {
    fn envelope_type_id(&self) -> torii::etl::envelope::TypeId {
        torii::etl::envelope::TypeId::new("erc1155.uri")
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

/// ERC1155 event decoder
///
/// Decodes multiple ERC1155 events:
/// - TransferSingle(operator, from, to, id, value)
/// - TransferBatch(operator, from, to, ids, values)
/// - ApprovalForAll(owner, operator, approved)
///
/// Supports both modern (keys) and legacy (data-only) formats.
pub struct Erc1155Decoder;

impl Erc1155Decoder {
    pub fn new() -> Self {
        Self
    }

    fn felt_to_u256(felt: Felt) -> U256 {
        bytes_to_u256(&felt.to_bytes_be())
    }

    fn decode_string_result(result: &[Felt]) -> Option<String> {
        if result.is_empty() {
            return None;
        }

        if result.len() == 1 {
            return parse_cairo_short_string(&result[0])
                .ok()
                .filter(|s| !s.is_empty());
        }

        if let Ok(byte_array) = ByteArray::decode(result) {
            if let Ok(s) = String::try_from(byte_array) {
                if !s.is_empty() {
                    return Some(s);
                }
            }
        }

        let len: usize = result[0].try_into().unwrap_or(0usize);
        if len > 0 && len < 1000 && result.len() > len {
            let mut out = String::new();
            for felt in &result[1..=len] {
                if let Ok(chunk) = parse_cairo_short_string(felt) {
                    out.push_str(&chunk);
                }
            }
            if !out.is_empty() {
                return Some(out);
            }
        }

        None
    }

    /// Decode TransferSingle event into envelope
    ///
    /// TransferSingle event signatures:
    ///
    /// Modern ERC1155:
    /// - keys[0]: TransferSingle selector
    /// - keys[1]: operator address
    /// - keys[2]: from address
    /// - keys[3]: to address
    /// - data[0]: id_low (u128)
    /// - data[1]: id_high (u128)
    /// - data[2]: value_low (u128)
    /// - data[3]: value_high (u128)
    ///
    /// Legacy ERC1155:
    /// - keys[0]: TransferSingle selector
    /// - data[0]: operator address
    /// - data[1]: from address
    /// - data[2]: to address
    /// - data[3]: id_low (u128)
    /// - data[4]: id_high (u128)
    /// - data[5]: value_low (u128)
    /// - data[6]: value_high (u128)
    async fn decode_transfer_single(
        &self,
        from_address: &Felt,
        block_number: u64,
        transaction_hash: &Felt,
        keys: &[Felt],
        data: &[Felt],
    ) -> Result<Option<TransferSingle>> {
        let operator;
        let from;
        let to;
        let id: U256;
        let value: U256;

        if keys.len() == 4 && data.len() == 4 {
            // Modern format: operator, from, to in keys; id, value in data
            operator = keys[1];
            from = keys[2];
            to = keys[3];
            let id_low: u128 = data[0].try_into().unwrap_or(0);
            let id_high: u128 = data[1].try_into().unwrap_or(0);
            id = U256::from_words(id_low, id_high);
            let value_low: u128 = data[2].try_into().unwrap_or(0);
            let value_high: u128 = data[3].try_into().unwrap_or(0);
            value = U256::from_words(value_low, value_high);
        } else if keys.len() == 1 && data.len() == 7 {
            // Legacy format: all in data
            operator = data[0];
            from = data[1];
            to = data[2];
            let id_low: u128 = data[3].try_into().unwrap_or(0);
            let id_high: u128 = data[4].try_into().unwrap_or(0);
            id = U256::from_words(id_low, id_high);
            let value_low: u128 = data[5].try_into().unwrap_or(0);
            let value_high: u128 = data[6].try_into().unwrap_or(0);
            value = U256::from_words(value_low, value_high);
        } else if keys.len() == 4 && data.len() == 2 {
            // Alternative modern format with single felt id and value
            operator = keys[1];
            from = keys[2];
            to = keys[3];
            id = Self::felt_to_u256(data[0]);
            value = Self::felt_to_u256(data[1]);
        } else {
            tracing::warn!(
                target: "torii_erc1155::decoder",
                token = %format!("{:#x}", from_address),
                tx_hash = %format!("{:#x}", transaction_hash),
                block_number = block_number,
                keys_len = keys.len(),
                data_len = data.len(),
                "Malformed ERC1155 TransferSingle event"
            );
            return Ok(None);
        }

        Ok(Some(TransferSingle {
            operator,
            from,
            to,
            id,
            value,
            token: *from_address,
            block_number,
            transaction_hash: *transaction_hash,
        }))
    }

    /// Decode TransferBatch event into multiple envelopes (one per id/value pair)
    ///
    /// TransferBatch event signatures:
    ///
    /// Modern ERC1155:
    /// - keys[0]: TransferBatch selector
    /// - keys[1]: operator address
    /// - keys[2]: from address
    /// - keys[3]: to address
    /// - data[0]: ids_len
    /// - data[1..1+ids_len*2]: ids (low, high pairs)
    /// - data[1+ids_len*2]: values_len
    /// - data[2+ids_len*2..]: values (low, high pairs)
    ///
    /// Legacy: all in data
    async fn decode_transfer_batch(
        &self,
        from_address: &Felt,
        block_number: u64,
        transaction_hash: &Felt,
        keys: &[Felt],
        data: &[Felt],
    ) -> Result<Vec<TransferBatch>> {
        let operator;
        let from;
        let to;
        let mut data_offset = 0;

        if keys.len() == 4 {
            // Modern format: operator, from, to in keys
            operator = keys[1];
            from = keys[2];
            to = keys[3];
        } else if keys.len() == 1 && data.len() >= 3 {
            // Legacy format: operator, from, to at start of data
            operator = data[0];
            from = data[1];
            to = data[2];
            data_offset = 3;
        } else {
            tracing::warn!(
                target: "torii_erc1155::decoder",
                token = %format!("{:#x}", from_address),
                tx_hash = %format!("{:#x}", transaction_hash),
                block_number = block_number,
                keys_len = keys.len(),
                data_len = data.len(),
                "Malformed ERC1155 TransferBatch event"
            );
            return Ok(vec![]);
        }

        // Parse ids array
        if data.len() <= data_offset {
            return Ok(vec![]);
        }

        let ids_len: usize = data[data_offset].try_into().unwrap_or(0);
        data_offset += 1;

        fn parse_u256_slice(items: &[Felt], as_pairs: bool) -> Vec<U256> {
            if as_pairs {
                let mut out = Vec::with_capacity(items.len() / 2);
                for chunk in items.chunks_exact(2) {
                    let low: u128 = chunk[0].try_into().unwrap_or(0);
                    let high: u128 = chunk[1].try_into().unwrap_or(0);
                    out.push(U256::from_words(low, high));
                }
                out
            } else {
                items
                    .iter()
                    .copied()
                    .map(Erc1155Decoder::felt_to_u256)
                    .collect()
            }
        }

        let mut ids: Vec<U256> = Vec::new();
        let mut values: Vec<U256> = Vec::new();
        let mut parsed = false;

        // Try both pair-based and single-felt array layouts for ids and values.
        // Standard Starknet ERC1155 uses U256 pairs, but some contracts emit felt arrays.
        for ids_as_pairs in [true, false] {
            let id_words = if ids_as_pairs { 2 } else { 1 };
            let ids_end = data_offset.saturating_add(ids_len.saturating_mul(id_words));
            if ids_end > data.len() || ids_end >= data.len() {
                continue;
            }

            let candidate_ids = parse_u256_slice(&data[data_offset..ids_end], ids_as_pairs);
            let values_len: usize = data[ids_end].try_into().unwrap_or(0);
            let values_start = ids_end + 1;

            for values_as_pairs in [true, false] {
                let value_words = if values_as_pairs { 2 } else { 1 };
                let values_end =
                    values_start.saturating_add(values_len.saturating_mul(value_words));
                if values_end > data.len() {
                    continue;
                }

                ids.clone_from(&candidate_ids);
                values = parse_u256_slice(&data[values_start..values_end], values_as_pairs);
                parsed = true;
                break;
            }

            if parsed {
                break;
            }
        }

        if !parsed {
            tracing::warn!(
                target: "torii_erc1155::decoder",
                token = %format!("{:#x}", from_address),
                tx_hash = %format!("{:#x}", transaction_hash),
                block_number = block_number,
                ids_len = ids_len,
                "Failed to parse ERC1155 TransferBatch ids/values arrays"
            );
            return Ok(vec![]);
        }

        let mut transfers = Vec::new();
        for (i, (id, value)) in ids.iter().zip(values.iter()).enumerate() {
            transfers.push(TransferBatch {
                operator,
                from,
                to,
                id: *id,
                value: *value,
                batch_index: i as u32,
                token: *from_address,
                block_number,
                transaction_hash: *transaction_hash,
            });
        }

        Ok(transfers)
    }

    /// Decode ApprovalForAll event into envelope
    async fn decode_approval_for_all(
        &self,
        from_address: &Felt,
        block_number: u64,
        transaction_hash: &Felt,
        keys: &[Felt],
        data: &[Felt],
    ) -> Result<Option<OperatorApproval>> {
        let owner;
        let operator;
        let approved: bool;

        if keys.len() == 3 && data.len() == 1 {
            // Modern format: owner, operator in keys; approved in data
            owner = keys[1];
            operator = keys[2];
            approved = data[0] != Felt::ZERO;
        } else if keys.len() == 1 && data.len() == 3 {
            // Legacy format: all in data
            owner = data[0];
            operator = data[1];
            approved = data[2] != Felt::ZERO;
        } else {
            tracing::warn!(
                target: "torii_erc1155::decoder",
                token = %format!("{:#x}", from_address),
                tx_hash = %format!("{:#x}", transaction_hash),
                block_number = block_number,
                keys_len = keys.len(),
                data_len = data.len(),
                "Malformed ERC1155 ApprovalForAll event"
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

    /// Decode URI event into envelope
    ///
    /// Common ERC1155 Starknet layout:
    /// - keys[0]: URI selector
    /// - keys[1]: token id (felt-encoded)
    /// - data: URI payload (short string or ByteArray)
    async fn decode_uri(
        &self,
        from_address: &Felt,
        block_number: u64,
        transaction_hash: &Felt,
        keys: &[Felt],
        data: &[Felt],
    ) -> Result<Option<UriUpdate>> {
        if keys.len() < 2 || data.is_empty() {
            return Ok(None);
        }

        let token_id = Self::felt_to_u256(keys[1]);
        let Some(uri) = Self::decode_string_result(data) else {
            return Ok(None);
        };

        Ok(Some(UriUpdate {
            token: *from_address,
            token_id,
            uri,
            block_number,
            transaction_hash: *transaction_hash,
        }))
    }

    async fn decode_erc1155_event(
        &self,
        from_address: &Felt,
        block_number: u64,
        transaction_hash: &Felt,
        keys: &[Felt],
        data: &[Felt],
    ) -> Result<Vec<Erc1155Msg>> {
        let selector = match keys.get(0) {
            Some(s) => *s,
            None => return Ok(vec![]),
        };
        match selector {
            ERC1155_TRANSFER_SINGLE_SELECTOR => self
                .decode_transfer_single(from_address, block_number, transaction_hash, keys, data)
                .await
                .map(|opt| opt.into_iter().map(Into::into).collect()),
            ERC1155_TRANSFER_BATCH_SELECTOR => self
                .decode_transfer_batch(from_address, block_number, transaction_hash, keys, data)
                .await
                .map(|v| v.into_iter().map(Into::into).collect()),
            ERC1155_APPROVAL_FOR_ALL_SELECTOR => self
                .decode_approval_for_all(from_address, block_number, transaction_hash, keys, data)
                .await
                .map(|opt| opt.into_iter().map(Into::into).collect()),
            ERC1155_URI_SELECTOR => self
                .decode_uri(from_address, block_number, transaction_hash, keys, data)
                .await
                .map(|opt| opt.into_iter().map(Into::into).collect()),
            _ => {
                tracing::trace!(
                    target: "torii_erc1155::decoder",
                    token = %format!("{:#x}", from_address),
                    selector = %format!("{:#x}", selector),
                    keys_len = keys.len(),
                    data_len = data.len(),
                    block_number = block_number,
                    tx_hash = %format!("{:#x}", transaction_hash),
                    "Unhandled event selector"
                );
                Ok(vec![])
            }
        }
    }
}

impl Default for Erc1155Decoder {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl Decoder for Erc1155Decoder {
    fn decoder_name(&self) -> &'static str {
        "erc1155"
    }

    async fn decode_event(
        &self,
        from_address: &Felt,
        block_number: u64,
        transaction_hash: &Felt,
        keys: &[Felt],
        data: &[Felt],
    ) -> Result<Vec<Box<dyn TypedBody>>> {
        let msgs = self
            .decode_erc1155_event(from_address, block_number, transaction_hash, keys, data)
            .await?;
        Ok(msgs
            .into_iter()
            .map(|m| -> Box<dyn TypedBody> { Box::new(m) })
            .collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_decode_transfer_single_modern() {
        let decoder = Erc1155Decoder::new();

        // Modern format: operator, from, to in keys; id, value in data
        let from_address = Felt::from(0x123u64);
        let keys = vec![
            ERC1155_TRANSFER_SINGLE_SELECTOR,
            Felt::from(0x1u64), // operator
            Felt::from(0x2u64), // from
            Felt::from(0x3u64), // to
        ];
        let data = vec![
            Felt::from(42u64),  // id_low
            Felt::ZERO,         // id_high
            Felt::from(100u64), // value_low
            Felt::ZERO,         // value_high
        ];
        let block_number = 100;
        let transaction_hash = Felt::from(0xabcdu64);

        let msgs = decoder
            .decode_event(&from_address, block_number, &transaction_hash, &keys, &data)
            .await
            .unwrap();
        assert_eq!(msgs.len(), 1);

        let msg = msgs[0].as_any().downcast_ref::<Erc1155Msg>().unwrap();
        match msg {
            Erc1155Msg::TransferSingle(t) => {
                assert_eq!(t.operator, Felt::from(0x1u64));
                assert_eq!(t.from, Felt::from(0x2u64));
                assert_eq!(t.to, Felt::from(0x3u64));
                assert_eq!(t.id, U256::from(42u64));
                assert_eq!(t.value, U256::from(100u64));
            }
            _ => panic!("Expected TransferSingle event"),
        }
    }

    #[tokio::test]
    async fn test_decode_approval_for_all() {
        let decoder = Erc1155Decoder::new();

        let from_address = Felt::from(0x456u64);
        let keys = vec![
            ERC1155_APPROVAL_FOR_ALL_SELECTOR,
            Felt::from(0xau64), // owner
            Felt::from(0xbu64), // operator
        ];
        let data = vec![Felt::from(1u64)]; // approved = true
        let block_number = 200;
        let transaction_hash = Felt::from(0xef01u64);

        let msgs = decoder
            .decode_event(&from_address, block_number, &transaction_hash, &keys, &data)
            .await
            .unwrap();
        assert_eq!(msgs.len(), 1);

        let msg = msgs[0].as_any().downcast_ref::<Erc1155Msg>().unwrap();
        match msg {
            Erc1155Msg::ApprovalForAll(a) => {
                assert_eq!(a.owner, Felt::from(0xau64));
                assert_eq!(a.operator, Felt::from(0xbu64));
                assert!(a.approved);
            }
            _ => panic!("Expected ApprovalForAll event"),
        }
    }

    #[tokio::test]
    async fn test_decode_transfer_single_single_felt_preserves_full_felt() {
        let decoder = Erc1155Decoder::new();

        let id_felt =
            Felt::from_hex("0x100000000000000000000000000000001").expect("invalid id felt");
        let value_felt =
            Felt::from_hex("0x200000000000000000000000000000003").expect("invalid value felt");

        let from_address = Felt::from(0x123u64);
        let keys = vec![
            ERC1155_TRANSFER_SINGLE_SELECTOR,
            Felt::from(0x1u64),
            Felt::from(0x2u64),
            Felt::from(0x3u64),
        ];
        let data = vec![id_felt, value_felt];
        let block_number = 101;
        let transaction_hash = Felt::from(0xabcdu64);

        let msgs = decoder
            .decode_event(&from_address, block_number, &transaction_hash, &keys, &data)
            .await
            .unwrap();
        assert_eq!(msgs.len(), 1);

        let msg = msgs[0].as_any().downcast_ref::<Erc1155Msg>().unwrap();
        match msg {
            Erc1155Msg::TransferSingle(t) => {
                assert_eq!(t.id, Erc1155Decoder::felt_to_u256(id_felt));
                assert_eq!(t.value, Erc1155Decoder::felt_to_u256(value_felt));
            }
            _ => panic!("Expected TransferSingle event"),
        }
    }

    #[tokio::test]
    async fn test_decode_transfer_batch_single_felt_arrays() {
        let decoder = Erc1155Decoder::new();

        let from_address = Felt::from(0x123u64);
        let keys = vec![
            ERC1155_TRANSFER_BATCH_SELECTOR,
            Felt::from(0x1u64), // operator
            Felt::from(0x2u64), // from
            Felt::from(0x3u64), // to
        ];
        let data = vec![
            Felt::from(2u64),   // ids_len
            Felt::from(11u64),  // id[0]
            Felt::from(12u64),  // id[1]
            Felt::from(2u64),   // values_len
            Felt::from(101u64), // value[0]
            Felt::from(102u64), // value[1]
        ];
        let block_number = 102;
        let transaction_hash = Felt::from(0xabcfu64);

        let msgs = decoder
            .decode_event(&from_address, block_number, &transaction_hash, &keys, &data)
            .await
            .unwrap();
        assert_eq!(msgs.len(), 2);

        let first = msgs[0].as_any().downcast_ref::<Erc1155Msg>().unwrap();
        let second = msgs[1].as_any().downcast_ref::<Erc1155Msg>().unwrap();

        match first {
            Erc1155Msg::TransferBatch(t) => {
                assert_eq!(t.id, U256::from(11u64));
                assert_eq!(t.value, U256::from(101u64));
            }
            _ => panic!("Expected TransferBatch event"),
        }
        match second {
            Erc1155Msg::TransferBatch(t) => {
                assert_eq!(t.id, U256::from(12u64));
                assert_eq!(t.value, U256::from(102u64));
            }
            _ => panic!("Expected TransferBatch event"),
        }
    }

    #[tokio::test]
    async fn test_decode_uri_event() {
        let decoder = Erc1155Decoder::new();

        // "abc" as short string felt
        let uri_felt = Felt::from(0x616263u64);
        let from_address = Felt::from(0x123u64);
        let keys = vec![
            ERC1155_URI_SELECTOR,
            Felt::from(7u64), // token id
        ];
        let data = vec![uri_felt];
        let block_number = 103;
        let transaction_hash = Felt::from(0xabd0u64);

        let msgs = decoder
            .decode_event(&from_address, block_number, &transaction_hash, &keys, &data)
            .await
            .unwrap();
        assert_eq!(msgs.len(), 1);

        let msg = msgs[0].as_any().downcast_ref::<Erc1155Msg>().unwrap();
        match msg {
            Erc1155Msg::UriUpdate(u) => {
                assert_eq!(u.token, Felt::from(0x123u64));
                assert_eq!(u.token_id, U256::from(7u64));
                assert_eq!(u.uri, "abc".to_string());
            }
            _ => panic!("Expected UriUpdate event"),
        }
    }
}
