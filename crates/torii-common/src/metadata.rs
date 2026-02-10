//! Token metadata fetcher for ERC20/ERC721/ERC1155 contracts.
//!
//! Fetches `name()`, `symbol()`, `decimals()`, and `token_uri(token_id)` by
//! making `starknet_call` requests. Handles both snake_case and camelCase
//! selectors, felt-encoded strings and ByteArray returns.

use anyhow::{Context, Result};
use starknet::core::types::{BlockId, BlockTag, Felt, FunctionCall};
use starknet::macros::selector;
use starknet::providers::jsonrpc::{HttpTransport, JsonRpcClient};
use starknet::providers::Provider;
use std::sync::Arc;

/// Token metadata (common fields for all ERC standards)
#[derive(Debug, Clone, Default)]
pub struct TokenMetadata {
    /// Token name (e.g. "Ether")
    pub name: Option<String>,
    /// Token symbol (e.g. "ETH")
    pub symbol: Option<String>,
    /// Token decimals (e.g. 18). Only meaningful for ERC20.
    pub decimals: Option<u8>,
}

/// Fetches token metadata from on-chain contracts via RPC calls.
pub struct MetadataFetcher {
    provider: Arc<JsonRpcClient<HttpTransport>>,
}

impl MetadataFetcher {
    pub fn new(provider: Arc<JsonRpcClient<HttpTransport>>) -> Self {
        Self { provider }
    }

    /// Fetch metadata for an ERC20 token (name, symbol, decimals).
    pub async fn fetch_erc20_metadata(&self, contract: Felt) -> TokenMetadata {
        let name = self.fetch_string(contract, "name").await;
        let symbol = self.fetch_string(contract, "symbol").await;
        let decimals = self.fetch_decimals(contract).await;

        TokenMetadata {
            name,
            symbol,
            decimals,
        }
    }

    /// Fetch metadata for an ERC721 contract (name, symbol).
    pub async fn fetch_erc721_metadata(&self, contract: Felt) -> TokenMetadata {
        let name = self.fetch_string(contract, "name").await;
        let symbol = self.fetch_string(contract, "symbol").await;

        TokenMetadata {
            name,
            symbol,
            decimals: None,
        }
    }

    /// Fetch metadata for an ERC1155 contract (name, symbol if available).
    /// ERC1155 doesn't mandate name/symbol but many implementations have them.
    pub async fn fetch_erc1155_metadata(&self, contract: Felt) -> TokenMetadata {
        let name = self.fetch_string(contract, "name").await;
        let symbol = self.fetch_string(contract, "symbol").await;

        TokenMetadata {
            name,
            symbol,
            decimals: None,
        }
    }

    /// Fetch `token_uri(token_id)` or `tokenURI(token_id)` for a specific NFT.
    ///
    /// Returns None if the call fails or returns empty data.
    pub async fn fetch_token_uri(&self, contract: Felt, token_id: Felt) -> Option<String> {
        // Try snake_case first, then camelCase
        for sel in [selector!("token_uri"), selector!("tokenURI")] {
            let call = FunctionCall {
                contract_address: contract,
                entry_point_selector: sel,
                calldata: vec![token_id, Felt::ZERO], // u256: (low, high)
            };

            if let Ok(result) = self
                .provider
                .call(call, BlockId::Tag(BlockTag::Latest))
                .await
            {
                if let Some(s) = Self::decode_string_result(&result) {
                    if !s.is_empty() {
                        return Some(s);
                    }
                }
            }
        }

        // Try with single felt arg (some contracts don't use u256 for token_id)
        for sel in [selector!("token_uri"), selector!("tokenURI")] {
            let call = FunctionCall {
                contract_address: contract,
                entry_point_selector: sel,
                calldata: vec![token_id],
            };

            if let Ok(result) = self
                .provider
                .call(call, BlockId::Tag(BlockTag::Latest))
                .await
            {
                if let Some(s) = Self::decode_string_result(&result) {
                    if !s.is_empty() {
                        return Some(s);
                    }
                }
            }
        }

        None
    }

    /// Fetch `uri(token_id)` for ERC1155 tokens.
    pub async fn fetch_uri(&self, contract: Felt, token_id: Felt) -> Option<String> {
        // ERC1155 uses `uri(token_id)` — u256 arg
        let call = FunctionCall {
            contract_address: contract,
            entry_point_selector: selector!("uri"),
            calldata: vec![token_id, Felt::ZERO],
        };

        if let Ok(result) = self
            .provider
            .call(call, BlockId::Tag(BlockTag::Latest))
            .await
        {
            if let Some(s) = Self::decode_string_result(&result) {
                if !s.is_empty() {
                    return Some(s);
                }
            }
        }

        // Try single felt arg
        let call = FunctionCall {
            contract_address: contract,
            entry_point_selector: selector!("uri"),
            calldata: vec![token_id],
        };

        if let Ok(result) = self
            .provider
            .call(call, BlockId::Tag(BlockTag::Latest))
            .await
        {
            if let Some(s) = Self::decode_string_result(&result) {
                if !s.is_empty() {
                    return Some(s);
                }
            }
        }

        None
    }

    /// Fetch a string value (name or symbol) from a contract.
    ///
    /// Tries snake_case first, returns None on failure.
    async fn fetch_string(&self, contract: Felt, fn_name: &str) -> Option<String> {
        let sel = match fn_name {
            "name" => selector!("name"),
            "symbol" => selector!("symbol"),
            _ => return None,
        };

        let call = FunctionCall {
            contract_address: contract,
            entry_point_selector: sel,
            calldata: vec![],
        };

        match self
            .provider
            .call(call, BlockId::Tag(BlockTag::Latest))
            .await
        {
            Ok(result) => Self::decode_string_result(&result),
            Err(e) => {
                tracing::debug!(
                    target: "torii_common::metadata",
                    contract = %format!("{:#x}", contract),
                    fn_name = fn_name,
                    error = %e,
                    "Failed to fetch string"
                );
                None
            }
        }
    }

    /// Fetch `decimals()` from an ERC20 contract.
    async fn fetch_decimals(&self, contract: Felt) -> Option<u8> {
        let call = FunctionCall {
            contract_address: contract,
            entry_point_selector: selector!("decimals"),
            calldata: vec![],
        };

        match self
            .provider
            .call(call, BlockId::Tag(BlockTag::Latest))
            .await
        {
            Ok(result) => {
                if result.is_empty() {
                    return None;
                }
                // decimals() returns a single felt (typically u8)
                let val: u64 = result[0].try_into().unwrap_or(0);
                if val > 255 {
                    tracing::warn!(
                        target: "torii_common::metadata",
                        contract = %format!("{:#x}", contract),
                        value = val,
                        "Unexpected decimals value"
                    );
                    return None;
                }
                Some(val as u8)
            }
            Err(e) => {
                tracing::debug!(
                    target: "torii_common::metadata",
                    contract = %format!("{:#x}", contract),
                    error = %e,
                    "Failed to fetch decimals"
                );
                None
            }
        }
    }

    /// Decode a string result from a contract call.
    ///
    /// Handles multiple return formats:
    /// 1. **Single short string** (felt): Characters packed into a single felt (≤31 bytes)
    /// 2. **Cairo ByteArray**: `[data_len, ...pending_word_chunks, pending_word, pending_word_len]`
    ///    where each chunk in data is a felt-encoded 31-byte segment
    /// 3. **Legacy array**: `[len, felt1, felt2, ...]` where each felt is a short string segment
    fn decode_string_result(result: &[Felt]) -> Option<String> {
        if result.is_empty() {
            return None;
        }

        // Single felt — short string (≤31 chars packed into felt)
        if result.len() == 1 {
            return Self::felt_to_short_string(result[0]);
        }

        // Try ByteArray format: [data_len, ...chunks, pending_word, pending_word_len]
        // data_len tells us how many 31-byte chunks follow
        let data_len: u64 = result[0].try_into().unwrap_or(u64::MAX);

        if data_len < 100 && result.len() >= (data_len as usize + 3) {
            // Looks like ByteArray format
            let mut s = String::new();

            // Decode data chunks (each is 31 bytes)
            for i in 0..data_len as usize {
                if let Some(chunk) = Self::felt_to_fixed_string(result[1 + i], 31) {
                    s.push_str(&chunk);
                }
            }

            // Decode pending word
            let pending_idx = 1 + data_len as usize;
            let pending_len_idx = pending_idx + 1;

            if pending_len_idx < result.len() {
                let pending_word = result[pending_idx];
                let pending_len: u64 = result[pending_len_idx].try_into().unwrap_or(0);

                if pending_len > 0 && pending_len <= 31 {
                    if let Some(chunk) = Self::felt_to_fixed_string(pending_word, pending_len as usize) {
                        s.push_str(&chunk);
                    }
                }
            }

            if !s.is_empty() {
                return Some(s);
            }
        }

        // Fallback: try as legacy array [len, felt1, felt2, ...]
        if result.len() >= 2 {
            let array_len: u64 = result[0].try_into().unwrap_or(0);
            if array_len > 0 && array_len < 100 && result.len() >= (array_len as usize + 1) {
                let mut s = String::new();
                for i in 0..array_len as usize {
                    if let Some(chunk) = Self::felt_to_short_string(result[1 + i]) {
                        s.push_str(&chunk);
                    }
                }
                if !s.is_empty() {
                    return Some(s);
                }
            }
        }

        // Last resort: try first felt as short string
        Self::felt_to_short_string(result[0])
    }

    /// Convert a felt to a short string (up to 31 ASCII bytes packed in big-endian).
    fn felt_to_short_string(felt: Felt) -> Option<String> {
        if felt == Felt::ZERO {
            return None;
        }

        let bytes = felt.to_bytes_be();
        // Find first non-zero byte
        let start = bytes.iter().position(|&b| b != 0)?;
        let slice = &bytes[start..];

        // Check if all bytes are valid ASCII/UTF-8
        match std::str::from_utf8(slice) {
            Ok(s) if !s.is_empty() => Some(s.to_string()),
            _ => None,
        }
    }

    /// Convert a felt to a string of exactly `len` bytes (from the right side of the felt).
    fn felt_to_fixed_string(felt: Felt, len: usize) -> Option<String> {
        if len == 0 || len > 31 {
            return None;
        }

        let bytes = felt.to_bytes_be();
        let slice = &bytes[32 - len..];

        match std::str::from_utf8(slice) {
            Ok(s) if !s.is_empty() => Some(s.to_string()),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_felt_to_short_string() {
        // "ETH" = 0x455448
        let felt = Felt::from(0x455448u64);
        assert_eq!(
            MetadataFetcher::felt_to_short_string(felt),
            Some("ETH".to_string())
        );
    }

    #[test]
    fn test_felt_to_short_string_zero() {
        assert_eq!(MetadataFetcher::felt_to_short_string(Felt::ZERO), None);
    }

    #[test]
    fn test_decode_single_felt_string() {
        let result = vec![Felt::from(0x455448u64)]; // "ETH"
        assert_eq!(
            MetadataFetcher::decode_string_result(&result),
            Some("ETH".to_string())
        );
    }

    #[test]
    fn test_decode_byte_array() {
        // ByteArray: [data_len=0, pending_word="ETH", pending_word_len=3]
        let result = vec![
            Felt::from(0u64),       // data_len = 0 chunks
            Felt::from(0x455448u64), // pending_word = "ETH"
            Felt::from(3u64),       // pending_word_len = 3
        ];
        assert_eq!(
            MetadataFetcher::decode_string_result(&result),
            Some("ETH".to_string())
        );
    }

    #[test]
    fn test_decode_empty() {
        assert_eq!(MetadataFetcher::decode_string_result(&[]), None);
    }
}
