//! Balance fetcher for making starknet_call to balance_of
//!
//! Supports batch RPC requests for efficiency when fetching multiple balances
//! at specific block heights (used for inconsistency detection).

use anyhow::{Context, Result};

/// Maximum number of requests per batch to avoid RPC limits
const MAX_BATCH_SIZE: usize = 500;
use starknet::core::types::{requests::CallRequest, BlockId, Felt, FunctionCall, U256};
use starknet::macros::selector;
use starknet::providers::jsonrpc::{HttpTransport, JsonRpcClient};
use starknet::providers::{Provider, ProviderRequestData, ProviderResponseData};
use std::sync::Arc;

/// Request for fetching a balance at a specific block
#[derive(Debug, Clone)]
pub struct BalanceFetchRequest {
    /// ERC20 token contract address
    pub token: Felt,
    /// Wallet address to fetch balance for
    pub wallet: Felt,
    /// Block number to fetch balance at (typically block N-1 before the transfer)
    pub block_number: u64,
}

/// Balance fetcher for ERC20 tokens
///
/// Makes starknet_call requests to fetch balance_of at specific block heights.
/// Used for detecting and correcting balance inconsistencies caused by
/// genesis allocations, airdrops, or other transfers without events.
pub struct BalanceFetcher {
    provider: Arc<JsonRpcClient<HttpTransport>>,
}

impl BalanceFetcher {
    /// Create a new balance fetcher with the given provider
    pub fn new(provider: Arc<JsonRpcClient<HttpTransport>>) -> Self {
        Self { provider }
    }

    /// Fetch a single balance at a specific block
    ///
    /// Returns the balance, or 0 if the call fails (contract may not exist yet, etc.)
    pub async fn fetch_balance(
        &self,
        token: Felt,
        wallet: Felt,
        block_number: u64,
    ) -> Result<U256> {
        let call = FunctionCall {
            contract_address: token,
            // TODO: Some contracts use balance_of (snake_case), others use balanceOf (camelCase).
            // We need a strategy to handle both - possibly try one, fallback to other, or use ABI.
            entry_point_selector: selector!("balanceOf"),
            calldata: vec![wallet],
        };

        let block_id = BlockId::Number(block_number);

        match self.provider.call(call, block_id).await {
            Ok(result) => Ok(parse_u256_result(&result)),
            Err(e) => {
                tracing::warn!(
                    target: "torii_erc20::balance_fetcher",
                    token = %token,
                    wallet = %wallet,
                    block = block_number,
                    error = %e,
                    "Failed to fetch balance, returning 0"
                );
                Ok(U256::from(0u64))
            }
        }
    }

    /// Batch fetch multiple balances at specific blocks
    ///
    /// Uses JSON-RPC batch requests for efficiency.
    /// Returns a vector of (token, wallet, balance) tuples.
    /// On RPC failure for any request, sets that balance to 0 and continues.
    ///
    /// Requests are chunked into batches of MAX_BATCH_SIZE (500) to avoid RPC limits.
    pub async fn fetch_balances_batch(
        &self,
        requests: &[BalanceFetchRequest],
    ) -> Result<Vec<(Felt, Felt, U256)>> {
        if requests.is_empty() {
            return Ok(Vec::new());
        }

        let mut all_results = Vec::with_capacity(requests.len());

        // Process in chunks of MAX_BATCH_SIZE
        for chunk in requests.chunks(MAX_BATCH_SIZE) {
            // Build batch request for this chunk
            // TODO: Some contracts use balance_of (snake_case), others use balanceOf (camelCase).
            // We need a strategy to handle both - possibly try one, fallback to other, or use ABI.
            let rpc_requests: Vec<ProviderRequestData> = chunk
                .iter()
                .map(|req| {
                    ProviderRequestData::Call(CallRequest {
                        request: FunctionCall {
                            contract_address: req.token,
                            entry_point_selector: selector!("balanceOf"),
                            calldata: vec![req.wallet],
                        },
                        block_id: BlockId::Number(req.block_number),
                    })
                })
                .collect();

            // Execute batch request
            let responses = self
                .provider
                .batch_requests(&rpc_requests)
                .await
                .context("Failed to execute batch balance_of requests")?;

            // Process responses
            for (idx, response) in responses.into_iter().enumerate() {
                let req = &chunk[idx];
                let balance = if let ProviderResponseData::Call(felts) = response {
                    parse_u256_result(&felts)
                } else {
                    tracing::warn!(
                        target: "torii_erc20::balance_fetcher",
                        token = %req.token,
                        wallet = %req.wallet,
                        block = req.block_number,
                        "Unexpected response type for balance_of, using 0"
                    );
                    U256::from(0u64)
                };
                all_results.push((req.token, req.wallet, balance));
            }
        }

        tracing::debug!(
            target: "torii_erc20::balance_fetcher",
            count = all_results.len(),
            "Fetched balances batch"
        );

        Ok(all_results)
    }
}

/// Parse a U256 result from balance_of return value
///
/// ERC20 balance_of typically returns:
/// - Cairo 0: A single felt (fits in 252 bits, usually enough for balances)
/// - Cairo 1 with u256: Two felts [low, high] representing a 256-bit value
fn parse_u256_result(result: &[Felt]) -> U256 {
    match result.len() {
        0 => U256::from(0u64),
        1 => {
            // Single felt - convert to U256
            // Felt is 252 bits max, so it fits in the low part
            let bytes = result[0].to_bytes_be();
            // Take the lower 16 bytes for u128 (fits any felt value)
            let low = u128::from_be_bytes(bytes[16..32].try_into().unwrap());
            U256::from_words(low, 0)
        }
        _ => {
            // Two felts: [low, high] for u256
            let low_bytes = result[0].to_bytes_be();
            let high_bytes = result[1].to_bytes_be();

            let low = u128::from_be_bytes(low_bytes[16..32].try_into().unwrap());
            let high = u128::from_be_bytes(high_bytes[16..32].try_into().unwrap());

            U256::from_words(low, high)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_u256_empty() {
        let result = parse_u256_result(&[]);
        assert_eq!(result, U256::from(0u64));
    }

    #[test]
    fn test_parse_u256_single_felt() {
        let felt = Felt::from(1000u64);
        let result = parse_u256_result(&[felt]);
        assert_eq!(result, U256::from(1000u64));
    }

    #[test]
    fn test_parse_u256_two_felts() {
        // low = 100, high = 0
        let low = Felt::from(100u64);
        let high = Felt::from(0u64);
        let result = parse_u256_result(&[low, high]);
        assert_eq!(result, U256::from(100u64));

        // Test with high value
        let low = Felt::from(0u64);
        let high = Felt::from(1u64);
        let result = parse_u256_result(&[low, high]);
        // high = 1 means value = 1 * 2^128
        let expected = U256::from_words(0, 1);
        assert_eq!(result, expected);
    }
}
