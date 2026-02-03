//! Event-based backfill engine for historical data sync
//!
//! Uses `starknet_getEvents` with contract address filtering for efficient
//! historical event fetching. This is ideal for catching up new contracts
//! without re-processing entire block ranges.
//!
//! # Usage
//!
//! ```rust,ignore
//! use torii_common::backfill::{EventBackfill, ContractBackfillConfig, TokenType};
//! use starknet::providers::jsonrpc::{JsonRpcClient, HttpTransport};
//!
//! let provider = Arc::new(JsonRpcClient::new(HttpTransport::new(url)));
//!
//! let backfill = EventBackfill::new(provider, vec![
//!     ContractBackfillConfig {
//!         address: eth_address,
//!         token_type: TokenType::Erc20,
//!         from_block: 100_000,
//!         to_block: 500_000,
//!     },
//! ]);
//!
//! // Progress callback receives (contract, current_block, total_events)
//! backfill.run(|contract, block, events| {
//!     println!("Contract {}: block {}, {} events", contract, block, events);
//! }).await?;
//! ```

use anyhow::{Context, Result};
use starknet::core::types::{BlockId, EmittedEvent, EventFilter, Felt};
use starknet::providers::jsonrpc::{HttpTransport, JsonRpcClient};
use starknet::providers::Provider;
use std::sync::Arc;

/// Token type for backfill configuration
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TokenType {
    Erc20,
    Erc721,
    Erc1155,
}

impl std::fmt::Display for TokenType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TokenType::Erc20 => write!(f, "ERC20"),
            TokenType::Erc721 => write!(f, "ERC721"),
            TokenType::Erc1155 => write!(f, "ERC1155"),
        }
    }
}

/// Configuration for backfilling a single contract
#[derive(Debug, Clone)]
pub struct ContractBackfillConfig {
    /// Contract address to fetch events from
    pub address: Felt,
    /// Token type (ERC20, ERC721, ERC1155)
    pub token_type: TokenType,
    /// Starting block number
    pub from_block: u64,
    /// Ending block number
    pub to_block: u64,
}

/// Statistics from a backfill run
#[derive(Debug, Default)]
pub struct BackfillStats {
    /// Total events fetched across all contracts
    pub total_events: u64,
    /// Events per contract address
    pub events_by_contract: std::collections::HashMap<Felt, u64>,
    /// Number of RPC requests made
    pub rpc_requests: u64,
}

/// Backfill progress information
#[derive(Debug, Clone)]
pub struct BackfillProgress {
    /// Contract being processed
    pub contract: Felt,
    /// Token type
    pub token_type: TokenType,
    /// Current block being processed
    pub current_block: u64,
    /// Target block
    pub to_block: u64,
    /// Events fetched so far for this contract
    pub events_fetched: u64,
}

/// Event backfill engine
///
/// Fetches historical events for specific contracts using `starknet_getEvents`.
/// This is more efficient than block-based extraction when catching up a small
/// number of contracts over a large block range.
pub struct EventBackfill {
    /// Provider for fetching events
    provider: Arc<JsonRpcClient<HttpTransport>>,
    /// Contracts to backfill
    contracts: Vec<ContractBackfillConfig>,
    /// Events per RPC request (max 1024 for most providers)
    chunk_size: u64,
}

impl EventBackfill {
    /// Create a new backfill engine
    ///
    /// # Arguments
    ///
    /// * `provider` - Starknet JSON-RPC provider
    /// * `contracts` - List of contracts to backfill
    pub fn new(
        provider: Arc<JsonRpcClient<HttpTransport>>,
        contracts: Vec<ContractBackfillConfig>,
    ) -> Self {
        Self {
            provider,
            contracts,
            chunk_size: 1000, // Safe default for most providers
        }
    }

    /// Set the chunk size (events per RPC request)
    ///
    /// Most providers support up to 1024 events per request.
    pub fn with_chunk_size(mut self, chunk_size: u64) -> Self {
        self.chunk_size = chunk_size;
        self
    }

    /// Fetch all events for a single contract
    ///
    /// Uses pagination to handle large event sets.
    ///
    /// # Arguments
    ///
    /// * `config` - Contract configuration
    /// * `on_progress` - Optional callback for progress updates
    ///
    /// # Returns
    ///
    /// Vector of all emitted events for the contract in the block range
    pub async fn fetch_contract_events<F>(
        &self,
        config: &ContractBackfillConfig,
        mut on_progress: Option<F>,
    ) -> Result<Vec<EmittedEvent>>
    where
        F: FnMut(BackfillProgress),
    {
        let mut all_events = Vec::new();
        let mut continuation_token: Option<String> = None;

        let filter = EventFilter {
            from_block: Some(BlockId::Number(config.from_block)),
            to_block: Some(BlockId::Number(config.to_block)),
            address: Some(config.address),
            keys: None, // Fetch all events from this contract
        };

        loop {
            let page = self
                .provider
                .get_events(filter.clone(), continuation_token.clone(), self.chunk_size)
                .await
                .with_context(|| {
                    format!(
                        "Failed to fetch events for contract {:#x} (blocks {}-{})",
                        config.address, config.from_block, config.to_block
                    )
                })?;

            all_events.extend(page.events);

            // Report progress
            if let Some(ref mut callback) = on_progress {
                // Get the last block from fetched events
                let current_block = all_events
                    .last()
                    .and_then(|e| e.block_number)
                    .unwrap_or(config.from_block);

                callback(BackfillProgress {
                    contract: config.address,
                    token_type: config.token_type,
                    current_block,
                    to_block: config.to_block,
                    events_fetched: all_events.len() as u64,
                });
            }

            // Check if there are more pages
            match page.continuation_token {
                Some(token) => continuation_token = Some(token),
                None => break, // No more pages
            }
        }

        Ok(all_events)
    }

    /// Run backfill for all configured contracts
    ///
    /// Processes contracts sequentially (could be parallelized in future).
    ///
    /// # Arguments
    ///
    /// * `on_progress` - Callback for progress updates
    ///
    /// # Returns
    ///
    /// Tuple of (all events grouped by token type, statistics)
    pub async fn run<F>(
        &self,
        mut on_progress: F,
    ) -> Result<(BackfillResult, BackfillStats)>
    where
        F: FnMut(BackfillProgress),
    {
        let mut stats = BackfillStats::default();
        let mut result = BackfillResult::default();

        for config in &self.contracts {
            tracing::info!(
                target: "torii_common::backfill",
                contract = %format!("{:#x}", config.address),
                token_type = %config.token_type,
                from_block = config.from_block,
                to_block = config.to_block,
                "Starting backfill for contract"
            );

            let events = self
                .fetch_contract_events(config, Some(&mut on_progress))
                .await?;

            let event_count = events.len() as u64;
            stats.total_events += event_count;
            stats.events_by_contract.insert(config.address, event_count);

            // Group events by token type
            match config.token_type {
                TokenType::Erc20 => result.erc20_events.extend(events),
                TokenType::Erc721 => result.erc721_events.extend(events),
                TokenType::Erc1155 => result.erc1155_events.extend(events),
            }

            tracing::info!(
                target: "torii_common::backfill",
                contract = %format!("{:#x}", config.address),
                events = event_count,
                "Completed backfill for contract"
            );
        }

        Ok((result, stats))
    }

    /// Get the list of contracts configured for backfill
    pub fn contracts(&self) -> &[ContractBackfillConfig] {
        &self.contracts
    }
}

/// Result of a backfill run, grouped by token type
#[derive(Debug, Default)]
pub struct BackfillResult {
    /// ERC20 events (Transfer, Approval)
    pub erc20_events: Vec<EmittedEvent>,
    /// ERC721 events (Transfer, Approval, ApprovalForAll)
    pub erc721_events: Vec<EmittedEvent>,
    /// ERC1155 events (TransferSingle, TransferBatch, ApprovalForAll)
    pub erc1155_events: Vec<EmittedEvent>,
}

impl BackfillResult {
    /// Check if the result is empty
    pub fn is_empty(&self) -> bool {
        self.erc20_events.is_empty()
            && self.erc721_events.is_empty()
            && self.erc1155_events.is_empty()
    }

    /// Total number of events across all token types
    pub fn total_events(&self) -> usize {
        self.erc20_events.len() + self.erc721_events.len() + self.erc1155_events.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_token_type_display() {
        assert_eq!(format!("{}", TokenType::Erc20), "ERC20");
        assert_eq!(format!("{}", TokenType::Erc721), "ERC721");
        assert_eq!(format!("{}", TokenType::Erc1155), "ERC1155");
    }

    #[test]
    fn test_backfill_result_empty() {
        let result = BackfillResult::default();
        assert!(result.is_empty());
        assert_eq!(result.total_events(), 0);
    }
}
