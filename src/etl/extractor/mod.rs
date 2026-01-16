//! Extractor trait for fetching events from various sources

pub mod block_range;
pub mod retry;
pub mod sample;
pub mod starknet_helpers;

use crate::etl::engine_db::EngineDb;
use anyhow::Result;
use async_trait::async_trait;
use starknet::core::types::{EmittedEvent, Felt};
use starknet::providers::jsonrpc::{HttpTransport, JsonRpcClient};
use std::collections::HashMap;
use std::sync::Arc;

pub use block_range::{BlockRangeConfig, BlockRangeExtractor};
pub use retry::RetryPolicy;
pub use sample::SampleExtractor;
pub use starknet_helpers::ContractAbi;

/// Block context information
#[derive(Debug, Clone)]
pub struct BlockContext {
    pub number: u64,
    pub hash: Felt,
    pub parent_hash: Felt,
    pub timestamp: u64,
}

/// Transaction context information
#[derive(Debug, Clone)]
pub struct TransactionContext {
    pub hash: Felt,
    pub block_number: u64,
    pub sender_address: Option<Felt>,
    pub calldata: Vec<Felt>,
}

/// Declared class information
#[derive(Debug, Clone)]
pub struct DeclaredClass {
    pub class_hash: Felt,
    pub compiled_class_hash: Option<Felt>, // Only for Cairo 1.0+ (V2+)
    pub transaction_hash: Felt,
}

/// Deployed contract information
#[derive(Debug, Clone)]
pub struct DeployedContract {
    pub contract_address: Felt,
    pub class_hash: Felt,
    pub transaction_hash: Felt,
}

/// Complete block data with all extracted information
#[derive(Debug, Clone)]
pub struct BlockData {
    pub block_context: BlockContext,
    pub transactions: Vec<TransactionContext>,
    pub events: Vec<EmittedEvent>,
    pub declared_classes: Vec<DeclaredClass>,
    pub deployed_contracts: Vec<DeployedContract>,
}

/// Enriched extraction batch with events and context
///
/// This structure optimizes memory by **deduplicating** blocks and transactions:
/// - Multiple events from the same block share a single `BlockContext`.
/// - Multiple events from the same transaction share a single `TransactionContext`.
///
/// # Memory Optimization Example
///
/// For 100 events from 1 block and 10 transactions:
/// - `events`: 100 items (raw events)
/// - `blocks`: 1 item (deduplicated)
/// - `transactions`: 10 items (deduplicated)
///
/// Instead of storing block/transaction data 100 times, we store it 11 times total.
///
/// # Usage in Sinks
///
/// Sinks receive this batch along with decoded envelopes. To access context:
/// ```rust,ignore
/// // Fast O(1) lookups via HashMap
/// let block = &batch.blocks[&block_number];
/// let tx = &batch.transactions[&tx_hash];
///
/// // Slow O(n) - avoid iterating events
/// for event in &batch.events { /* ... */ }
/// ```
#[derive(Debug, Clone)]
pub struct ExtractionBatch {
    /// Events extracted (may contain duplicates from same block/tx)
    pub events: Vec<EmittedEvent>,

    /// Block context (deduplicated by block_number for memory efficiency)
    pub blocks: HashMap<u64, BlockContext>,

    /// Transaction context (deduplicated by tx_hash for memory efficiency)
    pub transactions: HashMap<Felt, TransactionContext>,

    /// Declared classes from Declare transactions
    pub declared_classes: Vec<DeclaredClass>,

    /// Deployed contracts from Deploy and DeployAccount transactions
    pub deployed_contracts: Vec<DeployedContract>,

    /// Opaque cursor for pagination (continuation token or cursor string)
    pub cursor: Option<String>,
}

impl ExtractionBatch {
    /// Create an empty batch
    pub fn empty() -> Self {
        Self {
            events: Vec::new(),
            blocks: HashMap::new(),
            transactions: HashMap::new(),
            declared_classes: Vec::new(),
            deployed_contracts: Vec::new(),
            cursor: None,
        }
    }

    /// Check if batch is empty
    pub fn is_empty(&self) -> bool {
        self.events.is_empty()
    }

    /// Get number of events
    pub fn len(&self) -> usize {
        self.events.len()
    }
}

/// Extractor trait for fetching enriched event batches
#[async_trait]
pub trait Extractor: Send + Sync {
    /// Extract events with enriched context (blocks, transactions)
    ///
    /// The cursor parameter is an opaque string that allows resuming from a previous extraction.
    /// - None: Start from the beginning or use extractor's internal state
    /// - Some(cursor): Resume from the given cursor
    ///
    /// Returns an ExtractionBatch with:
    /// - events: The extracted events
    /// - blocks/transactions: Deduplicated context
    /// - cursor: Opaque cursor for next extraction
    ///
    /// # Return Value Semantics
    ///
    /// - Non-empty batch: Process events, call `extract()` again
    /// - Empty batch + `is_finished() = false`: Waiting for new blocks, sleep and retry
    /// - Empty batch + `is_finished() = true`: Extractor reached its end, stop calling
    async fn extract(
        &mut self,
        cursor: Option<String>,
        engine_db: &EngineDb,
    ) -> Result<ExtractionBatch>;

    /// Check if the extractor has finished its configured range
    ///
    /// Returns `true` when the extractor has reached its configured end point
    /// and will not produce more data, even if called again.
    ///
    /// Returns `false` when the extractor can potentially produce more data:
    /// - Still has blocks to fetch in the configured range
    /// - Following chain head indefinitely (no end block configured)
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// loop {
    ///     let batch = extractor.extract(cursor, engine_db).await?;
    ///
    ///     if !batch.is_empty() {
    ///         process_batch(batch);
    ///     }
    ///
    ///     if extractor.is_finished() {
    ///         break; // Done, reached configured end
    ///     }
    ///
    ///     if batch.is_empty() {
    ///         // Waiting for new blocks, sleep and retry
    ///         tokio::time::sleep(Duration::from_secs(5)).await;
    ///     }
    ///
    ///     cursor = batch.cursor;
    /// }
    /// ```
    fn is_finished(&self) -> bool;

    /// Downcast to Any for type checking
    fn as_any(&self) -> &dyn std::any::Any;
}
