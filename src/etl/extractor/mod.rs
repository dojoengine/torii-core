//! Extractor trait for fetching events from various sources

pub mod sample;

use crate::etl::engine_db::EngineDb;
use anyhow::Result;
use async_trait::async_trait;
use starknet::core::types::{EmittedEvent, Felt};
use std::collections::HashMap;

pub use sample::SampleExtractor;

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

    /// Opaque cursor for pagination (continuation token or cursor string)
    /// None means no more data available
    pub cursor: Option<String>,

    /// Whether there is more data available
    /// If true, the extractor should be called again with the cursor
    pub has_more: bool,
}

impl ExtractionBatch {
    /// Create an empty batch with no more data
    pub fn empty() -> Self {
        Self {
            events: Vec::new(),
            blocks: HashMap::new(),
            transactions: HashMap::new(),
            cursor: None,
            has_more: false,
        }
    }

    /// Create an empty batch but signal that more data is available
    pub fn empty_with_more(cursor: Option<String>) -> Self {
        Self {
            events: Vec::new(),
            blocks: HashMap::new(),
            transactions: HashMap::new(),
            cursor,
            has_more: true,
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
    /// - cursor: Opaque cursor for next extraction (None if no more data)
    /// - has_more: Whether there is more data available
    async fn extract(
        &mut self,
        cursor: Option<String>,
        engine_db: &EngineDb,
    ) -> Result<ExtractionBatch>;

    /// Downcast to Any for type checking
    fn as_any(&self) -> &dyn std::any::Any;
}
