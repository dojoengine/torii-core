//! Extractor trait for fetching events from various sources

pub mod block_range;
pub mod composite;
pub mod event;
pub mod event_common;
pub mod global_event;
pub mod retry;
pub mod sample;
pub mod starknet_helpers;
pub mod synthetic;
pub mod synthetic_adapter;
pub mod synthetic_erc20;

use crate::etl::engine_db::EngineDb;
use anyhow::Result;
use async_trait::async_trait;
use starknet::core::types::{EmittedEvent, Felt};
use std::collections::HashMap;
use std::sync::Arc;

pub use block_range::{BlockRangeConfig, BlockRangeExtractor};
pub use composite::CompositeExtractor;
pub use event::{ContractEventConfig, EventExtractor, EventExtractorConfig};
pub use global_event::{GlobalEventExtractor, GlobalEventExtractorConfig};
pub use retry::RetryPolicy;
pub use sample::SampleExtractor;
pub use starknet_helpers::ContractAbi;
pub use synthetic::SyntheticExtractor;
pub use synthetic_adapter::SyntheticExtractorAdapter;
pub use synthetic_erc20::{SyntheticErc20Config, SyntheticErc20Extractor};

/// Block context information
#[derive(Debug, Clone, Default)]
pub struct BlockContext {
    pub number: u64,
    pub hash: Felt,
    pub parent_hash: Felt,
    pub timestamp: u64,
}

/// Transaction context information
#[derive(Debug, Clone, Default)]
pub struct TransactionContext {
    pub hash: Felt,
    pub block_number: u64,
    pub sender_address: Option<Felt>,
    pub calldata: Vec<Felt>,
}

#[derive(Debug, Clone, Default)]
pub struct EventContext {
    pub from_address: Felt,
    pub transaction: Arc<TransactionContext>,
    pub block: Arc<BlockContext>,
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

pub struct EventBatch {
    pub contract_transactions: HashMap<Felt, Vec<Transaction>>,
}

pub struct ContractTransactions {
    pub contract_address: Felt,
    pub transactions: Vec<Transaction>,
}

pub struct Transaction {
    pub block_number: u64,
    pub hash: Felt,
    pub contract_events: Vec<EventData>,
}

pub struct EventData {
    pub keys: Vec<Felt>,
    pub data: Vec<Felt>,
}

struct Event {
    pub from_address: Felt,
    pub transaction_hash: Felt,
    pub block_number: u64,
    pub keys: Vec<Felt>,
    pub data: Vec<Felt>,
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
            chain_head: None,
        }
    }
    // Create a batch with pre-allocated capacities for vectors and hashmaps 0 for unallocated
    pub fn with_capacities(
        events: usize,
        blocks: usize,
        transactions: usize,
        declared_classes: usize,
        deployed_contracts: usize,
    ) -> Self {
        Self {
            events: Vec::with_capacity(events),
            blocks: HashMap::with_capacity(blocks),
            transactions: HashMap::with_capacity(transactions),
            declared_classes: Vec::with_capacity(declared_classes),
            deployed_contracts: Vec::with_capacity(deployed_contracts),
            cursor: None,
            chain_head: None,
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

    /// Get the maximum block number in this batch.
    pub fn max_block(&self) -> Option<u64> {
        self.blocks.keys().max().copied()
    }

    /// Check if this batch is "live" (near chain head).
    ///
    /// Returns true if:
    /// - chain_head is known AND
    /// - the max block in this batch is within `threshold` blocks of chain_head
    ///
    /// This is useful for sinks to decide whether to broadcast events to real-time
    /// subscribers. During historical indexing, broadcasting millions of events
    /// would overwhelm clients and slow down the indexer.
    ///
    /// # Example
    /// ```ignore
    /// // Only broadcast if within 100 blocks of chain head
    /// if batch.is_live(100) {
    ///     grpc_service.broadcast_transfer(proto_transfer);
    /// }
    /// ```
    pub fn is_live(&self, threshold: u64) -> bool {
        match (self.chain_head, self.max_block()) {
            (Some(head), Some(max_block)) => head.saturating_sub(max_block) <= threshold,
            // If we don't know chain head, assume not live (safer for historical indexing)
            _ => false,
        }
    }
    // Add block context to the batch
    pub fn add_block_context(
        &mut self,
        number: u64,
        hash: Felt,
        parent_hash: Felt,
        timestamp: u64,
    ) {
        self.blocks.insert(
            number,
            Arc::new(BlockContext {
                number,
                hash,
                parent_hash,
                timestamp,
            }),
        );
    }
    // Add transaction context to the batch
    pub fn add_transaction_context(
        &mut self,
        hash: Felt,
        block_number: u64,
        sender_address: Option<Felt>,
        calldata: Vec<Felt>,
    ) {
        self.transactions.insert(
            hash,
            Arc::new(TransactionContext {
                hash,
                block_number,
                sender_address,
                calldata,
            }),
        );
    }
    // Add an event to the batch
    pub fn add_event(&mut self, event: EmittedEvent) {
        self.events.push(event);
    }
    // Add an event with transaction context (block_number and sender_address) to the batch
    // Returns None if block_number is missing from event and does not updated, since we need it to add transaction context
    pub fn add_event_with_tx_context(
        &mut self,
        event: EmittedEvent,
        sender_address: Option<Felt>,
        calldata: Vec<Felt>,
    ) -> Option<()> {
        self.add_transaction_context(
            event.transaction_hash,
            event.block_number?,
            sender_address,
            calldata,
        );
        self.events.push(event);
        Some(())
    }

    // Add multiple events to the batch
    pub fn add_events(&mut self, events: Vec<EmittedEvent>) {
        self.events.extend(events);
    }
    // add a declared class to the batch
    pub fn add_declared_class(
        &mut self,
        class_hash: Felt,
        compiled_class_hash: Option<Felt>,
        transaction_hash: Felt,
    ) {
        self.declared_classes.push(Arc::new(DeclaredClass {
            class_hash,
            compiled_class_hash,
            transaction_hash,
        }));
    }
    // add a deployed contract to the batch
    pub fn add_deployed_contract(
        &mut self,
        contract_address: Felt,
        class_hash: Felt,
        transaction_hash: Felt,
    ) {
        self.deployed_contracts.push(Arc::new(DeployedContract {
            contract_address,
            class_hash,
            transaction_hash,
        }));
    }

    // Set the cursor
    pub fn set_cursor(&mut self, cursor: String) {
        self.cursor = Some(cursor);
    }
    // Set the chain head block number
    pub fn set_chain_head(&mut self, chain_head: u64) {
        self.chain_head = Some(chain_head);
    }

    pub fn remove_cursor(&mut self) {
        self.cursor = None;
    }

    pub fn remove_chain_head(&mut self) {
        self.chain_head = None;
    }

    pub fn get_event_context(&self, tx_hash: &Felt, from_address: Felt) -> Option<EventContext> {
        let transaction = self.transactions.get(tx_hash)?.clone();
        let block = self.blocks.get(&transaction.block_number)?.clone();

        Some(EventContext {
            from_address,
            transaction,
            block,
        })
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

    /// Commit the cursor after successful processing.
    ///
    /// This method is called AFTER sink processing completes successfully.
    /// By separating cursor persistence from extraction, we ensure no data loss
    /// if the process is killed between extraction and sink processing.
    ///
    /// # Arguments
    ///
    /// * `cursor` - The cursor string to commit (e.g., "block:12345")
    /// * `engine_db` - The engine database for state persistence
    ///
    /// # Default Implementation
    ///
    /// The default implementation does nothing (no-op). Extractors that need
    /// cursor persistence should override this method.
    async fn commit_cursor(&mut self, _cursor: &str, _engine_db: &EngineDb) -> Result<()> {
        Ok(())
    }

    /// Downcast to Any for type checking
    fn as_any(&self) -> &dyn std::any::Any;
}
