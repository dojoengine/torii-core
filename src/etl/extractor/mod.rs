//! Extractor trait for fetching events from various sources

pub mod block;
pub mod block_range;
pub mod contract;
pub mod event_common;
pub mod events;
pub mod retry;
pub mod sample;
pub mod starknet_helpers;
pub mod synthetic;
pub mod synthetic_adapter;
pub mod synthetic_erc20;

use crate::etl::engine_db::EngineDb;
use anyhow::Result;
use async_trait::async_trait;
use starknet::core::types::{BlockWithReceipts, Felt};
use std::cmp::min;
use std::collections::{HashMap, HashSet};

pub use block_range::{BlockRangeConfig, BlockRangeExtractor};
pub use events::{
    BlockEvents, BlockTransactionEvents, ContractEvents, EventData, ExtractedEvents,
    TransactionEvents,
};
pub use retry::RetryPolicy;
pub use sample::SampleExtractor;
pub use starknet_helpers::{ContractAbi, ProviderResult};
pub use synthetic::SyntheticExtractor;
pub use synthetic_adapter::SyntheticExtractorAdapter;
pub use synthetic_erc20::{SyntheticErc20Config, SyntheticErc20Extractor};

pub type FeltMap<V> = HashMap<Felt, V, ahash::RandomState>;
pub type FeltSet = HashSet<Felt, ahash::RandomState>;

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
    ) -> Result<ExtractedEvents>;

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

pub trait HybridExtractor {
    type Error;
    async fn next_batch(&mut self) -> Result<ExtractedEvents, Self::Error>;
}

pub trait ContractsExtractor {
    type Error: Send;
    async fn next_events(&mut self) -> Vec<Result<ContractEvents, Self::Error>>;
    fn at_end_block(&self) -> bool;
}

#[async_trait]
pub trait BlockExtractor: Sync {
    type Error: Send;
    async fn fetch_block_with_receipts(
        &self,
        block_number: u64,
    ) -> Result<BlockWithReceipts, Self::Error>;
    async fn wait_for_block_with_receipts(
        &self,
        block_number: u64,
    ) -> Result<BlockWithReceipts, Self::Error>;

    async fn fetch_head(&self) -> Result<u64, Self::Error>;
    fn set_head(&mut self, block_number: u64);
    fn current_head(&self) -> u64;

    fn next_block_to_fetch(&self) -> u64;
    fn set_next_block(&mut self, next_block: u64);

    async fn fetch_blocks_with_receipts(
        &self,
        blocks: Vec<u64>,
    ) -> Result<Vec<BlockWithReceipts>, Self::Error> {
        let fetches = blocks
            .into_iter()
            .map(|block| self.fetch_block_with_receipts(block));
        futures::future::try_join_all(fetches).await
    }
    async fn wait_for_block_events(
        &mut self,
        block_number: u64,
    ) -> Result<BlockEvents, Self::Error> {
        let block = self
            .wait_for_block_with_receipts(block_number)
            .await?
            .into();
        self.set_next_block(block_number + 1);
        self.update_current_head(block_number);
        Ok(block)
    }
    async fn get_block_events(&self, block_number: u64) -> Result<BlockEvents, Self::Error> {
        self.fetch_block_with_receipts(block_number)
            .await
            .map(Into::into)
    }
    async fn get_blocks_events(
        &self,
        block_numbers: Vec<u64>,
    ) -> Result<Vec<BlockEvents>, Self::Error> {
        let fetches = block_numbers
            .into_iter()
            .map(|block| self.get_block_events(block));
        futures::future::try_join_all(fetches).await
    }
    async fn next_block_events(&mut self) -> Result<BlockEvents, Self::Error> {
        let next_block = self.next_block_to_fetch();
        let block: BlockEvents = self.get_block_events(next_block).await?;
        self.update_current_head(block.block.block_number);
        self.set_next_block(block.block.block_number + 1);
        Ok(block)
    }

    async fn next_blocks_events(
        &mut self,
        batch_size: u64,
    ) -> Result<Vec<BlockEvents>, Self::Error> {
        let (start, end) = self.next_batch_range(batch_size).await?;
        let blocks = self.get_blocks_events((start..end).collect()).await?;
        self.set_next_block(end);
        Ok(blocks)
    }

    async fn update_latest_block_number(&mut self) -> Result<u64, Self::Error> {
        let head = self.fetch_head().await?;
        self.set_head(head);
        Ok(head)
    }

    fn update_current_head(&mut self, block_number: u64) {
        if block_number > self.current_head() {
            self.set_head(block_number);
        }
    }

    async fn next_batch_range(&mut self, batch_size: u64) -> Result<(u64, u64), Self::Error> {
        let start = self.next_block_to_fetch();
        let end = start + batch_size;
        if end <= self.current_head() + 1 {
            Ok((start, end))
        } else {
            Ok((start, min(end, self.update_latest_block_number().await?)))
        }
    }
    async fn wait_for_next_batch(
        &mut self,
        batch_size: u64,
    ) -> Result<Vec<BlockEvents>, Self::Error> {
        let (start, end) = self.next_batch_range(batch_size).await?;
        if start < end {
            let blocks = self.get_blocks_events((start..=end).collect()).await?;
            self.set_next_block(end);
            Ok(blocks)
        } else {
            Ok(vec![self.wait_for_block_events(start).await?])
        }
    }
}

// pub trait Contract
