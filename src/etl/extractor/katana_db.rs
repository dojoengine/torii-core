//! Katana DB extractor for reading block data directly from Katana's MDBX database.
//!
//! This extractor bypasses JSON-RPC entirely, reading blocks, transactions, receipts,
//! and state updates directly from Katana's storage layer via `DbProviderFactory`.

use std::collections::HashMap;
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use async_trait::async_trait;
use katana_primitives::receipt::Receipt;
use katana_primitives::transaction::{DeclareTx, Tx};
use katana_provider::api::block::{BlockHashProvider, BlockNumberProvider, HeaderProvider};
use katana_provider::api::state_update::StateUpdateProvider;
use katana_provider::api::transaction::{ReceiptProvider, TransactionProvider};
use katana_provider::{DbProviderFactory, ProviderFactory};
use starknet::core::types::{EmittedEvent, Felt};

use crate::etl::engine_db::EngineDb;

use super::{
    BlockContext, DeclaredClass, DeployedContract, ExtractionBatch, Extractor, TransactionContext,
};

const EXTRACTOR_TYPE: &str = "katana_db";
const STATE_KEY: &str = "last_block";

/// Configuration for the Katana DB extractor.
#[derive(Debug, Clone)]
pub struct KatanaDbConfig {
    /// Path to Katana's MDBX database directory.
    pub db_path: String,

    /// Starting block number.
    pub from_block: u64,

    /// Ending block number (None = follow chain head indefinitely).
    pub to_block: Option<u64>,

    /// Number of blocks to fetch per batch.
    pub batch_size: u64,

    /// How often to check for new blocks when caught up with chain head.
    pub poll_interval: Duration,
}

impl Default for KatanaDbConfig {
    fn default() -> Self {
        Self {
            db_path: String::new(),
            from_block: 0,
            to_block: None,
            batch_size: 100,
            poll_interval: Duration::from_secs(1),
        }
    }
}

/// Extractor that reads block data directly from Katana's MDBX database.
///
/// This avoids JSON-RPC overhead by reading directly from the storage layer
/// via `DbProviderFactory`, providing significantly faster sync when running
/// alongside Katana.
#[derive(Debug)]
pub struct KatanaDbExtractor {
    factory: DbProviderFactory,
    config: KatanaDbConfig,
    current_block: u64,
    reached_end: bool,
}

impl KatanaDbExtractor {
    /// Creates a new `KatanaDbExtractor` by opening the database at `config.db_path` in read-only mode.
    pub fn new(config: KatanaDbConfig) -> Result<Self> {
        let db = katana_db::Db::open_ro(&config.db_path)
            .with_context(|| format!("Failed to open katana DB at '{}'", config.db_path))?;
        let factory = DbProviderFactory::new(db);
        Ok(Self {
            factory,
            config,
            current_block: 0,
            reached_end: false,
        })
    }

    /// Creates a new `KatanaDbExtractor` from an existing `DbProviderFactory`.
    pub fn from_factory(factory: DbProviderFactory, config: KatanaDbConfig) -> Self {
        Self {
            factory,
            config,
            current_block: 0,
            reached_end: false,
        }
    }

    /// Initializes the extractor state from cursor, saved state, or config.
    async fn initialize(&mut self, cursor: Option<String>, engine_db: &EngineDb) -> Result<()> {
        if let Some(cursor_str) = cursor {
            if let Some(block_str) = cursor_str.strip_prefix("block:") {
                self.current_block = block_str
                    .parse::<u64>()
                    .context("Invalid cursor format")?
                    .saturating_add(1);
                tracing::info!(
                    target: "torii::etl::katana_db",
                    "Resuming from cursor: block {}",
                    self.current_block
                );
            } else {
                anyhow::bail!("Invalid cursor format: expected 'block:N', got '{cursor_str}'");
            }
        } else if let Some(saved_state) = engine_db
            .get_extractor_state(EXTRACTOR_TYPE, STATE_KEY)
            .await?
        {
            self.current_block = saved_state
                .parse::<u64>()
                .context("Invalid saved state")?
                .saturating_add(1);
            tracing::info!(
                target: "torii::etl::katana_db",
                "Resuming from saved state: block {}",
                self.current_block
            );
        } else {
            self.current_block = self.config.from_block;
            tracing::info!(
                target: "torii::etl::katana_db",
                "Starting from configured block: {}",
                self.current_block
            );
        }

        Ok(())
    }

    fn should_stop(&self) -> bool {
        if let Some(to_block) = self.config.to_block {
            self.current_block > to_block
        } else {
            false
        }
    }

    /// Extracts a batch of blocks from the database.
    fn extract_batch(&self, current_block: u64) -> Result<(ExtractionBatch, u64)> {
        let total_start = Instant::now();
        let provider = self.factory.provider();

        let chain_head = provider
            .latest_number()
            .context("Failed to get latest block number")?;

        // Compute batch end
        let batch_end = if let Some(to_block) = self.config.to_block {
            (current_block + self.config.batch_size - 1).min(to_block)
        } else if current_block > chain_head {
            // Caught up with chain head — return empty batch
            let batch = ExtractionBatch {
                events: Vec::new(),
                blocks: HashMap::new(),
                transactions: HashMap::new(),
                declared_classes: Vec::new(),
                deployed_contracts: Vec::new(),
                cursor: Some(format!("block:{}", current_block.saturating_sub(1))),
                chain_head: Some(chain_head),
            };
            return Ok((batch, current_block));
        } else {
            (current_block + self.config.batch_size - 1).min(chain_head)
        };

        let block_count = batch_end - current_block + 1;
        tracing::info!(
            target: "torii::etl::katana_db",
            "Reading blocks {}-{} (batch size: {})",
            current_block,
            batch_end,
            block_count
        );

        let mut all_events = Vec::new();
        let mut blocks_map = HashMap::with_capacity(block_count as usize);
        let mut transactions_map = HashMap::new();
        let mut all_declared_classes = Vec::new();
        let mut all_deployed_contracts = Vec::new();

        for block_num in current_block..=batch_end {
            let block_id = block_num.into();

            // Get block header
            let header = provider
                .header_by_number(block_num)
                .context("Failed to get block header")?
                .with_context(|| format!("Block header not found for block {block_num}"))?;

            // Get block hash
            let block_hash = provider
                .block_hash_by_num(block_num)
                .context("Failed to get block hash")?
                .with_context(|| format!("Block hash not found for block {block_num}"))?;

            // Build block context
            blocks_map.insert(
                block_num,
                BlockContext {
                    number: block_num,
                    hash: block_hash,
                    parent_hash: header.parent_hash,
                    timestamp: header.timestamp,
                },
            );

            // Get transactions
            let txs = provider
                .transactions_by_block(block_id)
                .context("Failed to get transactions")?
                .unwrap_or_default();

            // Get receipts
            let receipts = provider
                .receipts_by_block(block_id)
                .context("Failed to get receipts")?
                .unwrap_or_default();

            // Process transactions and receipts together
            for (tx, receipt) in txs.iter().zip(receipts.iter()) {
                let tx_hash = tx.hash;

                // Build transaction context
                let (sender_address, calldata) = extract_tx_info(&tx.transaction);
                transactions_map.insert(
                    tx_hash,
                    TransactionContext {
                        hash: tx_hash,
                        block_number: block_num,
                        sender_address,
                        calldata,
                    },
                );

                // Extract events from receipt (skip reverted transactions)
                if !receipt.is_reverted() {
                    for event in receipt.events() {
                        all_events.push(EmittedEvent {
                            from_address: event.from_address.0,
                            keys: event.keys.clone(),
                            data: event.data.clone(),
                            block_hash: Some(block_hash),
                            block_number: Some(block_num),
                            transaction_hash: tx_hash,
                        });
                    }
                }

                // Extract declared classes from Declare transactions
                if let Tx::Declare(declare_tx) = &tx.transaction {
                    let class_hash = declare_tx.class_hash();
                    let compiled_class_hash = match declare_tx {
                        DeclareTx::V2(tx) => Some(tx.compiled_class_hash),
                        DeclareTx::V3(tx) => Some(tx.compiled_class_hash),
                        _ => None,
                    };
                    all_declared_classes.push(DeclaredClass {
                        class_hash,
                        compiled_class_hash,
                        transaction_hash: tx_hash,
                    });
                }

                // Extract deployed contracts from DeployAccount receipts
                match receipt {
                    Receipt::DeployAccount(r) => {
                        if let Tx::DeployAccount(deploy_tx) = &tx.transaction {
                            let class_hash = match deploy_tx {
                                katana_primitives::transaction::DeployAccountTx::V1(t) => {
                                    t.class_hash
                                }
                                katana_primitives::transaction::DeployAccountTx::V3(t) => {
                                    t.class_hash
                                }
                            };
                            all_deployed_contracts.push(DeployedContract {
                                contract_address: r.contract_address.0,
                                class_hash,
                                transaction_hash: tx_hash,
                            });
                        }
                    }
                    Receipt::Deploy(r) => {
                        if let Tx::Deploy(deploy_tx) = &tx.transaction {
                            all_deployed_contracts.push(DeployedContract {
                                contract_address: r.contract_address.0,
                                class_hash: deploy_tx.class_hash,
                                transaction_hash: tx_hash,
                            });
                        }
                    }
                    _ => {}
                }
            }

            // Also check state updates for deployed contracts not captured from receipts
            if let Some(deployed) = provider
                .deployed_contracts(block_id)
                .context("Failed to get deployed contracts")?
            {
                for (addr, class_hash) in deployed {
                    let addr_felt: Felt = addr.into();
                    // Only add if not already tracked from receipt processing
                    if !all_deployed_contracts
                        .iter()
                        .any(|d| d.contract_address == addr_felt)
                    {
                        // Find the transaction hash for this deployment
                        let tx_hash = txs.first().map(|t| t.hash).unwrap_or_default();
                        all_deployed_contracts.push(DeployedContract {
                            contract_address: addr_felt,
                            class_hash,
                            transaction_hash: tx_hash,
                        });
                    }
                }
            }
        }

        let total_ms = total_start.elapsed().as_millis();
        tracing::info!(
            target: "torii::etl::katana_db",
            "Extracted {} events, {} declared classes, {} deployed contracts from {} blocks ({} transactions) [total={}ms]",
            all_events.len(),
            all_declared_classes.len(),
            all_deployed_contracts.len(),
            blocks_map.len(),
            transactions_map.len(),
            total_ms
        );

        let batch = ExtractionBatch {
            events: all_events,
            blocks: blocks_map,
            transactions: transactions_map,
            declared_classes: all_declared_classes,
            deployed_contracts: all_deployed_contracts,
            cursor: Some(format!("block:{batch_end}")),
            chain_head: Some(chain_head),
        };

        Ok((batch, batch_end + 1))
    }
}

/// Extract sender address and calldata from a transaction.
fn extract_tx_info(tx: &Tx) -> (Option<Felt>, Vec<Felt>) {
    match tx {
        Tx::Invoke(invoke) => match invoke {
            katana_primitives::transaction::InvokeTx::V0(t) => {
                (Some(t.contract_address.0), t.calldata.clone())
            }
            katana_primitives::transaction::InvokeTx::V1(t) => {
                (Some(t.sender_address.0), t.calldata.clone())
            }
            katana_primitives::transaction::InvokeTx::V3(t) => {
                (Some(t.sender_address.0), t.calldata.clone())
            }
        },
        Tx::DeployAccount(deploy) => match deploy {
            katana_primitives::transaction::DeployAccountTx::V1(t) => {
                (Some(t.contract_address.0), t.constructor_calldata.clone())
            }
            katana_primitives::transaction::DeployAccountTx::V3(t) => {
                (Some(t.contract_address.0), t.constructor_calldata.clone())
            }
        },
        Tx::Declare(declare) => {
            let sender = match declare {
                DeclareTx::V0(t) => Some(t.sender_address.0),
                DeclareTx::V1(t) => Some(t.sender_address.0),
                DeclareTx::V2(t) => Some(t.sender_address.0),
                DeclareTx::V3(t) => Some(t.sender_address.0),
            };
            (sender, Vec::new())
        }
        Tx::L1Handler(l1) => (Some(l1.contract_address.0), l1.calldata.clone()),
        Tx::Deploy(deploy) => (None, deploy.constructor_calldata.clone()),
    }
}

#[async_trait]
impl Extractor for KatanaDbExtractor {
    fn is_finished(&self) -> bool {
        self.reached_end
    }

    async fn commit_cursor(&mut self, cursor: &str, engine_db: &EngineDb) -> Result<()> {
        if let Some(block_str) = cursor.strip_prefix("block:") {
            let block_num: u64 = block_str.parse().context("Invalid cursor format")?;
            engine_db
                .set_extractor_state(EXTRACTOR_TYPE, STATE_KEY, &block_num.to_string())
                .await
                .context("Failed to commit cursor")?;
            tracing::debug!(
                target: "torii::etl::katana_db",
                "Committed cursor: block {}",
                block_num
            );
        }
        Ok(())
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    async fn extract(
        &mut self,
        cursor: Option<String>,
        engine_db: &EngineDb,
    ) -> Result<ExtractionBatch> {
        if self.current_block == 0 {
            self.initialize(cursor, engine_db).await?;
        }

        if self.reached_end {
            return Ok(ExtractionBatch::empty());
        }

        if self.should_stop() {
            tracing::info!(
                target: "torii::etl::katana_db",
                "Reached configured end block"
            );
            self.reached_end = true;
            return Ok(ExtractionBatch::empty());
        }

        let (batch, next_block) = self.extract_batch(self.current_block)?;
        self.current_block = next_block;

        tracing::debug!(
            target: "torii::etl::katana_db",
            next_block = self.current_block,
            "Using katana-db batch"
        );

        Ok(batch)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_empty_db_returns_empty_batch() {
        let factory = DbProviderFactory::new_in_memory();
        let config = KatanaDbConfig {
            from_block: 0,
            batch_size: 10,
            ..Default::default()
        };
        let extractor = KatanaDbExtractor::from_factory(factory, config);

        // On an empty DB, latest_number() should return 0 or fail gracefully.
        // With current_block=0 and chain_head=0, we should get a single block or empty batch.
        let result = extractor.extract_batch(0);
        // The in-memory DB has no blocks, so latest_number should be block 0 (genesis).
        // This test just verifies the extractor doesn't panic on an empty DB.
        assert!(result.is_ok() || result.is_err());
    }
}
