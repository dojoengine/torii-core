//! ERC20 sink for processing transfers and maintaining balances

use crate::decoder::Transfer;
use crate::storage::{Erc20Storage, TransferData};
use anyhow::Result;
use async_trait::async_trait;
use axum::Router;
use std::sync::Arc;
use torii::etl::sink::{EventBus, TopicInfo};
use torii::etl::{Envelope, ExtractionBatch, Sink, TypeId};

/// ERC20 transfer sink
///
/// Processes ERC20 Transfer events and:
/// - Stores transfer records in the database
/// - Updates sender and receiver balances
/// - Publishes updates via gRPC subscriptions
pub struct Erc20Sink {
    storage: Arc<Erc20Storage>,
    event_bus: Option<Arc<EventBus>>,
}

impl Erc20Sink {
    pub fn new(storage: Arc<Erc20Storage>) -> Self {
        Self {
            storage,
            event_bus: None,
        }
    }
}

#[async_trait]
impl Sink for Erc20Sink {
    fn name(&self) -> &str {
        "erc20"
    }

    fn interested_types(&self) -> Vec<TypeId> {
        vec![TypeId::new("erc20.transfer")]
    }

    async fn initialize(
        &mut self,
        event_bus: Arc<EventBus>,
        _context: &torii::etl::sink::SinkContext,
    ) -> Result<()> {
        self.event_bus = Some(event_bus);
        tracing::info!("ERC20 sink initialized");
        Ok(())
    }

    async fn process(&self, envelopes: &[Envelope], batch: &ExtractionBatch) -> Result<()> {
        // Collect all transfers for batch insertion
        let mut transfers: Vec<TransferData> = Vec::with_capacity(envelopes.len());

        for envelope in envelopes {
            // Filter for erc20.transfer envelopes
            if envelope.type_id != TypeId::new("erc20.transfer") {
                continue;
            }

            // Downcast to Transfer
            let transfer = match envelope.body.as_any().downcast_ref::<Transfer>() {
                Some(t) => t,
                None => {
                    tracing::warn!("Failed to downcast envelope to Transfer");
                    continue;
                }
            };

            transfers.push(TransferData {
                id: None,  // Will be set after insertion
                token: transfer.token,
                from: transfer.from,
                to: transfer.to,
                amount: transfer.amount,
                block_number: transfer.block_number,
                tx_hash: transfer.transaction_hash,
            });
        }

        if transfers.is_empty() {
            return Ok(());
        }

        // Batch insert all transfers in a single transaction
        let transfer_count = match self.storage.insert_transfers_batch(&transfers) {
            Ok(count) => count,
            Err(e) => {
                tracing::error!(
                    target: "torii_erc20::sink",
                    "Failed to batch insert {} transfers: {}",
                    transfers.len(),
                    e
                );
                return Err(e);
            }
        };

        if transfer_count > 0 {
            tracing::info!(
                target: "torii_erc20::sink",
                "Batch inserted {} transfers ({} envelopes) across {} blocks",
                transfer_count,
                envelopes.len(),
                batch.blocks.len()
            );

            // Log statistics
            if let Ok(total_transfers) = self.storage.get_transfer_count() {
                if let Ok(token_count) = self.storage.get_token_count() {
                    tracing::info!(
                        target: "torii_erc20::sink",
                        "Total: {} transfers across {} tokens",
                        total_transfers,
                        token_count
                    );
                }
            }
        }

        Ok(())
    }

    fn topics(&self) -> Vec<TopicInfo> {
        vec![TopicInfo::new(
            "erc20.transfer",
            vec!["token".to_string(), "from".to_string(), "to".to_string()],
            "ERC20 token transfers with balance updates",
        )]
    }

    fn build_routes(&self) -> Router {
        // No custom HTTP routes for now
        Router::new()
    }
}
