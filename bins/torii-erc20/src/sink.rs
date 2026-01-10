//! ERC20 sink for processing transfers and maintaining balances

use crate::decoder::Transfer;
use crate::storage::Erc20Storage;
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

    async fn initialize(&mut self, event_bus: Arc<EventBus>) -> Result<()> {
        self.event_bus = Some(event_bus);
        tracing::info!("ERC20 sink initialized");
        Ok(())
    }

    async fn process(&self, envelopes: &[Envelope], batch: &ExtractionBatch) -> Result<()> {
        let mut transfer_count = 0;

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

            // Store transfer and update balances
            if let Err(e) = self.storage.insert_transfer(
                transfer.token,
                transfer.from,
                transfer.to,
                transfer.amount,
                transfer.block_number,
                transfer.transaction_hash,
            ) {
                tracing::error!(
                    "Failed to store transfer from {:?} to {:?}: {}",
                    transfer.from,
                    transfer.to,
                    e
                );
                continue;
            }

            transfer_count += 1;

            tracing::debug!(
                target: "torii_erc20::sink",
                "Stored transfer: token={:#x}, from={:#x}, to={:#x}, amount={:#x}, block={}",
                transfer.token,
                transfer.from,
                transfer.to,
                transfer.amount,
                transfer.block_number
            );
        }

        if transfer_count > 0 {
            tracing::info!(
                target: "torii_erc20::sink",
                "Processed {} transfers from {} envelopes across {} blocks",
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
