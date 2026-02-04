//! ERC1155 sink for processing token transfers and operator approvals

use crate::decoder::{OperatorApproval as DecodedOperatorApproval, TransferBatch as DecodedTransferBatch, TransferSingle as DecodedTransferSingle};
use crate::grpc_service::Erc1155Service;
use crate::proto;
use crate::storage::{Erc1155Storage, OperatorApprovalData, TokenTransferData};
use anyhow::Result;
use async_trait::async_trait;
use axum::Router;
use prost::Message;
use prost_types::Any;
use std::collections::HashMap;
use std::sync::Arc;
use torii::etl::sink::{EventBus, TopicInfo};
use torii::etl::{Envelope, ExtractionBatch, Sink, TypeId};
use torii::grpc::UpdateType;
use torii_common::u256_to_bytes;

/// Default threshold for "live" detection: 100 blocks from chain head.
/// Events from blocks older than this won't be broadcast to real-time subscribers.
const LIVE_THRESHOLD_BLOCKS: u64 = 100;

/// ERC1155 token sink
///
/// Processes ERC1155 TransferSingle, TransferBatch, and ApprovalForAll events:
/// - Stores transfer records in the database
/// - Publishes events via EventBus for real-time subscriptions (only when live)
/// - Broadcasts events via gRPC service for rich subscriptions (only when live)
///
/// Note: Like ERC20, we only track transfer history - NOT balances.
/// During historical indexing (more than 100 blocks from chain head), events are
/// stored but not broadcast to avoid overwhelming real-time subscribers.
pub struct Erc1155Sink {
    storage: Arc<Erc1155Storage>,
    event_bus: Option<Arc<EventBus>>,
    grpc_service: Option<Erc1155Service>,
}

impl Erc1155Sink {
    pub fn new(storage: Arc<Erc1155Storage>) -> Self {
        Self {
            storage,
            event_bus: None,
            grpc_service: None,
        }
    }

    /// Set the gRPC service for dual publishing
    pub fn with_grpc_service(mut self, service: Erc1155Service) -> Self {
        self.grpc_service = Some(service);
        self
    }

    /// Get a reference to the storage
    pub fn storage(&self) -> &Arc<Erc1155Storage> {
        &self.storage
    }

    /// Filter function for ERC1155 transfer events
    fn matches_transfer_filters(
        transfer: &proto::TokenTransfer,
        filters: &HashMap<String, String>,
    ) -> bool {
        if filters.is_empty() {
            return true;
        }

        // Wallet filter with OR logic (matches from OR to)
        if let Some(wallet_filter) = filters.get("wallet") {
            let from_hex = format!("0x{}", hex::encode(&transfer.from));
            let to_hex = format!("0x{}", hex::encode(&transfer.to));
            if !from_hex.eq_ignore_ascii_case(wallet_filter)
                && !to_hex.eq_ignore_ascii_case(wallet_filter)
            {
                return false;
            }
        }

        // Exact token filter
        if let Some(token_filter) = filters.get("token") {
            let token_hex = format!("0x{}", hex::encode(&transfer.token));
            if !token_hex.eq_ignore_ascii_case(token_filter) {
                return false;
            }
        }

        // Exact from filter
        if let Some(from_filter) = filters.get("from") {
            let from_hex = format!("0x{}", hex::encode(&transfer.from));
            if !from_hex.eq_ignore_ascii_case(from_filter) {
                return false;
            }
        }

        // Exact to filter
        if let Some(to_filter) = filters.get("to") {
            let to_hex = format!("0x{}", hex::encode(&transfer.to));
            if !to_hex.eq_ignore_ascii_case(to_filter) {
                return false;
            }
        }

        true
    }

}

#[async_trait]
impl Sink for Erc1155Sink {
    fn name(&self) -> &str {
        "erc1155"
    }

    fn interested_types(&self) -> Vec<TypeId> {
        vec![
            TypeId::new("erc1155.transfer_single"),
            TypeId::new("erc1155.transfer_batch"),
            TypeId::new("erc1155.approval_for_all"),
        ]
    }

    async fn initialize(
        &mut self,
        event_bus: Arc<EventBus>,
        _context: &torii::etl::sink::SinkContext,
    ) -> Result<()> {
        self.event_bus = Some(event_bus);
        tracing::info!(target: "torii_erc1155::sink", "ERC1155 sink initialized");
        Ok(())
    }

    async fn process(&self, envelopes: &[Envelope], batch: &ExtractionBatch) -> Result<()> {
        let mut transfers: Vec<TokenTransferData> = Vec::new();
        let mut operator_approvals: Vec<OperatorApprovalData> = Vec::new();

        // Get block timestamps from batch
        let block_timestamps: HashMap<u64, i64> = batch
            .blocks
            .iter()
            .map(|(num, block)| (*num, block.timestamp as i64))
            .collect();

        for envelope in envelopes {
            // Handle single transfers
            if envelope.type_id == TypeId::new("erc1155.transfer_single") {
                if let Some(transfer) = envelope.body.as_any().downcast_ref::<DecodedTransferSingle>() {
                    let timestamp = block_timestamps.get(&transfer.block_number).copied();
                    transfers.push(TokenTransferData {
                        id: None,
                        token: transfer.token,
                        operator: transfer.operator,
                        from: transfer.from,
                        to: transfer.to,
                        token_id: transfer.id,
                        amount: transfer.value,
                        is_batch: false,
                        batch_index: 0,
                        block_number: transfer.block_number,
                        tx_hash: transfer.transaction_hash,
                        timestamp,
                    });
                }
            }
            // Handle batch transfers
            else if envelope.type_id == TypeId::new("erc1155.transfer_batch") {
                if let Some(transfer) = envelope.body.as_any().downcast_ref::<DecodedTransferBatch>() {
                    let timestamp = block_timestamps.get(&transfer.block_number).copied();
                    transfers.push(TokenTransferData {
                        id: None,
                        token: transfer.token,
                        operator: transfer.operator,
                        from: transfer.from,
                        to: transfer.to,
                        token_id: transfer.id,
                        amount: transfer.value,
                        is_batch: true,
                        batch_index: transfer.batch_index,
                        block_number: transfer.block_number,
                        tx_hash: transfer.transaction_hash,
                        timestamp,
                    });
                }
            }
            // Handle approval for all
            else if envelope.type_id == TypeId::new("erc1155.approval_for_all") {
                if let Some(approval) = envelope.body.as_any().downcast_ref::<DecodedOperatorApproval>() {
                    let timestamp = block_timestamps.get(&approval.block_number).copied();
                    operator_approvals.push(OperatorApprovalData {
                        id: None,
                        token: approval.token,
                        owner: approval.owner,
                        operator: approval.operator,
                        approved: approval.approved,
                        block_number: approval.block_number,
                        tx_hash: approval.transaction_hash,
                        timestamp,
                    });
                }
            }
        }

        // Batch insert transfers
        if !transfers.is_empty() {
            let transfer_count = match self.storage.insert_transfers_batch(&transfers) {
                Ok(count) => count,
                Err(e) => {
                    tracing::error!(
                        target: "torii_erc1155::sink",
                        count = transfers.len(),
                        error = %e,
                        "Failed to batch insert transfers"
                    );
                    return Err(e);
                }
            };

            if transfer_count > 0 {
                tracing::info!(
                    target: "torii_erc1155::sink",
                    count = transfer_count,
                    "Batch inserted token transfers"
                );

                // Only broadcast to real-time subscribers when near chain head
                let is_live = batch.is_live(LIVE_THRESHOLD_BLOCKS);
                if is_live {
                    // Publish transfer events
                    for transfer in &transfers {
                        let proto_transfer = proto::TokenTransfer {
                            token: transfer.token.to_bytes_be().to_vec(),
                            operator: transfer.operator.to_bytes_be().to_vec(),
                            from: transfer.from.to_bytes_be().to_vec(),
                            to: transfer.to.to_bytes_be().to_vec(),
                            token_id: u256_to_bytes(transfer.token_id),
                            amount: u256_to_bytes(transfer.amount),
                            block_number: transfer.block_number,
                            tx_hash: transfer.tx_hash.to_bytes_be().to_vec(),
                            timestamp: transfer.timestamp.unwrap_or(0),
                            is_batch: transfer.is_batch,
                            batch_index: transfer.batch_index,
                        };

                        // Publish to EventBus
                        if let Some(event_bus) = &self.event_bus {
                            let mut buf = Vec::new();
                            proto_transfer.encode(&mut buf)?;
                            let any = Any {
                                type_url: "type.googleapis.com/torii.sinks.erc1155.TokenTransfer".to_string(),
                                value: buf,
                            };

                            event_bus.publish_protobuf(
                                "erc1155.transfer",
                                "erc1155.transfer",
                                &any,
                                &proto_transfer,
                                UpdateType::Created,
                                Self::matches_transfer_filters,
                            );
                        }

                        // Broadcast to gRPC service
                        if let Some(grpc_service) = &self.grpc_service {
                            grpc_service.broadcast_transfer(proto_transfer);
                        }
                    }
                }
            }
        }

        // Batch insert operator approvals
        if !operator_approvals.is_empty() {
            match self.storage.insert_operator_approvals_batch(&operator_approvals) {
                Ok(count) => {
                    tracing::info!(
                        target: "torii_erc1155::sink",
                        count = count,
                        "Batch inserted operator approvals"
                    );
                }
                Err(e) => {
                    tracing::error!(
                        target: "torii_erc1155::sink",
                        "Failed to batch insert {} operator approvals: {}",
                        operator_approvals.len(),
                        e
                    );
                    return Err(e);
                }
            }
        }

        // Log combined statistics
        if !transfers.is_empty() || !operator_approvals.is_empty() {
            if let Ok(total_transfers) = self.storage.get_transfer_count() {
                if let Ok(token_id_count) = self.storage.get_token_id_count() {
                    if let Ok(token_count) = self.storage.get_token_count() {
                        tracing::info!(
                            target: "torii_erc1155::sink",
                            transfers = total_transfers,
                            token_ids = token_id_count,
                            contracts = token_count,
                            blocks = batch.blocks.len(),
                            "Total statistics"
                        );
                    }
                }
            }
        }

        Ok(())
    }

    fn topics(&self) -> Vec<TopicInfo> {
        vec![TopicInfo::new(
            "erc1155.transfer",
            vec![
                "token".to_string(),
                "from".to_string(),
                "to".to_string(),
                "wallet".to_string(),
            ],
            "ERC1155 token transfers. Use 'wallet' filter for from OR to matching.",
        )]
    }

    fn build_routes(&self) -> Router {
        Router::new()
    }
}
