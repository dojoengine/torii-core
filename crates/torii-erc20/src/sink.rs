//! ERC20 sink for processing transfers and approvals
//!
//! This sink:
//! - Stores transfer and approval records in the database
//! - Publishes events via EventBus for real-time subscriptions (simple clients)
//! - Broadcasts events via gRPC service for rich subscriptions (advanced clients)
//!
//! Note: Balance tracking was intentionally removed. Clients should fetch
//! actual balances from the chain when needed, as transfer-derived balances
//! are inherently incorrect (genesis allocations, airdrops, etc.).

use crate::decoder::{Approval as DecodedApproval, Transfer as DecodedTransfer};
use crate::grpc_service::Erc20Service;
use crate::proto;
use crate::storage::{ApprovalData, Erc20Storage, TransferData};
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

/// ERC20 transfer and approval sink
///
/// Processes ERC20 Transfer and Approval events and:
/// - Stores records in the database
/// - Publishes events via EventBus for real-time subscriptions
/// - Broadcasts events via gRPC service for rich subscriptions
pub struct Erc20Sink {
    storage: Arc<Erc20Storage>,
    event_bus: Option<Arc<EventBus>>,
    grpc_service: Option<Erc20Service>,
}

impl Erc20Sink {
    pub fn new(storage: Arc<Erc20Storage>) -> Self {
        Self {
            storage,
            event_bus: None,
            grpc_service: None,
        }
    }

    /// Set the gRPC service for dual publishing
    pub fn with_grpc_service(mut self, service: Erc20Service) -> Self {
        self.grpc_service = Some(service);
        self
    }

    /// Get a reference to the storage
    pub fn storage(&self) -> &Arc<Erc20Storage> {
        &self.storage
    }

    /// Filter function for ERC20 transfer events
    ///
    /// Supports filters:
    /// - "token": Filter by token contract address (hex string)
    /// - "from": Filter by sender address (hex string)
    /// - "to": Filter by receiver address (hex string)
    /// - "wallet": Filter by wallet address - matches from OR to (OR logic)
    fn matches_transfer_filters(
        transfer: &proto::Transfer,
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

    /// Filter function for ERC20 approval events
    ///
    /// Supports filters:
    /// - "token": Filter by token contract address (hex string)
    /// - "owner": Filter by owner address (hex string)
    /// - "spender": Filter by spender address (hex string)
    /// - "account": Filter by account address - matches owner OR spender (OR logic)
    fn matches_approval_filters(
        approval: &proto::Approval,
        filters: &HashMap<String, String>,
    ) -> bool {
        if filters.is_empty() {
            return true;
        }

        // Account filter with OR logic (matches owner OR spender)
        if let Some(account_filter) = filters.get("account") {
            let owner_hex = format!("0x{}", hex::encode(&approval.owner));
            let spender_hex = format!("0x{}", hex::encode(&approval.spender));
            if !owner_hex.eq_ignore_ascii_case(account_filter)
                && !spender_hex.eq_ignore_ascii_case(account_filter)
            {
                return false;
            }
        }

        // Exact token filter
        if let Some(token_filter) = filters.get("token") {
            let token_hex = format!("0x{}", hex::encode(&approval.token));
            if !token_hex.eq_ignore_ascii_case(token_filter) {
                return false;
            }
        }

        // Exact owner filter
        if let Some(owner_filter) = filters.get("owner") {
            let owner_hex = format!("0x{}", hex::encode(&approval.owner));
            if !owner_hex.eq_ignore_ascii_case(owner_filter) {
                return false;
            }
        }

        // Exact spender filter
        if let Some(spender_filter) = filters.get("spender") {
            let spender_hex = format!("0x{}", hex::encode(&approval.spender));
            if !spender_hex.eq_ignore_ascii_case(spender_filter) {
                return false;
            }
        }

        true
    }

}

#[async_trait]
impl Sink for Erc20Sink {
    fn name(&self) -> &str {
        "erc20"
    }

    fn interested_types(&self) -> Vec<TypeId> {
        vec![
            TypeId::new("erc20.transfer"),
            TypeId::new("erc20.approval"),
        ]
    }

    async fn initialize(
        &mut self,
        event_bus: Arc<EventBus>,
        _context: &torii::etl::sink::SinkContext,
    ) -> Result<()> {
        self.event_bus = Some(event_bus);
        tracing::info!(target: "torii_erc20::sink", "ERC20 sink initialized");
        Ok(())
    }

    async fn process(&self, envelopes: &[Envelope], batch: &ExtractionBatch) -> Result<()> {
        // Collect all transfers and approvals for batch insertion
        let mut transfers: Vec<TransferData> = Vec::new();
        let mut approvals: Vec<ApprovalData> = Vec::new();

        // Get block timestamps from batch for enrichment
        let block_timestamps: HashMap<u64, i64> = batch
            .blocks
            .iter()
            .map(|(num, block)| (*num, block.timestamp as i64))
            .collect();

        for envelope in envelopes {
            // Handle transfers
            if envelope.type_id == TypeId::new("erc20.transfer") {
                if let Some(transfer) = envelope.body.as_any().downcast_ref::<DecodedTransfer>() {
                    let timestamp = block_timestamps.get(&transfer.block_number).copied();
                    transfers.push(TransferData {
                        id: None,
                        token: transfer.token,
                        from: transfer.from,
                        to: transfer.to,
                        amount: transfer.amount,
                        block_number: transfer.block_number,
                        tx_hash: transfer.transaction_hash,
                        timestamp,
                    });
                }
            }
            // Handle approvals
            else if envelope.type_id == TypeId::new("erc20.approval") {
                if let Some(approval) = envelope.body.as_any().downcast_ref::<DecodedApproval>() {
                    let timestamp = block_timestamps.get(&approval.block_number).copied();
                    approvals.push(ApprovalData {
                        id: None,
                        token: approval.token,
                        owner: approval.owner,
                        spender: approval.spender,
                        amount: approval.amount,
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
                        target: "torii_erc20::sink",
                        count = transfers.len(),
                        error = %e,
                        "Failed to batch insert transfers"
                    );
                    return Err(e);
                }
            };

            if transfer_count > 0 {
                tracing::info!(
                    target: "torii_erc20::sink",
                    count = transfer_count,
                    "Batch inserted transfers"
                );

                // Publish transfer events
                for transfer in &transfers {
                    let proto_transfer = proto::Transfer {
                        token: transfer.token.to_bytes_be().to_vec(),
                        from: transfer.from.to_bytes_be().to_vec(),
                        to: transfer.to.to_bytes_be().to_vec(),
                        amount: u256_to_bytes(transfer.amount),
                        block_number: transfer.block_number,
                        tx_hash: transfer.tx_hash.to_bytes_be().to_vec(),
                        timestamp: transfer.timestamp.unwrap_or(0),
                    };

                    // Publish to EventBus (simple clients)
                    if let Some(event_bus) = &self.event_bus {
                        let mut buf = Vec::new();
                        proto_transfer.encode(&mut buf)?;
                        let any = Any {
                            type_url: "type.googleapis.com/torii.sinks.erc20.Transfer".to_string(),
                            value: buf,
                        };

                        event_bus.publish_protobuf(
                            "erc20.transfer",
                            "erc20.transfer",
                            &any,
                            &proto_transfer,
                            UpdateType::Created,
                            Self::matches_transfer_filters,
                        );
                    }

                    // Broadcast to gRPC service (rich clients)
                    if let Some(grpc_service) = &self.grpc_service {
                        grpc_service.broadcast_transfer(proto_transfer);
                    }
                }
            }
        }

        // Batch insert approvals
        if !approvals.is_empty() {
            let approval_count = match self.storage.insert_approvals_batch(&approvals) {
                Ok(count) => count,
                Err(e) => {
                    tracing::error!(
                        target: "torii_erc20::sink",
                        count = approvals.len(),
                        error = %e,
                        "Failed to batch insert approvals"
                    );
                    return Err(e);
                }
            };

            if approval_count > 0 {
                tracing::info!(
                    target: "torii_erc20::sink",
                    count = approval_count,
                    "Batch inserted approvals"
                );

                // Publish approval events
                for approval in &approvals {
                    let proto_approval = proto::Approval {
                        token: approval.token.to_bytes_be().to_vec(),
                        owner: approval.owner.to_bytes_be().to_vec(),
                        spender: approval.spender.to_bytes_be().to_vec(),
                        amount: u256_to_bytes(approval.amount),
                        block_number: approval.block_number,
                        tx_hash: approval.tx_hash.to_bytes_be().to_vec(),
                        timestamp: approval.timestamp.unwrap_or(0),
                    };

                    // Publish to EventBus (simple clients)
                    if let Some(event_bus) = &self.event_bus {
                        let mut buf = Vec::new();
                        proto_approval.encode(&mut buf)?;
                        let any = Any {
                            type_url: "type.googleapis.com/torii.sinks.erc20.Approval".to_string(),
                            value: buf,
                        };

                        event_bus.publish_protobuf(
                            "erc20.approval",
                            "erc20.approval",
                            &any,
                            &proto_approval,
                            UpdateType::Created,
                            Self::matches_approval_filters,
                        );
                    }

                    // Broadcast to gRPC service (rich clients)
                    if let Some(grpc_service) = &self.grpc_service {
                        grpc_service.broadcast_approval(proto_approval);
                    }
                }
            }
        }

        // Log combined statistics
        if !transfers.is_empty() || !approvals.is_empty() {
            if let Ok(total_transfers) = self.storage.get_transfer_count() {
                if let Ok(total_approvals) = self.storage.get_approval_count() {
                    if let Ok(token_count) = self.storage.get_token_count() {
                        tracing::info!(
                            target: "torii_erc20::sink",
                            transfers = total_transfers,
                            approvals = total_approvals,
                            tokens = token_count,
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
        vec![
            TopicInfo::new(
                "erc20.transfer",
                vec![
                    "token".to_string(),
                    "from".to_string(),
                    "to".to_string(),
                    "wallet".to_string(),
                ],
                "ERC20 token transfers. Use 'wallet' filter for from OR to matching.",
            ),
            TopicInfo::new(
                "erc20.approval",
                vec![
                    "token".to_string(),
                    "owner".to_string(),
                    "spender".to_string(),
                    "account".to_string(),
                ],
                "ERC20 token approvals. Use 'account' filter for owner OR spender matching.",
            ),
        ]
    }

    fn build_routes(&self) -> Router {
        // No custom HTTP routes for now
        Router::new()
    }
}
