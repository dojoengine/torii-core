//! ERC721 sink for processing NFT transfers, approvals, and ownership

use crate::decoder::{
    BatchMetadataUpdate as DecodedBatchMetadataUpdate, MetadataUpdate as DecodedMetadataUpdate,
    NftTransfer as DecodedNftTransfer, OperatorApproval as DecodedOperatorApproval,
};
use crate::grpc_service::Erc721Service;
use crate::handlers::{FetchErc721MetadataCommand, RefreshErc721TokenUriCommand};
use crate::proto;
use crate::storage::{Erc721Storage, NftTransferData, OperatorApprovalData};
use anyhow::Result;
use async_trait::async_trait;
use axum::Router;
use prost::Message;
use prost_types::Any;
use starknet::core::types::{Felt, U256};
use starknet::providers::jsonrpc::{HttpTransport, JsonRpcClient};
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use torii::command::CommandBusSender;
use torii::etl::sink::{EventBus, TopicInfo};
use torii::etl::{Envelope, ExtractionBatch, Sink, TypeId};
use torii::grpc::UpdateType;
use torii_common::{u256_to_bytes, TokenUriSender};

/// Default threshold for "live" detection: 100 blocks from chain head.
/// Events from blocks older than this won't be broadcast to real-time subscribers.
const LIVE_THRESHOLD_BLOCKS: u64 = 100;

/// ERC721 NFT sink
///
/// Processes ERC721 Transfer, Approval, and ApprovalForAll events:
/// - Stores transfer records and tracks ownership in the database
/// - Publishes events via EventBus for real-time subscriptions (only when live)
/// - Broadcasts events via gRPC service for rich subscriptions (only when live)
///
/// During historical indexing (more than 100 blocks from chain head), events are
/// stored but not broadcast to avoid overwhelming real-time subscribers.
pub struct Erc721Sink {
    storage: Arc<Erc721Storage>,
    event_bus: Option<Arc<EventBus>>,
    grpc_service: Option<Erc721Service>,
    /// Whether contract metadata commands should be dispatched.
    metadata_commands_enabled: bool,
    /// Whether token URI commands should be dispatched.
    token_uri_commands_enabled: bool,
    /// Command bus sender for background metadata and token URI work.
    command_bus: Option<CommandBusSender>,
    /// Commands already queued but not yet observed in storage.
    pending_metadata_commands: tokio::sync::Mutex<HashSet<Felt>>,
    pending_token_uri_commands: tokio::sync::Mutex<HashSet<(Felt, U256)>>,
    /// In-memory counters to avoid full-table COUNT(*) in the ingest hot path.
    total_transfers: AtomicU64,
    total_operator_approvals: AtomicU64,
}

impl Erc721Sink {
    pub fn new(storage: Arc<Erc721Storage>) -> Self {
        Self {
            storage,
            event_bus: None,
            grpc_service: None,
            metadata_commands_enabled: false,
            token_uri_commands_enabled: false,
            command_bus: None,
            pending_metadata_commands: tokio::sync::Mutex::new(HashSet::new()),
            pending_token_uri_commands: tokio::sync::Mutex::new(HashSet::new()),
            // Avoid startup full-table COUNT(*) scans on large datasets.
            total_transfers: AtomicU64::new(0),
            total_operator_approvals: AtomicU64::new(0),
        }
    }

    /// Set the gRPC service for dual publishing
    pub fn with_grpc_service(mut self, service: Erc721Service) -> Self {
        self.grpc_service = Some(service);
        self
    }

    /// Get a reference to the storage
    pub fn storage(&self) -> &Arc<Erc721Storage> {
        &self.storage
    }

    /// Filter function for ERC721 transfer events
    fn matches_transfer_filters(
        transfer: &proto::NftTransfer,
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

    /// Filter function for ERC721 token metadata updates.
    ///
    /// Supports filters:
    /// - "token": Filter by token contract address (hex string)
    fn matches_metadata_filters(
        metadata: &proto::TokenMetadataEntry,
        filters: &HashMap<String, String>,
    ) -> bool {
        if filters.is_empty() {
            return true;
        }

        if let Some(token_filter) = filters.get("token") {
            let token_hex = format!("0x{}", hex::encode(&metadata.token));
            if !token_hex.eq_ignore_ascii_case(token_filter) {
                return false;
            }
        }

        true
    }
}

#[async_trait]
impl Sink for Erc721Sink {
    fn name(&self) -> &'static str {
        "erc721"
    }

    fn interested_types(&self) -> Vec<TypeId> {
        vec![
            TypeId::new("erc721.transfer"),
            TypeId::new("erc721.approval"),
            TypeId::new("erc721.approval_for_all"),
            TypeId::new("erc721.metadata_update"),
            TypeId::new("erc721.batch_metadata_update"),
        ]
    }

    async fn initialize(
        &mut self,
        event_bus: Arc<EventBus>,
        context: &torii::etl::sink::SinkContext,
    ) -> Result<()> {
        self.event_bus = Some(event_bus);
        self.command_bus = Some(context.command_bus.clone());
        tracing::info!(target: "torii_erc721::sink", "ERC721 sink initialized");
        Ok(())
    }

    async fn process(&self, envelopes: &[Envelope], batch: &ExtractionBatch) -> Result<()> {
        let mut transfers: Vec<NftTransferData> = Vec::with_capacity(envelopes.len());
        let mut operator_approvals: Vec<OperatorApprovalData> = Vec::with_capacity(envelopes.len());
        let mut inserted_transfers: u64 = 0;
        let mut inserted_operator_approvals: u64 = 0;

        // Get block timestamps from batch
        let block_timestamps: HashMap<u64, i64> = batch
            .blocks
            .iter()
            .map(|(num, block)| (*num, block.timestamp as i64))
            .collect();

        for envelope in envelopes {
            // Handle transfers
            if envelope.type_id == TypeId::new("erc721.transfer") {
                if let Some(transfer) = envelope.body.as_any().downcast_ref::<DecodedNftTransfer>()
                {
                    let timestamp = block_timestamps.get(&transfer.block_number).copied();
                    transfers.push(NftTransferData {
                        id: None,
                        token: transfer.token,
                        token_id: transfer.token_id,
                        from: transfer.from,
                        to: transfer.to,
                        block_number: transfer.block_number,
                        tx_hash: transfer.transaction_hash,
                        timestamp,
                    });
                }
            }
            // Handle approval for all
            else if envelope.type_id == TypeId::new("erc721.approval_for_all") {
                if let Some(approval) = envelope
                    .body
                    .as_any()
                    .downcast_ref::<DecodedOperatorApproval>()
                {
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
            // Handle MetadataUpdate (EIP-4906) — single token
            else if envelope.type_id == TypeId::new("erc721.metadata_update") {
                if let Some(update) = envelope
                    .body
                    .as_any()
                    .downcast_ref::<DecodedMetadataUpdate>()
                {
                    if self.token_uri_commands_enabled {
                        if let Some(ref command_bus) = self.command_bus {
                            if let Err(error) = command_bus.dispatch(RefreshErc721TokenUriCommand {
                                contract: update.token,
                                token_id: update.token_id,
                            }) {
                                tracing::warn!(
                                    target: "torii_erc721::sink",
                                    error = %error,
                                    "Failed to dispatch ERC721 token URI refresh"
                                );
                            }
                        }
                    }
                }
            }
            // Handle BatchMetadataUpdate (EIP-4906) — range of tokens
            else if envelope.type_id == TypeId::new("erc721.batch_metadata_update") {
                if let Some(update) = envelope
                    .body
                    .as_any()
                    .downcast_ref::<DecodedBatchMetadataUpdate>()
                {
                    if self.token_uri_commands_enabled {
                        if let Some(ref command_bus) = self.command_bus {
                            // For batch updates, we need to know which token IDs exist in the range.
                            // Fetch them from storage and request URI updates for each.
                            if let Ok(uris) =
                                self.storage.get_token_uris_by_contract(update.token).await
                            {
                                for (token_id, _, _) in &uris {
                                    if *token_id >= update.from_token_id
                                        && *token_id <= update.to_token_id
                                    {
                                        if let Err(error) =
                                            command_bus.dispatch(RefreshErc721TokenUriCommand {
                                                contract: update.token,
                                                token_id: *token_id,
                                            })
                                        {
                                            tracing::warn!(
                                                target: "torii_erc721::sink",
                                                error = %error,
                                                "Failed to dispatch ERC721 batch token URI refresh"
                                            );
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
            // Note: erc721.approval (single token approval) could be handled similarly
            // but is less commonly needed for indexing purposes
        }

        // Fetch metadata for any new token contracts.
        if self.metadata_commands_enabled {
            if let Some(ref command_bus) = self.command_bus {
                let candidate_tokens: Vec<Felt> =
                    transfers.iter().map(|transfer| transfer.token).collect::<HashSet<_>>().into_iter().collect();
                let unchecked_tokens = {
                    let pending = self.pending_metadata_commands.lock().await;
                    candidate_tokens
                        .into_iter()
                        .filter(|token| !pending.contains(token))
                        .collect::<Vec<_>>()
                };

                match self.storage.has_token_metadata_batch(&unchecked_tokens).await {
                    Ok(existing_tokens) => {
                        let mut pending = self.pending_metadata_commands.lock().await;
                        for token in &existing_tokens {
                            pending.remove(token);
                        }

                        for token in unchecked_tokens
                            .into_iter()
                            .filter(|token| !existing_tokens.contains(token))
                        {
                            if !pending.insert(token) {
                                continue;
                            }
                            if let Err(error) =
                                command_bus.dispatch(FetchErc721MetadataCommand { token })
                            {
                                pending.remove(&token);
                                tracing::warn!(
                                    target: "torii_erc721::sink",
                                    token = %format!("{:#x}", token),
                                    error = %error,
                                    "Failed to dispatch ERC721 metadata command"
                                );
                            }
                        }
                    }
                    Err(error) => {
                        tracing::warn!(
                            target: "torii_erc721::sink",
                            error = %error,
                            "Failed to batch-check token metadata"
                        );
                    }
                }
            }
        }

        // Request token URI fetches for new token IDs.
        if self.token_uri_commands_enabled {
            if let Some(ref command_bus) = self.command_bus {
                let candidate_tokens: Vec<(Felt, U256)> = transfers
                    .iter()
                    .map(|transfer| (transfer.token, transfer.token_id))
                    .collect::<HashSet<_>>()
                    .into_iter()
                    .collect();
                let unchecked_tokens = {
                    let pending = self.pending_token_uri_commands.lock().await;
                    candidate_tokens
                        .into_iter()
                        .filter(|token| !pending.contains(token))
                        .collect::<Vec<_>>()
                };

                match self.storage.has_token_uri_batch(&unchecked_tokens).await {
                    Ok(existing_tokens) => {
                        let mut pending = self.pending_token_uri_commands.lock().await;
                        for token in &existing_tokens {
                            pending.remove(token);
                        }

                        for (contract, token_id) in unchecked_tokens
                            .into_iter()
                            .filter(|token| !existing_tokens.contains(token))
                        {
                            if !pending.insert((contract, token_id)) {
                                continue;
                            }
                            if let Err(error) =
                                command_bus.dispatch(RefreshErc721TokenUriCommand {
                                    contract,
                                    token_id,
                                })
                            {
                                pending.remove(&(contract, token_id));
                                tracing::warn!(
                                    target: "torii_erc721::sink",
                                    error = %error,
                                    "Failed to dispatch ERC721 token URI command"
                                );
                            }
                        }
                    }
                    Err(error) => {
                        tracing::warn!(
                            target: "torii_erc721::sink",
                            error = %error,
                            "Failed to batch-check token URI existence"
                        );
                    }
                }
            }
        }

        // Batch insert transfers
        if !transfers.is_empty() {
            let transfer_count = match self.storage.insert_transfers_batch(&transfers).await {
                Ok(count) => count,
                Err(e) => {
                    tracing::error!(
                        target: "torii_erc721::sink",
                        count = transfers.len(),
                        error = %e,
                        "Failed to batch insert transfers"
                    );
                    return Err(e);
                }
            };

            if transfer_count > 0 {
                inserted_transfers = transfer_count as u64;
                self.total_transfers
                    .fetch_add(inserted_transfers, Ordering::Relaxed);

                tracing::info!(
                    target: "torii_erc721::sink",
                    count = transfer_count,
                    "Batch inserted NFT transfers"
                );

                // Only broadcast to real-time subscribers when near chain head
                let is_live = batch.is_live(LIVE_THRESHOLD_BLOCKS);
                if is_live {
                    // Publish transfer events
                    for transfer in &transfers {
                        let proto_transfer = proto::NftTransfer {
                            token: transfer.token.to_bytes_be().to_vec(),
                            token_id: u256_to_bytes(transfer.token_id),
                            from: transfer.from.to_bytes_be().to_vec(),
                            to: transfer.to.to_bytes_be().to_vec(),
                            block_number: transfer.block_number,
                            tx_hash: transfer.tx_hash.to_bytes_be().to_vec(),
                            timestamp: transfer.timestamp.unwrap_or(0),
                        };

                        // Publish to EventBus
                        if let Some(event_bus) = &self.event_bus {
                            let mut buf = Vec::new();
                            proto_transfer.encode(&mut buf)?;
                            let any = Any {
                                type_url: "type.googleapis.com/torii.sinks.erc721.NftTransfer"
                                    .to_string(),
                                value: buf,
                            };

                            event_bus.publish_protobuf(
                                "erc721.transfer",
                                "erc721.transfer",
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
            match self
                .storage
                .insert_operator_approvals_batch(&operator_approvals)
                .await
            {
                Ok(count) => {
                    inserted_operator_approvals = count as u64;
                    self.total_operator_approvals
                        .fetch_add(inserted_operator_approvals, Ordering::Relaxed);

                    tracing::info!(
                        target: "torii_erc721::sink",
                        count = count,
                        "Batch inserted operator approvals"
                    );
                }
                Err(e) => {
                    tracing::error!(
                        target: "torii_erc721::sink",
                        "Failed to batch insert {} operator approvals: {}",
                        operator_approvals.len(),
                        e
                    );
                    return Err(e);
                }
            }
        }

        // Log combined statistics without full-table scans.
        if inserted_transfers > 0 || inserted_operator_approvals > 0 {
            tracing::info!(
                target: "torii_erc721::sink",
                batch_transfers = inserted_transfers,
                batch_operator_approvals = inserted_operator_approvals,
                total_transfers = self.total_transfers.load(Ordering::Relaxed),
                total_operator_approvals = self.total_operator_approvals.load(Ordering::Relaxed),
                blocks = batch.blocks.len(),
                "Total statistics"
            );
        }

        Ok(())
    }

    fn topics(&self) -> Vec<TopicInfo> {
        vec![
            TopicInfo::new(
                "erc721.transfer",
                vec![
                    "token".to_string(),
                    "from".to_string(),
                    "to".to_string(),
                    "wallet".to_string(),
                ],
                "ERC721 NFT transfers. Use 'wallet' filter for from OR to matching.",
            ),
            TopicInfo::new(
                "erc721.metadata",
                vec!["token".to_string()],
                "ERC721 token metadata updates (registered/updated token attributes).",
            ),
        ]
    }

    fn build_routes(&self) -> Router {
        Router::new()
    }
}

impl Erc721Sink {
    pub fn with_metadata_commands(mut self) -> Self {
        self.metadata_commands_enabled = true;
        self
    }

    pub fn with_metadata_fetching(self, _provider: Arc<JsonRpcClient<HttpTransport>>) -> Self {
        self.with_metadata_commands()
    }

    pub fn with_token_uri_commands(mut self) -> Self {
        self.token_uri_commands_enabled = true;
        self
    }

    pub fn with_token_uri_sender(self, _sender: TokenUriSender) -> Self {
        self.with_token_uri_commands()
    }
}
