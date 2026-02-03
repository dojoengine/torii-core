//! gRPC service implementation for ERC721 queries and subscriptions

use crate::proto::{
    erc721_server::Erc721 as Erc721Trait, Cursor, GetOwnerRequest, GetOwnerResponse,
    GetOwnershipRequest, GetOwnershipResponse, GetStatsRequest, GetStatsResponse,
    GetTransfersRequest, GetTransfersResponse, NftTransfer, Ownership, SubscribeTransfersRequest,
    TransferFilter, TransferUpdate,
};
use crate::storage::{Erc721Storage, NftTransferData, TransferCursor};
use async_trait::async_trait;
use futures::stream::Stream;
use starknet::core::types::{Felt, U256};
use std::pin::Pin;
use std::sync::Arc;
use tokio::sync::broadcast;
use tonic::{Request, Response, Status};

/// gRPC service implementation for ERC721
#[derive(Clone)]
pub struct Erc721Service {
    storage: Arc<Erc721Storage>,
    /// Broadcast channel for real-time transfer updates
    pub transfer_tx: broadcast::Sender<TransferUpdate>,
}

impl Erc721Service {
    /// Creates a new Erc721Service
    pub fn new(storage: Arc<Erc721Storage>) -> Self {
        let (transfer_tx, _) = broadcast::channel(1000);

        Self {
            storage,
            transfer_tx,
        }
    }

    /// Broadcasts a transfer to all subscribers
    pub fn broadcast_transfer(&self, transfer: NftTransfer) {
        let update = TransferUpdate {
            transfer: Some(transfer),
            timestamp: chrono::Utc::now().timestamp(),
        };
        let _ = self.transfer_tx.send(update);
    }

    /// Convert storage NftTransferData to proto NftTransfer
    fn transfer_data_to_proto(data: &NftTransferData) -> NftTransfer {
        NftTransfer {
            token: data.token.to_bytes_be().to_vec(),
            token_id: u256_to_bytes(data.token_id),
            from: data.from.to_bytes_be().to_vec(),
            to: data.to.to_bytes_be().to_vec(),
            block_number: data.block_number,
            tx_hash: data.tx_hash.to_bytes_be().to_vec(),
            timestamp: data.timestamp.unwrap_or(0),
        }
    }

    /// Check if a transfer matches a filter (for subscriptions)
    fn matches_transfer_filter(transfer: &NftTransfer, filter: &TransferFilter) -> bool {
        // Wallet filter (OR logic: matches from OR to)
        if let Some(ref wallet) = filter.wallet {
            let matches_from = transfer.from == *wallet;
            let matches_to = transfer.to == *wallet;
            if !matches_from && !matches_to {
                return false;
            }
        }

        // Exact from filter
        if let Some(ref from) = filter.from {
            if transfer.from != *from {
                return false;
            }
        }

        // Exact to filter
        if let Some(ref to) = filter.to {
            if transfer.to != *to {
                return false;
            }
        }

        // Token whitelist
        if !filter.tokens.is_empty() && !filter.tokens.iter().any(|t| *t == transfer.token) {
            return false;
        }

        // Token ID whitelist
        if !filter.token_ids.is_empty() && !filter.token_ids.iter().any(|tid| *tid == transfer.token_id) {
            return false;
        }

        // Block range filters
        if let Some(block_from) = filter.block_from {
            if transfer.block_number < block_from {
                return false;
            }
        }

        if let Some(block_to) = filter.block_to {
            if transfer.block_number > block_to {
                return false;
            }
        }

        true
    }
}

/// Convert U256 to bytes for proto (big-endian, compact)
fn u256_to_bytes(value: U256) -> Vec<u8> {
    let high = value.high();
    let low = value.low();

    if high == 0 {
        if low == 0 {
            return vec![0u8];
        }
        let bytes = low.to_be_bytes();
        let start = bytes.iter().position(|&b| b != 0).unwrap_or(15);
        return bytes[start..].to_vec();
    }

    let mut result = Vec::with_capacity(32);
    let high_bytes = high.to_be_bytes();
    let high_start = high_bytes.iter().position(|&b| b != 0).unwrap_or(15);
    result.extend_from_slice(&high_bytes[high_start..]);
    result.extend_from_slice(&low.to_be_bytes());
    result
}

/// Parse bytes to Felt (returns None if invalid)
fn bytes_to_felt(bytes: &[u8]) -> Option<Felt> {
    if bytes.len() > 32 {
        return None;
    }
    let mut arr = [0u8; 32];
    arr[32 - bytes.len()..].copy_from_slice(bytes);
    Some(Felt::from_bytes_be(&arr))
}

/// Parse bytes to U256
fn bytes_to_u256(bytes: &[u8]) -> U256 {
    let len = bytes.len();
    if len == 0 {
        return U256::from(0u64);
    }

    if len <= 16 {
        let mut low_bytes = [0u8; 16];
        low_bytes[16 - len..].copy_from_slice(bytes);
        let low = u128::from_be_bytes(low_bytes);
        U256::from_words(low, 0)
    } else {
        let high_len = len - 16;
        let mut high_bytes = [0u8; 16];
        high_bytes[16 - high_len..].copy_from_slice(&bytes[..high_len]);
        let high = u128::from_be_bytes(high_bytes);

        let mut low_bytes = [0u8; 16];
        low_bytes.copy_from_slice(&bytes[high_len..]);
        let low = u128::from_be_bytes(low_bytes);

        U256::from_words(low, high)
    }
}

#[async_trait]
impl Erc721Trait for Erc721Service {
    /// Query historical transfers with filtering and pagination
    async fn get_transfers(
        &self,
        request: Request<GetTransfersRequest>,
    ) -> Result<Response<GetTransfersResponse>, Status> {
        let req = request.into_inner();
        let filter = req.filter.unwrap_or_default();

        let wallet = filter.wallet.as_ref().and_then(|b| bytes_to_felt(b));
        let from = filter.from.as_ref().and_then(|b| bytes_to_felt(b));
        let to = filter.to.as_ref().and_then(|b| bytes_to_felt(b));
        let tokens: Vec<Felt> = filter
            .tokens
            .iter()
            .filter_map(|b| bytes_to_felt(b))
            .collect();
        let token_ids: Vec<U256> = filter
            .token_ids
            .iter()
            .map(|b| bytes_to_u256(b))
            .collect();

        let cursor = req.cursor.map(|c| TransferCursor {
            block_number: c.block_number,
            id: c.id,
        });

        let limit = if req.limit == 0 {
            100
        } else {
            req.limit.min(1000)
        };

        let (transfers, next_cursor) = self
            .storage
            .get_transfers_filtered(
                wallet,
                from,
                to,
                &tokens,
                &token_ids,
                filter.block_from,
                filter.block_to,
                cursor,
                limit,
            )
            .map_err(|e| Status::internal(format!("Query failed: {}", e)))?;

        let proto_transfers: Vec<NftTransfer> =
            transfers.iter().map(Self::transfer_data_to_proto).collect();

        let proto_cursor = next_cursor.map(|c| Cursor {
            block_number: c.block_number,
            id: c.id,
        });

        Ok(Response::new(GetTransfersResponse {
            transfers: proto_transfers,
            next_cursor: proto_cursor,
        }))
    }

    /// Query current NFT ownership
    async fn get_ownership(
        &self,
        request: Request<GetOwnershipRequest>,
    ) -> Result<Response<GetOwnershipResponse>, Status> {
        let req = request.into_inner();
        let filter = req.filter.unwrap_or_default();

        let owner = filter.owner.as_ref().and_then(|b| bytes_to_felt(b));
        let tokens: Vec<Felt> = filter
            .tokens
            .iter()
            .filter_map(|b| bytes_to_felt(b))
            .collect();

        let cursor = req.cursor.map(|c| crate::storage::OwnershipCursor {
            block_number: c.block_number,
            id: c.id,
        });

        let limit = if req.limit == 0 {
            100
        } else {
            req.limit.min(1000)
        };

        let owner = owner.ok_or_else(|| Status::invalid_argument("owner filter is required"))?;

        let (ownership, next_cursor) = self
            .storage
            .get_ownership_by_owner(owner, &tokens, cursor, limit)
            .map_err(|e| Status::internal(format!("Query failed: {}", e)))?;

        let proto_ownership: Vec<Ownership> = ownership
            .iter()
            .map(|o| Ownership {
                token: o.token.to_bytes_be().to_vec(),
                token_id: u256_to_bytes(o.token_id),
                owner: o.owner.to_bytes_be().to_vec(),
                block_number: o.block_number,
            })
            .collect();

        let proto_cursor = next_cursor.map(|c| Cursor {
            block_number: c.block_number,
            id: c.id,
        });

        Ok(Response::new(GetOwnershipResponse {
            ownership: proto_ownership,
            next_cursor: proto_cursor,
        }))
    }

    /// Get the current owner of a specific NFT
    async fn get_owner(
        &self,
        request: Request<GetOwnerRequest>,
    ) -> Result<Response<GetOwnerResponse>, Status> {
        let req = request.into_inner();

        let token = bytes_to_felt(&req.token)
            .ok_or_else(|| Status::invalid_argument("invalid token address"))?;
        let token_id = bytes_to_u256(&req.token_id);

        let owner = self
            .storage
            .get_owner(token, token_id)
            .map_err(|e| Status::internal(format!("Query failed: {}", e)))?;

        Ok(Response::new(GetOwnerResponse {
            owner: owner.map(|o| o.to_bytes_be().to_vec()),
        }))
    }

    /// Subscribe to real-time transfer events
    type SubscribeTransfersStream =
        Pin<Box<dyn Stream<Item = Result<TransferUpdate, Status>> + Send>>;

    async fn subscribe_transfers(
        &self,
        request: Request<SubscribeTransfersRequest>,
    ) -> Result<Response<Self::SubscribeTransfersStream>, Status> {
        let req = request.into_inner();
        let filter = req.filter.unwrap_or_default();

        tracing::info!(
            target: "torii_erc721::grpc",
            "New transfer subscription from client: {}",
            req.client_id
        );

        let mut rx = self.transfer_tx.subscribe();

        let stream = async_stream::try_stream! {
            loop {
                match rx.recv().await {
                    Ok(update) => {
                        if let Some(ref transfer) = update.transfer {
                            if !Self::matches_transfer_filter(transfer, &filter) {
                                continue;
                            }
                        }
                        yield update;
                    }
                    Err(broadcast::error::RecvError::Lagged(skipped)) => {
                        tracing::warn!(
                            target: "torii_erc721::grpc",
                            "Client {} lagged, skipped {} updates",
                            req.client_id,
                            skipped
                        );
                        continue;
                    }
                    Err(broadcast::error::RecvError::Closed) => {
                        break;
                    }
                }
            }
        };

        Ok(Response::new(Box::pin(stream)))
    }

    /// Get indexer statistics
    async fn get_stats(
        &self,
        _request: Request<GetStatsRequest>,
    ) -> Result<Response<GetStatsResponse>, Status> {
        let total_transfers = self
            .storage
            .get_transfer_count()
            .map_err(|e| Status::internal(format!("Failed to get transfer count: {}", e)))?;

        let unique_tokens = self
            .storage
            .get_token_count()
            .map_err(|e| Status::internal(format!("Failed to get token count: {}", e)))?;

        let unique_nfts = self
            .storage
            .get_nft_count()
            .map_err(|e| Status::internal(format!("Failed to get NFT count: {}", e)))?;

        let latest_block = self
            .storage
            .get_latest_block()
            .map_err(|e| Status::internal(format!("Failed to get latest block: {}", e)))?
            .unwrap_or(0);

        Ok(Response::new(GetStatsResponse {
            total_transfers,
            unique_tokens,
            unique_nfts,
            latest_block,
        }))
    }
}
