//! Backfill subcommand for historical token data sync
//!
//! Uses event-based fetching (`starknet_getEvents`) to efficiently backfill
//! historical data for specific contracts without re-processing entire block ranges.
//!
//! # Usage
//!
//! ```bash
//! # Backfill a single ERC20 contract from block 100000 to 500000
//! torii-tokens backfill \
//!     --erc20 0x123...abc:100000 \
//!     --to-block 500000 \
//!     --db-path ./tokens-data.db
//!
//! # Backfill multiple contracts
//! torii-tokens backfill \
//!     --erc20 0x123...abc:100000 \
//!     --erc20 0x456...def:200000 \
//!     --erc721 0x789...ghi:50000 \
//!     --to-block 500000
//! ```

use anyhow::{Context, Result};
use clap::Parser;
use starknet::core::types::Felt;
use std::sync::Arc;
use torii::etl::{Decoder, TypeId};
use torii_common::{BackfillProgress, ContractBackfillConfig, EventBackfill, TokenType};
use torii_erc1155::{Erc1155Decoder, Erc1155Storage};
use torii_erc20::{Erc20Decoder, Erc20Storage};
use torii_erc721::{Erc721Decoder, Erc721Storage};

/// Backfill CLI arguments
#[derive(Parser, Debug)]
#[command(name = "backfill")]
#[command(about = "Backfill historical token data for specific contracts")]
pub struct BackfillArgs {
    /// ERC20 contracts to backfill (format: address:start_block)
    ///
    /// Example: --erc20 0x049D...DC7:100000
    #[arg(long, value_delimiter = ',')]
    pub erc20: Vec<String>,

    /// ERC721 contracts to backfill (format: address:start_block)
    ///
    /// Example: --erc721 0x...nft:50000
    #[arg(long, value_delimiter = ',')]
    pub erc721: Vec<String>,

    /// ERC1155 contracts to backfill (format: address:start_block)
    ///
    /// Example: --erc1155 0x...game:75000
    #[arg(long, value_delimiter = ',')]
    pub erc1155: Vec<String>,

    /// End block number (defaults to current chain head)
    #[arg(long)]
    pub to_block: Option<u64>,

    /// Database path (must match the main torii-tokens database)
    #[arg(long, default_value = "./tokens-data.db")]
    pub db_path: String,

    /// Starknet RPC URL
    #[arg(
        long,
        env = "STARKNET_RPC_URL",
        default_value = "https://api.cartridge.gg/x/starknet/mainnet"
    )]
    pub rpc_url: String,

    /// Events per RPC request (max 1024 for most providers)
    #[arg(long, default_value = "1000")]
    pub chunk_size: u64,
}

impl BackfillArgs {
    /// Parse a contract:start_block string into (address, start_block)
    fn parse_contract_spec(spec: &str) -> Result<(Felt, u64)> {
        let parts: Vec<&str> = spec.split(':').collect();
        if parts.len() != 2 {
            anyhow::bail!(
                "Invalid contract specification '{}'. Expected format: address:start_block",
                spec
            );
        }

        let address =
            Felt::from_hex(parts[0]).with_context(|| format!("Invalid address: {}", parts[0]))?;
        let start_block: u64 = parts[1]
            .parse()
            .with_context(|| format!("Invalid start block: {}", parts[1]))?;

        Ok((address, start_block))
    }

    /// Check if any contracts are configured for backfill
    pub fn has_contracts(&self) -> bool {
        !self.erc20.is_empty() || !self.erc721.is_empty() || !self.erc1155.is_empty()
    }

    /// Build contract configurations from CLI arguments
    pub fn build_configs(&self, to_block: u64) -> Result<Vec<ContractBackfillConfig>> {
        let mut configs = Vec::new();

        for spec in &self.erc20 {
            let (address, from_block) = Self::parse_contract_spec(spec)?;
            configs.push(ContractBackfillConfig {
                address,
                token_type: TokenType::Erc20,
                from_block,
                to_block,
            });
        }

        for spec in &self.erc721 {
            let (address, from_block) = Self::parse_contract_spec(spec)?;
            configs.push(ContractBackfillConfig {
                address,
                token_type: TokenType::Erc721,
                from_block,
                to_block,
            });
        }

        for spec in &self.erc1155 {
            let (address, from_block) = Self::parse_contract_spec(spec)?;
            configs.push(ContractBackfillConfig {
                address,
                token_type: TokenType::Erc1155,
                from_block,
                to_block,
            });
        }

        Ok(configs)
    }
}

/// Run the backfill process
pub async fn run_backfill(args: BackfillArgs) -> Result<()> {
    // Validate configuration
    if !args.has_contracts() {
        tracing::warn!("No contracts specified for backfill.");
        tracing::warn!("Usage: torii-tokens backfill --erc20 0x...address:start_block");
        return Ok(());
    }

    // Create Starknet provider
    let provider = Arc::new(starknet::providers::jsonrpc::JsonRpcClient::new(
        starknet::providers::jsonrpc::HttpTransport::new(
            url::Url::parse(&args.rpc_url).context("Invalid RPC URL")?,
        ),
    ));

    // Get chain head if to_block not specified
    let to_block = match args.to_block {
        Some(block) => block,
        None => {
            use starknet::providers::Provider;
            let head = provider.block_number().await?;
            tracing::info!("Using chain head as to_block: {}", head);
            head
        }
    };

    // Build contract configurations
    let configs = args.build_configs(to_block)?;

    tracing::info!(
        "Starting backfill for {} contract(s) to block {}",
        configs.len(),
        to_block
    );

    for config in &configs {
        tracing::info!(
            "  {} {:#x} from block {}",
            config.token_type,
            config.address,
            config.from_block
        );
    }

    // Determine which token types we're backfilling
    let has_erc20 = configs.iter().any(|c| c.token_type == TokenType::Erc20);
    let has_erc721 = configs.iter().any(|c| c.token_type == TokenType::Erc721);
    let has_erc1155 = configs.iter().any(|c| c.token_type == TokenType::Erc1155);

    // Initialize storages for enabled token types
    let erc20_storage = if has_erc20 {
        let db_path = format!("{}-erc20.db", args.db_path.trim_end_matches(".db"));
        tracing::info!("Opening ERC20 database: {}", db_path);
        Some(Arc::new(Erc20Storage::new(&db_path)?))
    } else {
        None
    };

    let erc721_storage = if has_erc721 {
        let db_path = format!("{}-erc721.db", args.db_path.trim_end_matches(".db"));
        tracing::info!("Opening ERC721 database: {}", db_path);
        Some(Arc::new(Erc721Storage::new(&db_path)?))
    } else {
        None
    };

    let erc1155_storage = if has_erc1155 {
        let db_path = format!("{}-erc1155.db", args.db_path.trim_end_matches(".db"));
        tracing::info!("Opening ERC1155 database: {}", db_path);
        Some(Arc::new(Erc1155Storage::new(&db_path)?))
    } else {
        None
    };

    // Create backfill engine
    // Clone configs for progress callback (to avoid borrow issues)
    let configs_for_progress: std::collections::HashMap<Felt, u64> = configs
        .iter()
        .map(|c| (c.address, c.from_block))
        .collect();

    let backfill = EventBackfill::new(provider.clone(), configs).with_chunk_size(args.chunk_size);

    // Run backfill with progress reporting
    let (result, stats) = backfill
        .run(|progress: BackfillProgress| {
            let from_block = configs_for_progress
                .get(&progress.contract)
                .copied()
                .unwrap_or(0);

            let pct = if progress.to_block > from_block {
                ((progress.current_block.saturating_sub(from_block)) as f64
                    / (progress.to_block.saturating_sub(from_block)) as f64
                    * 100.0) as u32
            } else {
                100
            };
            tracing::info!(
                "{} {:#x}: block {}/{} ({}%) - {} events",
                progress.token_type,
                progress.contract,
                progress.current_block,
                progress.to_block,
                pct,
                progress.events_fetched
            );
        })
        .await?;

    tracing::info!(
        "Backfill complete: {} total events fetched",
        stats.total_events
    );

    // Process ERC20 events
    if !result.erc20_events.is_empty() {
        if let Some(storage) = &erc20_storage {
            tracing::info!("Processing {} ERC20 events...", result.erc20_events.len());
            process_erc20_events(&result.erc20_events, storage).await?;
        }
    }

    // Process ERC721 events
    if !result.erc721_events.is_empty() {
        if let Some(storage) = &erc721_storage {
            tracing::info!("Processing {} ERC721 events...", result.erc721_events.len());
            process_erc721_events(&result.erc721_events, storage).await?;
        }
    }

    // Process ERC1155 events
    if !result.erc1155_events.is_empty() {
        if let Some(storage) = &erc1155_storage {
            tracing::info!(
                "Processing {} ERC1155 events...",
                result.erc1155_events.len()
            );
            process_erc1155_events(&result.erc1155_events, storage).await?;
        }
    }

    // Print summary
    tracing::info!("=== Backfill Summary ===");
    tracing::info!("Total events: {}", stats.total_events);
    for (contract, count) in &stats.events_by_contract {
        tracing::info!("  {:#x}: {} events", contract, count);
    }

    Ok(())
}

/// Process ERC20 events through the decoder and storage
async fn process_erc20_events(
    events: &[starknet::core::types::EmittedEvent],
    storage: &Arc<Erc20Storage>,
) -> Result<()> {
    use torii_erc20::{Approval as DecodedApproval, Transfer as DecodedTransfer};
    use torii_erc20::{ApprovalData, TransferData};

    let decoder = Erc20Decoder::new();
    let mut transfers: Vec<TransferData> = Vec::new();
    let mut approvals: Vec<ApprovalData> = Vec::new();

    for event in events {
        let envelopes = decoder.decode_event(event).await?;

        for envelope in envelopes {
            if envelope.type_id == TypeId::new("erc20.transfer") {
                if let Some(transfer) = envelope.body.as_any().downcast_ref::<DecodedTransfer>() {
                    transfers.push(TransferData {
                        id: None,
                        token: transfer.token,
                        from: transfer.from,
                        to: transfer.to,
                        amount: transfer.amount,
                        block_number: transfer.block_number,
                        tx_hash: transfer.transaction_hash,
                        timestamp: None, // Not available from events
                    });
                }
            } else if envelope.type_id == TypeId::new("erc20.approval") {
                if let Some(approval) = envelope.body.as_any().downcast_ref::<DecodedApproval>() {
                    approvals.push(ApprovalData {
                        id: None,
                        token: approval.token,
                        owner: approval.owner,
                        spender: approval.spender,
                        amount: approval.amount,
                        block_number: approval.block_number,
                        tx_hash: approval.transaction_hash,
                        timestamp: None,
                    });
                }
            }
        }
    }

    // Batch insert
    if !transfers.is_empty() {
        let count = storage.insert_transfers_batch(&transfers)?;
        tracing::info!("Inserted {} ERC20 transfers", count);
    }

    if !approvals.is_empty() {
        let count = storage.insert_approvals_batch(&approvals)?;
        tracing::info!("Inserted {} ERC20 approvals", count);
    }

    Ok(())
}

/// Process ERC721 events through the decoder and storage
async fn process_erc721_events(
    events: &[starknet::core::types::EmittedEvent],
    storage: &Arc<Erc721Storage>,
) -> Result<()> {
    use torii_erc721::storage::{NftTransferData, OperatorApprovalData};
    use torii_erc721::{NftApproval, NftTransfer, OperatorApproval};

    let decoder = Erc721Decoder::new();
    let mut transfers: Vec<NftTransferData> = Vec::new();
    let mut operator_approvals: Vec<OperatorApprovalData> = Vec::new();

    for event in events {
        let envelopes = decoder.decode_event(event).await?;

        for envelope in envelopes {
            if envelope.type_id == TypeId::new("erc721.transfer") {
                if let Some(transfer) = envelope.body.as_any().downcast_ref::<NftTransfer>() {
                    transfers.push(NftTransferData {
                        id: None,
                        token: transfer.token,
                        token_id: transfer.token_id,
                        from: transfer.from,
                        to: transfer.to,
                        block_number: transfer.block_number,
                        tx_hash: transfer.transaction_hash,
                        timestamp: None,
                    });
                }
            } else if envelope.type_id == TypeId::new("erc721.approval") {
                // Single token approval - we track but don't store separately in backfill
                // The sink handles this via ownership tracking
                if let Some(_approval) = envelope.body.as_any().downcast_ref::<NftApproval>() {
                    // Skip single approvals for now - focus on transfers and operator approvals
                    tracing::trace!("Skipping ERC721 single approval in backfill");
                }
            } else if envelope.type_id == TypeId::new("erc721.approval_for_all") {
                if let Some(approval) = envelope.body.as_any().downcast_ref::<OperatorApproval>() {
                    operator_approvals.push(OperatorApprovalData {
                        id: None,
                        token: approval.token,
                        owner: approval.owner,
                        operator: approval.operator,
                        approved: approval.approved,
                        block_number: approval.block_number,
                        tx_hash: approval.transaction_hash,
                        timestamp: None,
                    });
                }
            }
        }
    }

    // Batch insert
    if !transfers.is_empty() {
        let count = storage.insert_transfers_batch(&transfers)?;
        tracing::info!("Inserted {} ERC721 transfers", count);
    }

    if !operator_approvals.is_empty() {
        let count = storage.insert_operator_approvals_batch(&operator_approvals)?;
        tracing::info!("Inserted {} ERC721 operator approvals", count);
    }

    Ok(())
}

/// Process ERC1155 events through the decoder and storage
async fn process_erc1155_events(
    events: &[starknet::core::types::EmittedEvent],
    storage: &Arc<Erc1155Storage>,
) -> Result<()> {
    use torii_erc1155::storage::{OperatorApprovalData, TokenTransferData};
    use torii_erc1155::{OperatorApproval, TransferBatch, TransferSingle};

    let decoder = Erc1155Decoder::new();
    let mut transfers: Vec<TokenTransferData> = Vec::new();
    let mut operator_approvals: Vec<OperatorApprovalData> = Vec::new();

    for event in events {
        let envelopes = decoder.decode_event(event).await?;

        for envelope in envelopes {
            if envelope.type_id == TypeId::new("erc1155.transfer_single") {
                if let Some(transfer) = envelope.body.as_any().downcast_ref::<TransferSingle>() {
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
                        timestamp: None,
                    });
                }
            } else if envelope.type_id == TypeId::new("erc1155.transfer_batch") {
                // TransferBatch is denormalized by the decoder - each envelope has one transfer
                if let Some(batch) = envelope.body.as_any().downcast_ref::<TransferBatch>() {
                    transfers.push(TokenTransferData {
                        id: None,
                        token: batch.token,
                        operator: batch.operator,
                        from: batch.from,
                        to: batch.to,
                        token_id: batch.id,
                        amount: batch.value,
                        is_batch: true,
                        batch_index: batch.batch_index,
                        block_number: batch.block_number,
                        tx_hash: batch.transaction_hash,
                        timestamp: None,
                    });
                }
            } else if envelope.type_id == TypeId::new("erc1155.approval_for_all") {
                if let Some(approval) = envelope.body.as_any().downcast_ref::<OperatorApproval>() {
                    operator_approvals.push(OperatorApprovalData {
                        id: None,
                        token: approval.token,
                        owner: approval.owner,
                        operator: approval.operator,
                        approved: approval.approved,
                        block_number: approval.block_number,
                        tx_hash: approval.transaction_hash,
                        timestamp: None,
                    });
                }
            }
        }
    }

    // Batch insert
    if !transfers.is_empty() {
        let count = storage.insert_transfers_batch(&transfers)?;
        tracing::info!("Inserted {} ERC1155 transfers", count);
    }

    if !operator_approvals.is_empty() {
        let count = storage.insert_operator_approvals_batch(&operator_approvals)?;
        tracing::info!("Inserted {} ERC1155 operator approvals", count);
    }

    Ok(())
}
