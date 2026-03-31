//! Configuration for the unified token indexer

use anyhow::Result;
use clap::{Parser, ValueEnum};
use starknet::core::types::Felt;

/// Extraction mode for the token indexer.
///
/// - **BlockRange**: Fetches ALL events from each block using a single global cursor.
///   Best for full chain indexing and auto-discovery of contracts.
///
/// - **Event**: Uses `starknet_getEvents` with per-contract cursors.
///   Best for indexing specific contracts - easy to add new ones without re-indexing.
///
/// - **GlobalEvent**: Uses `starknet_getEvents` with a single global cursor.
///   Best for event-mode auto-discovery across all contracts.
///
#[derive(Clone, Debug, Default, ValueEnum, PartialEq, Eq)]
pub enum ExtractionMode {
    /// Fetch all events from blocks (single global cursor)
    #[default]
    BlockRange,
    /// Fetch events per contract (per-contract cursors)
    Event,
    /// Fetch all events with one global cursor
    GlobalEvent,
}

/// Metadata fetching behavior.
#[derive(Clone, Debug, ValueEnum, PartialEq, Eq)]
pub enum MetadataMode {
    /// Fetch metadata during indexing.
    Inline,
    /// Skip metadata fetching during indexing (faster backfills).
    Deferred,
}

/// Unified Token Indexer for Starknet
///
/// Indexes ERC20, ERC721, and ERC1155 token transfers and events.
///
/// # Extraction Modes
///
/// - **block-range** (default): Fetches ALL events from each block.
///   Single global cursor. Best for full chain indexing.
///
/// - **event**: Uses `starknet_getEvents` with per-contract cursors.
///   Easy to add new contracts without re-indexing existing ones.
///
/// - **global-event**: Uses `starknet_getEvents` with one global cursor.
///   Supports runtime auto-discovery without preconfigured contracts.
///
/// # Examples
///
/// ```bash
/// # Block range mode (default) - index ETH and STRK
/// torii-tokens --include-well-known --from-block 0
///
/// # Enable observability (Prometheus metrics)
/// torii-tokens --observability --include-well-known --from-block 0
///
/// # Event mode - per-contract cursors
/// torii-tokens --mode event --erc20 0x...ETH,0x...STRK --from-block 0
///
/// # Add a new contract in event mode (just restart with updated list)
/// torii-tokens --mode event --erc20 0x...ETH,0x...STRK,0x...USDC --from-block 0
/// # USDC starts from block 0, ETH and STRK resume from their cursors
///
/// ```
#[derive(Parser, Debug)]
#[command(name = "torii-tokens")]
#[command(about = "Index ERC20, ERC721, and ERC1155 tokens on Starknet", long_about = None)]
pub struct Config {
    /// Extraction mode
    ///
    /// - block-range: Fetch all events from blocks (single global cursor)
    /// - event: Fetch events per contract (per-contract cursors, easy to add new contracts)
    /// - global-event: Fetch all events via getEvents (single global cursor)
    #[arg(long, value_enum, default_value = "block-range")]
    pub mode: ExtractionMode,

    /// Starknet RPC URL
    #[arg(
        long,
        env = "STARKNET_RPC_URL",
        default_value = "https://api.cartridge.gg/x/starknet/mainnet"
    )]
    pub rpc_url: String,

    /// Starting block number
    #[arg(long, default_value = "0")]
    pub from_block: u64,

    /// Ending block number (None = follow chain head)
    #[arg(long)]
    pub to_block: Option<u64>,

    /// Directory where all databases will be stored
    ///
    /// Creates: engine.db, erc20.db, erc721.db, erc1155.db
    #[arg(long, default_value = "./torii-data")]
    pub db_dir: String,

    /// Optional engine database URL/path.
    ///
    /// Supports PostgreSQL (`postgres://...`) and SQLite (`sqlite:...` or file path).
    /// When omitted, engine state uses `<db-dir>/engine.db`.
    #[arg(long, env = "DATABASE_URL")]
    pub database_url: Option<String>,

    /// Optional token storage database URL/path.
    ///
    /// Supports PostgreSQL (`postgres://...`) and SQLite (`sqlite:...` or file path).
    /// When omitted, token storages use `--database-url` if set, otherwise `<db-dir>/*.db`.
    #[arg(long, env = "STORAGE_DATABASE_URL")]
    pub storage_database_url: Option<String>,

    /// Port for the HTTP/gRPC API
    #[arg(long, default_value = "3000")]
    pub port: u16,

    /// Enable observability features (Prometheus metrics endpoint and metric collection)
    ///
    /// If not set, observability is disabled.
    #[arg(long)]
    pub observability: bool,

    /// ERC20 contracts to index (comma-separated hex addresses)
    ///
    /// Example: --erc20 0x049D36570D4e46f48e99674bd3fcc84644DdD6b96F7C741B1562B82f9e004dC7,0x04718f5a0Fc34cC1AF16A1cdee98fFB20C31f5cD61D6Ab07201858f4287c938D
    #[arg(long, value_delimiter = ',')]
    pub erc20: Vec<String>,

    /// ERC721 contracts to index (comma-separated hex addresses)
    ///
    /// Example: --erc721 0x...nft_contract
    #[arg(long, value_delimiter = ',')]
    pub erc721: Vec<String>,

    /// ERC1155 contracts to index (comma-separated hex addresses)
    ///
    /// Example: --erc1155 0x...game_items
    #[arg(long, value_delimiter = ',')]
    pub erc1155: Vec<String>,

    /// Include well-known ERC20 contracts (ETH, STRK)
    #[arg(long)]
    pub include_well_known: bool,

    /// Number of blocks to fetch per batch (block-range mode)
    ///
    /// Higher values improve throughput but use more memory.
    /// Lower values reduce memory usage but may slow down indexing.
    #[arg(long, default_value = "50")]
    pub batch_size: u64,

    /// Events per RPC request (event mode, max 1024 for most providers)
    #[arg(long, default_value = "1000")]
    pub event_chunk_size: u64,

    /// Block range to query per iteration in event mode
    #[arg(long, default_value = "10000")]
    pub event_block_batch_size: u64,

    /// Number of blocks to scan for automatic event-mode bootstrap discovery.
    #[arg(long, default_value = "20000")]
    pub event_bootstrap_blocks: u64,

    /// Number of extracted batches to prefetch ahead of decode/store.
    #[arg(long, default_value = "2")]
    pub max_prefetch_batches: usize,

    /// Delay between ETL idle/retry cycles in seconds.
    #[arg(long, default_value = "3")]
    pub cycle_interval: u64,

    /// Maximum chunked RPC requests to run concurrently (`0` = auto).
    #[arg(long, default_value = "0")]
    pub rpc_parallelism: usize,

    /// Concurrent workers for async token metadata fetching.
    #[arg(long, default_value = "8")]
    pub metadata_parallelism: usize,

    /// Queue capacity for async metadata jobs.
    #[arg(long, default_value = "2048")]
    pub metadata_queue_capacity: usize,

    /// Max retries for metadata fetch/store with capped backoff.
    #[arg(long, default_value = "5")]
    pub metadata_max_retries: u8,

    /// Metadata fetching mode.
    ///
    /// If omitted: defaults to `inline`.
    #[arg(long, value_enum)]
    pub metadata_mode: Option<MetadataMode>,

    /// Run metadata-only flow and exit.
    ///
    /// Note: currently implemented as a guarded no-op placeholder.
    #[arg(long)]
    pub metadata_backfill_only: bool,
}

impl Config {
    /// Parse a hex address string to Felt
    pub fn parse_address(addr: &str) -> Result<Felt> {
        Felt::from_hex(addr).map_err(|e| anyhow::anyhow!("Invalid address {addr}: {e}"))
    }

    /// Get well-known ERC20 contracts (ETH, STRK)
    pub fn well_known_erc20_contracts() -> Vec<(Felt, &'static str)> {
        vec![
            (
                Felt::from_hex_unchecked(
                    "0x049D36570D4e46f48e99674bd3fcc84644DdD6b96F7C741B1562B82f9e004dC7",
                ),
                "ETH",
            ),
            (
                Felt::from_hex_unchecked(
                    "0x04718f5a0Fc34cC1AF16A1cdee98fFB20C31f5cD61D6Ab07201858f4287c938D",
                ),
                "STRK",
            ),
        ]
    }

    /// Check if any token types are configured
    pub fn has_tokens(&self) -> bool {
        !self.erc20.is_empty()
            || !self.erc721.is_empty()
            || !self.erc1155.is_empty()
            || self.include_well_known
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;

    #[test]
    fn observability_defaults_to_disabled() {
        let cfg = Config::parse_from(["torii-tokens"]);
        assert!(!cfg.observability);
    }

    #[test]
    fn observability_flag_enables_features() {
        let cfg = Config::parse_from(["torii-tokens", "--observability"]);
        assert!(cfg.observability);
    }

    #[test]
    fn concurrency_flags_parse() {
        let cfg = Config::parse_from([
            "torii-tokens",
            "--max-prefetch-batches",
            "4",
            "--rpc-parallelism",
            "6",
            "--metadata-parallelism",
            "12",
            "--metadata-queue-capacity",
            "4096",
            "--metadata-max-retries",
            "5",
        ]);
        assert_eq!(cfg.max_prefetch_batches, 4);
        assert_eq!(cfg.rpc_parallelism, 6);
        assert_eq!(cfg.metadata_parallelism, 12);
        assert_eq!(cfg.metadata_queue_capacity, 4096);
        assert_eq!(cfg.metadata_max_retries, 5);
    }

    #[test]
    fn supports_global_event_mode() {
        let cfg = Config::parse_from(["torii-tokens", "--mode", "global-event"]);
        assert_eq!(cfg.mode, ExtractionMode::GlobalEvent);
    }
}
