//! Configuration for the unified token indexer

use clap::Parser;
use starknet::core::types::Felt;

/// Unified Token Indexer for Starknet
///
/// Indexes ERC20, ERC721, and ERC1155 token transfers and events.
#[derive(Parser, Debug)]
#[command(name = "torii-tokens")]
#[command(about = "Index ERC20, ERC721, and ERC1155 tokens on Starknet", long_about = None)]
pub struct Config {
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

    /// Database path for storing token data
    #[arg(long, default_value = "./tokens-data.db")]
    pub db_path: String,

    /// Port for the HTTP/gRPC API
    #[arg(long, default_value = "3000")]
    pub port: u16,

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

    /// Number of blocks to fetch per batch
    ///
    /// Higher values improve throughput but use more memory.
    /// Lower values reduce memory usage but may slow down indexing.
    #[arg(long, default_value = "1000")]
    pub batch_size: u64,
}

impl Config {
    /// Parse a hex address string to Felt
    pub fn parse_address(addr: &str) -> anyhow::Result<Felt> {
        Felt::from_hex(addr).map_err(|e| anyhow::anyhow!("Invalid address {}: {}", addr, e))
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
