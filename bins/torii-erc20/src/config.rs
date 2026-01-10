//! Configuration for the ERC20 indexer

use clap::Parser;
use starknet::core::types::Felt;

/// ERC20 Token Indexer for Starknet
///
/// Indexes ERC20 token transfers and maintains real-time balances.
#[derive(Parser, Debug)]
#[command(name = "torii-erc20")]
#[command(about = "Index ERC20 token transfers on Starknet", long_about = None)]
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

    /// Database path for storing transfers and balances
    #[arg(long, default_value = "./erc20-data.db")]
    pub db_path: String,

    /// Disable auto-discovery of ERC20 contracts
    ///
    /// When enabled, only explicitly configured contracts will be indexed.
    /// This is useful for production deployments where you want strict control
    /// over which contracts are indexed.
    #[arg(long)]
    pub no_auto_discovery: bool,

    /// Explicitly indexed ERC20 contracts (comma-separated hex addresses)
    ///
    /// Example: --contracts 0x123...,0x456...
    #[arg(long, value_delimiter = ',')]
    pub contracts: Vec<String>,

    /// Port for the HTTP API
    #[arg(long, default_value = "3000")]
    pub port: u16,
}

impl Config {
    /// Get the identification mode based on auto-discovery setting
    pub fn identification_mode(&self) -> torii::etl::extractor::ContractIdentificationMode {
        use torii::etl::extractor::ContractIdentificationMode;

        if self.no_auto_discovery {
            // Strict mode: no auto-discovery, only explicit mappings work
            // We set an empty mode (no flags), so rules are registered but won't trigger
            ContractIdentificationMode::empty()
        } else {
            // Full auto-discovery: use both SRC-5 and ABI heuristics
            ContractIdentificationMode::SRC5 | ContractIdentificationMode::ABI_HEURISTICS
        }
    }

    /// Parse explicit contract addresses
    pub fn parse_contracts(&self) -> anyhow::Result<Vec<Felt>> {
        self.contracts
            .iter()
            .map(|addr| {
                Felt::from_hex(addr)
                    .map_err(|e| anyhow::anyhow!("Invalid contract address '{}': {}", addr, e))
            })
            .collect()
    }

    /// Get well-known ERC20 contracts (ETH, STRK) based on network
    pub fn well_known_contracts(&self) -> Vec<(Felt, &'static str)> {
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
            (
                Felt::from_hex_unchecked(
                    "0x042DD777885AD2C116be96d4D634abC90A26A790ffB5871E037Dd5Ae7d2Ec86B",
                ),
                "SURVIVOR",
            ),
        ]
    }
}
