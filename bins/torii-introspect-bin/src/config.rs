use anyhow::{bail, Result};
use clap::Parser;
use starknet::core::types::Felt;
use std::path::Path;

/// Dojo introspect indexer backed by a SQL sink.
///
/// This binary targets explicitly configured Dojo contracts and persists the
/// decoded introspect messages into PostgreSQL or SQLite tables.
#[derive(Parser, Debug)]
#[command(name = "torii-introspect")]
#[command(
    about = "Index Dojo introspect events into PostgreSQL or SQLite",
    long_about = None
)]
pub struct Config {
    /// Starknet RPC URL.
    #[arg(
        long,
        env = "STARKNET_RPC_URL",
        default_value = "https://api.cartridge.gg/x/starknet/mainnet"
    )]
    pub rpc_url: String,

    /// Dojo contracts to index (comma-separated hex addresses).
    #[arg(long, value_delimiter = ',', required = true)]
    pub contracts: Vec<String>,

    /// Starting block number for fresh extraction, or when `--ignore-saved-state` is set.
    #[arg(long, default_value = "0")]
    pub from_block: u64,

    /// Ending block number (None = follow chain head).
    #[arg(long)]
    pub to_block: Option<u64>,

    /// Directory where the default sqlite database is stored.
    #[arg(long, default_value = "./torii-data")]
    pub db_dir: String,

    /// Optional SQL database URL used by the introspect sink.
    ///
    /// Supports PostgreSQL (`postgres://...`) and SQLite (`sqlite:...` or file path).
    /// When omitted, uses `<db-dir>/introspect.db`.
    #[arg(long, env = "STORAGE_DATABASE_URL")]
    pub storage_database_url: Option<String>,

    /// Port for the Torii gRPC/HTTP server.
    #[arg(long, default_value = "3000")]
    pub port: u16,

    /// Enable Prometheus metrics collection.
    #[arg(long)]
    pub observability: bool,

    /// Events per `starknet_getEvents` request.
    #[arg(long, default_value = "1000")]
    pub event_chunk_size: u64,

    /// Block range to query per iteration.
    #[arg(long, default_value = "10000")]
    pub event_block_batch_size: u64,

    /// Maximum database connections for the sink.
    #[arg(long)]
    pub max_db_connections: Option<u32>,

    /// Ignore persisted extractor state and start extraction from `from_block`.
    #[arg(long)]
    pub ignore_saved_state: bool,

    /// Allow falling back to latest schema state when historical bootstrap is unavailable.
    #[arg(long)]
    pub allow_unsafe_latest_schema_bootstrap: bool,
}

impl Config {
    pub fn contract_addresses(&self) -> Result<Vec<Felt>> {
        self.contracts
            .iter()
            .map(|addr| {
                Felt::from_hex(addr).map_err(|e| anyhow::anyhow!("Invalid contract {addr}: {e}"))
            })
            .collect()
    }

    pub fn engine_database_url(&self) -> String {
        self.resolved_storage_database_url()
    }

    pub fn resolved_storage_database_url(&self) -> String {
        self.storage_database_url.clone().unwrap_or_else(|| {
            Path::new(&self.db_dir)
                .join("introspect.db")
                .to_string_lossy()
                .to_string()
        })
    }

    pub fn storage_database_url(&self) -> Result<String> {
        let storage_database_url = self.resolved_storage_database_url();
        if storage_database_url.starts_with("postgres://")
            || storage_database_url.starts_with("postgresql://")
            || storage_database_url.starts_with("sqlite:")
            || storage_database_url == ":memory:"
            || !storage_database_url.contains("://")
        {
            Ok(storage_database_url)
        } else {
            bail!("--storage-database-url must be a PostgreSQL or SQLite URL/path")
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;

    #[test]
    fn observability_defaults_to_disabled() {
        let cfg = Config::parse_from(["torii-introspect", "--contracts", "0x1"]);
        assert!(!cfg.observability);
    }

    #[test]
    fn observability_flag_enables_metrics() {
        let cfg = Config::parse_from(["torii-introspect", "--contracts", "0x1", "--observability"]);
        assert!(cfg.observability);
    }

    #[test]
    fn storage_database_url_accepts_sqlite() {
        let cfg = Config::parse_from([
            "torii-introspect",
            "--contracts",
            "0x1",
            "--storage-database-url",
            "sqlite://torii.db",
        ]);

        assert_eq!(cfg.storage_database_url().unwrap(), "sqlite://torii.db");
    }

    #[test]
    fn storage_database_url_defaults_to_sqlite_file_in_db_dir() {
        let cfg = Config::parse_from([
            "torii-introspect",
            "--contracts",
            "0x1",
            "--db-dir",
            "./custom-data",
        ]);

        assert_eq!(
            cfg.storage_database_url().unwrap(),
            "./custom-data/introspect.db"
        );
    }

    #[test]
    fn contract_addresses_parse_from_hex() {
        let cfg = Config::parse_from(["torii-introspect", "--contracts", "0x1,0x2"]);

        let contracts = cfg.contract_addresses().unwrap();
        assert_eq!(contracts.len(), 2);
        assert_eq!(contracts[0], Felt::ONE);
        assert_eq!(contracts[1], Felt::TWO);
    }
}
