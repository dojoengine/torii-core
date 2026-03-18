use anyhow::{bail, Result};
use clap::Parser;
use starknet::core::types::Felt;
use std::path::Path;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StorageBackend {
    Postgres,
    Sqlite,
}

/// Dojo introspect indexer backed by PostgreSQL or SQLite.
///
/// This binary targets explicitly configured Dojo contracts and persists the
/// decoded introspect messages into SQL tables.
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

    /// Directory where local SQLite databases will be stored.
    ///
    /// Used only when `--storage-database-url` is omitted.
    #[arg(long, default_value = "./torii-data")]
    pub db_dir: String,

    /// Optional engine database URL/path.
    ///
    /// Supports PostgreSQL (`postgres://...`) and SQLite (`sqlite:...` or file path).
    /// When omitted, the engine uses `--storage-database-url` in PostgreSQL mode
    /// or `<db-dir>/engine.db` in SQLite mode.
    #[arg(long, env = "DATABASE_URL")]
    pub database_url: Option<String>,

    /// Optional PostgreSQL storage database URL.
    ///
    /// When omitted, the binary uses SQLite storage at `<db-dir>/introspect.db`.
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

    /// Maximum SQL connections for the storage backend.
    #[arg(long)]
    pub max_db_connections: Option<u32>,

    /// Ignore persisted extractor state and force extraction from `from_block`.
    #[arg(long)]
    pub ignore_saved_state: bool,
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

    pub fn storage_backend(&self) -> StorageBackend {
        if self.storage_database_url.is_some() {
            StorageBackend::Postgres
        } else {
            StorageBackend::Sqlite
        }
    }

    pub fn engine_database_url(&self, db_dir: &Path) -> String {
        self.database_url
            .clone()
            .unwrap_or_else(|| match &self.storage_database_url {
                Some(url) => url.clone(),
                None => db_dir.join("engine.db").to_string_lossy().to_string(),
            })
    }

    pub fn storage_database_url(&self, db_dir: &Path) -> Result<String> {
        match &self.storage_database_url {
            Some(url) if url.starts_with("postgres://") || url.starts_with("postgresql://") => {
                Ok(url.clone())
            }
            Some(_) => bail!("--storage-database-url must be a PostgreSQL URL"),
            None => Ok(db_dir.join("introspect.db").to_string_lossy().to_string()),
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

        assert!(cfg.storage_database_url(Path::new(".")).is_err());
    }

    #[test]
    fn sqlite_is_default_when_storage_database_url_is_omitted() {
        let cfg = Config::parse_from(["torii-introspect", "--contracts", "0x1"]);

        assert_eq!(cfg.storage_backend(), StorageBackend::Sqlite);
        assert!(cfg
            .storage_database_url(Path::new("./torii-data"))
            .unwrap()
            .ends_with("torii-data/introspect.db"));
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
