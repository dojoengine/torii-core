use anyhow::{bail, Result};
use clap::{ArgGroup, Parser};
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
#[command(name = "torii-server")]
#[command(
    about = "Index Dojo introspect events and token transfers into PostgreSQL or SQLite",
    long_about = None
)]
#[command(group(
    ArgGroup::new("targets")
        .args(["contracts", "erc20", "erc721", "erc1155"])
        .multiple(true)
        .required(true)
))]
pub struct Config {
    /// Starknet RPC URL.
    #[arg(
        long,
        env = "STARKNET_RPC_URL",
        default_value = "https://api.cartridge.gg/x/starknet/mainnet"
    )]
    pub rpc_url: String,

    /// Dojo contracts to index (comma-separated hex addresses).
    #[arg(long = "contract", visible_alias = "contracts", value_delimiter = ',')]
    pub contracts: Vec<String>,

    /// ERC20 contracts to index (comma-separated hex addresses).
    #[arg(long, value_delimiter = ',')]
    pub erc20: Vec<String>,

    /// ERC721 contracts to index (comma-separated hex addresses).
    #[arg(long, value_delimiter = ',')]
    pub erc721: Vec<String>,

    /// ERC1155 contracts to index (comma-separated hex addresses).
    #[arg(long, value_delimiter = ',')]
    pub erc1155: Vec<String>,

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
    /// When omitted, the binary uses SQLite storage at `<db-dir>/introspect.db` for
    /// introspect data and `<db-dir>/erc20.db`, `<db-dir>/erc721.db`, `<db-dir>/erc1155.db`
    /// for token data.
    #[arg(long, env = "STORAGE_DATABASE_URL")]
    pub storage_database_url: Option<String>,

    /// Port for the Torii gRPC/HTTP server.
    #[arg(long, default_value = "3000")]
    pub port: u16,

    /// Cartridge-compatible GraphQL API used to fetch controller usernames.
    #[arg(long, default_value = "https://api.cartridge.gg/query")]
    pub controllers_api_url: String,

    /// Enable controller synchronization into the introspect database.
    #[arg(long)]
    pub controllers: bool,

    /// Enable Prometheus metrics collection.
    #[arg(long)]
    pub observability: bool,

    /// Events per `starknet_getEvents` request.
    #[arg(long, visible_alias = "chunk-size", default_value = "1000")]
    pub event_chunk_size: u64,

    /// Block range to query per iteration.
    #[arg(long, visible_alias = "batch-size", default_value = "10000")]
    pub event_block_batch_size: u64,

    /// Number of extracted batches to prefetch ahead of decode/store.
    #[arg(long, default_value = "2")]
    pub max_prefetch_batches: usize,

    /// Maximum chunked RPC requests to run concurrently (`0` = auto).
    #[arg(long, default_value = "0")]
    pub rpc_parallelism: usize,

    /// Maximum SQL connections for the storage backend.
    #[arg(long)]
    pub max_db_connections: Option<u32>,

    /// Ignore persisted extractor state and force extraction from `from_block`.
    #[arg(long)]
    pub ignore_saved_state: bool,

    /// Enable runtime indexing for Dojo `ExternalContractRegistered` events.
    #[arg(long)]
    pub index_external_contracts: bool,

    /// Exact Dojo model names to mirror into append-only `_historical` tables.
    #[arg(long, value_delimiter = ',')]
    pub historical: Vec<String>,
}

impl Config {
    fn parse_addresses(kind: &str, addrs: &[String]) -> Result<Vec<Felt>> {
        addrs
            .iter()
            .map(|addr| {
                Felt::from_hex(addr)
                    .map_err(|e| anyhow::anyhow!("Invalid {kind} contract {addr}: {e}"))
            })
            .collect()
    }

    pub fn contract_addresses(&self) -> Result<Vec<Felt>> {
        Self::parse_addresses("Dojo", &self.contracts)
    }

    pub fn erc20_addresses(&self) -> Result<Vec<Felt>> {
        Self::parse_addresses("ERC20", &self.erc20)
    }

    pub fn erc721_addresses(&self) -> Result<Vec<Felt>> {
        Self::parse_addresses("ERC721", &self.erc721)
    }

    pub fn erc1155_addresses(&self) -> Result<Vec<Felt>> {
        Self::parse_addresses("ERC1155", &self.erc1155)
    }

    pub fn historical_models(&self) -> Vec<String> {
        let mut models = Vec::with_capacity(self.historical.len());
        for model in &self.historical {
            if !model.is_empty() && !models.contains(model) {
                models.push(model.clone());
            }
        }
        models
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
        let cfg = Config::parse_from(["torii-server", "--contract", "0x1"]);
        assert!(!cfg.observability);
    }

    #[test]
    fn observability_flag_enables_metrics() {
        let cfg = Config::parse_from(["torii-server", "--contract", "0x1", "--observability"]);
        assert!(cfg.observability);
    }

    #[test]
    fn controllers_sync_defaults_to_disabled() {
        let cfg = Config::parse_from(["torii-server", "--contract", "0x1"]);
        assert_eq!(cfg.controllers_api_url, "https://api.cartridge.gg/query");
        assert!(!cfg.controllers);
    }

    #[test]
    fn controllers_flag_enables_sync() {
        let cfg = Config::parse_from(["torii-server", "--contract", "0x1", "--controllers"]);
        assert!(cfg.controllers);
    }

    #[test]
    fn storage_database_url_accepts_sqlite() {
        let cfg = Config::parse_from([
            "torii-server",
            "--contract",
            "0x1",
            "--storage-database-url",
            "sqlite://torii.db",
        ]);

        assert!(cfg.storage_database_url(Path::new(".")).is_err());
    }

    #[test]
    fn sqlite_is_default_when_storage_database_url_is_omitted() {
        let cfg = Config::parse_from(["torii-server", "--contract", "0x1"]);

        assert_eq!(cfg.storage_backend(), StorageBackend::Sqlite);
        assert!(cfg
            .storage_database_url(Path::new("./torii-data"))
            .unwrap()
            .ends_with("torii-data/introspect.db"));
    }

    #[test]
    fn contract_addresses_parse_from_hex() {
        let cfg = Config::parse_from(["torii-server", "--contract", "0x1,0x2"]);

        let contracts = cfg.contract_addresses().unwrap();
        assert_eq!(contracts.len(), 2);
        assert_eq!(contracts[0], Felt::ONE);
        assert_eq!(contracts[1], Felt::TWO);
    }

    #[test]
    fn concurrency_flags_parse() {
        let cfg = Config::parse_from([
            "torii-server",
            "--contract",
            "0x1",
            "--chunk-size",
            "777",
            "--batch-size",
            "8888",
            "--max-prefetch-batches",
            "4",
            "--rpc-parallelism",
            "6",
        ]);

        assert_eq!(cfg.event_chunk_size, 777);
        assert_eq!(cfg.event_block_batch_size, 8888);
        assert_eq!(cfg.max_prefetch_batches, 4);
        assert_eq!(cfg.rpc_parallelism, 6);
    }

    #[test]
    fn token_flags_parse() {
        let cfg = Config::parse_from([
            "torii-server",
            "--erc20",
            "0x1,0x2",
            "--erc721",
            "0x3",
            "--erc1155",
            "0x4",
        ]);

        assert_eq!(cfg.erc20_addresses().unwrap(), vec![Felt::ONE, Felt::TWO]);
        assert_eq!(cfg.erc721_addresses().unwrap(), vec![Felt::from(3_u64)]);
        assert_eq!(cfg.erc1155_addresses().unwrap(), vec![Felt::from(4_u64)]);
    }

    #[test]
    fn external_contract_indexing_defaults_to_disabled() {
        let cfg = Config::parse_from(["torii-server", "--contract", "0x1"]);
        assert!(!cfg.index_external_contracts);
    }

    #[test]
    fn external_contract_indexing_flag_enables_runtime_registration() {
        let cfg = Config::parse_from([
            "torii-server",
            "--contract",
            "0x1",
            "--index-external-contracts",
        ]);
        assert!(cfg.index_external_contracts);
    }

    #[test]
    fn historical_models_parse_as_exact_names() {
        let cfg = Config::parse_from([
            "torii-server",
            "--contract",
            "0x1",
            "--historical",
            "NUMS-Game,NUMS-Config,NUMS-Game",
        ]);

        assert_eq!(
            cfg.historical_models(),
            vec!["NUMS-Game".to_string(), "NUMS-Config".to_string()]
        );
    }
}
