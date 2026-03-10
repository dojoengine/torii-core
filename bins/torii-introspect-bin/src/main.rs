//! Torii Introspect - Dojo introspect indexer backed by PostgreSQL or SQLite.

mod config;

use anyhow::{bail, Result};
use clap::Parser;
use config::{Config, StorageBackend};
use sqlx::postgres::PgPoolOptions;
use sqlx::sqlite::SqlitePoolOptions;
use starknet::core::types::Felt;
use std::path::Path;
use std::sync::Arc;
use torii::etl::decoder::DecoderId;
use torii::etl::engine_db::{EngineDb, EngineDbConfig};
use torii::etl::extractor::{
    ContractEventConfig, EventExtractor, EventExtractorConfig, RetryPolicy,
};
use torii_config_common::apply_observability_env;
use torii_dojo::decoder::DojoDecoder;
use torii_dojo::store::postgres::PgStore;
use torii_dojo::store::sqlite::SqliteStore;
use torii_introspect_postgres_sink::processor::IntrospectPgDb;
use torii_introspect_sqlite_sink::processor::IntrospectSqliteDb;
use torii_sqlite::{is_sqlite_memory_path, sqlite_connect_options};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum DbBackend {
    Postgres,
    Sqlite,
}

fn detect_database_backend(database_url: &str) -> DbBackend {
    if database_url.starts_with("postgres://") || database_url.starts_with("postgresql://") {
        DbBackend::Postgres
    } else {
        DbBackend::Sqlite
    }
}

fn sqlite_connect_options(path: &str) -> Result<SqliteConnectOptions> {
    if path == ":memory:" || path == "sqlite::memory:" {
        return SqliteConnectOptions::from_str("sqlite::memory:")
            .map_err(|err| anyhow::anyhow!("Failed to parse sqlite URL: {err}"));
    }

    let options = if path.starts_with("sqlite:") {
        SqliteConnectOptions::from_str(path)
            .map_err(|err| anyhow::anyhow!("Failed to parse sqlite URL {path}: {err}"))?
    } else {
        SqliteConnectOptions::new().filename(path)
    };

    if path.starts_with("sqlite:") && path.contains("mode=") {
        Ok(options)
    } else {
        Ok(options.create_if_missing(true))
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let env_filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"));

    tracing_subscriber::fmt()
        .with_env_filter(env_filter)
        .with_target(true)
        .init();

    let config = Config::parse();
    run_indexer(config).await
}

async fn run_indexer(config: Config) -> Result<()> {
    tracing::info!("Starting Torii Introspect Indexer");

    apply_observability_env(config.observability);

    let db_dir = Path::new(&config.db_dir);
    let storage_database_url = config.storage_database_url(db_dir)?;
    let engine_database_url = config.engine_database_url(db_dir);
    let contracts = config.contract_addresses()?;
    let backend = config.storage_backend();

    tracing::info!("RPC URL: {}", config.rpc_url);
    tracing::info!("Contracts: {}", contracts.len());
    tracing::info!("From block: {}", config.from_block);
    if let Some(to_block) = config.to_block {
        tracing::info!("To block: {}", to_block);
    } else {
        tracing::info!("To block: following chain head");
    }
    tracing::info!("Storage backend: {:?}", backend);
    tracing::info!("Engine database URL: {}", engine_database_url);
    tracing::info!("Storage database URL: {}", storage_database_url);
    tracing::info!("Database backend: {:?}", backend);
    tracing::info!(
        "Saved state handling: {}",
        if config.ignore_saved_state {
            "ignoring persisted extractor state"
        } else {
            "failing closed if persisted extractor state exists"
        }
    );
    tracing::info!(
        "Observability: {}",
        if config.observability {
            "enabled"
        } else {
            "disabled"
        }
    );

    let provider = starknet::providers::jsonrpc::JsonRpcClient::new(
        starknet::providers::jsonrpc::HttpTransport::new(
            url::Url::parse(&config.rpc_url).expect("Invalid RPC URL"),
        ),
    );
    let extractor_provider = Arc::new(provider.clone());
    let extractor = Box::new(EventExtractor::new(
        extractor_provider.clone(),
        EventExtractorConfig {
            contracts: contracts
                .iter()
                .copied()
                .map(|address| ContractEventConfig {
                    address,
                    from_block: config.from_block,
                    to_block: config.to_block.unwrap_or(u64::MAX),
                })
                .collect(),
            chunk_size: config.event_chunk_size,
            block_batch_size: config.event_block_batch_size,
            retry_policy: RetryPolicy::default(),
            ignore_saved_state: config.ignore_saved_state,
            rpc_parallelism: 0,
        },
    ));

    if matches!(backend, StorageBackend::Sqlite) {
        tokio::fs::create_dir_all(db_dir).await?;
    }

    let engine_db = EngineDb::new(EngineDbConfig {
        path: engine_database_url.clone(),
    })
    .await?;
    ensure_no_saved_state(&engine_db, &contracts, config.ignore_saved_state).await?;

    match backend {
        StorageBackend::Postgres => {
            run_with_postgres(
                &config,
                &storage_database_url,
                engine_database_url,
                contracts,
                provider,
                extractor,
            )
            .await?;
        }
        StorageBackend::Sqlite => {
            run_with_sqlite(
                &config,
                &storage_database_url,
                engine_database_url,
                contracts,
                provider,
                extractor,
            )
            .await?;
        }
    }

    tracing::info!("Torii shutdown complete");
    Ok(())
}

async fn run_with_postgres(
    config: &Config,
    storage_database_url: &str,
    engine_database_url: String,
    contracts: Vec<Felt>,
    provider: starknet::providers::jsonrpc::JsonRpcClient<
        starknet::providers::jsonrpc::HttpTransport,
    >,
    extractor: Box<dyn torii::etl::extractor::Extractor>,
) -> Result<()> {
    let max_db_connections = config.max_db_connections.unwrap_or(5);
    let pool = Arc::new(
        PgPoolOptions::new()
            .max_connections(max_db_connections)
            .connect(storage_database_url)
            .await?,
    );

    let decoder = DojoDecoder::<PgStore<_>, _>::new(pool.clone(), provider);
    decoder.store.initialize().await?;
    decoder.load_tables(&[]).await?;

    let decoder: Arc<dyn torii::etl::Decoder> = Arc::new(decoder);

    let ecs_sink = EcsSink::new(&storage_database_url, config.max_db_connections).await?;
    let ecs_grpc_service = ecs_sink.get_grpc_service_impl();

    let reflection = tonic_reflection::server::Builder::configure()
        .register_encoded_file_descriptor_set(torii::TORII_DESCRIPTOR_SET)
        .register_encoded_file_descriptor_set(ECS_DESCRIPTOR_SET)
        .build_v1()
        .expect("failed to build ECS reflection service");

    let grpc_router = tonic::transport::Server::builder()
        .accept_http1(true)
        .add_service(tonic_web::enable(WorldServer::new(
            (*ecs_grpc_service).clone(),
        )))
        .add_service(tonic_web::enable(reflection));

    let mut torii_config = torii::ToriiConfig::builder()
        .port(config.port)
        .with_grpc_router(grpc_router)
        .with_custom_reflection(true)
        .engine_database_url(engine_database_url)
        .with_extractor(extractor)
        .add_decoder(decoder)
        .add_sink_boxed(Box::new(IntrospectPgDb::new(pool.clone(), ())));

    let decoder_id = DecoderId::new("dojo-introspect");
    for contract in contracts {
        tracing::info!("Mapping Dojo contract {:#x} to dojo-introspect", contract);
        torii_config = torii_config.map_contract(contract, vec![decoder_id]);
    }

    tracing::info!("Torii configured, starting ETL pipeline...");
    tracing::info!("gRPC service available on port {}", config.port);
    tracing::info!("  - torii.Torii (core subscriptions and metrics endpoint)");
    tracing::info!("  - world.World (legacy ECS gRPC service)");

    torii::run(torii_config.build())
        .await
        .map_err(|e| anyhow::anyhow!("Torii error: {e}"))?;
    Ok(())
}

async fn run_with_sqlite(
    config: &Config,
    storage_database_url: &str,
    engine_database_url: String,
    contracts: Vec<Felt>,
    provider: starknet::providers::jsonrpc::JsonRpcClient<
        starknet::providers::jsonrpc::HttpTransport,
    >,
    extractor: Box<dyn torii::etl::extractor::Extractor>,
) -> Result<()> {
    let options = sqlite_connect_options(storage_database_url)?;
    let max_db_connections = match config.max_db_connections {
        Some(limit) => limit.max(1),
        None if is_sqlite_memory_path(storage_database_url) => 1,
        None => 1,
    };
    let pool = Arc::new(
        SqlitePoolOptions::new()
            .max_connections(max_db_connections)
            .connect_with(options)
            .await?,
    );

    sqlx::query("PRAGMA journal_mode=WAL")
        .execute(pool.as_ref())
        .await?;
    sqlx::query("PRAGMA synchronous=NORMAL")
        .execute(pool.as_ref())
        .await?;
    sqlx::query("PRAGMA foreign_keys=ON")
        .execute(pool.as_ref())
        .await?;

    let decoder = DojoDecoder::<SqliteStore<_>, _>::new(pool.clone(), provider);
    decoder.store.initialize().await?;
    decoder.load_tables(&[]).await?;

    let decoder: Arc<dyn torii::etl::Decoder> = Arc::new(decoder);

    let mut torii_config = torii::ToriiConfig::builder()
        .port(config.port)
        .engine_database_url(engine_database_url)
        .with_extractor(extractor)
        .add_decoder(decoder)
        .add_sink_boxed(Box::new(IntrospectSqliteDb::new(pool.clone(), ())));

    let decoder_id = DecoderId::new("dojo-introspect");
    for contract in contracts {
        tracing::info!("Mapping Dojo contract {:#x} to dojo-introspect", contract);
        torii_config = torii_config.map_contract(contract, vec![decoder_id]);
    }

    tracing::info!("Torii configured, starting ETL pipeline...");
    tracing::info!("gRPC service available on port {}", config.port);
    tracing::info!("  - torii.Torii (core subscriptions and metrics endpoint)");

    torii::run(torii_config.build())
        .await
        .map_err(|e| anyhow::anyhow!("Torii error: {e}"))?;
    Ok(())
}

async fn ensure_no_saved_state(
    engine_db: &EngineDb,
    contracts: &[Felt],
    ignore_saved_state: bool,
) -> Result<()> {
    if ignore_saved_state {
        return Ok(());
    }

    let mut owners_with_saved_state = Vec::new();
    for owner in contracts {
        let state_key = format!("{owner:#x}");
        if engine_db
            .get_extractor_state("event", &state_key)
            .await?
            .is_some()
        {
            owners_with_saved_state.push(*owner);
        }
    }

    if owners_with_saved_state.is_empty() {
        return Ok(());
    }

    bail!(
        "persisted extractor state exists for contract(s): {}. \
         Historical schema bootstrap is not supported on this branch; \
         re-run with --ignore-saved-state or use a fresh database",
        owners_with_saved_state
            .iter()
            .map(|owner| format!("{owner:#x}"))
            .collect::<Vec<_>>()
            .join(", ")
    )
}
