//! Torii Introspect - Dojo introspect indexer backed by PostgreSQL.

mod config;

use anyhow::{bail, Result};
use clap::Parser;
use config::Config;
use sqlx::postgres::PgPoolOptions;
use starknet::core::types::Felt;
use std::sync::Arc;
use torii::etl::decoder::DecoderId;
use torii::etl::extractor::{
    ContractEventConfig, EventExtractor, EventExtractorConfig, RetryPolicy,
};
use torii_dojo::decoder::DojoDecoder;
use torii_dojo::store::postgres::PgStore;
use torii_introspect_postgres_sink::processor::IntrospectPgDb;

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

    let metrics_enabled = if config.observability {
        "true"
    } else {
        "false"
    };
    std::env::set_var("TORII_METRICS_ENABLED", metrics_enabled);

    let storage_database_url = config.storage_database_url()?.to_string();
    let engine_database_url = config.engine_database_url();
    let contracts = config.contract_addresses()?;

    tracing::info!("RPC URL: {}", config.rpc_url);
    tracing::info!("Contracts: {}", contracts.len());
    tracing::info!("From block: {}", config.from_block);
    if let Some(to_block) = config.to_block {
        tracing::info!("To block: {}", to_block);
    } else {
        tracing::info!("To block: following chain head");
    }
    tracing::info!("Engine database URL: {}", engine_database_url);
    tracing::info!("Storage database URL: {}", storage_database_url);
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
    let max_db_connections = config.max_db_connections.unwrap_or(5);
    let pool = Arc::new(
        PgPoolOptions::new()
            .max_connections(max_db_connections)
            .connect(&storage_database_url)
            .await?,
    );

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
        },
    ));

    ensure_no_saved_state(pool.as_ref(), &contracts, config.ignore_saved_state).await?;

    let decoder = DojoDecoder::<PgStore<_>, _>::new(pool.clone(), provider);
    decoder.store.initialize().await?;
    decoder.load_tables(&[]).await?;

    let decoder: Arc<dyn torii::etl::Decoder> = Arc::new(decoder);

    let mut torii_config = torii::ToriiConfig::builder()
        .port(config.port)
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

    torii::run(torii_config.build())
        .await
        .map_err(|e| anyhow::anyhow!("Torii error: {e}"))?;

    tracing::info!("Torii shutdown complete");
    Ok(())
}

async fn ensure_no_saved_state(
    pool: &sqlx::PgPool,
    contracts: &[Felt],
    ignore_saved_state: bool,
) -> Result<()> {
    if ignore_saved_state {
        return Ok(());
    }

    let engine_table_exists: bool = sqlx::query_scalar(
        r"
        SELECT EXISTS (
            SELECT 1
            FROM information_schema.tables
            WHERE table_schema = 'engine' AND table_name = 'extractor_state'
        )
        ",
    )
    .fetch_one(pool)
    .await
    .unwrap_or(false);

    if !engine_table_exists {
        return Ok(());
    }

    let mut owners_with_saved_state = Vec::new();
    for owner in contracts {
        let state_key = format!("{owner:#x}");
        let saved_state: Option<String> = sqlx::query_scalar(
            r"
            SELECT state_value
            FROM engine.extractor_state
            WHERE extractor_type = 'event' AND state_key = $1
            ",
        )
        .bind(&state_key)
        .fetch_optional(pool)
        .await?;

        if saved_state.is_some() {
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
