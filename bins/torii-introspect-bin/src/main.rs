//! Torii Introspect - Dojo introspect indexer backed by PostgreSQL.

mod config;

use anyhow::Result;
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
use torii_dojo::store::DojoStoreTrait;

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
        },
    ));

    let bootstrap_points = resolve_bootstrap_points(
        &storage_database_url,
        &contracts,
        config.from_block,
        config.ignore_saved_state,
    )
    .await?;
    for point in &bootstrap_points {
        tracing::info!(
            target: "torii::dojo::metadata",
            owner = %point.owner,
            block_number = point.block_number,
            had_saved_state = point.had_saved_state,
            "Resolved Dojo schema bootstrap point"
        );
    }
    let pool = Arc::new(
        PgPoolOptions::new()
            .max_connections(5)
            .connect(&storage_database_url)
            .await?,
    );

    if !historical_bootstrap.unsafe_owners.is_empty()
        && !config.allow_unsafe_latest_schema_bootstrap
    {
        anyhow::bail!(
            "historical Dojo schema bootstrap is unavailable for contract(s): {}. \
             Re-run with --ignore-saved-state, use a fresh database, or pass \
             --allow-unsafe-latest-schema-bootstrap to keep the previous best-effort behavior",
            historical_bootstrap
                .unsafe_owners
                .iter()
                .map(|owner| format!("{owner:#x}"))
                .collect::<Vec<_>>()
                .join(", ")
        );
    }
    initialize_dojo_schema(&secondary_store).await?;
    let merged_store = MergedStore::new(primary_store, secondary_store);
    let preloaded_tables = if historical_bootstrap.unsafe_owners.is_empty() {
        tracing::info!(
            target: "torii::dojo::metadata",
            latest_tables = historical_bootstrap.latest_table_count,
            historical_tables = historical_bootstrap.historical_table_count,
            "Bootstrapping decoder from historical Dojo schema snapshot"
        );
        historical_bootstrap.tables
    } else {
        tracing::warn!(
            target: "torii::dojo::metadata",
            owners = historical_bootstrap
                .unsafe_owners
                .iter()
                .map(|owner| format!("{owner:#x}"))
                .collect::<Vec<_>>()
                .join(", "),
            "Historical Dojo schema bootstrap unavailable; falling back to latest persisted schema"
        );
        merged_store.load_tables(&[]).await?
    };

    let bootstrap_tables = metadata_store.create_table_messages()?;
    let decoder = DojoDecoder::<PgStore<_>, _>::new(pool.clone(), provider);
    decoder.store.initialize().await?;
    decoder.load_tables(&[]).await?;

    let decoder: Arc<dyn torii::etl::Decoder> = Arc::new(decoder);

    let mut torii_config = torii::ToriiConfig::builder()
        .port(config.port)
        .engine_database_url(engine_database_url)
        .with_extractor(extractor)
        .add_decoder(decoder)
        .add_sink_boxed(Box::new(
            IntrospectPostgresSink::new(storage_database_url, config.max_db_connections)
                .with_bootstrap_tables(bootstrap_tables),
        ));

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

async fn resolve_bootstrap_points(
    database_url: &str,
    contracts: &[Felt],
    from_block: u64,
    ignore_saved_state: bool,
) -> Result<Vec<SchemaBootstrapPoint>> {
    if ignore_saved_state {
        return Ok(contracts
            .iter()
            .copied()
            .map(|owner| SchemaBootstrapPoint {
                owner,
                block_number: from_block,
                had_saved_state: false,
            })
            .collect());
    }

    let pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(database_url)
        .await?;

    let engine_table_exists: bool = sqlx::query_scalar(
        r"
        SELECT EXISTS (
            SELECT 1
            FROM information_schema.tables
            WHERE table_schema = 'engine' AND table_name = 'extractor_state'
        )
        ",
    )
    .fetch_one(&pool)
    .await
    .unwrap_or(false);

    if !engine_table_exists {
        return Ok(contracts
            .iter()
            .copied()
            .map(|owner| SchemaBootstrapPoint {
                owner,
                block_number: from_block,
                had_saved_state: false,
            })
            .collect());
    }

    let mut points = Vec::with_capacity(contracts.len());
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
        .fetch_optional(&pool)
        .await?;

        let block_number = saved_state
            .as_deref()
            .and_then(parse_saved_state_block)
            .unwrap_or(from_block);
        points.push(SchemaBootstrapPoint {
            owner: *owner,
            block_number,
            had_saved_state: saved_state.is_some(),
        });
    }

    Ok(points)
}

fn parse_saved_state_block(saved_state: &str) -> Option<u64> {
    saved_state
        .split('|')
        .next()
        .and_then(|part| part.strip_prefix("block:"))
        .and_then(|block| block.parse::<u64>().ok())
}
