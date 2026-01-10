//! Torii ERC20 - Starknet ERC20 Token Indexer
//!
//! A production-ready indexer for ERC20 tokens on Starknet.
//!
//! # Features
//!
//! - Explicit contract mapping (ETH, STRK, custom tokens)
//! - Auto-discovery of ERC20 contracts (can be disabled)
//! - Real-time transfer tracking
//! - Balance maintenance per address
//! - SQLite persistence
//! - gRPC subscriptions for real-time updates
//!
//! # Usage
//!
//! ```bash
//! # Index from block 100,000 with auto-discovery
//! torii-erc20 --from-block 100000
//!
//! # Strict mode: only index ETH and STRK (no auto-discovery)
//! torii-erc20 --no-auto-discovery
//!
//! # Index specific contracts
//! torii-erc20 --contracts 0x123...,0x456... --no-auto-discovery
//! ```

mod config;
mod decoder;
mod sink;
mod storage;

use anyhow::Result;
use clap::Parser;
use config::Config;
use decoder::Erc20Decoder;
use sink::Erc20Sink;
use std::sync::Arc;
use storage::Erc20Storage;
use tokio::sync::Mutex;
use torii::etl::extractor::{
    BlockRangeConfig, BlockRangeExtractor, ContractRegistry, DecoderId, Erc20Rule, Extractor,
};
use torii::etl::{Decoder, MultiDecoder, Sink};

#[tokio::main]
async fn main() -> Result<()> {
    // Start profiler if enabled
    #[cfg(feature = "profiling")]
    let guard = pprof::ProfilerGuardBuilder::default()
        .frequency(1000)
        .blocklist(&["libc", "libgcc", "pthread", "vdso"])
        .build()
        .unwrap();

    // Parse CLI arguments
    let config = Config::parse();

    // Initialize logging
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_target(true)
        .init();

    tracing::info!("🚀 Starting Torii ERC20 Indexer");
    tracing::info!("RPC URL: {}", config.rpc_url);
    tracing::info!("From block: {}", config.from_block);
    tracing::info!("Database: {}", config.db_path);
    tracing::info!(
        "Auto-discovery: {}",
        if config.no_auto_discovery { "disabled" } else { "enabled" }
    );

    // Create storage
    let storage = Arc::new(Erc20Storage::new(&config.db_path)?);
    tracing::info!("✓ Database initialized");

    // Create Starknet provider
    let provider = Arc::new(starknet::providers::jsonrpc::JsonRpcClient::new(
        starknet::providers::jsonrpc::HttpTransport::new(
            url::Url::parse(&config.rpc_url).expect("Invalid RPC URL"),
        ),
    ));
    tracing::info!("✓ Connected to Starknet RPC");

    // Create ContractRegistry with provider
    let identification_mode = config.identification_mode();
    let registry = Arc::new(Mutex::new(ContractRegistry::with_provider(
        identification_mode,
        provider.clone(),
    )));

    // Configure registry
    {
        let mut registry_lock = registry.lock().await;

        // Add ERC20 identification rule (if auto-discovery enabled)
        if !config.no_auto_discovery {
            registry_lock.add_rule(Box::new(Erc20Rule));
            tracing::info!("✓ ERC20 auto-discovery rule registered");
        }

        // Add well-known contracts (ETH, STRK)
        let well_known = config.well_known_contracts();
        for (addr, name) in &well_known {
            registry_lock.add_explicit_mapping(*addr, vec![DecoderId::new("erc20")]);
            tracing::info!("✓ Explicit mapping: {} at {:#x}", name, addr);
        }

        // Add user-specified contracts
        if !config.contracts.is_empty() {
            let contracts = config.parse_contracts()?;
            for addr in contracts {
                registry_lock.add_explicit_mapping(addr, vec![DecoderId::new("erc20")]);
                tracing::info!("✓ Explicit mapping: custom contract at {:#x}", addr);
            }
        }
    }

    // Create ERC20 decoder
    let erc20_decoder = Arc::new(Erc20Decoder::new());
    let decoders: Vec<Arc<dyn Decoder>> = vec![erc20_decoder];

    // Create MultiDecoder with registry
    let multi_decoder = Arc::new(MultiDecoder::with_registry(decoders, registry.clone()));
    tracing::info!("✓ Decoder configured");

    // Create ERC20 sink (needs to be mutable for initialization)
    let mut erc20_sink = Erc20Sink::new(storage.clone());

    // Initialize sink (normally done by Torii, but we're doing ETL manually)
    let subscription_manager = Arc::new(torii::grpc::SubscriptionManager::new());
    let event_bus = Arc::new(torii::etl::sink::EventBus::new(subscription_manager));
    erc20_sink.initialize(event_bus).await?;

    tracing::info!("✓ Sink initialized");

    // Create BlockRangeExtractor
    let extractor_config = BlockRangeConfig {
        rpc_url: config.rpc_url.clone(),
        from_block: config.from_block,
        to_block: config.to_block,
        batch_size: 1000,
        retry_policy: torii::etl::extractor::RetryPolicy::default(),
    };

    let mut extractor = BlockRangeExtractor::new(provider.clone(), extractor_config);
    tracing::info!("✓ Extractor initialized");

    tracing::info!("🎯 Starting ETL loop...");

    // ETL loop
    let engine_db_config = torii::etl::engine_db::EngineDbConfig {
        path: ":memory:".to_string(),
    };
    let engine_db = torii::etl::engine_db::EngineDb::new(engine_db_config).await?;

    let mut cursor: Option<String> = None;
    let mut batch_count = 0u64;

    loop {
        let loop_start = std::time::Instant::now();

        // Extract batch
        let extract_start = std::time::Instant::now();
        let batch = match extractor.extract(cursor.clone(), &engine_db).await {
            Ok(batch) => batch,
            Err(e) => {
                tracing::error!("Extraction failed: {}", e);
                tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
                continue;
            }
        };
        let extract_duration = extract_start.elapsed();

        // Update cursor
        cursor = batch.cursor.clone();

        if batch.is_empty() {
            if extractor.is_finished() {
                tracing::info!("✓ Extraction complete");
                break;
            }

            // Wait for new blocks
            tracing::debug!("Waiting for new blocks...");
            tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
            continue;
        }

        batch_count += 1;

        tracing::info!(
            "📦 Batch #{}: Extracted {} events from {} blocks (extract_time: {:.2}ms)",
            batch_count,
            batch.events.len(),
            batch.blocks.len(),
            extract_duration.as_secs_f64() * 1000.0
        );

        // Update engine stats
        if let Some(&latest_block) = batch.blocks.keys().max() {
            if let Err(e) = engine_db.update_head(latest_block, batch.events.len() as u64).await {
                tracing::warn!("Failed to update engine DB: {}", e);
            }
        }

        // Decode events (identification happens automatically inside)
        // No need to pre-iterate events!
        let decode_start = std::time::Instant::now();
        let envelopes = match multi_decoder.decode(&batch.events).await {
            Ok(envelopes) => envelopes,
            Err(e) => {
                tracing::error!("Decode failed: {}", e);
                continue;
            }
        };
        let decode_duration = decode_start.elapsed();

        tracing::info!(
            "   ✓ Decoded into {} envelopes (decode_time: {:.2}ms)",
            envelopes.len(),
            decode_duration.as_secs_f64() * 1000.0
        );

        // Process envelopes through sink
        let sink_start = std::time::Instant::now();
        if let Err(e) = erc20_sink.process(&envelopes, &batch).await {
            tracing::error!("Sink processing failed: {}", e);
            continue;
        }
        let sink_duration = sink_start.elapsed();
        let loop_duration = loop_start.elapsed();

        tracing::info!(
            "   ✓ Processed through sink (sink_time: {:.2}ms) | Total loop: {:.2}ms",
            sink_duration.as_secs_f64() * 1000.0,
            loop_duration.as_secs_f64() * 1000.0
        );
    }

    // Print final statistics
    tracing::info!("📊 Final Statistics:");
    if let Ok(transfer_count) = storage.get_transfer_count() {
        tracing::info!("  Total transfers: {}", transfer_count);
    }
    if let Ok(token_count) = storage.get_token_count() {
        tracing::info!("  Unique tokens: {}", token_count);
    }
    if let Ok(Some(latest_block)) = storage.get_latest_block() {
        tracing::info!("  Latest block: {}", latest_block);
    }

    // Generate flamegraph if profiling enabled
    #[cfg(feature = "profiling")]
    {
        if let Ok(report) = guard.report().build() {
            let file = std::fs::File::create("flamegraph.svg").unwrap();
            report.flamegraph(file).unwrap();
            tracing::info!("🔥 Flamegraph generated: flamegraph.svg");
        }
    }

    Ok(())
}
