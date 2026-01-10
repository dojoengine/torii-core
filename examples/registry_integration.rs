//! Example: Integrating ContractRegistry with BlockRangeExtractor
//!
//! This example shows how to:
//! 1. Create a ContractRegistry with identification rules
//! 2. Set up MultiDecoder with registry-aware routing
//! 3. Identify contracts in the ETL loop before decoding
//! 4. Route events to appropriate decoders based on contract type
//!
//! # Running
//!
//! ```bash
//! cargo run --example registry_integration
//! ```

use anyhow::Result;
use std::sync::Arc;
use tokio::sync::Mutex;
use torii::etl::{
    extractor::{
        BlockRangeConfig, BlockRangeExtractor, ContractIdentificationMode, ContractRegistry,
        DecoderId, Erc20Rule, Erc721Rule, Extractor,
    },
    Decoder, MultiDecoder,
};

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize tracing for logging
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .init();

    // 1. Create a Starknet provider (RPC endpoint)
    let rpc_url = "https://starknet-sepolia.public.blastapi.io/rpc/v0_7";
    let provider = Arc::new(starknet::providers::jsonrpc::JsonRpcClient::new(
        starknet::providers::jsonrpc::HttpTransport::new(
            url::Url::parse(rpc_url).expect("Invalid RPC URL"),
        ),
    ));

    // 2. Create ContractRegistry with identification rules
    let identification_mode =
        ContractIdentificationMode::SRC5 | ContractIdentificationMode::ABI_HEURISTICS;

    let registry = Arc::new(Mutex::new(ContractRegistry::with_provider(
        identification_mode,
        provider.clone(),
    )));

    // Add example identification rules
    {
        let mut registry_lock = registry.lock().await;
        registry_lock.add_rule(Box::new(Erc20Rule));
        registry_lock.add_rule(Box::new(Erc721Rule));

        // Example: Add explicit mapping for a known contract
        // let known_erc20 = Felt::from_hex_unchecked("0x123...");
        // registry_lock.add_explicit_mapping(known_erc20, vec![DecoderId::new("erc20")]);
    }

    // 3. Create your decoders (these would be your actual decoder implementations)
    let decoders: Vec<Arc<dyn Decoder>> = vec![
        // Example: Arc::new(Erc20Decoder::new()),
        // Example: Arc::new(Erc721Decoder::new()),
    ];

    // 4. Create MultiDecoder with registry
    let multi_decoder = Arc::new(MultiDecoder::with_registry(
        decoders,
        registry.clone(),
    ));

    // 5. Create BlockRangeExtractor
    let config = torii::etl::extractor::BlockRangeConfig {
        rpc_url: rpc_url.to_string(),
        from_block: 100_000,
        to_block: Some(100_010), // Extract 10 blocks
        retry_policy: None,
    };

    let mut extractor = BlockRangeExtractor::new(provider.clone(), config);

    // 6. ETL loop - identification happens automatically during decode
    let engine_db_config = torii::etl::engine_db::EngineDbConfig {
        path: ":memory:".to_string(),
    };
    let engine_db = torii::etl::engine_db::EngineDb::new(engine_db_config).await?;

    loop {
        // Extract events
        let batch = extractor.extract(None, &engine_db).await?;

        if batch.is_empty() {
            if extractor.is_finished() {
                tracing::info!("Extraction complete");
                break;
            }
            tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
            continue;
        }

        tracing::info!(
            "Extracted {} events from {} blocks",
            batch.events.len(),
            batch.blocks.len()
        );

        // Decode events - identification happens automatically inside decode()
        // No need to pre-iterate events to collect contract addresses!
        let envelopes = multi_decoder.decode(&batch.events).await?;

        tracing::info!(
            "Decoded {} events into {} envelopes",
            batch.events.len(),
            envelopes.len()
        );

        // Process envelopes with your sinks...
        // multi_sink.process(&envelopes, &batch).await?;

        if extractor.is_finished() {
            break;
        }
    }

    // Print registry statistics
    let stats = {
        let registry_lock = registry.lock().await;
        registry_lock.stats()
    };

    tracing::info!(
        "Registry stats: {} known contracts, {} cached ABIs, {} explicit mappings, {} rules",
        stats.known_contracts,
        stats.cached_abis,
        stats.explicit_mappings,
        stats.identification_rules
    );

    Ok(())
}
