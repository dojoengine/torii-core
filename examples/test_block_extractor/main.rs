//! Simple example to test BlockRangeExtractor without sinks
//!
//! This example:
//! - Creates a BlockRangeExtractor with a real Starknet RPC provider
//! - Extracts a small range of blocks
//! - Prints extracted events, blocks, and transactions
//!
//! Run: `cargo run --example test_block_extractor`

use std::sync::Arc;

use anyhow::Result;
use starknet::providers::jsonrpc::{HttpTransport, JsonRpcClient};
use starknet::providers::Url;

use torii::etl::engine_db::{EngineDb, EngineDbConfig};
use torii::etl::extractor::{BlockRangeConfig, BlockRangeExtractor, Extractor, RetryPolicy};

// const STARKNET_RPC_URL: &str = "http://localhost:5050";
const STARKNET_RPC_URL: &str = "https://api.cartridge.gg/x/starknet/sepolia";

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "torii=debug,test_block_extractor=info".into()),
        )
        .init();

    let engine_db_config = EngineDbConfig {
        path: ":memory:".to_string(),
    };
    let engine_db = EngineDb::new(engine_db_config).await?;

    let from_block = 0;
    let to_block = 1000;

    let config = BlockRangeConfig {
        rpc_url: STARKNET_RPC_URL.to_string(),
        from_block,
        to_block: Some(to_block),
        batch_size: 5,
        retry_policy: RetryPolicy::default(),
    };

    let provider = JsonRpcClient::new(HttpTransport::new(Url::parse(&config.rpc_url)?));

    let mut extractor = BlockRangeExtractor::new(Arc::new(provider), config);

    // Main extraction loop
    let mut cursor = None;
    loop {
        let batch = extractor.extract(cursor.clone(), &engine_db).await?;

        if !batch.is_empty() {
            println!(
                "Extracted blocks {} to {} ({} events, {} declared classes, {} deployed contracts)",
                batch.blocks.keys().min().unwrap(),
                batch.blocks.keys().max().unwrap(),
                batch.events.len(),
                batch.declared_classes.len(),
                batch.deployed_contracts.len()
            );
        }

        // Check if extractor is done
        if extractor.is_finished() {
            println!("✅ Extractor finished - reached configured end block");
            break;
        }

        // Check if we need to wait for new blocks
        if batch.is_empty() {
            println!("⏳ At chain head, waiting for new blocks...");
            tokio::time::sleep(std::time::Duration::from_secs(5)).await;
        }

        cursor = batch.cursor;
    }

    Ok(())
}
