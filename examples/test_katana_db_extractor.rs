//! Quick test of KatanaDbExtractor against a real Katana database.
//!
//! Usage: cargo run --example test_katana_db_extractor -- <db_path>

use std::env;
use torii::etl::extractor::{KatanaDbConfig, KatanaDbExtractor};
use torii::etl::extractor::Extractor;
use torii::etl::engine_db::{EngineDb, EngineDbConfig};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let db_path = env::args().nth(1).expect("Usage: test_katana_db_extractor <db_path>");

    println!("Opening Katana DB at: {db_path}");

    let config = KatanaDbConfig {
        db_path: db_path.clone(),
        from_block: 0,
        to_block: Some(100), // just first 100 blocks as a smoke test
        batch_size: 10,
        ..Default::default()
    };

    let mut extractor = KatanaDbExtractor::new(config)?;
    let engine_db = EngineDb::new(EngineDbConfig {
        path: "sqlite::memory:".to_string(),
    }).await?;

    let mut total_events = 0;
    let mut total_blocks = 0;
    let mut total_txs = 0;
    let mut total_declared = 0;
    let mut total_deployed = 0;

    loop {
        let batch = extractor.extract(None, &engine_db).await?;

        if batch.is_empty() && extractor.is_finished() {
            break;
        }

        if !batch.is_empty() {
            let max_block = batch.blocks.keys().max().copied().unwrap_or(0);
            let min_block = batch.blocks.keys().min().copied().unwrap_or(0);

            println!(
                "Batch: blocks {}-{} | {} events, {} txs, {} declared, {} deployed | chain_head={:?}",
                min_block,
                max_block,
                batch.events.len(),
                batch.transactions.len(),
                batch.declared_classes.len(),
                batch.deployed_contracts.len(),
                batch.chain_head,
            );

            total_events += batch.events.len();
            total_blocks += batch.blocks.len();
            total_txs += batch.transactions.len();
            total_declared += batch.declared_classes.len();
            total_deployed += batch.deployed_contracts.len();

            if let Some(cursor) = &batch.cursor {
                extractor.commit_cursor(cursor, &engine_db).await?;
            }
        }
    }

    println!("\n=== Summary ===");
    println!("Total blocks:    {total_blocks}");
    println!("Total txs:       {total_txs}");
    println!("Total events:    {total_events}");
    println!("Total declared:  {total_declared}");
    println!("Total deployed:  {total_deployed}");

    Ok(())
}
