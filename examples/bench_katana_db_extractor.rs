//! Benchmark KatanaDbExtractor throughput against a full Katana database.
//!
//! Usage: cargo run --release --example bench_katana_db_extractor -- <db_path> [batch_size]
//!
//! Prints per-batch stats and a final summary with average rates.

use std::env;
use std::time::{Duration, Instant};
use torii::etl::engine_db::{EngineDb, EngineDbConfig};
use torii::etl::extractor::{Extractor, KatanaDbConfig, KatanaDbExtractor};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let db_path = env::args()
        .nth(1)
        .expect("Usage: bench_katana_db_extractor <db_path> [batch_size] [from_block]");
    let batch_size: u64 = env::args()
        .nth(2)
        .and_then(|s| s.parse().ok())
        .unwrap_or(1000);
    let from_block: u64 = env::args()
        .nth(3)
        .and_then(|s| s.parse().ok())
        .unwrap_or(0);

    println!("Opening Katana DB at: {db_path}");
    println!("Batch size: {batch_size}");
    println!("From block: {from_block}");
    println!();

    let config = KatanaDbConfig {
        db_path: db_path.clone(),
        from_block,
        to_block: None, // run through the entire DB
        batch_size,
        ..Default::default()
    };

    let mut extractor = KatanaDbExtractor::new(config)?;
    let engine_db = EngineDb::new(EngineDbConfig {
        path: "sqlite::memory:".to_string(),
    })
    .await?;

    let mut total_events: u64 = 0;
    let mut total_blocks: u64 = 0;
    let mut total_txs: u64 = 0;
    let mut total_declared: u64 = 0;
    let mut total_deployed: u64 = 0;
    let mut batch_count: u64 = 0;

    let start = Instant::now();
    let mut last_report = Instant::now();

    loop {
        let batch_start = Instant::now();
        let batch = extractor.extract(None, &engine_db).await?;

        if batch.is_empty() {
            // Extractor either finished or caught up with chain head.
            // For benchmarking we stop in both cases.
            break;
        }

        if !batch.is_empty() {
            let batch_elapsed = batch_start.elapsed();
            let max_block = batch.blocks.keys().max().copied().unwrap_or(0);
            let min_block = batch.blocks.keys().min().copied().unwrap_or(0);
            let n_events = batch.events.len() as u64;
            let n_txs = batch.transactions.len() as u64;
            let n_blocks = batch.blocks.len() as u64;

            total_events += n_events;
            total_blocks += n_blocks;
            total_txs += n_txs;
            total_declared += batch.declared_classes.len() as u64;
            total_deployed += batch.deployed_contracts.len() as u64;
            batch_count += 1;

            // Print progress every 5 seconds
            if last_report.elapsed() >= Duration::from_secs(5) {
                let overall = start.elapsed();
                let overall_secs = overall.as_secs_f64();
                println!(
                    "[{:>8.1}s] blocks {min_block:>7}-{max_block:<7} | batch: {n_events:>6} evts, {n_txs:>5} txs in {:>6.1}ms | cumulative: {total_events:>9} evts ({:>8.0} evt/s), {total_txs:>8} txs ({:>8.0} tx/s), {total_blocks:>7} blocks ({:>7.0} blk/s)",
                    overall_secs,
                    batch_elapsed.as_secs_f64() * 1000.0,
                    total_events as f64 / overall_secs,
                    total_txs as f64 / overall_secs,
                    total_blocks as f64 / overall_secs,
                );
                last_report = Instant::now();
            }

            if let Some(cursor) = &batch.cursor {
                extractor.commit_cursor(cursor, &engine_db).await?;
            }
        }
    }

    let elapsed = start.elapsed();
    let secs = elapsed.as_secs_f64();

    println!();
    println!("==============================");
    println!("        BENCHMARK RESULTS     ");
    println!("==============================");
    println!("Total time:       {:.2}s", secs);
    println!("Total blocks:     {total_blocks}");
    println!("Total txs:        {total_txs}");
    println!("Total events:     {total_events}");
    println!("Total declared:   {total_declared}");
    println!("Total deployed:   {total_deployed}");
    println!("Batches:          {batch_count}");
    println!("------------------------------");
    println!("Blocks/sec:       {:.0}", total_blocks as f64 / secs);
    println!("Transactions/sec: {:.0}", total_txs as f64 / secs);
    println!("Events/sec:       {:.0}", total_events as f64 / secs);
    println!("Avg batch time:   {:.1}ms", (secs * 1000.0) / batch_count as f64);
    println!("==============================");

    Ok(())
}
