//! Minimal reproduction of extract() hang.

use std::time::Instant;
use torii::etl::engine_db::{EngineDb, EngineDbConfig};
use torii::etl::extractor::{Extractor, KatanaDbConfig, KatanaDbExtractor};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let db_path = std::env::args().nth(1).expect("Usage: probe_extract <db_path>");
    let from: u64 = std::env::args().nth(2).and_then(|s| s.parse().ok()).unwrap_or(869600);

    eprintln!("Step 1: Creating EngineDb...");
    let t = Instant::now();
    let engine_db = EngineDb::new(EngineDbConfig {
        path: "sqlite::memory:".to_string(),
    }).await?;
    eprintln!("  Done [{:.1}ms]", t.elapsed().as_secs_f64() * 1000.0);

    eprintln!("Step 2: Creating KatanaDbExtractor...");
    let t = Instant::now();
    let config = KatanaDbConfig {
        db_path,
        from_block: from,
        to_block: None,
        batch_size: 5,
        ..Default::default()
    };
    let mut extractor = KatanaDbExtractor::new(config)?;
    eprintln!("  Done [{:.1}ms]", t.elapsed().as_secs_f64() * 1000.0);

    eprintln!("Step 3: Extracting in loop until finished...");
    let start = Instant::now();
    let mut batch_count = 0u64;
    let mut total_events = 0u64;
    loop {
        let t = Instant::now();
        let batch = extractor.extract(None, &engine_db).await?;
        let ms = t.elapsed().as_secs_f64() * 1000.0;

        if batch.is_empty() && extractor.is_finished() {
            break;
        }

        batch_count += 1;
        total_events += batch.events.len() as u64;

        if let Some(cursor) = &batch.cursor {
            extractor.commit_cursor(cursor, &engine_db).await?;
        }

        let max_block = batch.blocks.keys().max().copied().unwrap_or(0);
        eprintln!(
            "  batch {batch_count}: block {max_block}, {} events in {ms:.1}ms (total: {total_events})",
            batch.events.len()
        );
    }
    let elapsed = start.elapsed();
    eprintln!("Done! {batch_count} batches, {total_events} events in {:.1}s", elapsed.as_secs_f64());

    Ok(())
}
