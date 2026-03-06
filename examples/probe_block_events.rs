//! Probe event counts per block in a range.
//!
//! Usage: cargo run --release --example probe_block_events -- <db_path> <from> <to>

use std::env;
use katana_provider::api::block::BlockNumberProvider;
use katana_provider::api::transaction::ReceiptProvider;
use katana_provider::{DbProviderFactory, ProviderFactory};

fn main() -> anyhow::Result<()> {
    let db_path = env::args().nth(1).expect("Usage: probe_block_events <db_path> <from> <to>");
    let from: u64 = env::args().nth(2).and_then(|s| s.parse().ok()).unwrap_or(869960);
    let to: u64 = env::args().nth(3).and_then(|s| s.parse().ok()).unwrap_or(870010);

    let db = katana_db::Db::open_ro(&db_path)?;
    let factory = DbProviderFactory::new(db);
    let provider = factory.provider();

    let chain_head = provider.latest_number()?;
    println!("Chain head: {chain_head}");
    println!("Probing blocks {from}-{to}");
    println!();

    let to = to.min(chain_head);
    for block_num in from..=to {
        let receipts = provider
            .receipts_by_block(block_num.into())?
            .unwrap_or_default();

        let mut total_events = 0usize;
        let mut tx_count = 0usize;
        let mut max_events_in_tx = 0usize;
        for receipt in &receipts {
            let n = receipt.events().len();
            total_events += n;
            tx_count += 1;
            max_events_in_tx = max_events_in_tx.max(n);
        }

        if total_events > 1000 {
            println!(
                "Block {block_num}: {total_events:>10} events, {tx_count:>5} txs, max {max_events_in_tx} events/tx  *** DENSE ***"
            );
        } else {
            println!(
                "Block {block_num}: {total_events:>10} events, {tx_count:>5} txs"
            );
        }
    }

    Ok(())
}
