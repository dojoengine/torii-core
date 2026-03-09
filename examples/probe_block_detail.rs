//! Probe individual block queries to find which provider call hangs.
//!
//! Usage: cargo run --release --example probe_block_detail -- <db_path> <block_num>

use std::env;
use std::time::Instant;
use katana_provider::api::block::{BlockHashProvider, BlockNumberProvider, HeaderProvider};
use katana_provider::api::state_update::StateUpdateProvider;
use katana_provider::api::transaction::{ReceiptProvider, TransactionProvider};
use katana_provider::{DbProviderFactory, ProviderFactory};

fn main() -> anyhow::Result<()> {
    let db_path = env::args().nth(1).expect("Usage: probe_block_detail <db_path> <block_num>");
    let block_num: u64 = env::args().nth(2).and_then(|s| s.parse().ok()).unwrap_or(869604);

    let db = katana_db::Db::open_ro(&db_path)?;
    let factory = DbProviderFactory::new(db);
    let provider = factory.provider();

    println!("Probing block {block_num} step by step...");

    let t = Instant::now();
    let head = provider.latest_number()?;
    println!("  latest_number() = {head} [{:.1}ms]", t.elapsed().as_secs_f64() * 1000.0);

    let t = Instant::now();
    let header = provider.header_by_number(block_num)?;
    println!("  header_by_number() = {:?} [{:.1}ms]", header.is_some(), t.elapsed().as_secs_f64() * 1000.0);

    let t = Instant::now();
    let hash = provider.block_hash_by_num(block_num)?;
    println!("  block_hash_by_num() = {:?} [{:.1}ms]", hash.is_some(), t.elapsed().as_secs_f64() * 1000.0);

    let t = Instant::now();
    let txs = provider.transactions_by_block(block_num.into())?;
    println!("  transactions_by_block() = {} txs [{:.1}ms]", txs.as_ref().map_or(0, |v| v.len()), t.elapsed().as_secs_f64() * 1000.0);

    let t = Instant::now();
    let receipts = provider.receipts_by_block(block_num.into())?;
    println!("  receipts_by_block() = {} receipts [{:.1}ms]", receipts.as_ref().map_or(0, |v| v.len()), t.elapsed().as_secs_f64() * 1000.0);

    let t = Instant::now();
    let deployed = provider.deployed_contracts(block_num.into())?;
    println!("  deployed_contracts() = {:?} [{:.1}ms]", deployed.as_ref().map(|v| v.len()), t.elapsed().as_secs_f64() * 1000.0);

    let t = Instant::now();
    let declared = provider.declared_classes(block_num.into())?;
    println!("  declared_classes() = {:?} [{:.1}ms]", declared.as_ref().map(|v| v.len()), t.elapsed().as_secs_f64() * 1000.0);

    println!("Done!");
    Ok(())
}
