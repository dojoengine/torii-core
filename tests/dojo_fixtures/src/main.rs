//! Dojo fixtures CLI
//!
//! Generate and manage test fixtures for Torii.

use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use katana_runner::{KatanaRunner, KatanaRunnerConfig};
use starknet::core::{
    types::{BlockId, BlockTag, EventFilter, Felt},
    utils::get_selector_from_name,
};
use std::{fs::File, io::Write, path::PathBuf};

mod abigen;
mod constants;
mod deployment;
mod events;
mod sozo;
mod transactions;
mod types;

use constants::*;
use types::FixtureOutput;

#[derive(Parser)]
#[command(name = "dojo-fixtures")]
#[command(about = "Generate and manage Dojo test fixtures", long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Migrates baseline and upgrade contracts to Katana.
    Migrate {
        /// Output file for events JSON.
        #[arg(short, long, default_value = "dojo_fixtures_events.json")]
        #[arg(help = "Output file for events JSON.")]
        output_events: PathBuf,

        /// Katana database directory.
        #[arg(long)]
        #[arg(help = "Katana database directory.")]
        katana_db_dir: PathBuf,

        /// Contract tag to get event for.
        #[arg(long, default_value = "ns-c1")]
        #[arg(help = "Contract tag to get event for.")]
        contract_tag: String,
    },

    /// Fetches all the events for the given contract address and selectors.
    Fetch {
        /// Contract address.
        #[arg(long)]
        #[arg(help = "Contract address to fetch the events from.")]
        contract_address: Felt,

        /// Event names to filter the events by.
        #[arg(long)]
        #[arg(help = "Event names to filter the events by. They will be computed into selectors.")]
        event_names: Option<Vec<String>>,

        /// Start block.
        #[arg(long, default_value = "0")]
        #[arg(help = "Start block to fetch the events from.")]
        from_block: u64,

        /// End block, defaulting to the latest block.
        #[arg(long)]
        #[arg(
            help = "End block to fetch the events from, defaulting to the latest block if not provided."
        )]
        to_block: Option<u64>,

        /// RPC URL.
        #[arg(long)]
        #[arg(help = "RPC URL to use for the JSON-RPC client.")]
        rpc_url: String,

        /// Output file for events JSON.
        #[arg(long)]
        #[arg(help = "Output file for events JSON.")]
        output_events: PathBuf,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Commands::Migrate {
            output_events,
            katana_db_dir,
            contract_tag,
        } => {
            migrate_command(output_events, katana_db_dir, contract_tag).await?;
        }
        Commands::Fetch {
            contract_address,
            event_names,
            from_block,
            to_block,
            rpc_url,
            output_events,
        } => {
            fetch_command(
                contract_address,
                event_names.unwrap_or_default(),
                from_block,
                to_block,
                rpc_url,
                output_events,
            )
            .await?;
        }
    }

    Ok(())
}

/// Migrates the baseline and upgrade contracts to Katana.
///
/// The baseline contract is migrated first, and then the upgrade contract is migrated.
/// The upgrade contract is migrated by sending transactions to the upgrade contract
/// to exercise all the functions.
///
/// The events are fetched from the Katana database and saved to the output file.
/// Since the Katana database is configurable and not pruned, we may compress it later
/// for integration tests of the fetcher.
async fn migrate_command(
    output_path: PathBuf,
    katana_db_dir: PathBuf,
    contract_tag: String,
) -> Result<()> {
    if katana_db_dir.exists() {
        std::fs::remove_dir_all(&katana_db_dir)?;
    }

    std::fs::create_dir_all(&katana_db_dir)?;

    let katana_cfg = KatanaRunnerConfig {
        db_dir: Some(katana_db_dir.to_path_buf()),
        n_accounts: 10,
        dev: true,
        ..Default::default()
    };

    let katana_runner = KatanaRunner::new_with_config(katana_cfg)?;
    let katana_url = katana_runner.url().to_string();

    let manifest_dir = PathBuf::from(std::env::var("CARGO_MANIFEST_DIR")?);
    let baseline_dir = manifest_dir.join(BASELINE_CAIRO_DIR);
    let upgrade_dir = manifest_dir.join(UPGRADE_CAIRO_DIR);

    sozo::run(&baseline_dir, &["migrate", "--rpc-url", &katana_url])?;
    // Currently, the baseline info should remain the truth, since the upgrade will
    // not change the contract address.
    let deployment_info = deployment::extract_info(&baseline_dir, &contract_tag)?;
    println!(
        "  world: {:#066x}\n  contract ({}): {:#066x}",
        deployment_info.world_address, contract_tag, deployment_info.contract_address
    );

    transactions::send_baseline(&katana_runner, &deployment_info).await?;

    sozo::run(&upgrade_dir, &["migrate", "--rpc-url", &katana_url])?;

    transactions::send_upgrade(&katana_runner, &deployment_info).await?;

    let filter = EventFilter {
        from_block: Some(BlockId::Number(0)),
        to_block: Some(BlockId::Tag(BlockTag::Latest)),
        address: Some(deployment_info.world_address),
        keys: Some(vec![]),
    };

    let all_events = events::fetch_all(&katana_url, &filter).await?;

    println!("{} events fetched", all_events.len());

    let fixture = FixtureOutput {
        world_address: deployment_info.world_address,
        events: all_events,
    };

    let mut file = File::create(&output_path)
        .with_context(|| format!("failed to create {}", output_path.display()))?;

    file.write_all(serde_json::to_vec_pretty(&fixture)?.as_slice())?;

    println!("Events saved to {}", output_path.display());

    Ok(())
}

/// Fetches all the events for the given contract address and selectors.
async fn fetch_command(
    contract_address: Felt,
    event_names: Vec<String>,
    from_block: u64,
    to_block: Option<u64>,
    rpc_url: String,
    output_events: PathBuf,
) -> Result<()> {
    let keys = if !event_names.is_empty() {
        Some(vec![event_names
            .iter()
            .map(|name| get_selector_from_name(name).unwrap())
            .collect::<Vec<_>>()])
    } else {
        None
    };

    let filter = EventFilter {
        from_block: Some(BlockId::Number(from_block)),
        to_block: to_block
            .map(BlockId::Number)
            .or(Some(BlockId::Tag(BlockTag::Latest))),
        address: Some(contract_address),
        keys,
    };

    let all_events = events::fetch_all(&rpc_url, &filter).await?;

    println!("{} events fetched", all_events.len());

    let mut file = File::create(&output_events)
        .with_context(|| format!("failed to create {}", output_events.display()))?;

    file.write_all(serde_json::to_vec_pretty(&all_events)?.as_slice())?;

    println!("Events saved to {}", output_events.display());

    Ok(())
}
